//go:build linux
// +build linux

package auditdreceiver

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/elastic/go-libaudit/v2"
	"github.com/elastic/go-libaudit/v2/auparse"
	"github.com/elastic/go-libaudit/v2/rule"
	"github.com/elastic/go-libaudit/v2/rule/flags"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/receiver"
	"go.uber.org/zap"
)

type Auditd struct {
	rules    []string
	client   *libaudit.AuditClient
	consumer consumer.Logs
	logger   *zap.Logger
	done     chan struct{}
	internal metrics // Benchmark
}

// Benchmark
type metrics struct {
	total_duration  int64 // µs
	events_recorded int64
}

func newAuditd(cfg *AuditdReceiverConfig, consumer consumer.Logs, settings receiver.Settings) (*Auditd, error) {
	return &Auditd{
		rules:    cfg.Rules,
		consumer: consumer,
		logger:   settings.Logger,
		internal: metrics{0, 0}, // Benchmark
	}, nil
}

// NB: The auditd messages does not contain a Timestamp and because 'Timestamp' is optional (https://opentelemetry.io/docs/specs/otel/logs/data-model/#field-timestamp).
func createLogs(ts time.Time, messageType auparse.AuditMessageType, messageData []byte) plog.Logs {
	logs := plog.NewLogs()
	resourceLogs := logs.ResourceLogs().AppendEmpty()
	logSlice := resourceLogs.ScopeLogs().AppendEmpty().LogRecords()
	logRecord := logSlice.AppendEmpty()
	logRecord.SetSeverityNumber(plog.SeverityNumberInfo)
	logRecord.SetSeverityText(plog.SeverityNumberInfo.String())
	logRecord.Attributes().PutStr("type", messageType.String())
	logRecord.Attributes().PutStr("data", string(messageData))
	logRecord.SetObservedTimestamp(pcommon.NewTimestampFromTime(time.Now()))
	return logs
}

func (aud *Auditd) receive(ctx context.Context) {
	aud.logger.Info("starting listening for events")
	for {
		select {
		case _, ok := <-aud.done:
			_ = ok
			return
		case <-ctx.Done():
			return
		default:
			rawEvent, err := aud.client.Receive(false)
			if err != nil {
				aud.logger.Error("receive failed", zap.Error(err))
			}
			ts := time.Now()
			// Messages from 1100-2999 are valid audit messages.
			if rawEvent.Type < auparse.AUDIT_USER_AUTH ||
				rawEvent.Type > auparse.AUDIT_LAST_USER_MSG2 {
				continue
			}
			logs := createLogs(ts, rawEvent.Type, rawEvent.Data)
			aud.consumer.ConsumeLogs(ctx, logs)
		}
	}
}

func (aud *Auditd) prepareRules() error {
	_, err := aud.client.DeleteRules()
	if err != nil {
		return fmt.Errorf("[ERROR] failed to delete all roles %w", err)
	}
	for _, rawRule := range aud.rules {
		r, err := flags.Parse(rawRule)
		if err != nil {
			return fmt.Errorf("[ERROR] failed to parse rule %w", err)
		}
		wireRule, err := rule.Build(r)
		if err != nil {
			return fmt.Errorf("[ERROR] failed to build rule %w", err)
		}
		aud.client.AddRule(wireRule)
		if err != nil {
			return fmt.Errorf("[ERROR] failed to add rule: %v. (%v)", rawRule, err)
		}
	}
	return nil
}

func (aud *Auditd) initAuditing() error {
	status, err := aud.client.GetStatus()
	if err != nil {
		return fmt.Errorf("failed to get audit status: %w", err)
	}
	if status.Enabled == 0 {
		aud.logger.Info("enabling auditing in the kernel")
		if err = aud.client.SetEnabled(true, libaudit.WaitForReply); err != nil {
			return fmt.Errorf("failed to set enabled=true: %w", err)
		}
	}
	aud.logger.Info("telling kernel this client should get the auditd logs")
	if err = aud.client.SetPID(libaudit.NoWait); err != nil {
		return fmt.Errorf("failed to set audit PID: %w", err)
	}

	return nil
}

func (aud *Auditd) Start(ctx context.Context, host component.Host) error {
	var w io.Writer
	client, err := libaudit.NewAuditClient(w)
	if err != nil {
		return fmt.Errorf("failed to create client %w", err)
	}
	aud.client = client

	_, err = aud.client.DeleteRules()
	if err != nil {
		return fmt.Errorf("failed to delete all roles %w", err)
	}
	err = aud.prepareRules()
	if err != nil {
		return fmt.Errorf("failed to setup rules: %v", err)
	}

	err = aud.initAuditing()
	if err != nil {
		return fmt.Errorf("failed to initialise auditing: %v", err)
	}

	go aud.receive(ctx)
	return nil
}

func (aud *Auditd) Shutdown(_ context.Context) error {
	if aud.done != nil {
		aud.done <- struct{}{}
		close(aud.done)
		aud.client.Close()
		aud.done = nil
	}
	return nil
}

// Benchmark
func (aud *Auditd) Benchmark() metrics {
	return aud.internal
}

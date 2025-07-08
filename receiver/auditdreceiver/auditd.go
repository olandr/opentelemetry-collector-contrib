//go:build linux
// +build linux

package auditdreceiver

import (
	"context"
	"io"
	"log"
	"time"

	"github.com/elastic/go-libaudit/v2"
	"github.com/elastic/go-libaudit/v2/auparse"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/receiver"
	"go.uber.org/zap"
)

type Auditd struct {
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
		consumer: consumer,
		logger:   settings.Logger,
		internal: metrics{0, 0}, // Benchmark
	}, nil
}

func createLogs(ts time.Time, messageType auparse.AuditMessageType, messageData []byte) plog.Logs {
	logs := plog.NewLogs()
	resourceLogs := logs.ResourceLogs().AppendEmpty()
	logSlice := resourceLogs.ScopeLogs().AppendEmpty().LogRecords()
	logRecord := logSlice.AppendEmpty()
	logRecord.SetSeverityNumber(plog.SeverityNumberInfo)
	logRecord.SetSeverityText(plog.SeverityNumberInfo.String())
	logRecord.SetTimestamp(pcommon.NewTimestampFromTime(ts))
	logRecord.Attributes().PutStr("type", messageType.String())
	logRecord.Attributes().PutStr("data", string(messageData))
	logRecord.SetObservedTimestamp(pcommon.NewTimestampFromTime(time.Now()))
	return logs
}

func (aud *Auditd) receive(ctx context.Context) {
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
			// fixme: maybe it makes sense to record time before the receive? Or somehow exactly when the receive actually happens within the dependency? Because now timestamp is more or less the same as observed (only difference being createLogs overhead).
			ts := time.Now()
			// Messages from 1100-2999 are valid audit messages.
			if rawEvent.Type < auparse.AUDIT_USER_AUTH ||
				rawEvent.Type > auparse.AUDIT_LAST_USER_MSG2 {
				continue
			}
			createLogs(ts, rawEvent.Type, rawEvent.Data)
		}
	}
}

func (aud *Auditd) Start(ctx context.Context, host component.Host) error {
	var w io.Writer
	client, err := libaudit.NewMulticastAuditClient(w)
	if err != nil {
		log.Fatal(err)
	}
	aud.client = client

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

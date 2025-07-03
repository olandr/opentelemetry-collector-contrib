package auditdreceiver

import (
	"context"
	"time"

	"github.com/olandr/notify"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/receiver"
	"go.uber.org/zap"
)

type Auditd struct {
	include  []string
	exclude  []string
	consumer consumer.Logs
	logger   *zap.Logger
	watcher  chan notify.EventInfo
	notify   notify.Notify
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
		include:  cfg.Include,
		exclude:  cfg.Exclude,
		consumer: consumer,
		logger:   settings.Logger,
		internal: metrics{0, 0}, // Benchmark
	}, nil
}

func createLogs(ts time.Time, path, operation string) plog.Logs {
	logs := plog.NewLogs()
	resourceLogs := logs.ResourceLogs().AppendEmpty()
	logSlice := resourceLogs.ScopeLogs().AppendEmpty().LogRecords()
	logRecord := logSlice.AppendEmpty()
	logRecord.SetSeverityNumber(plog.SeverityNumberInfo)
	logRecord.SetSeverityText(plog.SeverityNumberInfo.String())
	logRecord.SetTimestamp(pcommon.NewTimestampFromTime(ts))
	logRecord.Attributes().PutStr("path", path)
	logRecord.Attributes().PutStr("operation", operation)
	logRecord.SetObservedTimestamp(pcommon.NewTimestampFromTime(time.Now()))
	return logs
}

func (fsn *Auditd) Start(ctx context.Context, host component.Host) error {
	return nil
}

func (fsn *Auditd) Shutdown(_ context.Context) error {
	return nil
}

// Benchmark
func (fsn *Auditd) Benchmark() metrics {
	return fsn.internal
}

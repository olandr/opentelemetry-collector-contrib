package filewatchreceiver

import (
	"context"
	"fmt"
	"time"

	"github.com/olandr/notify"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/receiver"
	"go.uber.org/zap"
)

type FileWatcher struct {
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
	data            []int64
	total_duration  int64 // µs
	events_recorded int64
}

func newNotify(cfg *NotifyReceiverConfig, consumer consumer.Logs, settings receiver.Settings) (*FileWatcher, error) {
	return &FileWatcher{
		include:  cfg.Include,
		exclude:  cfg.Exclude,
		consumer: consumer,
		logger:   settings.Logger,
		internal: metrics{make([]int64, 0), 0, 0}, // Benchmark
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

func (fsn *FileWatcher) watch(ctx context.Context, watcher chan (notify.EventInfo)) {
	defer fsn.notify.Stop(fsn.watcher)
	for {
		select {
		case <-ctx.Done():
			return
		case _, ok := <-fsn.done:
			_ = ok
			return
		case event := <-watcher:
			b := time.Now() // Benchmark
			// FIXME: this feels like a slow check; needs some benchmarking to see how this performs under load.
			ts := time.Unix(event.Timestamp(), 0)
			fsn.logger.Debug("event", zap.Time("ts", ts), zap.String("path", event.Path()), zap.String("operation", event.Event().String()))
			logs := createLogs(ts, event.Path(), event.Event().String())
			fsn.consumer.ConsumeLogs(ctx, logs)
			// Benchmark
			fsn.internal.data = append(fsn.internal.data, time.Since(b).Microseconds())
			fsn.internal.total_duration += (time.Since(b).Microseconds())
			fsn.internal.events_recorded++
		}
	}
}

func (fsn *FileWatcher) Start(ctx context.Context, host component.Host) error {
	fsn.watcher = make(chan notify.EventInfo, 128)
	fsn.done = make(chan struct{})
	fsn.notify = notify.NewNotify()
	go fsn.watch(ctx, fsn.watcher)
	var err error
	if len(fsn.include) == 0 {
		return nil
	}
	// Setup watches by include paths and prepare exclusions
	watches := len(fsn.include)
	for _, ex := range fsn.exclude {
		fsn.notify.Exclude(ex)
	}
	for _, f := range fsn.include {
		err = fsn.notify.Watch(f, fsn.watcher, EVENTS_TO_WATCH)
		// We are more lenient with problematic include paths
		if err != nil {
			fsn.logger.Error("cannot creating watch, skipping", zap.String("path", f), zap.Error(err))
			watches--
		}
	}
	if watches == 0 {
		return fmt.Errorf("could not create any watches on the supplied 'include' paths")
	}
	return nil
}

func (fsn *FileWatcher) Shutdown(_ context.Context) error {
	if fsn.done != nil {
		fsn.done <- struct{}{}
		close(fsn.done)
		fsn.notify.Stop(fsn.watcher)
		fsn.notify.Close()
		close(fsn.watcher)
		fsn.done = nil
	}
	return nil
}

// Benchmark
func (fsn *FileWatcher) Benchmark() metrics {
	return fsn.internal
}

package filewatchreceiver

import (
	"context"
	_ "net/http/pprof"
	"path/filepath"

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

func createLogs(name, operation string) plog.Logs {
	logs := plog.NewLogs()
	resourceLogs := logs.ResourceLogs().AppendEmpty()
	logSlice := resourceLogs.ScopeLogs().AppendEmpty().LogRecords()
	logRecord := logSlice.AppendEmpty()
	logRecord.SetSeverityNumber(plog.SeverityNumberInfo)
	logRecord.SetSeverityText("INFO")
	logRecord.Attributes().PutStr("event", name)
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
			// FIXME: this feels like a slow check; needs some benchmarking to see how this performs under load.
			if fsn.shouldInclude(event.Path()) {
				fsn.logger.Debug("event", zap.String("name", event.Path()), zap.String("operation", event.Event().String()))
				logs := createLogs(event.Path(), event.Event().String())
				fsn.consumer.ConsumeLogs(ctx, logs)
			}
		}
	}
}

func (fsn *FileWatcher) shouldInclude(path string) bool {
	for _, ex := range fsn.exclude {
		exclude, err := filepath.Match(ex, path)
		if exclude {
			return false
		}
		if err != nil {
			fsn.logger.Error("could not find match, excluding anyways", zap.Error(err))
			return false
		}
	}
	return true
}

func (fsn *FileWatcher) Start(ctx context.Context, host component.Host) error {
	fsn.watcher = make(chan notify.EventInfo, 20)
	fsn.done = make(chan struct{})
	fsn.notify = notify.NewNotify()
	go fsn.watch(ctx, fsn.watcher)
	var err error
	for _, f := range fsn.include {
		err = fsn.notify.Watch(f, fsn.watcher, EVENTS_TO_WATCH)
	}
	return err
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

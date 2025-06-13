package filewatcher

import (
	"context"
	"time"

	"github.com/syncthing/notify"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/receiver"
	"go.uber.org/zap"
)

var (
	EVENTS_TO_WATCH = notify.All
)

type FileWatcher struct {
	include  []string
	exclude  []string
	consumer consumer.Logs
	logger   *zap.Logger
	watcher  chan notify.EventInfo
	done     chan struct{}
}

func newNotify(cfg *NotifyReceiverConfig, consumer consumer.Logs, settings receiver.Settings) (*FileWatcher, error) {
	return &FileWatcher{
		include:  cfg.Include,
		exclude:  cfg.Exclude,
		consumer: consumer,
		logger:   settings.Logger,
	}, nil
}

func createLogs(name, operation string) plog.Logs {
	logs := plog.NewLogs()
	resourceLogs := logs.ResourceLogs().AppendEmpty()
	logSlice := resourceLogs.ScopeLogs().AppendEmpty().LogRecords()
	logRecord := logSlice.AppendEmpty()
	logRecord.SetSeverityNumber(plog.SeverityNumberInfo)
	logRecord.SetSeverityText("INFO filewatcher")
	logRecord.Attributes().PutStr("event", name)
	logRecord.Attributes().PutStr("operation", operation)
	logRecord.SetObservedTimestamp(pcommon.NewTimestampFromTime(time.Now()))
	return logs
}

func (fsn *FileWatcher) watch(ctx context.Context, watcher chan (notify.EventInfo)) {
	defer notify.Stop(fsn.watcher)
	for {
		select {
		case <-ctx.Done():
			return
		case _, ok := <-fsn.done:
			_ = ok
			return
		case event := <-watcher:
			fsn.logger.Debug("event", zap.String("name", event.Path()), zap.String("operation", event.Event().String()))
			logs := createLogs(event.Path(), event.Event().String())
			fsn.consumer.ConsumeLogs(ctx, logs)
		}
	}
}

func (fsn *FileWatcher) Start(ctx context.Context, host component.Host) error {
	fsn.watcher = make(chan notify.EventInfo, 20)
	fsn.done = make(chan struct{})
	go fsn.watch(ctx, fsn.watcher)
	var err error
	for _, f := range fsn.include {
		err = notify.Watch(f, fsn.watcher, EVENTS_TO_WATCH)
	}
	return err
}

func (fsn *FileWatcher) Shutdown(_ context.Context) error {
	fsn.done <- struct{}{}
	close(fsn.done)
	close(fsn.watcher)
	fsn.done = nil
	return nil
}

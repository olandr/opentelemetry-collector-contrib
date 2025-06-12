package filewatcher

import (
	"context"
	"time"

	"github.com/charmbracelet/log"
	"github.com/fsnotify/fsnotify"
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
	watcher  *fsnotify.Watcher
	done     chan struct{}
}

func newfsNotify(cfg *FSNotifyReceiverConfig, consumer consumer.Logs, settings receiver.Settings) (*FileWatcher, error) {
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

func (fsn *FileWatcher) watch(ctx context.Context, watcher *fsnotify.Watcher) {
	defer watcher.Close()
	for {
		select {
		case <-ctx.Done():
			return
		case _, ok := <-fsn.done:
			_ = ok
			return
		case err := <-watcher.Errors:
			log.Printf("an error has occurred: %v", err)
			return
		case action := <-watcher.Events:
			fsn.logger.Debug("action", zap.String("name", action.Name), zap.String("operation", action.Op.String()))
			logs := createLogs(action.Name, action.Op.String())
			fsn.consumer.ConsumeLogs(ctx, logs)
		}
	}
}

func (fsn *FileWatcher) Start(ctx context.Context, host component.Host) error {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Fatalf("error creating FS Broker: %v", err)
	}
	fsn.watcher = watcher
	fsn.done = make(chan struct{})
	go fsn.watch(ctx, fsn.watcher)
	for _, f := range fsn.include {
		err = watcher.Add(f)
	}
	return err
}

func (fsn *FileWatcher) Shutdown(_ context.Context) error {
	fsn.done <- struct{}{}
	close(fsn.done)
	fsn.done = nil
	return nil
}

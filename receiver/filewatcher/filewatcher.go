package filewatcher

import (
	"context"
	"errors"
	"time"

	"github.com/charmbracelet/log"
	"github.com/fsnotify/fsnotify"
	"github.com/helshabini/fsbroker"
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

func (fsn *FileWatcher) watch(ctx context.Context, broker *fsbroker.FSBroker) {
	broker.Start()
	defer broker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case _, ok := <-fsn.done:
			_ = ok
			return
		case err := <-broker.Error():
			log.Printf("an error has occurred: %v", err)
			return
		case action := <-broker.Next():
			fsn.logger.Debug("action", zap.String("name", action.Subject.Path), zap.String("operation", action.Type.String()))
			logs := createLogs(action.Subject.Path, action.Type.String())
			fsn.consumer.ConsumeLogs(ctx, logs)
		}
	}
}

func (fsn *FileWatcher) Start(ctx context.Context, host component.Host) error {
	config := fsbroker.DefaultFSConfig()
	broker, err := fsbroker.NewFSBroker(config)
	if err != nil {
		log.Fatalf("error creating FS Broker: %v", err)
	}
	// FIXME: for now we do not care about NoOps (creates/write on deleted files). Decide if this is the intended behaviour.
	broker.Filter = func(action *fsbroker.FSAction) bool { return action.Type != fsbroker.NoOp }
	if fsn.done != nil {
		return errors.New("filewatcher is already running")
	}
	fsn.done = make(chan struct{})

	go fsn.watch(ctx, broker)
	for _, f := range fsn.include {
		err = broker.AddRecursiveWatch(f)
	}
	return err
}

func (fsn *FileWatcher) Shutdown(_ context.Context) error {
	fsn.done <- struct{}{}
	close(fsn.done)
	fsn.done = nil
	return nil
}

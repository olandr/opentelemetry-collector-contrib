package filewatchreceiver

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/receiver"
)

var Type = component.MustNewType("filewatch")

func NewFactory() receiver.Factory {
	return receiver.NewFactory(
		Type,
		createDefaultConfig,
		receiver.WithLogs(createLogsReceiver, component.StabilityLevelAlpha))
}

func createLogsReceiver(_ context.Context, settings receiver.Settings, baseCfg component.Config, consumer consumer.Logs) (receiver.Logs, error) {
	cfg := baseCfg.(*FileWatchReceiverConfig)
	return newNotify(cfg, consumer, settings)
}

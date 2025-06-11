package filewatcher

import (
	"go.opentelemetry.io/collector/component"
)

type FSNotifyReceiverConfig struct {
	Path string `mapstructure:"path,omitempty"`

	_ struct{}
}

func createDefaultConfig() component.Config {
	return &FSNotifyReceiverConfig{
		Path: "",
	}
}

package filewatcher

import (
	"go.opentelemetry.io/collector/component"
)

type FSNotifyReceiverConfig struct {
	Include []string `mapstructure:"include,omitempty"`
	Exclude []string `mapstructure:"exclude,omitempty"`

	_ struct{}
}

func createDefaultConfig() component.Config {
	return &FSNotifyReceiverConfig{
		Include: []string{},
		Exclude: []string{},
	}
}

package filewatcher

import (
	"go.opentelemetry.io/collector/component"
)

type NotifyReceiverConfig struct {
	Include []string `mapstructure:"include,omitempty"`
	Exclude []string `mapstructure:"exclude,omitempty"`

	_ struct{}
}

func createDefaultConfig() component.Config {
	return &NotifyReceiverConfig{
		Include: []string{},
		Exclude: []string{},
	}
}

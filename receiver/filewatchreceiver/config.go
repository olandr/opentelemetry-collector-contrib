package filewatchreceiver

import (
	"go.opentelemetry.io/collector/component"
)

type FileWatchReceiverConfig struct {
	Include []string `mapstructure:"include,omitempty"`
	Exclude []string `mapstructure:"exclude,omitempty"`
	Events  []string `mapstructure:"events,omitempty"`

	_ struct{}
}

func createDefaultConfig() component.Config {
	return &FileWatchReceiverConfig{
		Include: []string{},
		Exclude: []string{},
		Events:  []string{},
	}
}

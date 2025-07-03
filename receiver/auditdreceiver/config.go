package auditdreceiver

import (
	"go.opentelemetry.io/collector/component"
)

type AuditdReceiverConfig struct {
	Include []string `mapstructure:"include,omitempty"`
	Exclude []string `mapstructure:"exclude,omitempty"`

	_ struct{}
}

func createDefaultConfig() component.Config {
	return &AuditdReceiverConfig{
		Include: []string{},
		Exclude: []string{},
	}
}

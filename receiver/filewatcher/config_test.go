package filewatcher

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/confmap/confmaptest"
	"go.opentelemetry.io/collector/confmap/xconfmap"
)

type ConfigTestCase struct {
	Name     string
	Expected *FSNotifyReceiverConfig
	Error    error
}

func TestFactoryCreate(t *testing.T) {
	t.Parallel()
	tmpPath := TEST_PATH
	tests := []ConfigTestCase{
		{
			Name: "regular",
			Expected: &FSNotifyReceiverConfig{
				Path: fmt.Sprintf("%v/regular", tmpPath),
			},
			Error: nil,
		},
		{
			Name: "empty",
			Expected: &FSNotifyReceiverConfig{
				Path: "",
			},
			Error: nil,
		},
		{
			Name: "problematic",
			Expected: &FSNotifyReceiverConfig{
				Path: fmt.Sprintf("%v/-#{:,", tmpPath),
			},
			Error: nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.Name, func(t *testing.T) {
			// Arrange
			configFile, _ := confmaptest.LoadConf(TEST_CONFIG_PATH)
			receivers, _ := configFile.Sub("receivers")
			filewatcher, err := receivers.Sub(fmt.Sprintf("filewatcher/%v", tc.Name))
			require.NoError(t, err)
			// Act
			act := NewFactory().CreateDefaultConfig()
			require.NoError(t, filewatcher.Unmarshal(act))
			if tc.Error != nil {
				assert.Error(t, xconfmap.Validate(act))
				return
			}
			assert.NoError(t, xconfmap.Validate(act))
			// Assert
			assert.Equal(t, tc.Expected, act)
		})
	}
}

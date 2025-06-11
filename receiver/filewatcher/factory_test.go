package filewatcher

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/receiver/receivertest"
	"go.uber.org/zap"
)

func TestCanCreateNewFactort(t *testing.T) {

}

func TestCanCreateLogsReceiver(t *testing.T) {
	// Arrange
	receiverConfig := &FSNotifyReceiverConfig{
		Include: []string{TEST_INCLUDE_PATH},
		Exclude: []string{TEST_EXCLUDE_PATH},
	}
	expectedConsumer := new(consumertest.LogsSink)
	expectedLogsReceiver := &FileWatcher{
		include:  []string{TEST_INCLUDE_PATH},
		exclude:  []string{TEST_EXCLUDE_PATH},
		consumer: expectedConsumer,
		logger:   zap.NewNop(),
	}

	settings := receivertest.NewNopSettings(component.MustNewType("filewatcher"))
	settings.Logger = zap.NewNop()

	// Act
	actualLogsReceiver, err := createLogsReceiver(t.Context(), settings, receiverConfig, expectedConsumer)

	// Assert
	require.NoError(t, err)
	require.Equal(t, expectedLogsReceiver, actualLogsReceiver)
}

func TestCanStartingLogsReceiver(t *testing.T) {
	// Arrange
	receiverConfig := &FSNotifyReceiverConfig{
		Include: []string{TEST_INCLUDE_PATH},
		Exclude: []string{TEST_EXCLUDE_PATH},
	}
	settings := receivertest.NewNopSettings(component.MustNewType("filewatcher"))
	settings.Logger = zap.NewNop()

	// Act
	actualLogsReceiver, _ := createLogsReceiver(t.Context(), settings, receiverConfig, new(consumertest.LogsSink))

	// Assert
	require.NoError(t, actualLogsReceiver.Start(context.Background(), componenttest.NewNopHost()))
}

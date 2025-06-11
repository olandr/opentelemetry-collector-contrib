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
	expected_path := TEST_PATH
	receiverConfig := &FSNotifyReceiverConfig{
		Path: expected_path,
	}
	expectedConsumer := new(consumertest.LogsSink)
	expectedLogsReceiver := &FileWatcher{
		path:     expected_path,
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
		Path: TEST_PATH,
	}
	settings := receivertest.NewNopSettings(component.MustNewType("filewatcher"))
	settings.Logger = zap.NewNop()

	// Act
	actualLogsReceiver, _ := createLogsReceiver(t.Context(), settings, receiverConfig, new(consumertest.LogsSink))

	// Assert
	require.NoError(t, actualLogsReceiver.Start(context.Background(), componenttest.NewNopHost()))
}

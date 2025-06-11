package filewatcher

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/collector/receiver/receivertest"
	"go.uber.org/zap"
)

var (
	TEST_PATH         = "testdata"
	TEST_CONFIG_PATH  = "config.yaml"
	TEST_INCLUDE_PATH = "testdata/include"
	TEST_EXCLUDE_PATH = "testdata/exclude"
	TEST_INNER_PATH   = "testdata/include/inner"
)

func testSetup(t *testing.T, include, exclude string) (receiver.Logs, *consumertest.LogsSink) {
	testLogsConsumer := new(consumertest.LogsSink)

	config := &FSNotifyReceiverConfig{
		Include: []string{include},
		Exclude: []string{exclude},
	}
	settings := receivertest.NewNopSettings(component.MustNewType("filewatcher"))
	settings.Logger = zap.NewNop()
	logs, err := createLogsReceiver(t.Context(), settings, config, testLogsConsumer)
	require.NoError(t, err)
	require.NoError(t, logs.Start(t.Context(), componenttest.NewNopHost()))
	// FIXME: this sleep is to handle start-up time, maybe there is a smarter way to do it?
	return logs, testLogsConsumer
}

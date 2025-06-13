package filewatcher

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/confmap/confmaptest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/collector/receiver/receivertest"
	"go.uber.org/zap"
)

var (
	TEST_PATH                   = "testdata"
	TEST_CONFIG_PATH            = "config.yaml"
	TEST_INCLUDE_PATH           = "testdata/include"
	TEST_EXCLUDE_PATH           = "testdata/exclude"
	TEST_INCLUDE_RECURSIVE_PATH = "testdata/include/..."
	TEST_EXCLUDE_RECURSIVE_PATH = "testdata/exclude/..."
	TEST_INNER_PATH             = "testdata/include/inner"
)

func testSetup(t *testing.T) (receiver.Logs, *consumertest.LogsSink, string) {
	configFile, _ := confmaptest.LoadConf(TEST_CONFIG_PATH)
	receivers, _ := configFile.Sub("receivers")
	filewatcher, err := receivers.Sub("filewatcher/regular")
	config := NewFactory().CreateDefaultConfig()
	filewatcher.Unmarshal(config)

	testLogsConsumer := new(consumertest.LogsSink)
	settings := receivertest.NewNopSettings(component.MustNewType("filewatcher"))
	settings.Logger = zap.NewNop()
	logs, err := createLogsReceiver(t.Context(), settings, config, testLogsConsumer)
	require.NoError(t, err)
	require.NoError(t, logs.Start(t.Context(), componenttest.NewNopHost()))

	wd, err := os.Getwd()
	if err != nil {
		panic(err)
	}
	return logs, testLogsConsumer, wd
}

// logsToMap will take a list of logs and each LogRecord to a map which will count distinct events (up to path and operation). This is useful if testing the out-of-order arrival of log records between expected and actual consumers. Solves issue with ignoring order.
func logsToMap(t *testing.T, logs []plog.Logs, msgs ...interface{}) map[string]uint {
	ret := make(map[string]uint)
	for _, log := range logs {
		for i := 0; i < log.ResourceLogs().Len(); i++ {
			for j := 0; j < log.ResourceLogs().At(i).ScopeLogs().Len(); j++ {
				event, _ := log.ResourceLogs().At(i).ScopeLogs().At(j).LogRecords().At(0).Attributes().Get("event")
				operation, _ := log.ResourceLogs().At(i).ScopeLogs().At(j).LogRecords().At(0).Attributes().Get("operation")
				hash := fmt.Sprintf("%s-%s", filepath.Base(event.AsString()), operation.AsString())
				t.Logf("%s, hash=%v", msgs, hash)

				ret[hash] += 1
			}
		}
	}
	return ret
}

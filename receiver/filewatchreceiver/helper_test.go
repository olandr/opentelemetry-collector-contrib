package filewatchreceiver

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/brianvoe/gofakeit/v7"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
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

func beforeAll(t *testing.T, path string) {
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	test_destination_parent := filepath.Join(wd, path)

	testTeardown(t, test_destination_parent)
	err = os.Mkdir(test_destination_parent, 0o777)
	if err != nil {
		t.Fatal(err)
	}
}

func beforeEach(t *testing.T, should_create_inner_dir bool) (receiver.Logs, *consumertest.LogsSink, string) {
	wd, err := os.Getwd()
	if err != nil {
		panic(err)
	}
	test_destination := filepath.Join(wd, TEST_INCLUDE_PATH, gofakeit.LetterN(5))
	err = os.Mkdir(test_destination, 0o777)
	if err != nil {
		t.Fatal(err)
	}

	if should_create_inner_dir {
		err = os.Mkdir(filepath.Join(test_destination, "inner"), 0o777)
		if err != nil {
			t.Fatal(err)
		}
	}
	// this sleep is needed because any events (CREATE, WRITE or otherwise) made on the test_destination dir is NOT to be caught by the log receiver.
	time.Sleep(1000 * time.Millisecond)

	include_path := fmt.Sprintf("%v/...", test_destination)
	config := createDefaultConfig()
	config.(*NotifyReceiverConfig).Include = []string{include_path}

	testLogsConsumer := new(consumertest.LogsSink)
	settings := receivertest.NewNopSettings(component.MustNewType("filewatch"))
	settings.Logger = zap.NewNop()
	logs, err := createLogsReceiver(t.Context(), settings, config, testLogsConsumer)
	require.NoError(t, err)
	require.NoError(t, logs.Start(t.Context(), componenttest.NewNopHost()))

	return logs, testLogsConsumer, test_destination
}

func testTeardown(t *testing.T, test_destination string) {
	t.Logf("removing test_destination: %v", test_destination)
	err := os.RemoveAll(test_destination)
	if err != nil {
		t.Fatal(err)
	}
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

func consumeLogs(t *testing.T, consumer *consumertest.LogsSink, logs []plog.Logs) {
	for _, log := range logs {
		consumer.ConsumeLogs(t.Context(), log)
	}
}

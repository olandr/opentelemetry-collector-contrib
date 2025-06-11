package filewatcher

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/brianvoe/gofakeit/v7"
	"github.com/charmbracelet/log"
	"github.com/helshabini/fsbroker"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/pdatatest/plogtest"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/plog"
)

type FactoryTestCase struct {
	Name     string
	Expected *FSNotifyReceiverConfig
	Error    error
}

func assertLogRecords(t *testing.T, expected, actual plog.Logs) {
	require.NoError(t, plogtest.CompareLogs(
		expected,
		actual,
		plogtest.IgnoreObservedTimestamp(),
		plogtest.IgnoreTimestamp(),
	))
}
func eventuallyExpect(t *testing.T, expected int, actual int) {
	require.Eventually(t, func() bool { return expected == actual }, 10*time.Second, 5*time.Millisecond,
		"expected %d, but got %d", expected, actual)
}

func TestFSNotifyReveiver(t *testing.T) {
	// Arrange
	expectedLogsConsumer := new(consumertest.LogsSink)
	logs, actualLogsConsumer := testSetup(t, TEST_PATH)

	// Act
	TEST_FILES := 1
	createFiles := make([]string, TEST_FILES)
	for tc := range TEST_FILES {
		createFiles[tc] = fmt.Sprintf("%v/%v.txt", TEST_PATH, gofakeit.LetterN(5))
		t.Log("test-case", "file", createFiles[tc])
		f, err := os.OpenFile(createFiles[tc], os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			log.Fatal(err)
		}

		expectedLogsConsumer.ConsumeLogs(t.Context(), createLogs(createFiles[tc], fsbroker.Create.String()))

		time.Sleep(800 * time.Millisecond) // Sleeping here because the filewatcher (or something) does not like when you do things quickly --- seems to not be emitting anything.
		_, err = f.Write([]byte(gofakeit.LetterN(10)))
		if err != nil {
			log.Fatal(err)
		}
		expectedLogsConsumer.ConsumeLogs(t.Context(), createLogs(createFiles[tc], fsbroker.Write.String()))

		f.Close()
		time.Sleep(800 * time.Millisecond) // Sleeping here because the filewatcher (or something) does not like when you do things quickly --- seems to not be emitting anything.

		err = os.Remove(createFiles[tc])
		if err != nil {
			log.Fatal(err)
		}
		expectedLogsConsumer.ConsumeLogs(t.Context(), createLogs(createFiles[tc], fsbroker.Remove.String()))
		time.Sleep(800 * time.Millisecond)

		// Assert
		eventuallyExpect(t, expectedLogsConsumer.LogRecordCount(), actualLogsConsumer.LogRecordCount())
		for i, expected := range expectedLogsConsumer.AllLogs() {
			actual := actualLogsConsumer.AllLogs()[i]
			assertLogRecords(t, expected, actual)
		}
	}
	require.NoError(t, logs.Shutdown(context.Background()))
}

func TestFSNotifyReveiverListenToNewDir(t *testing.T) {
	// Arrange
	expectedLogsConsumer := new(consumertest.LogsSink)
	// We want to only listen to the outer path, but add files to a dir within
	logs, actualLogsConsumer := testSetup(t, TEST_PATH)
	// Act
	TEST_FILES := 1
	createFiles := make([]string, TEST_FILES)
	for tc := range TEST_FILES {
		innerDir := fmt.Sprintf("%v/%v", TEST_PATH, gofakeit.LetterN(5))
		err := os.Mkdir(innerDir, 0777)
		if err != nil {
			log.Fatal(err)
		}
		expectedLogsConsumer.ConsumeLogs(t.Context(), createLogs(innerDir, fsbroker.Create.String()))
		time.Sleep(800 * time.Millisecond)
		createFiles[tc] = fmt.Sprintf("%v/%v.txt", innerDir, gofakeit.LetterN(5))
		t.Log("test-case", "file", createFiles[tc])
		f, err := os.OpenFile(createFiles[tc], os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			log.Fatal(err)
		}

		expectedLogsConsumer.ConsumeLogs(t.Context(), createLogs(createFiles[tc], fsbroker.Create.String()))

		time.Sleep(800 * time.Millisecond) // Sleeping here because the filewatcher (or something) does not like when you do things quickly --- seems to not be emitting anything.
		_, err = f.Write([]byte(gofakeit.LetterN(10)))
		if err != nil {
			log.Fatal(err)
		}
		expectedLogsConsumer.ConsumeLogs(t.Context(), createLogs(createFiles[tc], fsbroker.Write.String()))

		f.Close()
		time.Sleep(800 * time.Millisecond) // Sleeping here because the filewatcher (or something) does not like when you do things quickly --- seems to not be emitting anything.

		err = os.Remove(createFiles[tc])
		if err != nil {
			log.Fatal(err)
		}
		expectedLogsConsumer.ConsumeLogs(t.Context(), createLogs(createFiles[tc], fsbroker.Remove.String()))
		time.Sleep(800 * time.Millisecond)

		// Assert
		eventuallyExpect(t, expectedLogsConsumer.LogRecordCount(), actualLogsConsumer.LogRecordCount())

		err = os.Remove(innerDir)
		if err != nil {
			log.Fatal(err)
		}
		expectedLogsConsumer.ConsumeLogs(t.Context(), createLogs(innerDir, fsbroker.Remove.String()))
		time.Sleep(800 * time.Millisecond)

		for i, expected := range expectedLogsConsumer.AllLogs() {
			actual := actualLogsConsumer.AllLogs()[i]
			assertLogRecords(t, expected, actual)
		}
	}
	require.NoError(t, logs.Shutdown(context.Background()))
}

// Error with provider fsbroker https://github.com/helshabini/fsbroker/issues/5
func TestFSNotifyReveiverListenToExistingNestedDir(t *testing.T) {
	// Arrange
	expectedLogsConsumer := new(consumertest.LogsSink)
	// We want to only listen to the outer path, but add files to a dir within
	logs, actualLogsConsumer := testSetup(t, TEST_PATH)

	// Act
	TEST_FILES := 1
	createFiles := make([]string, TEST_FILES)
	for tc := range TEST_FILES {

		time.Sleep(800 * time.Millisecond)
		createFiles[tc] = fmt.Sprintf("%v/%v.txt", TEST_INNER_PATH, gofakeit.LetterN(5))
		t.Log("test-case", "file", createFiles[tc])
		f, err := os.OpenFile(createFiles[tc], os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			log.Fatal(err)
		}

		expectedLogsConsumer.ConsumeLogs(t.Context(), createLogs(createFiles[tc], fsbroker.Create.String()))

		time.Sleep(800 * time.Millisecond) // Sleeping here because the filewatcher (or something) does not like when you do things quickly --- seems to not be emitting anything.
		_, err = f.Write([]byte(gofakeit.LetterN(10)))
		if err != nil {
			log.Fatal(err)
		}
		expectedLogsConsumer.ConsumeLogs(t.Context(), createLogs(createFiles[tc], fsbroker.Write.String()))

		f.Close()
		time.Sleep(800 * time.Millisecond) // Sleeping here because the filewatcher (or something) does not like when you do things quickly --- seems to not be emitting anything.

		err = os.Remove(createFiles[tc])
		if err != nil {
			log.Fatal(err)
		}
		expectedLogsConsumer.ConsumeLogs(t.Context(), createLogs(createFiles[tc], fsbroker.Remove.String()))
		time.Sleep(800 * time.Millisecond)
		// Assert
		eventuallyExpect(t, expectedLogsConsumer.LogRecordCount(), actualLogsConsumer.LogRecordCount())

		for i, expected := range expectedLogsConsumer.AllLogs() {
			actual := actualLogsConsumer.AllLogs()[i]
			assertLogRecords(t, expected, actual)
		}
	}
	require.NoError(t, logs.Shutdown(context.Background()))
}

// Error with provider fsbroker: https://github.com/helshabini/fsbroker/issues/5
func TestFSNotifyReveiverListenToExistingNestedNewDir(t *testing.T) {
	// Arrange
	expectedLogsConsumer := new(consumertest.LogsSink)
	// We want to only listen to the outer path, but add files to a dir within
	logs, actualLogsConsumer := testSetup(t, TEST_PATH)

	// Act
	TEST_FILES := 1
	createFiles := make([]string, TEST_FILES)
	for tc := range TEST_FILES {

		innerDir := fmt.Sprintf("%v/%v", TEST_INNER_PATH, gofakeit.LetterN(5))
		err := os.Mkdir(innerDir, 0777)
		if err != nil {
			log.Fatal(err)
		}
		expectedLogsConsumer.ConsumeLogs(t.Context(), createLogs(innerDir, fsbroker.Create.String()))
		time.Sleep(800 * time.Millisecond)

		createFiles[tc] = fmt.Sprintf("%v/%v.txt", TEST_INNER_PATH, gofakeit.LetterN(5))
		t.Log("test-case", "file", createFiles[tc])
		f, err := os.OpenFile(createFiles[tc], os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			log.Fatal(err)
		}

		expectedLogsConsumer.ConsumeLogs(t.Context(), createLogs(createFiles[tc], fsbroker.Create.String()))

		time.Sleep(800 * time.Millisecond) // Sleeping here because the filewatcher (or something) does not like when you do things quickly --- seems to not be emitting anything.
		_, err = f.Write([]byte(gofakeit.LetterN(10)))
		if err != nil {
			log.Fatal(err)
		}
		expectedLogsConsumer.ConsumeLogs(t.Context(), createLogs(createFiles[tc], fsbroker.Write.String()))

		f.Close()
		time.Sleep(800 * time.Millisecond) // Sleeping here because the filewatcher (or something) does not like when you do things quickly --- seems to not be emitting anything.

		err = os.Remove(createFiles[tc])
		if err != nil {
			log.Fatal(err)
		}
		expectedLogsConsumer.ConsumeLogs(t.Context(), createLogs(createFiles[tc], fsbroker.Remove.String()))
		time.Sleep(800 * time.Millisecond)

		err = os.Remove(innerDir)
		if err != nil {
			log.Fatal(err)
		}
		expectedLogsConsumer.ConsumeLogs(t.Context(), createLogs(innerDir, fsbroker.Remove.String()))
		time.Sleep(800 * time.Millisecond)

		// Assert
		eventuallyExpect(t, expectedLogsConsumer.LogRecordCount(), actualLogsConsumer.LogRecordCount())

		for i, expected := range expectedLogsConsumer.AllLogs() {
			actual := actualLogsConsumer.AllLogs()[i]
			assertLogRecords(t, expected, actual)
		}
	}
	require.NoError(t, logs.Shutdown(context.Background()))
}
func TestDeletingQuicklyIgnoresNoOp(t *testing.T) {
	// Arrange
	expectedLogsConsumer := new(consumertest.LogsSink)
	logs, actualLogsConsumer := testSetup(t, TEST_PATH)

	// Act
	TEST_FILES := 1
	createFiles := make([]string, TEST_FILES)
	for tc := range TEST_FILES {
		createFiles[tc] = fmt.Sprintf("%v/%v.txt", TEST_PATH, gofakeit.LetterN(5))
		t.Log("test-case", "file", createFiles[tc])
		f, err := os.OpenFile(createFiles[tc], os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			log.Fatal(err)
		}

		f.Close()
		err = os.Remove(createFiles[tc])
		if err != nil {
			log.Fatal(err)
		}

		// Assert
		eventuallyExpect(t, 0, actualLogsConsumer.LogRecordCount())
		eventuallyExpect(t, expectedLogsConsumer.LogRecordCount(), actualLogsConsumer.LogRecordCount())
		for i, expected := range expectedLogsConsumer.AllLogs() {
			actual := actualLogsConsumer.AllLogs()[i]
			assertLogRecords(t, expected, actual)
		}
	}
	require.NoError(t, logs.Shutdown(context.Background()))
}

func TestRenameFileCanBeRemoved(t *testing.T) {
	// Arrange
	expectedLogsConsumer := new(consumertest.LogsSink)
	logs, actualLogsConsumer := testSetup(t, TEST_PATH)

	// Act
	TEST_FILES := 5
	createFiles := make([]string, TEST_FILES)
	for tc := range TEST_FILES {
		createFiles[tc] = fmt.Sprintf("%v/%v.txt", TEST_PATH, gofakeit.LetterN(5))
		t.Log("test-case", "file", createFiles[tc])
		f, err := os.OpenFile(createFiles[tc], os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			log.Fatal(err)
		}

		time.Sleep(800 * time.Millisecond) // Sleeping here because the filewatcher (or something) does not like when you do things quickly --- seems to not be emitting anything.
		expectedLogsConsumer.ConsumeLogs(t.Context(), createLogs(createFiles[tc], fsbroker.Create.String()))

		f.Close()

		newName := fmt.Sprintf("%v/%v.txt", TEST_PATH, gofakeit.LetterN(5))
		err = os.Rename(createFiles[tc], newName)
		if err != nil {
			log.Fatal(err)
		}

		expectedLogsConsumer.ConsumeLogs(t.Context(), createLogs(newName, fsbroker.Rename.String()))

		time.Sleep(800 * time.Millisecond) // Sleeping here because the filewatcher (or something) does not like when you do things quickly --- seems to not be emitting anything.

		err = os.Remove(newName)
		if err != nil {
			log.Fatal(err)
		}
		expectedLogsConsumer.ConsumeLogs(t.Context(), createLogs(newName, fsbroker.Remove.String()))
		time.Sleep(10 * time.Second) // Sleeping here because the filewatcher (or something) does not like when you do things quickly --- seems to not be emitting anything.

		// Assert
		eventuallyExpect(t, expectedLogsConsumer.LogRecordCount(), actualLogsConsumer.LogRecordCount())
		for i, expected := range expectedLogsConsumer.AllLogs() {
			actual := actualLogsConsumer.AllLogs()[i]
			assertLogRecords(t, expected, actual)
		}
	}
	require.NoError(t, logs.Shutdown(context.Background()))
}

func TestRenameFileNTimes(t *testing.T) {
	// Arrange
	expectedLogsConsumer := new(consumertest.LogsSink)
	logs, actualLogsConsumer := testSetup(t, TEST_PATH)

	// Act
	orignalName := fmt.Sprintf("%v/%v.txt", TEST_PATH, gofakeit.LetterN(5))
	oldName := orignalName
	t.Log("test-case", "file", oldName)
	f, err := os.OpenFile(oldName, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}
	time.Sleep(800 * time.Millisecond) // Sleeping here because the filewatcher (or something) does not like when you do things quickly --- seems to not be emitting anything.
	expectedLogsConsumer.ConsumeLogs(t.Context(), createLogs(oldName, fsbroker.Create.String()))

	f.Close()
	for range gofakeit.UintRange(5, 10) {
		newName := fmt.Sprintf("%v/%v.txt", TEST_PATH, gofakeit.LetterN(5))
		err = os.Rename(oldName, newName)
		if err != nil {
			log.Fatal(err)
		}
		expectedLogsConsumer.ConsumeLogs(t.Context(), createLogs(newName, fsbroker.Rename.String()))
		time.Sleep(800 * time.Millisecond) // Sleeping here because the filewatcher (or something) does not like when you do things quickly --- seems to not be emitting anything.
		oldName = newName
	}
	err = os.Rename(oldName, orignalName)
	if err != nil {
		log.Fatal(err)
	}

	expectedLogsConsumer.ConsumeLogs(t.Context(), createLogs(orignalName, fsbroker.Rename.String()))
	time.Sleep(800 * time.Millisecond)

	err = os.Remove(orignalName)
	if err != nil {
		log.Fatal(err)
	}
	expectedLogsConsumer.ConsumeLogs(t.Context(), createLogs(orignalName, fsbroker.Remove.String()))
	time.Sleep(800 * time.Millisecond)

	// Assert
	eventuallyExpect(t, expectedLogsConsumer.LogRecordCount(), actualLogsConsumer.LogRecordCount())
	for i, expected := range expectedLogsConsumer.AllLogs() {
		actual := actualLogsConsumer.AllLogs()[i]
		assertLogRecords(t, expected, actual)
	}
	require.NoError(t, logs.Shutdown(context.Background()))
}

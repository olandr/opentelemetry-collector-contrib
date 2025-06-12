package filewatcher

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/brianvoe/gofakeit/v7"
	"github.com/charmbracelet/log"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/pdatatest/plogtest"
	"github.com/stretchr/testify/require"
	"github.com/syncthing/notify"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/plog"
)

type FactoryTestCase struct {
	Name     string
	Expected *NotifyReceiverConfig
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

func TestNotifyReveiverSimple(t *testing.T) {
	// Arrange
	expectedLogsConsumer := new(consumertest.LogsSink)
	logs, actualLogsConsumer, wd := testSetup(t)

	// Act
	TEST_FILES := 1
	createFiles := make([]string, TEST_FILES)
	for tc := range TEST_FILES {
		createFiles[tc] = fmt.Sprintf("%v/%v/%v.txt", wd, TEST_INCLUDE_PATH, gofakeit.LetterN(5))
		t.Log("test-case", "file", createFiles[tc])
		f, err := os.OpenFile(createFiles[tc], os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			log.Fatal(err)
		}

		expectedLogsConsumer.ConsumeLogs(t.Context(), createLogs(createFiles[tc], notify.Create.String()))

		time.Sleep(800 * time.Millisecond) // Sleeping here because the filewatcher (or something) does not like when you do things quickly --- seems to not be emitting anything.
		_, err = f.Write([]byte(gofakeit.LetterN(10)))
		if err != nil {
			log.Fatal(err)
		}
		expectedLogsConsumer.ConsumeLogs(t.Context(), createLogs(createFiles[tc], notify.Write.String()))

		f.Close()
		time.Sleep(800 * time.Millisecond) // Sleeping here because the filewatcher (or something) does not like when you do things quickly --- seems to not be emitting anything.

		err = os.Remove(createFiles[tc])
		if err != nil {
			log.Fatal(err)
		}
		expectedLogsConsumer.ConsumeLogs(t.Context(), createLogs(createFiles[tc], notify.Remove.String()))
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

func TestNotifyReveiverListenToNewDir(t *testing.T) {
	// Arrange
	expectedLogsConsumer := new(consumertest.LogsSink)
	// We want to only listen to the outer path, but add files to a dir within
	logs, actualLogsConsumer, wd := testSetup(t)
	time.Sleep(800 * time.Millisecond)
	// Act
	TEST_FILES := 1
	createFiles := make([]string, TEST_FILES)
	for tc := range TEST_FILES {
		innerDir := fmt.Sprintf("%v/%v/%v", wd, TEST_INCLUDE_PATH, gofakeit.LetterN(5))

		t.Log("create", "dir", innerDir)
		err := os.Mkdir(innerDir, 0777)
		if err != nil {
			log.Fatal(err)
		}
		expectedLogsConsumer.ConsumeLogs(t.Context(), createLogs(innerDir, notify.Create.String()))
		time.Sleep(800 * time.Millisecond)

		createFiles[tc] = fmt.Sprintf("%v/%v.txt", innerDir, gofakeit.LetterN(5))
		t.Log("create", "file", createFiles[tc])
		f, err := os.OpenFile(createFiles[tc], os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			log.Fatal(err)
		}

		expectedLogsConsumer.ConsumeLogs(t.Context(), createLogs(createFiles[tc], notify.Create.String()))
		time.Sleep(800 * time.Millisecond) // Sleeping here because the filewatcher (or something) does not like when you do things quickly --- seems to not be emitting anything.
		t.Log("write", "file", createFiles[tc])
		_, err = f.Write([]byte(gofakeit.LetterN(10)))
		if err != nil {
			log.Fatal(err)
		}
		expectedLogsConsumer.ConsumeLogs(t.Context(), createLogs(createFiles[tc], notify.Write.String()))

		f.Close()
		time.Sleep(800 * time.Millisecond) // Sleeping here because the filewatcher (or something) does not like when you do things quickly --- seems to not be emitting anything.
		t.Log("remove", "file", createFiles[tc])
		err = os.Remove(createFiles[tc])
		if err != nil {
			log.Fatal(err)
		}
		expectedLogsConsumer.ConsumeLogs(t.Context(), createLogs(createFiles[tc], notify.Remove.String()))
		time.Sleep(800 * time.Millisecond)

		err = os.Remove(innerDir)
		if err != nil {
			log.Fatal(err)
		}
		expectedLogsConsumer.ConsumeLogs(t.Context(), createLogs(innerDir, notify.Remove.String()))
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

func TestNotifyReveiverListenToExistingNestedDir(t *testing.T) {
	// Arrange
	expectedLogsConsumer := new(consumertest.LogsSink)
	// We want to only listen to the outer path, but add files to a dir within
	logs, actualLogsConsumer, wd := testSetup(t)

	// Act
	TEST_FILES := 1
	createFiles := make([]string, TEST_FILES)
	for tc := range TEST_FILES {

		time.Sleep(800 * time.Millisecond)
		createFiles[tc] = fmt.Sprintf("%v/%v/%v.txt", wd, TEST_INNER_PATH, gofakeit.LetterN(5))
		t.Log("test-case", "file", createFiles[tc])
		f, err := os.OpenFile(createFiles[tc], os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			log.Fatal(err)
		}

		expectedLogsConsumer.ConsumeLogs(t.Context(), createLogs(createFiles[tc], notify.Create.String()))

		time.Sleep(800 * time.Millisecond) // Sleeping here because the filewatcher (or something) does not like when you do things quickly --- seems to not be emitting anything.
		_, err = f.Write([]byte(gofakeit.LetterN(10)))
		if err != nil {
			log.Fatal(err)
		}
		expectedLogsConsumer.ConsumeLogs(t.Context(), createLogs(createFiles[tc], notify.Write.String()))

		f.Close()
		time.Sleep(800 * time.Millisecond) // Sleeping here because the filewatcher (or something) does not like when you do things quickly --- seems to not be emitting anything.

		err = os.Remove(createFiles[tc])
		if err != nil {
			log.Fatal(err)
		}
		expectedLogsConsumer.ConsumeLogs(t.Context(), createLogs(createFiles[tc], notify.Remove.String()))
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

func TestNotifyReveiverListenToExistingNestedNewDir(t *testing.T) {
	// Arrange
	expectedLogsConsumer := new(consumertest.LogsSink)
	// We want to only listen to the outer path, but add files to a dir within
	logs, actualLogsConsumer, wd := testSetup(t)

	// Act
	TEST_FILES := 1
	createFiles := make([]string, TEST_FILES)
	for tc := range TEST_FILES {

		innerDir := fmt.Sprintf("%v/%v/%v", wd, TEST_INNER_PATH, gofakeit.LetterN(5))
		err := os.Mkdir(innerDir, 0777)
		if err != nil {
			log.Fatal(err)
		}
		expectedLogsConsumer.ConsumeLogs(t.Context(), createLogs(innerDir, notify.Create.String()))
		time.Sleep(800 * time.Millisecond)

		createFiles[tc] = fmt.Sprintf("%v/%v.txt", TEST_INNER_PATH, gofakeit.LetterN(5))
		t.Log("test-case", "file", createFiles[tc])
		f, err := os.OpenFile(createFiles[tc], os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			log.Fatal(err)
		}

		expectedLogsConsumer.ConsumeLogs(t.Context(), createLogs(createFiles[tc], notify.Create.String()))

		time.Sleep(800 * time.Millisecond) // Sleeping here because the filewatcher (or something) does not like when you do things quickly --- seems to not be emitting anything.
		_, err = f.Write([]byte(gofakeit.LetterN(10)))
		if err != nil {
			log.Fatal(err)
		}
		expectedLogsConsumer.ConsumeLogs(t.Context(), createLogs(createFiles[tc], notify.Write.String()))

		f.Close()
		time.Sleep(800 * time.Millisecond) // Sleeping here because the filewatcher (or something) does not like when you do things quickly --- seems to not be emitting anything.

		err = os.Remove(createFiles[tc])
		if err != nil {
			log.Fatal(err)
		}
		expectedLogsConsumer.ConsumeLogs(t.Context(), createLogs(createFiles[tc], notify.Remove.String()))
		time.Sleep(800 * time.Millisecond)

		err = os.Remove(innerDir)
		if err != nil {
			log.Fatal(err)
		}
		expectedLogsConsumer.ConsumeLogs(t.Context(), createLogs(innerDir, notify.Remove.String()))
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
	logs, actualLogsConsumer, wd := testSetup(t)

	// Act
	TEST_FILES := 1
	createFiles := make([]string, TEST_FILES)
	for tc := range TEST_FILES {
		createFiles[tc] = fmt.Sprintf("%v/%v/%v.txt", wd, TEST_INCLUDE_PATH, gofakeit.LetterN(5))
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
	logs, actualLogsConsumer, wd := testSetup(t)
	// Act
	TEST_FILES := 5
	createFiles := make([]string, TEST_FILES)
	for tc := range TEST_FILES {
		createFiles[tc] = fmt.Sprintf("%v/%v/%v.txt", wd, TEST_INCLUDE_PATH, gofakeit.LetterN(5))
		t.Log("test-case", "file", createFiles[tc])
		f, err := os.OpenFile(createFiles[tc], os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			log.Fatal(err)
		}

		time.Sleep(800 * time.Millisecond) // Sleeping here because the filewatcher (or something) does not like when you do things quickly --- seems to not be emitting anything.
		expectedLogsConsumer.ConsumeLogs(t.Context(), createLogs(createFiles[tc], notify.Create.String()))

		f.Close()

		newName := fmt.Sprintf("%v/%v/%v.txt", wd, TEST_INCLUDE_PATH, gofakeit.LetterN(5))
		err = os.Rename(createFiles[tc], newName)
		if err != nil {
			log.Fatal(err)
		}

		expectedLogsConsumer.ConsumeLogs(t.Context(), createLogs(newName, notify.Rename.String()))

		time.Sleep(800 * time.Millisecond) // Sleeping here because the filewatcher (or something) does not like when you do things quickly --- seems to not be emitting anything.

		err = os.Remove(newName)
		if err != nil {
			log.Fatal(err)
		}
		expectedLogsConsumer.ConsumeLogs(t.Context(), createLogs(newName, notify.Remove.String()))
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
	logs, actualLogsConsumer, wd := testSetup(t)

	// Act
	orignalName := fmt.Sprintf("%v/%v/%v.txt", wd, TEST_INCLUDE_PATH, gofakeit.LetterN(5))
	oldName := orignalName
	t.Log("test-case", "file", oldName)
	f, err := os.OpenFile(oldName, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}
	time.Sleep(800 * time.Millisecond) // Sleeping here because the filewatcher (or something) does not like when you do things quickly --- seems to not be emitting anything.
	expectedLogsConsumer.ConsumeLogs(t.Context(), createLogs(oldName, notify.Create.String()))

	f.Close()
	for range gofakeit.UintRange(5, 10) {
		newName := fmt.Sprintf("%v/%v/%v.txt", wd, TEST_INCLUDE_PATH, gofakeit.LetterN(5))
		err = os.Rename(oldName, newName)
		if err != nil {
			log.Fatal(err)
		}
		expectedLogsConsumer.ConsumeLogs(t.Context(), createLogs(newName, notify.Rename.String()))
		time.Sleep(800 * time.Millisecond) // Sleeping here because the filewatcher (or something) does not like when you do things quickly --- seems to not be emitting anything.
		oldName = newName
	}
	err = os.Rename(oldName, orignalName)
	if err != nil {
		log.Fatal(err)
	}

	expectedLogsConsumer.ConsumeLogs(t.Context(), createLogs(orignalName, notify.Rename.String()))
	time.Sleep(800 * time.Millisecond)

	err = os.Remove(orignalName)
	if err != nil {
		log.Fatal(err)
	}
	expectedLogsConsumer.ConsumeLogs(t.Context(), createLogs(orignalName, notify.Remove.String()))
	time.Sleep(800 * time.Millisecond)

	// Assert
	eventuallyExpect(t, expectedLogsConsumer.LogRecordCount(), actualLogsConsumer.LogRecordCount())
	for i, expected := range expectedLogsConsumer.AllLogs() {
		actual := actualLogsConsumer.AllLogs()[i]
		assertLogRecords(t, expected, actual)
	}
	require.NoError(t, logs.Shutdown(context.Background()))
}

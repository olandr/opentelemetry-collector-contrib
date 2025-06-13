package filewatcher

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/brianvoe/gofakeit/v7"
	"github.com/charmbracelet/log"
	"github.com/stretchr/testify/require"
	"github.com/syncthing/notify"
	"go.opentelemetry.io/collector/consumer/consumertest"
)

type FactoryTestCase struct {
	Name     string
	Expected *NotifyReceiverConfig
	Error    error
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
		f, err := os.OpenFile(createFiles[tc], os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			log.Fatal(err)
		}

		expectedLogsConsumer.ConsumeLogs(t.Context(), createLogs(createFiles[tc], notify.Create.String()))

		time.Sleep(300 * time.Millisecond)
		_, err = f.Write([]byte(gofakeit.LetterN(10)))
		if err != nil {
			log.Fatal(err)
		}
		expectedLogsConsumer.ConsumeLogs(t.Context(), createLogs(createFiles[tc], notify.Write.String()))

		f.Close()
		time.Sleep(300 * time.Millisecond)

		err = os.Remove(createFiles[tc])
		if err != nil {
			log.Fatal(err)
		}
		expectedLogsConsumer.ConsumeLogs(t.Context(), createLogs(createFiles[tc], notify.Remove.String()))
		time.Sleep(300 * time.Millisecond)

		// Assert
		eventuallyExpect(t, expectedLogsConsumer.LogRecordCount(), actualLogsConsumer.LogRecordCount())
		require.Equal(t, logsToMap(t, expectedLogsConsumer.AllLogs()), logsToMap(t, actualLogsConsumer.AllLogs()))

	}
	require.NoError(t, logs.Shutdown(context.Background()))
}

func TestNotifyReveiverListenToNewDir(t *testing.T) {
	// Arrange
	expectedLogsConsumer := new(consumertest.LogsSink)
	// We want to only listen to the outer path, but add files to a dir within
	logs, actualLogsConsumer, wd := testSetup(t)
	time.Sleep(300 * time.Millisecond)
	// Act
	TEST_FILES := 1
	createFiles := make([]string, TEST_FILES)
	for tc := range TEST_FILES {
		innerDir := fmt.Sprintf("%v/%v/%v", wd, TEST_INCLUDE_PATH, gofakeit.LetterN(5))

		err := os.Mkdir(innerDir, 0777)
		if err != nil {
			log.Fatal(err)
		}
		expectedLogsConsumer.ConsumeLogs(t.Context(), createLogs(innerDir, notify.Create.String()))
		time.Sleep(300 * time.Millisecond)

		createFiles[tc] = fmt.Sprintf("%v/%v.txt", innerDir, gofakeit.LetterN(5))
		f, err := os.OpenFile(createFiles[tc], os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			log.Fatal(err)
		}

		expectedLogsConsumer.ConsumeLogs(t.Context(), createLogs(createFiles[tc], notify.Create.String()))
		time.Sleep(300 * time.Millisecond)
		_, err = f.Write([]byte(gofakeit.LetterN(10)))
		if err != nil {
			log.Fatal(err)
		}
		expectedLogsConsumer.ConsumeLogs(t.Context(), createLogs(createFiles[tc], notify.Write.String()))

		f.Close()
		time.Sleep(300 * time.Millisecond)
		err = os.Remove(createFiles[tc])
		if err != nil {
			log.Fatal(err)
		}
		expectedLogsConsumer.ConsumeLogs(t.Context(), createLogs(createFiles[tc], notify.Remove.String()))
		time.Sleep(300 * time.Millisecond)

		err = os.Remove(innerDir)
		if err != nil {
			log.Fatal(err)
		}
		expectedLogsConsumer.ConsumeLogs(t.Context(), createLogs(innerDir, notify.Remove.String()))
		time.Sleep(300 * time.Millisecond)
		// Assert
		eventuallyExpect(t, expectedLogsConsumer.LogRecordCount(), actualLogsConsumer.LogRecordCount())
		require.Equal(t, logsToMap(t, expectedLogsConsumer.AllLogs()), logsToMap(t, actualLogsConsumer.AllLogs()))

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

		time.Sleep(300 * time.Millisecond)
		createFiles[tc] = fmt.Sprintf("%v/%v/%v.txt", wd, TEST_INNER_PATH, gofakeit.LetterN(5))
		f, err := os.OpenFile(createFiles[tc], os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			log.Fatal(err)
		}

		expectedLogsConsumer.ConsumeLogs(t.Context(), createLogs(createFiles[tc], notify.Create.String()))

		time.Sleep(300 * time.Millisecond)
		_, err = f.Write([]byte(gofakeit.LetterN(10)))
		if err != nil {
			log.Fatal(err)
		}
		expectedLogsConsumer.ConsumeLogs(t.Context(), createLogs(createFiles[tc], notify.Write.String()))

		f.Close()
		time.Sleep(300 * time.Millisecond)

		err = os.Remove(createFiles[tc])
		if err != nil {
			log.Fatal(err)
		}
		expectedLogsConsumer.ConsumeLogs(t.Context(), createLogs(createFiles[tc], notify.Remove.String()))
		time.Sleep(300 * time.Millisecond)
		// Assert
		eventuallyExpect(t, expectedLogsConsumer.LogRecordCount(), actualLogsConsumer.LogRecordCount())

		require.Equal(t, logsToMap(t, expectedLogsConsumer.AllLogs()), logsToMap(t, actualLogsConsumer.AllLogs()))

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
		time.Sleep(300 * time.Millisecond)

		createFiles[tc] = fmt.Sprintf("%v/%v.txt", TEST_INNER_PATH, gofakeit.LetterN(5))
		f, err := os.OpenFile(createFiles[tc], os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			log.Fatal(err)
		}

		expectedLogsConsumer.ConsumeLogs(t.Context(), createLogs(createFiles[tc], notify.Create.String()))

		time.Sleep(300 * time.Millisecond)
		_, err = f.Write([]byte(gofakeit.LetterN(10)))
		if err != nil {
			log.Fatal(err)
		}
		expectedLogsConsumer.ConsumeLogs(t.Context(), createLogs(createFiles[tc], notify.Write.String()))

		f.Close()
		time.Sleep(300 * time.Millisecond)

		err = os.Remove(createFiles[tc])
		if err != nil {
			log.Fatal(err)
		}
		expectedLogsConsumer.ConsumeLogs(t.Context(), createLogs(createFiles[tc], notify.Remove.String()))
		time.Sleep(300 * time.Millisecond)

		err = os.Remove(innerDir)
		if err != nil {
			log.Fatal(err)
		}
		expectedLogsConsumer.ConsumeLogs(t.Context(), createLogs(innerDir, notify.Remove.String()))
		time.Sleep(300 * time.Millisecond)

		// Assert
		eventuallyExpect(t, expectedLogsConsumer.LogRecordCount(), actualLogsConsumer.LogRecordCount())

		require.Equal(t, logsToMap(t, expectedLogsConsumer.AllLogs()), logsToMap(t, actualLogsConsumer.AllLogs()))

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
		require.Equal(t, logsToMap(t, expectedLogsConsumer.AllLogs()), logsToMap(t, actualLogsConsumer.AllLogs()))

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
		f, err := os.OpenFile(createFiles[tc], os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			log.Fatal(err)
		}

		time.Sleep(300 * time.Millisecond)
		expectedLogsConsumer.ConsumeLogs(t.Context(), createLogs(createFiles[tc], notify.Create.String()))

		f.Close()

		newName := fmt.Sprintf("%v/%v/%v.txt", wd, TEST_INCLUDE_PATH, gofakeit.LetterN(5))
		err = os.Rename(createFiles[tc], newName)
		if err != nil {
			log.Fatal(err)
		}

		expectedLogsConsumer.ConsumeLogs(t.Context(), createLogs(createFiles[tc], notify.Rename.String()))
		expectedLogsConsumer.ConsumeLogs(t.Context(), createLogs(newName, notify.Rename.String()))

		time.Sleep(300 * time.Millisecond)

		err = os.Remove(newName)
		if err != nil {
			log.Fatal(err)
		}
		expectedLogsConsumer.ConsumeLogs(t.Context(), createLogs(newName, notify.Rename.String()))
		expectedLogsConsumer.ConsumeLogs(t.Context(), createLogs(newName, notify.Remove.String()))
		time.Sleep(300 * time.Millisecond)

		// Assert
		eventuallyExpect(t, expectedLogsConsumer.LogRecordCount(), actualLogsConsumer.LogRecordCount())
		require.Equal(t, logsToMap(t, expectedLogsConsumer.AllLogs()), logsToMap(t, actualLogsConsumer.AllLogs()))
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
	f, err := os.OpenFile(oldName, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond)
	expectedLogsConsumer.ConsumeLogs(t.Context(), createLogs(oldName, notify.Create.String()))

	f.Close()
	for range gofakeit.UintRange(5, 10) {
		newName := fmt.Sprintf("%v/%v/%v.txt", wd, TEST_INCLUDE_PATH, gofakeit.LetterN(5))
		err = os.Rename(oldName, newName)
		if err != nil {
			log.Fatal(err)
		}
		expectedLogsConsumer.ConsumeLogs(t.Context(), createLogs(newName, notify.Rename.String()))
		expectedLogsConsumer.ConsumeLogs(t.Context(), createLogs(oldName, notify.Rename.String()))
		time.Sleep(300 * time.Millisecond)
		oldName = newName
	}
	err = os.Rename(oldName, orignalName)
	if err != nil {
		log.Fatal(err)
	}

	expectedLogsConsumer.ConsumeLogs(t.Context(), createLogs(orignalName, notify.Rename.String()))
	expectedLogsConsumer.ConsumeLogs(t.Context(), createLogs(oldName, notify.Rename.String()))
	time.Sleep(300 * time.Millisecond)

	err = os.Remove(orignalName)
	if err != nil {
		log.Fatal(err)
	}
	expectedLogsConsumer.ConsumeLogs(t.Context(), createLogs(orignalName, notify.Rename.String()))
	expectedLogsConsumer.ConsumeLogs(t.Context(), createLogs(orignalName, notify.Remove.String()))
	time.Sleep(300 * time.Millisecond)

	// Assert
	eventuallyExpect(t, expectedLogsConsumer.LogRecordCount(), actualLogsConsumer.LogRecordCount())
	require.Equal(t, logsToMap(t, expectedLogsConsumer.AllLogs()), logsToMap(t, actualLogsConsumer.AllLogs()))
	require.NoError(t, logs.Shutdown(context.Background()))
}

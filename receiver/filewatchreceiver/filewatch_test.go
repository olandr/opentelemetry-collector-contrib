//go:build darwin && !kqueue && cgo && !ios
// +build darwin,!kqueue,cgo,!ios

package filewatchreceiver

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/brianvoe/gofakeit/v7"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/consumer/consumertest"
)

type FactoryTestCase struct {
	Name     string
	Expected *NotifyReceiverConfig
	Error    error
}

func eventuallyExpect(t *testing.T, expected int, actual int) {
	time.Sleep(300 * time.Millisecond)
	require.Eventually(t, func() bool { return expected == actual }, 10*time.Second, 5*time.Millisecond,
		"expected %d, but got %d", expected, actual)
}

func TestFilewatcherReceiver(t *testing.T) {
	beforeAll(t, TEST_INCLUDE_PATH)

	time.Sleep(300 * time.Millisecond)
	TEST_RUNS := gofakeit.UintRange(5, 10)
	t.Run("can do simple crud", func(t *testing.T) {
		t.Parallel()
		// Arrange
		expectedLogsConsumer := new(consumertest.LogsSink)
		logs, actualLogsConsumer, wd := beforeEach(t, false)

		// Act
		TEST_FILES := 1
		createFiles := make([]string, TEST_FILES)
		for tc := range TEST_FILES {

			createFiles[tc] = fmt.Sprintf("%v/%v.txt", wd, gofakeit.LetterN(5))
			consumeLogs(t, expectedLogsConsumer, Create(createFiles[tc]))
			consumeLogs(t, expectedLogsConsumer, Write(createFiles[tc]))
			consumeLogs(t, expectedLogsConsumer, Remove(createFiles[tc]))

			// Assert
			time.Sleep(300 * time.Millisecond)

			expected := logsToMap(t, expectedLogsConsumer.AllLogs(), "expected")
			actual := logsToMap(t, actualLogsConsumer.AllLogs(), "actual")
			eventuallyExpect(t, expectedLogsConsumer.LogRecordCount(), actualLogsConsumer.LogRecordCount())
			require.Equal(t, expected, actual)
		}
		require.NoError(t, logs.Shutdown(context.Background()))
		testTeardown(t, wd)
	})

	t.Run("can watch a newly created dir", func(t *testing.T) {
		t.Parallel()
		// Arrange
		expectedLogsConsumer := new(consumertest.LogsSink)
		// We want to only listen to the outer path, but add files to a dir within
		logs, actualLogsConsumer, wd := beforeEach(t, false)
		time.Sleep(300 * time.Millisecond)
		// Act
		createFiles := make([]string, TEST_RUNS)
		for tc := range TEST_RUNS {
			innerDir := fmt.Sprintf("%v/%v", wd, gofakeit.LetterN(5))
			consumeLogs(t, expectedLogsConsumer, CreateDir(innerDir))
			createFiles[tc] = fmt.Sprintf("%v/%v.txt", innerDir, gofakeit.LetterN(5))
			consumeLogs(t, expectedLogsConsumer, Create(createFiles[tc]))
			consumeLogs(t, expectedLogsConsumer, Write(createFiles[tc]))
			consumeLogs(t, expectedLogsConsumer, Remove(createFiles[tc]))
			consumeLogs(t, expectedLogsConsumer, Remove(innerDir))

			// Assert
			time.Sleep(300 * time.Millisecond)
			expected := logsToMap(t, expectedLogsConsumer.AllLogs(), "expected")
			actual := logsToMap(t, actualLogsConsumer.AllLogs(), "actual")
			eventuallyExpect(t, expectedLogsConsumer.LogRecordCount(), actualLogsConsumer.LogRecordCount())
			require.Equal(t, expected, actual)
		}
		require.NoError(t, logs.Shutdown(context.Background()))
		testTeardown(t, wd)
	})

	t.Run("can watch existing dir", func(t *testing.T) {
		t.Parallel()
		// Arrange
		expectedLogsConsumer := new(consumertest.LogsSink)
		// We want to only listen to the outer path, but add files to a dir within
		logs, actualLogsConsumer, wd := beforeEach(t, true)
		wd_inner := fmt.Sprintf("%v/inner", wd)
		// Act
		TEST_FILES := 1
		createFiles := make([]string, TEST_FILES)
		for tc := range TEST_FILES {
			time.Sleep(300 * time.Millisecond)
			createFiles[tc] = fmt.Sprintf("%v/%v.txt", wd_inner, gofakeit.LetterN(5))

			consumeLogs(t, expectedLogsConsumer, Create(createFiles[tc]))
			consumeLogs(t, expectedLogsConsumer, Write(createFiles[tc]))
			consumeLogs(t, expectedLogsConsumer, Remove(createFiles[tc]))

			// Assert
			time.Sleep(300 * time.Millisecond)
			require.Equal(t, logsToMap(t, expectedLogsConsumer.AllLogs(), "expected"), logsToMap(t, actualLogsConsumer.AllLogs(), "actual"))
			eventuallyExpect(t, expectedLogsConsumer.LogRecordCount(), actualLogsConsumer.LogRecordCount())

		}
		require.NoError(t, logs.Shutdown(context.Background()))
		testTeardown(t, wd)
	})

	t.Run("can watch to nested dir", func(t *testing.T) {
		t.Parallel()
		// Arrange
		expectedLogsConsumer := new(consumertest.LogsSink)
		// We want to only listen to the outer path, but add files to a dir within
		logs, actualLogsConsumer, wd := beforeEach(t, true)
		wd_inner := fmt.Sprintf("%v/inner", wd)
		// Act
		TEST_FILES := 1
		createFiles := make([]string, TEST_FILES)
		for tc := range TEST_FILES {

			innerDir := fmt.Sprintf("%v/%v", wd_inner, gofakeit.LetterN(5))
			consumeLogs(t, expectedLogsConsumer, CreateDir(innerDir))

			createFiles[tc] = fmt.Sprintf("%v/%v.txt", wd_inner, gofakeit.LetterN(5))
			consumeLogs(t, expectedLogsConsumer, Create(createFiles[tc]))
			consumeLogs(t, expectedLogsConsumer, Write(createFiles[tc]))
			consumeLogs(t, expectedLogsConsumer, Remove(createFiles[tc]))
			consumeLogs(t, expectedLogsConsumer, Remove(innerDir))

			// Assert
			time.Sleep(300 * time.Millisecond)

			expected := logsToMap(t, expectedLogsConsumer.AllLogs(), "expected")
			actual := logsToMap(t, actualLogsConsumer.AllLogs(), "actual")
			eventuallyExpect(t, expectedLogsConsumer.LogRecordCount(), actualLogsConsumer.LogRecordCount())
			require.Equal(t, expected, actual)

		}
		require.NoError(t, logs.Shutdown(context.Background()))
		testTeardown(t, wd)
	})

	t.Run("renamed file is removed", func(t *testing.T) {
		t.Parallel()
		// Arrange
		expectedLogsConsumer := new(consumertest.LogsSink)
		logs, actualLogsConsumer, wd := beforeEach(t, false)
		// Act
		createFiles := make([]string, TEST_RUNS)
		for tc := range TEST_RUNS {
			createFiles[tc] = fmt.Sprintf("%v/%v.txt", wd, gofakeit.LetterN(5))
			consumeLogs(t, expectedLogsConsumer, Create(createFiles[tc]))
			consumeLogs(t, expectedLogsConsumer, WriteOnClose(createFiles[tc]))

			newName := fmt.Sprintf("%v/%v.txt", wd, gofakeit.LetterN(5))
			consumeLogs(t, expectedLogsConsumer, Rename(createFiles[tc], newName))
			consumeLogs(t, expectedLogsConsumer, RenameRemove(newName))
			// Assert
			time.Sleep(300 * time.Millisecond)

			expected := logsToMap(t, expectedLogsConsumer.AllLogs(), "expected")
			actual := logsToMap(t, actualLogsConsumer.AllLogs(), "actual")
			eventuallyExpect(t, expectedLogsConsumer.LogRecordCount(), actualLogsConsumer.LogRecordCount())
			require.Equal(t, expected, actual)
		}
		require.NoError(t, logs.Shutdown(context.Background()))
		testTeardown(t, wd)
	})

	t.Run(fmt.Sprintf("renaming a file %v times", TEST_RUNS), func(t *testing.T) {
		t.Parallel()
		// Arrange
		expectedLogsConsumer := new(consumertest.LogsSink)
		logs, actualLogsConsumer, wd := beforeEach(t, false)

		// Act
		orignalName := fmt.Sprintf("%v/%v.txt", wd, gofakeit.LetterN(5))
		oldName := orignalName
		consumeLogs(t, expectedLogsConsumer, Create(oldName))
		consumeLogs(t, expectedLogsConsumer, WriteOnClose(oldName))

		for range TEST_RUNS {
			newName := fmt.Sprintf("%v/%v.txt", wd, gofakeit.LetterN(5))
			consumeLogs(t, expectedLogsConsumer, Rename(oldName, newName))
			oldName = newName
		}
		consumeLogs(t, expectedLogsConsumer, Rename(oldName, orignalName))
		consumeLogs(t, expectedLogsConsumer, RenameRemove(orignalName))
		// Assert
		time.Sleep(300 * time.Millisecond)

		expected := logsToMap(t, expectedLogsConsumer.AllLogs(), "expected")
		actual := logsToMap(t, actualLogsConsumer.AllLogs(), "actual")
		eventuallyExpect(t, expectedLogsConsumer.LogRecordCount(), actualLogsConsumer.LogRecordCount())
		require.Equal(t, expected, actual)
		require.NoError(t, logs.Shutdown(context.Background()))
		testTeardown(t, wd)
	})
}

//go:build darwin && !kqueue && cgo && !ios
// +build darwin,!kqueue,cgo,!ios

package filewatchreceiver

import (
	"context"
	"fmt"
	"strings"
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
	time.Sleep(300 * time.Millisecond)
	TEST_RUNS := gofakeit.UintRange(2, 5)
	t.Run("can do simple crud", func(t *testing.T) {
		t.Parallel()
		// Arrange
		expectedLogsConsumer := new(consumertest.LogsSink)
		logs, actualLogsConsumer, cfg, root_dir := beforeEach(t, false)
		wd := strings.Replace((cfg.Include[0]), "/...", "", -1)
		// Act
		TEST_FILES := 1
		createFiles := make([]string, TEST_FILES)
		for tc := range TEST_FILES {

			createFiles[tc] = fmt.Sprintf("%v/%v.txt", wd, gofakeit.LetterN(5))
			consumeLogs(t, expectedLogsConsumer, Create(createFiles[tc], true))
			consumeLogs(t, expectedLogsConsumer, Write(createFiles[tc], true))
			consumeLogs(t, expectedLogsConsumer, Remove(createFiles[tc], true))

			// Assert
			time.Sleep(300 * time.Millisecond)

			expected := logsToMap(t, expectedLogsConsumer.AllLogs(), "expected")
			actual := logsToMap(t, actualLogsConsumer.AllLogs(), "actual")
			eventuallyExpect(t, expectedLogsConsumer.LogRecordCount(), actualLogsConsumer.LogRecordCount())
			require.Equal(t, expected, actual)
		}
		require.NoError(t, logs.Shutdown(context.Background()))
		testTeardown(t, root_dir)
	})

	t.Run("can watch a newly created dir", func(t *testing.T) {
		t.Parallel()
		// Arrange
		expectedLogsConsumer := new(consumertest.LogsSink)
		// We want to only listen to the outer path, but add files to a dir within
		logs, actualLogsConsumer, cfg, root_dir := beforeEach(t, false)
		wd := strings.Replace((cfg.Include[0]), "/...", "", -1)
		time.Sleep(300 * time.Millisecond)
		// Act
		createFiles := make([]string, TEST_RUNS)
		for tc := range TEST_RUNS {
			innerDir := fmt.Sprintf("%v/%v", wd, gofakeit.LetterN(5))
			consumeLogs(t, expectedLogsConsumer, CreateDir(innerDir, true))
			createFiles[tc] = fmt.Sprintf("%v/%v.txt", innerDir, gofakeit.LetterN(5))
			consumeLogs(t, expectedLogsConsumer, Create(createFiles[tc], true))
			consumeLogs(t, expectedLogsConsumer, Write(createFiles[tc], true))
			consumeLogs(t, expectedLogsConsumer, Remove(createFiles[tc], true))
			consumeLogs(t, expectedLogsConsumer, Remove(innerDir, true))

			// Assert
			time.Sleep(300 * time.Millisecond)
			expected := logsToMap(t, expectedLogsConsumer.AllLogs(), "expected")
			actual := logsToMap(t, actualLogsConsumer.AllLogs(), "actual")
			eventuallyExpect(t, expectedLogsConsumer.LogRecordCount(), actualLogsConsumer.LogRecordCount())
			require.Equal(t, expected, actual)
		}
		require.NoError(t, logs.Shutdown(context.Background()))
		testTeardown(t, root_dir)
	})

	t.Run("no logs on excluded crud", func(t *testing.T) {
		t.Parallel()
		// Arrange
		expectedLogsConsumer := new(consumertest.LogsSink)
		logs, actualLogsConsumer, cfg, root_dir := beforeEach(t, false)
		wd := strings.Replace(strings.Replace((cfg.Include[0]), "/...", "", -1), "/*", "", -1)

		// Act
		TEST_FILES := 1
		createFiles := make([]string, TEST_FILES)
		for tc := range TEST_FILES {

			createFiles[tc] = fmt.Sprintf("%v/exclude/%v.skip", wd, gofakeit.LetterN(5))
			Create(createFiles[tc], true)
			time.Sleep(300 * time.Millisecond)
			Write(createFiles[tc], true)
			time.Sleep(300 * time.Millisecond)
			Remove(createFiles[tc], true)
			time.Sleep(300 * time.Millisecond)

			// Assert
			time.Sleep(300 * time.Millisecond)

			expected := logsToMap(t, expectedLogsConsumer.AllLogs(), "expected")
			actual := logsToMap(t, actualLogsConsumer.AllLogs(), "actual")
			eventuallyExpect(t, expectedLogsConsumer.LogRecordCount(), actualLogsConsumer.LogRecordCount())
			require.Equal(t, expected, actual)
		}
		require.NoError(t, logs.Shutdown(context.Background()))
		testTeardown(t, root_dir)
	})

	t.Run("can watch existing dir", func(t *testing.T) {
		t.Parallel()
		// Arrange
		expectedLogsConsumer := new(consumertest.LogsSink)
		// We want to only listen to the outer path, but add files to a dir within
		logs, actualLogsConsumer, cfg, root_dir := beforeEach(t, true)
		wd := strings.Replace((cfg.Include[0]), "/...", "", -1)
		wd_inner := fmt.Sprintf("%v/inner", wd)
		// Act
		TEST_FILES := 1
		createFiles := make([]string, TEST_FILES)
		for tc := range TEST_FILES {
			time.Sleep(300 * time.Millisecond)
			createFiles[tc] = fmt.Sprintf("%v/%v.txt", wd_inner, gofakeit.LetterN(5))

			consumeLogs(t, expectedLogsConsumer, Create(createFiles[tc], true))
			consumeLogs(t, expectedLogsConsumer, Write(createFiles[tc], true))
			consumeLogs(t, expectedLogsConsumer, Remove(createFiles[tc], true))

			// Assert
			time.Sleep(300 * time.Millisecond)
			require.Equal(t, logsToMap(t, expectedLogsConsumer.AllLogs(), "expected"), logsToMap(t, actualLogsConsumer.AllLogs(), "actual"))
			eventuallyExpect(t, expectedLogsConsumer.LogRecordCount(), actualLogsConsumer.LogRecordCount())

		}
		require.NoError(t, logs.Shutdown(context.Background()))
		testTeardown(t, root_dir)
	})

	t.Run("can watch to nested dir", func(t *testing.T) {
		t.Parallel()
		// Arrange
		expectedLogsConsumer := new(consumertest.LogsSink)
		// We want to only listen to the outer path, but add files to a dir within
		logs, actualLogsConsumer, cfg, root_dir := beforeEach(t, true)
		wd := strings.Replace((cfg.Include[0]), "/...", "", -1)
		wd_inner := fmt.Sprintf("%v/inner", wd)
		// Act
		TEST_FILES := 1
		createFiles := make([]string, TEST_FILES)
		for tc := range TEST_FILES {

			innerDir := fmt.Sprintf("%v/%v", wd_inner, gofakeit.LetterN(5))
			consumeLogs(t, expectedLogsConsumer, CreateDir(innerDir, true))

			createFiles[tc] = fmt.Sprintf("%v/%v.txt", wd_inner, gofakeit.LetterN(5))
			consumeLogs(t, expectedLogsConsumer, Create(createFiles[tc], true))
			consumeLogs(t, expectedLogsConsumer, Write(createFiles[tc], true))
			consumeLogs(t, expectedLogsConsumer, Remove(createFiles[tc], true))
			consumeLogs(t, expectedLogsConsumer, Remove(innerDir, true))

			// Assert
			time.Sleep(300 * time.Millisecond)

			expected := logsToMap(t, expectedLogsConsumer.AllLogs(), "expected")
			actual := logsToMap(t, actualLogsConsumer.AllLogs(), "actual")
			eventuallyExpect(t, expectedLogsConsumer.LogRecordCount(), actualLogsConsumer.LogRecordCount())
			require.Equal(t, expected, actual)

		}
		require.NoError(t, logs.Shutdown(context.Background()))
		testTeardown(t, root_dir)
	})

	t.Run("renamed file is removed", func(t *testing.T) {
		t.Parallel()
		// Arrange
		expectedLogsConsumer := new(consumertest.LogsSink)
		logs, actualLogsConsumer, cfg, root_dir := beforeEach(t, false)
		wd := strings.Replace((cfg.Include[0]), "/...", "", -1)
		// Act
		createFiles := make([]string, TEST_RUNS)
		for tc := range TEST_RUNS {
			createFiles[tc] = fmt.Sprintf("%v/%v.txt", wd, gofakeit.LetterN(5))
			consumeLogs(t, expectedLogsConsumer, Create(createFiles[tc], true))
			consumeLogs(t, expectedLogsConsumer, WriteOnClose(createFiles[tc], true))

			newName := fmt.Sprintf("%v/%v.txt", wd, gofakeit.LetterN(5))
			consumeLogs(t, expectedLogsConsumer, Rename(createFiles[tc], newName, true))
			consumeLogs(t, expectedLogsConsumer, RenameRemove(newName, true))
			// Assert
			time.Sleep(300 * time.Millisecond)

			expected := logsToMap(t, expectedLogsConsumer.AllLogs(), "expected")
			actual := logsToMap(t, actualLogsConsumer.AllLogs(), "actual")
			eventuallyExpect(t, expectedLogsConsumer.LogRecordCount(), actualLogsConsumer.LogRecordCount())
			require.Equal(t, expected, actual)
		}
		require.NoError(t, logs.Shutdown(context.Background()))
		testTeardown(t, root_dir)
	})

	t.Run(fmt.Sprintf("renaming a file %v times", TEST_RUNS), func(t *testing.T) {
		t.Parallel()
		// Arrange
		expectedLogsConsumer := new(consumertest.LogsSink)
		logs, actualLogsConsumer, cfg, root_dir := beforeEach(t, false)
		wd := strings.Replace((cfg.Include[0]), "/...", "", -1)

		// Act
		orignalName := fmt.Sprintf("%v/%v.txt", wd, gofakeit.LetterN(5))
		oldName := orignalName
		consumeLogs(t, expectedLogsConsumer, Create(oldName, true))
		consumeLogs(t, expectedLogsConsumer, WriteOnClose(oldName, true))

		for range TEST_RUNS {
			newName := fmt.Sprintf("%v/%v.txt", wd, gofakeit.LetterN(5))
			consumeLogs(t, expectedLogsConsumer, Rename(oldName, newName, true))
			oldName = newName
		}
		consumeLogs(t, expectedLogsConsumer, Rename(oldName, orignalName, true))
		consumeLogs(t, expectedLogsConsumer, RenameRemove(orignalName, true))
		// Assert
		time.Sleep(300 * time.Millisecond)

		expected := logsToMap(t, expectedLogsConsumer.AllLogs(), "expected")
		actual := logsToMap(t, actualLogsConsumer.AllLogs(), "actual")
		eventuallyExpect(t, expectedLogsConsumer.LogRecordCount(), actualLogsConsumer.LogRecordCount())
		require.Equal(t, expected, actual)
		require.NoError(t, logs.Shutdown(context.Background()))
		testTeardown(t, root_dir)
	})
}

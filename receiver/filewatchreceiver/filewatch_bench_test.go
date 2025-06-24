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

func leftIntersect(left, right map[string]uint) (float64, float64) {
	// If there are no elements to the left we cannot check the ratio
	if len(left) == 0 {
		// If both are empty then they are the same (100%)
		if len(right) == 0 {
			return 0, 1
		}
		// If the left is empty but the right is not, then we are completely off (0%)
		return 0, 0
	}
	ret := 0.0
	total := 0.0
	for kl, vl := range left {
		if right[kl] == vl {
			ret++
		}
		total++
	}
	return ret, ret / total
}

func BenchmarkFilewatcherReceiver(b *testing.B) {
	writes := gofakeit.IntN(10)
	b.Run("includes", func(b *testing.B) {
		// Arrange
		expectedLogsConsumer := new(consumertest.LogsSink)
		logs, actualLogsConsumer, cfg, root_dir := beforeEach(b, false)
		wd := strings.Replace(strings.Replace((cfg.Include[0]), "/...", "", -1), "/*", "", -1)

		// Act
		createFiles := make([]string, 0)
		for b.Loop() {
			name := fmt.Sprintf("%v/%v.txt", wd, gofakeit.LetterN(5))
			consumeLogs(b, expectedLogsConsumer, Create(name, false))
			time.Sleep(15 * time.Millisecond)
			for range writes {
				consumeLogs(b, expectedLogsConsumer, Write(name, false))
				time.Sleep(15 * time.Millisecond)
			}
			consumeLogs(b, expectedLogsConsumer, Remove(name, false))
			time.Sleep(15 * time.Millisecond)
			// Assert
			createFiles = append(createFiles, name)
		}

		b.ReportMetric(float64(logs.(*FileWatcher).Benchmark().total_duration)/float64(logs.(*FileWatcher).Benchmark().events_recorded), "µs/event")
		b.ReportMetric(float64(logs.(*FileWatcher).Benchmark().events_recorded)/float64(b.N), "µs/loop")
		expected := logsToMap(b, expectedLogsConsumer.AllLogs(), "expected")
		actual := logsToMap(b, actualLogsConsumer.AllLogs(), "actual")
		_, ratio := leftIntersect(expected, actual)
		b.ReportMetric(ratio, "accuracy")
		// This extracts the difference between observedTimestamp and timestamp, this gives us a view into how long time it takes from receiving an event to that it is actually sent off.
		average, q95 := stats(logsObsTimestampDiffTimstamp(actualLogsConsumer.AllLogs()))
		b.ReportMetric(float64(average), "Q50∆s")
		b.ReportMetric(float64(q95), "Q95∆s")
		require.NoError(b, logs.Shutdown(context.Background()))
		testTeardown(b, root_dir)
	})

	b.Run("excludes", func(b *testing.B) {
		// Arrange
		expectedLogsConsumer := new(consumertest.LogsSink)
		logs, actualLogsConsumer, cfg, root_dir := beforeEach(b, false)
		wd := strings.Replace(strings.Replace((cfg.Include[0]), "/...", "", -1), "/*", "", -1)
		// Act
		createFiles := make([]string, 0)

		for b.Loop() {
			name := fmt.Sprintf("%v/%v.skip", wd, gofakeit.LetterN(5))
			Create(name, false)
			time.Sleep(15 * time.Millisecond)
			for range writes {
				Write(name, false)
				time.Sleep(15 * time.Millisecond)
			}
			Remove(name, false)
			time.Sleep(15 * time.Millisecond)
			// Assert
			createFiles = append(createFiles, name)
		}
		expected := logsToMap(b, expectedLogsConsumer.AllLogs(), "expected")
		actual := logsToMap(b, actualLogsConsumer.AllLogs(), "actual")
		_, ratio := leftIntersect(expected, actual)
		b.ReportMetric(ratio, "accuracy")
		require.NoError(b, logs.Shutdown(context.Background()))
		testTeardown(b, root_dir)
	})

	b.Run("accuracy instant", func(b *testing.B) {
		// Arrange
		expectedLogsConsumer := new(consumertest.LogsSink)
		logs, actualLogsConsumer, cfg, root_dir := beforeEach(b, false)
		wd := strings.Replace((cfg.Include[0]), "/...", "", -1)
		// Act
		createFiles := make([]string, 0)
		writes := gofakeit.IntN(10) + 1
		time.Sleep(1 * time.Second)
		for b.Loop() {
			for range 100 {
				name := fmt.Sprintf("%v/%v.txt", wd, gofakeit.LetterN(5))
				consumeLogs(b, expectedLogsConsumer, Create(name, false))
				for range writes {
					consumeLogs(b, expectedLogsConsumer, Write(name, false))
				}
				consumeLogs(b, expectedLogsConsumer, Remove(name, false))
				// Assert
				createFiles = append(createFiles, name)
			}
			expected := logsToMap(b, expectedLogsConsumer.AllLogs(), "expected")
			actual := logsToMap(b, actualLogsConsumer.AllLogs(), "actual")
			_, ratio := leftIntersect(expected, actual)
			b.ReportMetric(ratio, "accuracy")
		}
		require.NoError(b, logs.Shutdown(context.Background()))
		testTeardown(b, root_dir)
	})

	TEST_SLEEP := time.Duration(15)
	b.Run(fmt.Sprintf("accuracy sleep-%v", TEST_SLEEP), func(b *testing.B) {
		// Arrange
		expectedLogsConsumer := new(consumertest.LogsSink)
		logs, actualLogsConsumer, cfg, root_dir := beforeEach(b, false)
		wd := strings.Replace((cfg.Include[0]), "/...", "", -1)
		// Act
		createFiles := make([]string, 0)
		writes := gofakeit.IntN(10) + 1
		time.Sleep(1 * time.Second)
		for b.Loop() {
			for range 100 {
				name := fmt.Sprintf("%v/%v.txt", wd, gofakeit.LetterN(5))
				consumeLogs(b, expectedLogsConsumer, Create(name, false))
				time.Sleep(11 * time.Millisecond)
				for range writes {
					consumeLogs(b, expectedLogsConsumer, Write(name, false))
					time.Sleep(11 * time.Millisecond)
				}
				consumeLogs(b, expectedLogsConsumer, Remove(name, false))
				time.Sleep(11 * time.Millisecond)
				// Assert
				createFiles = append(createFiles, name)
			}
			expected := logsToMap(b, expectedLogsConsumer.AllLogs(), "expected")
			actual := logsToMap(b, actualLogsConsumer.AllLogs(), "actual")
			_, ratio := leftIntersect(expected, actual)
			b.ReportMetric(ratio, "accuracy")
		}
		require.NoError(b, logs.Shutdown(context.Background()))
		testTeardown(b, root_dir)
	})
}

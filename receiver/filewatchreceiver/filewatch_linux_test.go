//go:build linux

package filewatchreceiver

import (
	"log"
	"os"
	"time"

	"github.com/brianvoe/gofakeit/v7"
	"github.com/olandr/notify"
	"go.opentelemetry.io/collector/pdata/plog"
)

var (
	EVENTS_TO_WATCH = []string{"notify.InAccess", "notify.InOpen", "notify.InCreate", "notify.InDelete", "notify.InCloseWrite"}
	SLEEP_TIMEOUT   = 300
)

// Opeartions to test
func createDir(name string) {
	time.Sleep(time.Duration(SLEEP_TIMEOUT) * time.Millisecond)
	err := os.Mkdir(name, 0o777)
	if err != nil {
		log.Fatal(err)
	}
}

func create(name string) *os.File {
	time.Sleep(time.Duration(SLEEP_TIMEOUT) * time.Millisecond)
	f, err := os.OpenFile(name, os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		log.Fatal(err)
	}
	return f
}

func remove(name string) {
	time.Sleep(time.Duration(SLEEP_TIMEOUT) * time.Millisecond)
	err := os.Remove(name)
	if err != nil {
		log.Fatal(err)
	}
}

func write(name string) *os.File {
	time.Sleep(time.Duration(SLEEP_TIMEOUT) * time.Millisecond)
	f, err := os.OpenFile(name, os.O_WRONLY, 0o644)
	_, err = f.Write([]byte(gofakeit.LetterN(10)))
	if err != nil {
		log.Fatal(err)
	}
	return f
}

func rename(from, to string) {
	time.Sleep(time.Duration(SLEEP_TIMEOUT) * time.Millisecond)
	err := os.Rename(from, to)
	if err != nil {
		log.Fatal(err)
	}
}

func Create(name string) []plog.Logs {
	defer create(name).Close()
	return []plog.Logs{createLogs(time.Now(), name, notify.InCreate.String())}
}

func CreateDir(name string) []plog.Logs {
	createDir(name)
	return []plog.Logs{createLogs(time.Now(), name, notify.Create.String())}
}

func Remove(name string) []plog.Logs {
	remove(name)
	return []plog.Logs{createLogs(time.Now(), name, notify.InDelete.String())}
}

func RenameRemove(name string) []plog.Logs {
	return Remove(name)
}

func Write(name string) []plog.Logs {
	defer write(name).Close()
	return []plog.Logs{createLogs(time.Now(), name, notify.InCloseWrite.String())}
}

func WriteOnClose(name string) []plog.Logs {
	return []plog.Logs{createLogs(time.Now(), name, notify.InCloseWrite.String())}
}

func Rename(from, to string) []plog.Logs {
	rename(from, to)
	return []plog.Logs{createLogs(time.Now(), from, notify.InMovedFrom.String()), createLogs(time.Now(), to, notify.InMovedTo.String())}
}

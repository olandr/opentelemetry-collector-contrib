//go:build darwin && !kqueue && cgo && !ios
// +build darwin,!kqueue,cgo,!ios

package filewatchreceiver

import (
	"log"
	"os"
	"time"

	"github.com/brianvoe/gofakeit/v7"
	"github.com/olandr/notify"
	"go.opentelemetry.io/collector/pdata/plog"
)

// Opeartions to test
func createDir(name string, should_sleep bool) {
	if should_sleep {
		time.Sleep(15 * time.Millisecond)
	}
	err := os.Mkdir(name, 0o777)
	if err != nil {
		log.Fatal(err)
	}
}

func create(name string, should_sleep bool) *os.File {
	if should_sleep {
		time.Sleep(15 * time.Millisecond)
	}
	f, err := os.OpenFile(name, os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		log.Fatal(err)
	}
	return f
}

func remove(name string, should_sleep bool) {
	if should_sleep {
		time.Sleep(15 * time.Millisecond)
	}
	err := os.Remove(name)
	if err != nil {
		log.Fatal(err)
	}
}

func write(name string, should_sleep bool) *os.File {
	if should_sleep {
		time.Sleep(15 * time.Millisecond)
	}
	f, err := os.OpenFile(name, os.O_WRONLY, 0o644)
	_, err = f.Write([]byte(gofakeit.LetterN(10)))
	if err != nil {
		log.Fatal(err)
	}
	return f
}

func rename(from, to string, should_sleep bool) {
	if should_sleep {
		time.Sleep(15 * time.Millisecond)
	}
	err := os.Rename(from, to)
	if err != nil {
		log.Fatal(err)
	}
}

func Create(name string, should_sleep bool) []plog.Logs {
	defer create(name, should_sleep).Close()
	return []plog.Logs{createLogs(name, notify.Create.String())}
}

func CreateDir(name string, should_sleep bool) []plog.Logs {
	createDir(name, should_sleep)
	return []plog.Logs{createLogs(name, notify.Create.String())}
}

func Remove(name string, should_sleep bool) []plog.Logs {
	remove(name, should_sleep)
	return []plog.Logs{createLogs(name, notify.Remove.String())}
}

func RenameRemove(name string, should_sleep bool) []plog.Logs {
	remove(name, should_sleep)
	return []plog.Logs{createLogs(name, notify.Rename.String()), createLogs(name, notify.Remove.String())}
}

func Write(name string, should_sleep bool) []plog.Logs {
	defer write(name, should_sleep).Close()
	return []plog.Logs{createLogs(name, notify.Write.String())}
}

func WriteOnClose(name string, should_sleep bool) []plog.Logs {
	return nil
}

func Rename(from, to string, should_sleep bool) []plog.Logs {
	rename(from, to, should_sleep)
	return []plog.Logs{createLogs(from, notify.Rename.String()), createLogs(to, notify.Rename.String())}
}

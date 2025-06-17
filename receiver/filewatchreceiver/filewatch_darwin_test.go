//go:build darwin && !kqueue && cgo && !ios
// +build darwin,!kqueue,cgo,!ios

package filewatchreceiver

import (
	"log"
	"os"
	"time"

	"github.com/brianvoe/gofakeit/v7"
	"github.com/syncthing/notify"
	"go.opentelemetry.io/collector/pdata/plog"
)

// Opeartions to test
func createDir(name string) {
	time.Sleep(300 * time.Millisecond)
	err := os.Mkdir(name, 0o777)
	if err != nil {
		log.Fatal(err)
	}
}

func create(name string) *os.File {
	time.Sleep(300 * time.Millisecond)
	f, err := os.OpenFile(name, os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		log.Fatal(err)
	}
	return f
}

func remove(name string) {
	time.Sleep(300 * time.Millisecond)
	err := os.Remove(name)
	if err != nil {
		log.Fatal(err)
	}
}

func write(name string) *os.File {
	time.Sleep(300 * time.Millisecond)
	f, err := os.OpenFile(name, os.O_WRONLY, 0o644)
	_, err = f.Write([]byte(gofakeit.LetterN(10)))
	if err != nil {
		log.Fatal(err)
	}
	return f
}

func rename(from, to string) {
	time.Sleep(300 * time.Millisecond)
	err := os.Rename(from, to)
	if err != nil {
		log.Fatal(err)
	}
}

func Create(name string) []plog.Logs {
	defer create(name).Close()
	return []plog.Logs{createLogs(name, notify.Create.String())}
}

func CreateDir(name string) []plog.Logs {
	createDir(name)
	return []plog.Logs{createLogs(name, notify.Create.String())}
}

func Remove(name string) []plog.Logs {
	remove(name)
	return []plog.Logs{createLogs(name, notify.Remove.String())}
}

func RenameRemove(name string) []plog.Logs {
	remove(name)
	return []plog.Logs{createLogs(name, notify.Rename.String()), createLogs(name, notify.Remove.String())}
}

func Write(name string) []plog.Logs {
	defer write(name).Close()
	return []plog.Logs{createLogs(name, notify.Write.String())}
}

func WriteOnClose(name string) []plog.Logs {
	return nil
}

func Rename(from, to string) []plog.Logs {
	rename(from, to)
	return []plog.Logs{createLogs(from, notify.Rename.String()), createLogs(to, notify.Rename.String())}
}

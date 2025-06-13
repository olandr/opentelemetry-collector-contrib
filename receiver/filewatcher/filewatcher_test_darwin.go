//go:build darwin && !kqueue && cgo && !ios
// +build darwin,!kqueue,cgo,!ios

package filewatcher

import (
	"github.com/syncthing/notify"
	"go.opentelemetry.io/collector/pdata/plog"
)

func Create(name string) []plog.Logs {
	return []plog.Logs{createLogs(name, notify.Create.String())}
}
func CreateDir(name string) []plog.Logs {
	return Create(name)
}
func Remove(name string) []plog.Logs {
	return []plog.Logs{createLogs(name, notify.Remove.String())}
}
func RenameRemove(name string) []plog.Logs {
	return []plog.Logs{createLogs(name, notify.Rename.String()), createLogs(name, notify.Remove.String())}
}
func Write(name string) []plog.Logs {
	return []plog.Logs{createLogs(name, notify.Write.String())}
}

func WriteOnClose(name string) []plog.Logs {
	return nil
}

func Rename(from, to string) []plog.Logs {
	return []plog.Logs{createLogs(from, notify.Rename.String()), createLogs(to, notify.Rename.String())}
}

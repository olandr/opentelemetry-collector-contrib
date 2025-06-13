//go:build linux
// +build linux

package filewatcher

import (
	"github.com/syncthing/notify"
	"go.opentelemetry.io/collector/pdata/plog"
)

//	func Create(name string) []plog.Logs {
//		return []plog.Logs{createLogs(name, notify.InCreate.String()), createLogs(name, notify.InCloseWrite.String())}
//	}
func Create(name string) []plog.Logs {
	return []plog.Logs{createLogs(name, notify.InCreate.String())}
}
func Remove(name string) []plog.Logs {
	return []plog.Logs{createLogs(name, notify.InDelete.String())}
}
func RenameRemove(name string) []plog.Logs {
	return Remove(name)
}
func Write(name string) []plog.Logs {
	return []plog.Logs{createLogs(name, notify.InCloseWrite.String())}
}

func WriteOnClose(name string) []plog.Logs {
	return Write(name)
}

func Rename(from, to string) []plog.Logs {
	return []plog.Logs{createLogs(from, notify.InMovedFrom.String()), createLogs(to, notify.InMovedTo.String())}
}

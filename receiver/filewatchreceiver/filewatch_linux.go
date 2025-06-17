//go:build linux
// +build linux

package filewatchreceiver

import (
	"github.com/syncthing/notify"
)

var EVENTS_TO_WATCH = notify.InCreate | notify.InDelete | notify.InCloseWrite | notify.InMovedTo | notify.InMovedFrom

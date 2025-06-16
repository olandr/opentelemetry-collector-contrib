//go:build darwin && !kqueue && cgo && !ios
// +build darwin,!kqueue,cgo,!ios

package filewatcher

import (
	"github.com/syncthing/notify"
)

var (
	EVENTS_TO_WATCH = notify.Create | notify.Remove | notify.Rename | notify.Write
)

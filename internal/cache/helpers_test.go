package cache

import (
	"testing"
	"time"
	"unsafe"
)

func addr(b []byte) uintptr { return uintptr(unsafe.Pointer(&b[0])) } //nolint:gosec // alignment arithmetic only

func deadlineWait(t *testing.T, ok func() bool) {
	t.Helper()
	end := time.Now().Add(5 * time.Second)
	for time.Now().Before(end) {
		if ok() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("condition not reached in 5s")
}

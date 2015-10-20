package mgo

import (
	"time"
)

func HackPingDelay(newDelay time.Duration) (restore func()) {
	globalMutex.Lock()
	defer globalMutex.Unlock()

	oldDelay := pingDelay
	restore = func() {
		globalMutex.Lock()
		pingDelay = oldDelay
		globalMutex.Unlock()
	}
	pingDelay = newDelay
	return
}

func HackSyncSocketTimeout(newTimeout time.Duration) (restore func()) {
	globalMutex.Lock()
	defer globalMutex.Unlock()

	oldTimeout := syncSocketTimeout
	restore = func() {
		globalMutex.Lock()
		syncSocketTimeout = oldTimeout
		globalMutex.Unlock()
	}
	syncSocketTimeout = newTimeout
	return
}

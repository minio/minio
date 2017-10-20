// Package modules contains definitions for all of the major modules of Sia, as
// well as some helper functions for performing actions that are common to
// multiple modules.
package modules

import (
	"time"

	"github.com/NebulousLabs/Sia/build"
)

var (
	// SafeMutexDelay is the recommended timeout for the deadlock detecting
	// mutex. This value is DEPRECATED, as safe mutexes are no longer
	// recommended. Instead, the locking conventions should be followed and a
	// traditional mutex or a demote mutex should be used.
	SafeMutexDelay time.Duration
)

func init() {
	if build.Release == "dev" {
		SafeMutexDelay = 60 * time.Second
	} else if build.Release == "standard" {
		SafeMutexDelay = 90 * time.Second
	} else if build.Release == "testing" {
		SafeMutexDelay = 30 * time.Second
	}
}

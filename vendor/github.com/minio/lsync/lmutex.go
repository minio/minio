/*
 * Minio Cloud Storage, (C) 2017 Minio, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package lsync

import (
	"sync/atomic"
	"time"
)

// A LMutex is a mutual exclusion lock with timeouts.
type LMutex struct {
	state int64
}

// NewLMutex - initializes a new lsync mutex.
func NewLMutex() *LMutex {
	return &LMutex{}
}

// Lock holds a lock on lm.
//
// If the lock is already in use, the calling go routine
// blocks until the mutex is available.
func (lm *LMutex) Lock() {
	lm.lockLoop(time.Duration(1<<63 - 1))
}

// GetLock tries to get a write lock on lm before the timeout occurs.
func (lm *LMutex) GetLock(timeout time.Duration) (locked bool) {
	return lm.lockLoop(timeout)
}

// lockLoop will acquire either a read or a write lock
//
// The call will block until the lock is granted using a built-in
// timing randomized back-off algorithm to try again until successful
func (lm *LMutex) lockLoop(timeout time.Duration) bool {
	doneCh, start := make(chan struct{}), time.Now().UTC()
	defer close(doneCh)

	// We timed out on the previous lock, incrementally wait
	// for a longer back-off time and try again afterwards.
	for range newRetryTimerSimple(doneCh) {

		// Try to acquire the lock.
		if atomic.CompareAndSwapInt64(&lm.state, NOLOCKS, WRITELOCK) {
			return true
		} else if time.Now().UTC().Sub(start) >= timeout { // Are we past the timeout?
			break
		}
		// We timed out on the previous lock, incrementally wait
		// for a longer back-off time and try again afterwards.
	}
	return false
}

// Unlock unlocks the lock.
//
// It is a run-time error if lm is not locked on entry to Unlock.
func (lm *LMutex) Unlock() {
	if !atomic.CompareAndSwapInt64(&lm.state, WRITELOCK, NOLOCKS) {
		panic("Trying to Unlock() while no Lock() is active")
	}
}

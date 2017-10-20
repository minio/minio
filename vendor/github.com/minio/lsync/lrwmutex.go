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
	"sync"
	"time"
)

const (
     WRITELOCK = -1 + iota
     NOLOCKS
     READLOCKS
)

// A LRWMutex is a mutual exclusion lock with timeouts.
type LRWMutex struct {
	state int64
	m     sync.Mutex // Mutex to prevent multiple simultaneous locks
}

// NewLRWMutex - initializes a new lsync RW mutex.
func NewLRWMutex() *LRWMutex {
	return &LRWMutex{}
}

// Lock holds a write lock on lm.
//
// If the lock is already in use, the calling go routine
// blocks until the mutex is available.
func (lm *LRWMutex) Lock() {

	isWriteLock := true
	lm.lockLoop(time.Duration(1<<63-1), isWriteLock)
}

// GetLock tries to get a write lock on lm before the timeout occurs.
func (lm *LRWMutex) GetLock(timeout time.Duration) (locked bool) {

	isWriteLock := true
	return lm.lockLoop(timeout, isWriteLock)
}

// RLock holds a read lock on lm.
//
// If one or more read lock are already in use, it will grant another lock.
// Otherwise the calling go routine blocks until the mutex is available.
func (lm *LRWMutex) RLock() {

	isWriteLock := false
	lm.lockLoop(time.Duration(1<<63-1), isWriteLock)
}

// GetRLock tries to get a read lock on lm before the timeout occurs.
func (lm *LRWMutex) GetRLock(timeout time.Duration) (locked bool) {

	isWriteLock := false
	return lm.lockLoop(timeout, isWriteLock)
}

// lockLoop will acquire either a read or a write lock
//
// The call will block until the lock is granted using a built-in
// timing randomized back-off algorithm to try again until successful
func (lm *LRWMutex) lockLoop(timeout time.Duration, isWriteLock bool) bool {
	doneCh, start := make(chan struct{}), time.Now().UTC()
	defer close(doneCh)

	// We timed out on the previous lock, incrementally wait
	// for a longer back-off time and try again afterwards.
	for range newRetryTimerSimple(doneCh) {

		// Try to acquire the lock.
		var success bool
		{
			lm.m.Lock()

			if isWriteLock {
				if lm.state == NOLOCKS {
					lm.state = WRITELOCK
					success = true
				}
			} else {
				if lm.state != WRITELOCK {
					lm.state += 1
					success = true
				}
			}

			lm.m.Unlock()
		}
		if success {
			return true
		}
		if time.Now().UTC().Sub(start) >= timeout { // Are we past the timeout?
			break
		}
		// We timed out on the previous lock, incrementally wait
		// for a longer back-off time and try again afterwards.
	}
	return false
}

// Unlock unlocks the write lock.
//
// It is a run-time error if lm is not locked on entry to Unlock.
func (lm *LRWMutex) Unlock() {

	isWriteLock := true
	success := lm.unlock(isWriteLock)
	if !success {
		panic("Trying to Unlock() while no Lock() is active")
	}
}

// RUnlock releases a read lock held on lm.
//
// It is a run-time error if lm is not locked on entry to RUnlock.
func (lm *LRWMutex) RUnlock() {

	isWriteLock := false
	success := lm.unlock(isWriteLock)
	if !success {
		panic("Trying to RUnlock() while no RLock() is active")
	}
}

func (lm *LRWMutex) unlock(isWriteLock bool) (unlocked bool) {
	lm.m.Lock()

	// Try to release lock.
	if isWriteLock {
		if lm.state == WRITELOCK {
			lm.state = NOLOCKS
			unlocked = true
		}
	} else {
		if lm.state == WRITELOCK || lm.state == NOLOCKS {
			unlocked = false // unlocked called without any active read locks
		} else {
			lm.state -= 1
			unlocked = true
		}
	}

	lm.m.Unlock()
	return unlocked
}

// ForceUnlock will forcefully clear a write or read lock.
func (lm *LRWMutex) ForceUnlock() {
	lm.m.Lock()
	lm.state = NOLOCKS
	lm.m.Unlock()
}

// DRLocker returns a sync.Locker interface that implements
// the Lock and Unlock methods by calling drw.RLock and drw.RUnlock.
func (dm *LRWMutex) DRLocker() sync.Locker {
	return (*drlocker)(dm)
}

type drlocker LRWMutex

func (dr *drlocker) Lock()   { (*LRWMutex)(dr).RLock() }
func (dr *drlocker) Unlock() { (*LRWMutex)(dr).RUnlock() }

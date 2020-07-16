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
	"context"
	"math"
	"sync"
	"time"

	"github.com/minio/minio/pkg/retry"
)

// A LRWMutex is a mutual exclusion lock with timeouts.
type LRWMutex struct {
	id          string
	source      string
	isWriteLock bool
	ref         int
	m           sync.Mutex // Mutex to prevent multiple simultaneous locks
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

	const isWriteLock = true
	lm.lockLoop(context.Background(), lm.id, lm.source, time.Duration(math.MaxInt64), isWriteLock)
}

// GetLock tries to get a write lock on lm before the timeout occurs.
func (lm *LRWMutex) GetLock(ctx context.Context, id string, source string, timeout time.Duration) (locked bool) {

	const isWriteLock = true
	return lm.lockLoop(ctx, id, source, timeout, isWriteLock)
}

// RLock holds a read lock on lm.
//
// If one or more read lock are already in use, it will grant another lock.
// Otherwise the calling go routine blocks until the mutex is available.
func (lm *LRWMutex) RLock() {

	const isWriteLock = false
	lm.lockLoop(context.Background(), lm.id, lm.source, time.Duration(1<<63-1), isWriteLock)
}

// GetRLock tries to get a read lock on lm before the timeout occurs.
func (lm *LRWMutex) GetRLock(ctx context.Context, id string, source string, timeout time.Duration) (locked bool) {

	const isWriteLock = false
	return lm.lockLoop(ctx, id, source, timeout, isWriteLock)
}

func (lm *LRWMutex) lock(id, source string, isWriteLock bool) (locked bool) {
	lm.m.Lock()
	lm.id = id
	lm.source = source
	if isWriteLock {
		if lm.ref == 0 && !lm.isWriteLock {
			lm.ref = 1
			lm.isWriteLock = true
			locked = true
		}
	} else {
		if !lm.isWriteLock {
			lm.ref++
			locked = true
		}
	}
	lm.m.Unlock()

	return locked
}

// lockLoop will acquire either a read or a write lock
//
// The call will block until the lock is granted using a built-in
// timing randomized back-off algorithm to try again until successful
func (lm *LRWMutex) lockLoop(ctx context.Context, id, source string, timeout time.Duration, isWriteLock bool) (locked bool) {
	retryCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	// We timed out on the previous lock, incrementally wait
	// for a longer back-off time and try again afterwards.
	for range retry.NewTimer(retryCtx) {
		if lm.lock(id, source, isWriteLock) {
			return true
		}
	}

	// We timed out on the previous lock, incrementally wait
	// for a longer back-off time and try again afterwards.
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
		if lm.isWriteLock && lm.ref == 1 {
			lm.ref = 0
			lm.isWriteLock = false
			unlocked = true
		}
	} else {
		if !lm.isWriteLock {
			if lm.ref > 0 {
				lm.ref--
				unlocked = true
			}
		}
	}

	lm.m.Unlock()
	return unlocked
}

// ForceUnlock will forcefully clear a write or read lock.
func (lm *LRWMutex) ForceUnlock() {
	lm.m.Lock()
	lm.ref = 0
	lm.isWriteLock = false
	lm.m.Unlock()
}

// DRLocker returns a sync.Locker interface that implements
// the Lock and Unlock methods by calling drw.RLock and drw.RUnlock.
func (lm *LRWMutex) DRLocker() sync.Locker {
	return (*drlocker)(lm)
}

type drlocker LRWMutex

func (dr *drlocker) Lock()   { (*LRWMutex)(dr).RLock() }
func (dr *drlocker) Unlock() { (*LRWMutex)(dr).RUnlock() }

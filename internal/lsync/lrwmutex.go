// Copyright (c) 2015-2021 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package lsync

import (
	"context"
	"math"
	"math/rand"
	"sync"
	"time"
)

// A LRWMutex is a mutual exclusion lock with timeouts.
type LRWMutex struct {
	id          string
	source      string
	isWriteLock bool
	ref         int
	mu          sync.Mutex // Mutex to prevent multiple simultaneous locks
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
	lm.lockLoop(context.Background(), lm.id, lm.source, math.MaxInt64, isWriteLock)
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
	lm.lockLoop(context.Background(), lm.id, lm.source, 1<<63-1, isWriteLock)
}

// GetRLock tries to get a read lock on lm before the timeout occurs.
func (lm *LRWMutex) GetRLock(ctx context.Context, id string, source string, timeout time.Duration) (locked bool) {

	const isWriteLock = false
	return lm.lockLoop(ctx, id, source, timeout, isWriteLock)
}

func (lm *LRWMutex) lock(id, source string, isWriteLock bool) (locked bool) {
	lm.mu.Lock()
	defer lm.mu.Unlock()

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

	return locked
}

const (
	lockRetryInterval = 50 * time.Millisecond
)

// lockLoop will acquire either a read or a write lock
//
// The call will block until the lock is granted using a built-in
// timing randomized back-off algorithm to try again until successful
func (lm *LRWMutex) lockLoop(ctx context.Context, id, source string, timeout time.Duration, isWriteLock bool) (locked bool) {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	retryCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	for {
		select {
		case <-retryCtx.Done():
			// Caller context canceled or we timedout,
			// return false anyways for both situations.
			return false
		default:
			if lm.lock(id, source, isWriteLock) {
				return true
			}
			time.Sleep(time.Duration(r.Float64() * float64(lockRetryInterval)))
		}
	}
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
	lm.mu.Lock()
	defer lm.mu.Unlock()

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

	return unlocked
}

// ForceUnlock will forcefully clear a write or read lock.
func (lm *LRWMutex) ForceUnlock() {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	lm.ref = 0
	lm.isWriteLock = false

}

// DRLocker returns a sync.Locker interface that implements
// the Lock and Unlock methods by calling drw.RLock and drw.RUnlock.
func (lm *LRWMutex) DRLocker() sync.Locker {
	return (*drlocker)(lm)
}

type drlocker LRWMutex

func (dr *drlocker) Lock()   { (*LRWMutex)(dr).RLock() }
func (dr *drlocker) Unlock() { (*LRWMutex)(dr).RUnlock() }

package sync

import (
	"runtime"
	"sync"
	"sync/atomic"
)

const (
	readOffset  = uint64(1)
	writeOffset = uint64(1 << 20)
	tryOffset   = uint64(1 << 40)

	tryMask = uint64(1099511627775) // equivalent to setting the first 39 bits to '1'.
)

// TryRWMutex allows you to try to grab a RWMutex, failing if the mutex is
// unavailable. Standard blocking RLock and Lock calls also available.
//
// Note that there will be inconsistencies if there are more than 1 << 20
// operations active at once.
type TryRWMutex struct {
	lock uint64
	mu   sync.RWMutex
}

// Lock blocks until the mutex is available, and then locks it.
func (tm *TryRWMutex) Lock() {
	// Signal that a write lock is waiting.
	v := atomic.AddUint64(&tm.lock, writeOffset)

	// Spin until there is no contention from a Try call.
	for v > tryOffset {
		runtime.Gosched()
		v = atomic.LoadUint64(&tm.lock)
	}

	// Grab the lock.
	tm.mu.Lock()
}

// RLock blocks until the mutex is available, then grabs a read lock.
func (tm *TryRWMutex) RLock() {
	// Signal that a read lock is waiting.
	v := atomic.AddUint64(&tm.lock, readOffset)

	// Spin until there is no contention from a Try call.
	for v > tryOffset {
		runtime.Gosched()
		v = atomic.LoadUint64(&tm.lock)
	}

	// Grab the lock.
	tm.mu.RLock()
}

// RUnlock releases a read lock on the mutex.
func (tm *TryRWMutex) RUnlock() {
	// Release the lock, then signal that the read lock is no longer waiting.
	tm.mu.RUnlock()
	atomic.AddUint64(&tm.lock, ^(readOffset - 1))
}

// TryLock grabs a lock on the mutex, returning false if the mutex is
// unavailable.
func (tm *TryRWMutex) TryLock() bool {
	// If there are no readlocks waiting, and no writelocks waiting, signal
	// that a writelock is waiting and that there contention from a Try call.
	if atomic.CompareAndSwapUint64(&tm.lock, 0, writeOffset+tryOffset) {
		tm.mu.Lock()
		// Signal that the Try call contention is resolved.
		atomic.AddUint64(&tm.lock, ^(tryOffset - 1))
		return true
	}
	return false
}

// TryRLock grabs a read lock on the mutex, returning false if the mutex is
// already locked.
func (tm *TryRWMutex) TryRLock() bool {
	// Signal that a read lock is waiting, and that there is contention from a
	// Try call.
	v := atomic.AddUint64(&tm.lock, readOffset+tryOffset)
	// Mask the try offset when performing the comparison.
	v = v & tryMask
	if v > writeOffset {
		// If there is a write lock waiting, revert the signal and return
		// false.
		atomic.AddUint64(&tm.lock, ^(readOffset + tryOffset - 1))
		return false
	}
	// Grab the read lock and return true.
	tm.mu.RLock()
	// Signal that the Try call contention is resolved.
	atomic.AddUint64(&tm.lock, ^(tryOffset - 1))
	return true
}

// Unlock releases a lock on the mutex.
func (tm *TryRWMutex) Unlock() {
	tm.mu.Unlock()
	atomic.AddUint64(&tm.lock, ^(writeOffset - 1))
}

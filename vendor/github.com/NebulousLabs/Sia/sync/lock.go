package sync

import (
	"fmt"
	"os"
	"runtime"
	"sync"
	"time"
)

// RWMutex provides locking functions, and an ability to detect and remove
// deadlocks.
type RWMutex struct {
	openLocks        map[int]lockInfo
	openLocksCounter int
	openLocksMutex   sync.Mutex

	callDepth   int
	maxLockTime time.Duration

	mu sync.RWMutex
}

// lockInfo contains information about when and how a lock call was made.
type lockInfo struct {
	// When the lock was called.
	lockTime time.Time

	// Whether it was a RLock or a Lock.
	read bool

	// Call stack of the caller.
	callingFiles []string
	callingLines []int
}

// New takes a maxLockTime and returns a lock. The lock will never stay locked
// for more than maxLockTime, instead printing an error and unlocking after
// maxLockTime has passed.
func New(maxLockTime time.Duration, callDepth int) *RWMutex {
	rwm := &RWMutex{
		openLocks:   make(map[int]lockInfo),
		maxLockTime: maxLockTime,
		callDepth:   callDepth,
	}

	go rwm.threadedDeadlockFinder()

	return rwm
}

// threadedDeadlockFinder occasionally freezes the mutexes and scans all open mutexes,
// reporting any that have exceeded their time limit.
func (rwm *RWMutex) threadedDeadlockFinder() {
	for {
		rwm.openLocksMutex.Lock()
		for id, info := range rwm.openLocks {
			// Check if the lock has been held for longer than 'maxLockTime'.
			if time.Now().Sub(info.lockTime) > rwm.maxLockTime {
				str := fmt.Sprintf("A lock was held for too long, id '%v'. Call stack:\n", id)
				for i := 0; i <= rwm.callDepth; i++ {
					str += fmt.Sprintf("\tFile: '%v:%v'\n", info.callingFiles[i], info.callingLines[i])
				}
				os.Stderr.WriteString(str)
				os.Stderr.Sync()

				// Undo the deadlock and delete the entry from the map.
				if info.read {
					rwm.mu.RUnlock()
				} else {
					rwm.mu.Unlock()
				}
				delete(rwm.openLocks, id)
			}
		}
		rwm.openLocksMutex.Unlock()

		time.Sleep(rwm.maxLockTime)
	}
}

// safeLock is the generic function for doing safe locking. If the read flag is
// set, then a readlock will be used, otherwise a lock will be used.
func (rwm *RWMutex) safeLock(read bool) int {
	// Get the call stack.
	var li lockInfo
	li.read = read
	li.callingFiles = make([]string, rwm.callDepth+1)
	li.callingLines = make([]int, rwm.callDepth+1)
	for i := 0; i <= rwm.callDepth; i++ {
		_, li.callingFiles[i], li.callingLines[i], _ = runtime.Caller(2 + i)
	}

	// Lock the mutex.
	if read {
		rwm.mu.RLock()
	} else {
		rwm.mu.Lock()
	}

	// Safely register that a lock has been triggered.
	rwm.openLocksMutex.Lock()
	li.lockTime = time.Now()
	id := rwm.openLocksCounter
	rwm.openLocks[id] = li
	rwm.openLocksCounter++
	rwm.openLocksMutex.Unlock()

	return id
}

// safeUnlock is the generic function for doing safe unlocking. If the lock had
// to be removed because a deadlock was detected, an error is printed.
func (rwm *RWMutex) safeUnlock(read bool, id int) {
	rwm.openLocksMutex.Lock()
	defer rwm.openLocksMutex.Unlock()

	// Check if a deadlock has been detected and fixed manually.
	_, exists := rwm.openLocks[id]
	if !exists {
		// Get the call stack.
		callingFiles := make([]string, rwm.callDepth+1)
		callingLines := make([]int, rwm.callDepth+1)
		for i := 0; i <= rwm.callDepth; i++ {
			_, callingFiles[i], callingLines[i], _ = runtime.Caller(2 + i)
		}

		fmt.Printf("A lock was held until deadlock, subsequent call to unlock failed. id '%v'. Call stack:\n", id)
		for i := 0; i <= rwm.callDepth; i++ {
			fmt.Printf("\tFile: '%v:%v'\n", callingFiles[i], callingLines[i])
		}
		return
	}

	// Remove the lock and delete the entry from the map.
	if read {
		rwm.mu.RUnlock()
	} else {
		rwm.mu.Unlock()
	}
	delete(rwm.openLocks, id)
}

// RLock will read lock the RWMutex. The return value must be used as input
// when calling RUnlock.
func (rwm *RWMutex) RLock() int {
	return rwm.safeLock(true)
}

// RUnlock will read unlock the RWMutex. The return value of calling RLock must
// be used as input.
func (rwm *RWMutex) RUnlock(id int) {
	rwm.safeUnlock(true, id)
}

// Lock will lock the RWMutex. The return value must be used as input when
// calling RUnlock.
func (rwm *RWMutex) Lock() int {
	return rwm.safeLock(false)
}

// Unlock will unlock the RWMutex. The return value of calling Lock must be
// used as input.
func (rwm *RWMutex) Unlock(id int) {
	rwm.safeUnlock(false, id)
}

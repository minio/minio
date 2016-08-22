/*
 * Minio Cloud Storage, (C) 2016 Minio, Inc.
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

package cmd

import (
	"errors"
	"fmt"
	"runtime"
	"sync"
)

// nsParam - carries name space resource.
type nsParam struct {
	volume string
	path   string
}

// nsLock - provides primitives for locking critical namespace regions.
type nsLock struct {
	sync.RWMutex
	ref uint
}

// nsLockMap - namespace lock map, provides primitives to Lock,
// Unlock, RLock and RUnlock.
type nsLockMap struct {
	// lock counter using for lock debugging.
	globalLockCounter  int64                                   //total locks held.
	blockedCounter     int64                                   // total operations blocked waiting for locks.
	runningLockCounter int64                                   // total locks held but not released yet.
	debugLockMap       map[nsParam]*debugLockInfoPerVolumePath // info for instrumentation on locks.
	lockMap            map[nsParam]*nsLock                     // RLock, WLock locks used for operations.
	mutex              sync.Mutex
}

// Global name space lock.
var nsMutex *nsLockMap

// initNSLock - initialize name space lock map.
func initNSLock() {
	if globalDebugLock {
		// lock Debugging enabed, initialize nsLockMap with entry for debugging information.
		nsMutex = &nsLockMap{
			// entries of <volume,path> -> stateInfo of locks, for instrumentation purpose.
			debugLockMap: make(map[nsParam]*debugLockInfoPerVolumePath),
			lockMap:      make(map[nsParam]*nsLock),
		}
	} else {
		nsMutex = &nsLockMap{
			lockMap: make(map[nsParam]*nsLock),
		}
	}
}

func (n *nsLockMap) initLockInfoForVolumePath(param nsParam) {
	n.debugLockMap[param] = newDebugLockInfoPerVolumePath()
}

// Lock the namespace resource.
func (n *nsLockMap) lock(volume, path string, lockOrigin, opsID string, readLock bool) {
	n.mutex.Lock()

	// <volume, path> pair. This is the key for storing locks and state info of locks.
	param := nsParam{volume, path}
	// checking if any locks for givne <volume, pair> already held.
	nsLk, found := n.lockMap[param]
	if !found {
		nsLk = &nsLock{
			ref: 0,
		}
		n.lockMap[param] = nsLk
	}

	nsLk.ref++ // Update ref count here to avoid multiple races.
	// Unlock map before Locking NS which might block.

	if globalDebugLock {
		// change the state of the lock to be  blocked for the given pair of <volume, path> and <OperationID> till the lock unblocks.
		// The lock for accessing `nsMutex` is held inside the function itself.
		err := n.statusNoneToBlocked(param, lockOrigin, opsID, readLock)
		if err != nil {
			errorIf(err, "Failed to set lock state to blocked.")
		}
	}

	n.mutex.Unlock()
	// Locking here can block.
	if readLock {
		// read lock blocks if write lock is held by other routines.
		nsLk.RLock()
	} else {
		// write lock blocks if read/write lock for the resource is held by other routines.
		nsLk.Lock()
	}

	// check if lock debugging enabled.
	if globalDebugLock {
		// Changing the status of the operation from blocked to running.
		// change the state of the lock to be  running (from blocked) for the given pair of <volume, path> and <OperationID>.
		err := n.statusBlockedToRunning(param, lockOrigin, opsID, readLock)
		if err != nil {
			errorIf(err, "Failed to set the lock state to running.")
		}
	}
}

// Unlock the namespace resource.
func (n *nsLockMap) unlock(volume, path string, opsID string, readLock bool) {
	// nsLk.Unlock() will not block, hence locking the map for the entire function is fine.
	n.mutex.Lock()
	// locking the lock information structure for udpating.
	defer n.mutex.Unlock()

	param := nsParam{volume, path}
	// check if the volume, path pair exists in the entry.
	if nsLk, found := n.lockMap[param]; found {
		// if the lock held was readlock, then unlock the read lock.
		if readLock {
			nsLk.RUnlock()
		} else {
			// if the lock held was write lock, unlock the write lock.
			nsLk.Unlock()
		}
		// the reference count cannot be zero when unlock is requseted.
		if nsLk.ref == 0 {
			errorIf(errors.New("Namespace reference count cannot be 0."), "Invalid reference count detected.")
		}

		if nsLk.ref != 0 {
			nsLk.ref--
			// locking debug enabled, delete the lock state entry for given operation ID.
			if globalDebugLock {
				err := n.deleteLockInfoEntryForOps(param, opsID)
				if err != nil {
					errorIf(err, "Failed to delete lock info entry.")
				}
			}
		}

		if nsLk.ref == 0 {
			// Remove from the map if there are no more references.
			delete(n.lockMap, param)
			// locking debug enabled, delete the lock state entry for given <volume, path> pair.
			if globalDebugLock {
				err := n.deleteLockInfoEntryForVolumePath(param)
				if err != nil {
					errorIf(err, "Failed to delete lock info entry.")
				}
			}
		}
	}
}

// Lock - locks the given resource for writes, using a previously
// allocated name space lock or initializing a new one.
func (n *nsLockMap) Lock(volume, path string, opsID string) {
	var lockOrigin string
	// lock debugging enabled. The caller information of the lock held has be obtained here before calling any other function.
	if globalDebugLock {
		// fetching the package, function name and the line number of the caller from the runtime.
		// here is an example https://play.golang.org/p/perrmNRI9_ .
		pc, fn, line, success := runtime.Caller(1)
		if !success {
			errorIf(errors.New("Couldn't get caller info."), "Fetching caller info form runtime failed.")
		}
		lockOrigin = fmt.Sprintf("[lock held] in %s[%s:%d]", runtime.FuncForPC(pc).Name(), fn, line)
	}
	readLock := false
	n.lock(volume, path, lockOrigin, opsID, readLock)
}

// Unlock - unlocks any previously acquired write locks.
func (n *nsLockMap) Unlock(volume, path string, opsID string) {
	readLock := false
	n.unlock(volume, path, opsID, readLock)
}

// RLock - locks any previously acquired read locks.
func (n *nsLockMap) RLock(volume, path, opsID string) {
	var lockOrigin string
	// lock debugging enabled. The caller information of the lock held has be obtained here before calling any other function.
	if globalDebugLock {
		// fetching the package, function name and the line number of the caller from the runtime.
		// here is an example https://play.golang.org/p/perrmNRI9_ .
		pc, fn, line, success := runtime.Caller(1)
		if !success {
			errorIf(errors.New("Couldn't get caller info."), "Fetching caller info form runtime failed.")
		}
		lockOrigin = fmt.Sprintf("[lock held] in %s[%s:%d]", runtime.FuncForPC(pc).Name(), fn, line)
	}
	readLock := true
	n.lock(volume, path, lockOrigin, opsID, readLock)
}

// RUnlock - unlocks any previously acquired read locks.
func (n *nsLockMap) RUnlock(volume, path string, opsID string) {
	readLock := true
	n.unlock(volume, path, opsID, readLock)
}

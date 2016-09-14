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
	"fmt"
	"time"
)

const (
	debugRLockStr = "RLock"
	debugWLockStr = "WLock"
)

// struct containing information of status (ready/running/blocked) of an operation with given operation ID.
type debugLockInfo struct {
	lockType   string    // "Rlock" or "WLock".
	lockOrigin string    // contains the trace of the function which invoked the lock, obtained from runtime.
	status     string    // status can be running/ready/blocked.
	since      time.Time // time info of the since how long the status holds true.
}

// debugLockInfo - container for storing locking information for unique copy (volume,path) pair.
// ref variable holds the reference count for locks held for.
// `ref` values helps us understand the n locks held for given <volume, path> pair.
// `running` value helps us understand the total successful locks held (not blocked) for given <volume, path> pair and the operation is under execution.
// `blocked` value helps us understand the total number of operations blocked waiting on locks for given <volume,path> pair.
type debugLockInfoPerVolumePath struct {
	ref      int64                      // running + blocked operations.
	running  int64                      // count of successful lock acquire and running operations.
	blocked  int64                      // count of number of operations blocked waiting on lock.
	lockInfo (map[string]debugLockInfo) // map of [operationID] debugLockInfo{operation, status, since} .
}

// returns an instance of debugLockInfo.
// need to create this for every unique pair of {volume,path}.
// total locks, number of calls blocked on locks, and number of successful locks held but not unlocked yet.
func newDebugLockInfoPerVolumePath() *debugLockInfoPerVolumePath {
	return &debugLockInfoPerVolumePath{
		lockInfo: make(map[string]debugLockInfo),
		ref:      0,
		blocked:  0,
		running:  0,
	}
}

// LockInfoNil - Returned if the lock info map is not initialized.
type LockInfoNil struct {
}

func (l LockInfoNil) Error() string {
	return fmt.Sprintf("Debug Lock Map not initialized:\n1. Enable Lock Debugging using right ENV settings \n2. Make sure initNSLock() is called.")
}

// LockInfoOriginNotFound - While changing the state of the lock info its important that the entry for
// lock at a given origin exists, if not `LockInfoOriginNotFound` is returned.
type LockInfoOriginNotFound struct {
	volume      string
	path        string
	operationID string
	lockOrigin  string
}

func (l LockInfoOriginNotFound) Error() string {
	return fmt.Sprintf("No lock state stored for the lock origined at \"%s\", for <volume> %s, <path> %s, <operationID> %s.",
		l.lockOrigin, l.volume, l.path, l.operationID)
}

// LockInfoVolPathMssing - Error interface. Returned when the info the
type LockInfoVolPathMssing struct {
	volume string
	path   string
}

func (l LockInfoVolPathMssing) Error() string {
	return fmt.Sprintf("No entry in debug Lock Map for Volume: %s, path: %s.", l.volume, l.path)
}

// LockInfoOpsIDNotFound - Returned when the lock state info exists, but the entry for
// given operation ID doesn't exist.
type LockInfoOpsIDNotFound struct {
	volume      string
	path        string
	operationID string
}

func (l LockInfoOpsIDNotFound) Error() string {
	return fmt.Sprintf("No entry in lock info for <Operation ID> %s, <volume> %s, <path> %s.", l.operationID, l.volume, l.path)
}

// LockInfoStateNotBlocked - When an attempt to change the state of the lock form `blocked` to `running` is done,
// its necessary that the state before the transsition is "blocked", otherwise LockInfoStateNotBlocked returned.
type LockInfoStateNotBlocked struct {
	volume      string
	path        string
	operationID string
}

func (l LockInfoStateNotBlocked) Error() string {
	return fmt.Sprintf("Lock state should be \"Blocked\" for <volume> %s, <path> %s, <operationID> %s.", l.volume, l.path, l.operationID)
}

// change the state of the lock from Blocked to Running.
func (n *nsLockMap) statusBlockedToRunning(param nsParam, lockOrigin, operationID string, readLock bool) error {
	// This operation is not executed under the scope nsLockMap.mutex.Lock(), lock has to be explicitly held here.
	n.lockMapMutex.Lock()
	defer n.lockMapMutex.Unlock()
	if n.debugLockMap == nil {
		return LockInfoNil{}
	}
	// new state info to be set for the lock.
	newLockInfo := debugLockInfo{
		lockOrigin: lockOrigin,
		status:     "Running",
		since:      time.Now().UTC(),
	}

	// set lock type.
	if readLock {
		newLockInfo.lockType = debugRLockStr
	} else {
		newLockInfo.lockType = debugWLockStr
	}

	// check whether the lock info entry for <volume, path> pair already exists and its not `nil`.
	if debugLockMap, ok := n.debugLockMap[param]; ok {
		//  ``*debugLockInfoPerVolumePath` entry containing lock info for `param <volume, path>` is `nil`.
		if debugLockMap == nil {
			return LockInfoNil{}
		}
	} else {
		// The lock state info foe given <volume, path> pair should already exist.
		// If not return `LockInfoVolPathMssing`.
		return LockInfoVolPathMssing{param.volume, param.path}
	}

	// Lock info the for the given operation ID shouldn't be `nil`.
	if n.debugLockMap[param].lockInfo == nil {
		return LockInfoOpsIDNotFound{param.volume, param.path, operationID}
	}

	if lockInfo, ok := n.debugLockMap[param].lockInfo[operationID]; ok {
		// The entry for the lock origined at `lockOrigin` should already exist.
		// If not return `LockInfoOriginNotFound`.
		if lockInfo.lockOrigin != lockOrigin {
			return LockInfoOriginNotFound{param.volume, param.path, operationID, lockOrigin}
		}
		// Status of the lock should already be set to "Blocked".
		// If not return `LockInfoStateNotBlocked`.
		if lockInfo.status != "Blocked" {
			return LockInfoStateNotBlocked{param.volume, param.path, operationID}
		}
	} else {
		// The lock info entry for given `opsID` should already exist for given <volume, path> pair.
		// If not return `LockInfoOpsIDNotFound`.
		return LockInfoOpsIDNotFound{param.volume, param.path, operationID}
	}

	// All checks finished.
	// changing the status of the operation from blocked to running and updating the time.
	n.debugLockMap[param].lockInfo[operationID] = newLockInfo

	// After locking unblocks decrease the blocked counter.
	n.blockedCounter--
	// Increase the running counter.
	n.runningLockCounter++
	n.debugLockMap[param].blocked--
	n.debugLockMap[param].running++
	return nil
}

// change the state of the lock from Ready to Blocked.
func (n *nsLockMap) statusNoneToBlocked(param nsParam, lockOrigin, operationID string, readLock bool) error {
	if n.debugLockMap == nil {
		return LockInfoNil{}
	}

	newLockInfo := debugLockInfo{
		lockOrigin: lockOrigin,
		status:     "Blocked",
		since:      time.Now().UTC(),
	}
	if readLock {
		newLockInfo.lockType = debugRLockStr
	} else {
		newLockInfo.lockType = debugWLockStr
	}

	if lockInfo, ok := n.debugLockMap[param]; ok {
		if lockInfo == nil {
			//  *debugLockInfoPerVolumePath entry is nil, initialize here to avoid any case of `nil` pointer access.
			n.initLockInfoForVolumePath(param)
		}
	} else {
		// State info entry for the given <volume, pair> doesn't exist, initializing it.
		n.initLockInfoForVolumePath(param)
	}

	// lockInfo is a map[string]debugLockInfo, which holds map[OperationID]{status,time, origin} of the lock.
	if n.debugLockMap[param].lockInfo == nil {
		n.debugLockMap[param].lockInfo = make(map[string]debugLockInfo)
	}
	// The status of the operation with the given operation ID is marked blocked till its gets unblocked from the lock.
	n.debugLockMap[param].lockInfo[operationID] = newLockInfo
	// Increment the Global lock counter.
	n.globalLockCounter++
	// Increment the counter for number of blocked opertions, decrement it after the locking unblocks.
	n.blockedCounter++
	// increment the reference of the lock for the given <volume,path> pair.
	n.debugLockMap[param].ref++
	// increment the blocked counter for the given <volume, path> pair.
	n.debugLockMap[param].blocked++
	return nil
}

// deleteLockInfoEntry - Deletes the lock state information for given <volume, path> pair. Called when nsLk.ref count is 0.
func (n *nsLockMap) deleteLockInfoEntryForVolumePath(param nsParam) error {
	if n.debugLockMap == nil {
		return LockInfoNil{}
	}
	// delete the lock info for the given operation.
	if _, found := n.debugLockMap[param]; found {
		// Remove from the map if there are no more references for the given (volume,path) pair.
		delete(n.debugLockMap, param)
	} else {
		return LockInfoVolPathMssing{param.volume, param.path}
	}
	return nil
}

// deleteLockInfoEntry - Deletes the entry for given opsID in the lock state information of given <volume, path> pair.
// called when the nsLk ref count for the given <volume, path> pair is not 0.
func (n *nsLockMap) deleteLockInfoEntryForOps(param nsParam, operationID string) error {
	if n.debugLockMap == nil {
		return LockInfoNil{}
	}
	// delete the lock info for the given operation.
	if infoMap, found := n.debugLockMap[param]; found {
		// the opertion finished holding the lock on the resource, remove the entry for the given operation with the operation ID.
		if _, foundInfo := infoMap.lockInfo[operationID]; foundInfo {
			// decrease the global running and lock reference counter.
			n.runningLockCounter--
			n.globalLockCounter--
			// decrease the lock referee counter for the lock info for given <volume,path> pair.
			// decrease the running operation number. Its assumed that the operation is over once an attempt to release the lock is made.
			infoMap.running--
			// decrease the total reference count of locks jeld on <volume,path> pair.
			infoMap.ref--
			delete(infoMap.lockInfo, operationID)
		} else {
			// Unlock request with invalid opertion ID not accepted.
			return LockInfoOpsIDNotFound{param.volume, param.path, operationID}
		}
	} else {
		return LockInfoVolPathMssing{param.volume, param.path}
	}
	return nil
}

// return randomly generated string ID if lock debug is enabled,
// else returns empty string
func getOpsID() (opsID string) {
	// check if lock debug is enabled.
	if globalDebugLock {
		// generated random ID.
		opsID = string(generateRequestID())
	}
	return opsID
}

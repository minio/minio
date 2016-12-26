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
	"time"
)

type statusType string

const (
	runningStatus statusType = "Running"
	blockedStatus statusType = "Blocked"
)

type lockType string

const (
	debugRLockStr lockType = "RLock"
	debugWLockStr lockType = "WLock"
)

// Struct containing information of status (ready/running/blocked) of an operation with given operation ID.
type debugLockInfo struct {
	// "RLock" or "WLock".
	lType lockType
	// Contains the trace of the function which invoked the lock, obtained from runtime.
	lockSource string
	// Status can be running/ready/blocked.
	status statusType
	// Time info of the since how long the status holds true.
	since time.Time
}

// debugLockInfoPerVolumePath - lock state information on all locks held on (volume, path).
type debugLockInfoPerVolumePath struct {
	counters *lockStat                // Holds stats of lock held on (volume, path)
	lockInfo map[string]debugLockInfo // map of [opsID] debugLockInfo{operation, status, since} .
}

// LockInfoOriginNotFound - While changing the state of the lock info its important that the entry for
// lock at a given origin exists, if not `LockInfoOriginNotFound` is returned.
type LockInfoOriginNotFound struct {
	volume     string
	path       string
	opsID      string
	lockSource string
}

func (l LockInfoOriginNotFound) Error() string {
	return fmt.Sprintf("No lock state stored for the lock origined at \"%s\", for <volume> %s, <path> %s, <opsID> %s",
		l.lockSource, l.volume, l.path, l.opsID)
}

// LockInfoVolPathMissing - Error interface. Returned when the info the
type LockInfoVolPathMissing struct {
	volume string
	path   string
}

func (l LockInfoVolPathMissing) Error() string {
	return fmt.Sprintf("No entry in debug Lock Map for Volume: %s, path: %s", l.volume, l.path)
}

// LockInfoOpsIDNotFound - Returned when the lock state info exists, but the entry for
// given operation ID doesn't exist.
type LockInfoOpsIDNotFound struct {
	volume string
	path   string
	opsID  string
}

func (l LockInfoOpsIDNotFound) Error() string {
	return fmt.Sprintf("No entry in lock info for <Operation ID> %s, <volume> %s, <path> %s", l.opsID, l.volume, l.path)
}

// LockInfoStateNotBlocked - When an attempt to change the state of the lock form `blocked` to `running` is done,
// its necessary that the state before the transsition is "blocked", otherwise LockInfoStateNotBlocked returned.
type LockInfoStateNotBlocked struct {
	volume string
	path   string
	opsID  string
}

func (l LockInfoStateNotBlocked) Error() string {
	return fmt.Sprintf("Lock state should be \"Blocked\" for <volume> %s, <path> %s, <opsID> %s", l.volume, l.path, l.opsID)
}

var errLockNotInitialized = errors.New("Debug lockMap not initialized")

// Initialize lock info volume path.
func (n *nsLockMap) initLockInfoForVolumePath(param nsParam) {
	n.debugLockMap[param] = &debugLockInfoPerVolumePath{
		lockInfo: make(map[string]debugLockInfo),
		counters: &lockStat{},
	}
}

// Change the state of the lock from Blocked to Running.
func (n *nsLockMap) statusBlockedToRunning(param nsParam, lockSource, opsID string, readLock bool) error {
	// This operation is not executed under the scope nsLockMap.mutex.Lock(), lock has to be explicitly held here.
	n.lockMapMutex.Lock()
	defer n.lockMapMutex.Unlock()
	// new state info to be set for the lock.
	newLockInfo := debugLockInfo{
		lockSource: lockSource,
		status:     runningStatus,
		since:      time.Now().UTC(),
	}

	// Set lock type.
	if readLock {
		newLockInfo.lType = debugRLockStr
	} else {
		newLockInfo.lType = debugWLockStr
	}

	// Check whether the lock info entry for <volume, path> pair already exists and its not `nil`.
	debugLockMap, ok := n.debugLockMap[param]
	if !ok {
		// The lock state info foe given <volume, path> pair should already exist.
		// If not return `LockInfoVolPathMissing`.
		return traceError(LockInfoVolPathMissing{param.volume, param.path})
	}
	//  ``debugLockMap`` entry containing lock info for `param <volume, path>` is `nil`.
	if debugLockMap == nil {
		return traceError(errLockNotInitialized)
	}
	lockInfo, ok := n.debugLockMap[param].lockInfo[opsID]
	if !ok {
		// The lock info entry for given `opsID` should already exist for given <volume, path> pair.
		// If not return `LockInfoOpsIDNotFound`.
		return traceError(LockInfoOpsIDNotFound{param.volume, param.path, opsID})
	}
	// The entry for the lock origined at `lockSource` should already exist. If not return `LockInfoOriginNotFound`.
	if lockInfo.lockSource != lockSource {
		return traceError(LockInfoOriginNotFound{param.volume, param.path, opsID, lockSource})
	}
	// Status of the lock should already be set to "Blocked". If not return `LockInfoStateNotBlocked`.
	if lockInfo.status != blockedStatus {
		return traceError(LockInfoStateNotBlocked{param.volume, param.path, opsID})
	}
	// All checks finished. Changing the status of the operation from blocked to running and updating the time.
	n.debugLockMap[param].lockInfo[opsID] = newLockInfo

	// After locking unblocks decrease the blocked counter.
	// Increase the running counter.
	n.counters.lockGranted()
	n.debugLockMap[param].counters.lockGranted()
	return nil
}

// Change the state of the lock from Ready to Blocked.
func (n *nsLockMap) statusNoneToBlocked(param nsParam, lockSource, opsID string, readLock bool) error {
	newLockInfo := debugLockInfo{
		lockSource: lockSource,
		status:     blockedStatus,
		since:      time.Now().UTC(),
	}
	if readLock {
		newLockInfo.lType = debugRLockStr
	} else {
		newLockInfo.lType = debugWLockStr
	}

	lockInfo, ok := n.debugLockMap[param]
	if !ok {
		// State info entry for the given <volume, pair> doesn't exist, initializing it.
		n.initLockInfoForVolumePath(param)
	}
	if lockInfo == nil {
		//  *lockInfo is nil, initialize here.
		n.initLockInfoForVolumePath(param)
	}

	// lockInfo is a map[string]debugLockInfo, which holds map[OperationID]{status,time, origin} of the lock.
	if n.debugLockMap[param].lockInfo == nil {
		n.debugLockMap[param].lockInfo = make(map[string]debugLockInfo)
	}
	// The status of the operation with the given operation ID is marked blocked till its gets unblocked from the lock.
	n.debugLockMap[param].lockInfo[opsID] = newLockInfo
	// Update global lock stats.
	n.counters.lockWaiting()
	// Update (volume, path) lock stats.
	n.debugLockMap[param].counters.lockWaiting()
	return nil
}

// deleteLockInfoEntry - Deletes the lock state information for given
// <volume, path> pair. Called when nsLk.ref count is 0.
func (n *nsLockMap) deleteLockInfoEntryForVolumePath(param nsParam) error {
	// delete the lock info for the given operation.
	if _, found := n.debugLockMap[param]; !found {
		return traceError(LockInfoVolPathMissing{param.volume, param.path})
	}

	// The following stats update is relevant only in case of a
	// ForceUnlock. In case of the last unlock on a (volume,
	// path), this would be a no-op.
	volumePathLocks := n.debugLockMap[param]
	for _, lockInfo := range volumePathLocks.lockInfo {
		granted := lockInfo.status == runningStatus
		// Update global and (volume, path) stats.
		n.counters.lockRemoved(granted)
		volumePathLocks.counters.lockRemoved(granted)
	}
	delete(n.debugLockMap, param)
	return nil
}

// deleteLockInfoEntry - Deletes the entry for given opsID in the lock state information
// of given <volume, path> pair. Called when the nsLk ref count for the given
// <volume, path> pair is not 0.
func (n *nsLockMap) deleteLockInfoEntryForOps(param nsParam, opsID string) error {
	// delete the lock info for the given operation.
	infoMap, found := n.debugLockMap[param]
	if !found {
		return traceError(LockInfoVolPathMissing{param.volume, param.path})
	}
	// The operation finished holding the lock on the resource, remove
	// the entry for the given operation with the operation ID.
	opsIDLock, foundInfo := infoMap.lockInfo[opsID]
	if !foundInfo {
		// Unlock request with invalid opertion ID not accepted.
		return traceError(LockInfoOpsIDNotFound{param.volume, param.path, opsID})
	}
	// Update global and (volume, path) lock status.
	granted := opsIDLock.status == runningStatus
	n.counters.lockRemoved(granted)
	infoMap.counters.lockRemoved(granted)
	delete(infoMap.lockInfo, opsID)
	return nil
}

// Return randomly generated string ID
func getOpsID() string {
	return newRequestID()
}

/*
 * Minio Cloud Storage, (C) 2016, 2017 Minio, Inc.
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

	"github.com/minio/minio/pkg/errors"
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

// debugLockInfo - represents a single lock's information, uniquely identified by opsID.
// See debugLockInfoPerVolumePath for more context.
type debugLockInfo struct {
	// "RLock" or "WLock".
	lType lockType
	// Contains the backtrace of incl. the function which called (r)(un)lock.
	lockSource string
	// Status can be running/blocked.
	status statusType
	// Time of last status update.
	since time.Time
}

// debugLockInfoPerVolumePath - lock state information on all locks held on (volume, path).
type debugLockInfoPerVolumePath struct {
	counters *lockStat                // Holds stats of lock held on (volume, path)
	lockInfo map[string]debugLockInfo // Lock information per operation ID.
}

// LockInfoOriginMismatch - represents error when lock origin don't match.
type LockInfoOriginMismatch struct {
	volume     string
	path       string
	opsID      string
	lockSource string
}

func (l LockInfoOriginMismatch) Error() string {
	return fmt.Sprintf("No lock state stored for the lock originated at \"%s\", for <volume> %s, <path> %s, <opsID> %s",
		l.lockSource, l.volume, l.path, l.opsID)
}

// LockInfoVolPathMissing - represents error when lock information is missing for a given (volume, path).
type LockInfoVolPathMissing struct {
	volume string
	path   string
}

func (l LockInfoVolPathMissing) Error() string {
	return fmt.Sprintf("No entry in debug Lock Map for Volume: %s, path: %s", l.volume, l.path)
}

// LockInfoOpsIDNotFound - represents error when lock info entry for a given operation ID doesn't exist.
type LockInfoOpsIDNotFound struct {
	volume string
	path   string
	opsID  string
}

func (l LockInfoOpsIDNotFound) Error() string {
	return fmt.Sprintf("No entry in lock info for <Operation ID> %s, <volume> %s, <path> %s", l.opsID, l.volume, l.path)
}

// LockInfoStateNotBlocked - represents error when lock info isn't in blocked state when it should be.
type LockInfoStateNotBlocked struct {
	volume string
	path   string
	opsID  string
}

func (l LockInfoStateNotBlocked) Error() string {
	return fmt.Sprintf("Lock state should be \"Blocked\" for <volume> %s, <path> %s, <opsID> %s", l.volume, l.path, l.opsID)
}

// Initialize lock info for given (volume, path).
func (n *nsLockMap) initLockInfoForVolumePath(param nsParam) {
	n.debugLockMap[param] = &debugLockInfoPerVolumePath{
		lockInfo: make(map[string]debugLockInfo),
		counters: &lockStat{},
	}
}

// Change the state of the lock from Blocked to Running.
func (n *nsLockMap) statusBlockedToRunning(param nsParam, lockSource, opsID string, readLock bool) error {
	// This function is called outside nsLockMap.mutex.Lock(), so must be held explicitly.
	n.lockMapMutex.Lock()
	defer n.lockMapMutex.Unlock()

	// Check whether the lock info entry for <volume, path> pair already exists.
	_, ok := n.debugLockMap[param]
	if !ok {
		return errors.Trace(LockInfoVolPathMissing{param.volume, param.path})
	}

	// Check whether lock info entry for the given `opsID` exists.
	lockInfo, ok := n.debugLockMap[param].lockInfo[opsID]
	if !ok {
		return errors.Trace(LockInfoOpsIDNotFound{param.volume, param.path, opsID})
	}

	// Check whether lockSource is same.
	if lockInfo.lockSource != lockSource {
		return errors.Trace(LockInfoOriginMismatch{param.volume, param.path, opsID, lockSource})
	}

	// Status of the lock should be set to "Blocked".
	if lockInfo.status != blockedStatus {
		return errors.Trace(LockInfoStateNotBlocked{param.volume, param.path, opsID})
	}
	// Change lock status to running and update the time.
	n.debugLockMap[param].lockInfo[opsID] = newDebugLockInfo(lockSource, runningStatus, readLock)

	// Update global lock stats.
	n.counters.lockGranted()
	// Update (volume, pair) lock stats.
	n.debugLockMap[param].counters.lockGranted()
	return nil
}

// newDebugLockInfo - Constructs a debugLockInfo value given lock source, status and type.
func newDebugLockInfo(lockSource string, status statusType, readLock bool) debugLockInfo {
	var lType lockType
	if readLock {
		lType = debugRLockStr
	} else {
		lType = debugWLockStr
	}
	return debugLockInfo{
		lockSource: lockSource,
		lType:      lType,
		status:     status,
		since:      UTCNow(),
	}
}

// Change the state of the lock to Blocked.
func (n *nsLockMap) statusNoneToBlocked(param nsParam, lockSource, opsID string, readLock bool) error {
	_, ok := n.debugLockMap[param]
	if !ok {
		// Lock info entry for (volume, pair) doesn't exist, initialize it.
		n.initLockInfoForVolumePath(param)
	}

	// Mark lock status blocked for given opsID.
	n.debugLockMap[param].lockInfo[opsID] = newDebugLockInfo(lockSource, blockedStatus, readLock)
	// Update global lock stats.
	n.counters.lockWaiting()
	// Update (volume, path) lock stats.
	n.debugLockMap[param].counters.lockWaiting()
	return nil
}

// Change the state of the lock from Blocked to none.
func (n *nsLockMap) statusBlockedToNone(param nsParam, lockSource, opsID string, readLock bool) error {
	_, ok := n.debugLockMap[param]
	if !ok {
		return errors.Trace(LockInfoVolPathMissing{param.volume, param.path})
	}

	// Check whether lock info entry for the given `opsID` exists.
	lockInfo, ok := n.debugLockMap[param].lockInfo[opsID]
	if !ok {
		return errors.Trace(LockInfoOpsIDNotFound{param.volume, param.path, opsID})
	}

	// Check whether lockSource is same.
	if lockInfo.lockSource != lockSource {
		return errors.Trace(LockInfoOriginMismatch{param.volume, param.path, opsID, lockSource})
	}

	// Status of the lock should be set to "Blocked".
	if lockInfo.status != blockedStatus {
		return errors.Trace(LockInfoStateNotBlocked{param.volume, param.path, opsID})
	}
	// Clear the status by removing the entry for the given `opsID`.
	delete(n.debugLockMap[param].lockInfo, opsID)

	// Update global lock stats.
	n.counters.lockTimedOut()
	// Update (volume, path) lock stats.
	n.debugLockMap[param].counters.lockTimedOut()
	return nil
}

// deleteLockInfoEntry - Deletes the lock information for given (volume, path).
// Called when nsLk.ref count is 0.
func (n *nsLockMap) deleteLockInfoEntryForVolumePath(param nsParam) error {
	// delete the lock info for the given operation.
	if _, found := n.debugLockMap[param]; !found {
		return errors.Trace(LockInfoVolPathMissing{param.volume, param.path})
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

// deleteLockInfoEntry - Deletes lock info entry for given opsID.
// Called when the nsLk ref count for the given (volume, path) is
// not 0.
func (n *nsLockMap) deleteLockInfoEntryForOps(param nsParam, opsID string) error {
	// delete the lock info for the given operation.
	infoMap, found := n.debugLockMap[param]
	if !found {
		return errors.Trace(LockInfoVolPathMissing{param.volume, param.path})
	}
	// The operation finished holding the lock on the resource, remove
	// the entry for the given operation with the operation ID.
	opsIDLock, foundInfo := infoMap.lockInfo[opsID]
	if !foundInfo {
		// Unlock request with invalid operation ID not accepted.
		return errors.Trace(LockInfoOpsIDNotFound{param.volume, param.path, opsID})
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
	return mustGetUUID()
}

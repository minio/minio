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
	"time"
)

// SystemLockState - Structure to fill the lock state of entire object storage.
// That is the total locks held, total calls blocked on locks and state of all the locks for the entire system.
type SystemLockState struct {
	TotalLocks int64 `json:"totalLocks"`
	// Count of operations which are blocked waiting for the lock to
	// be released.
	TotalBlockedLocks int64 `json:"totalBlockedLocks"`
	// Count of operations which has successfully acquired the lock but
	// hasn't unlocked yet (operation in progress).
	TotalAcquiredLocks int64            `json:"totalAcquiredLocks"`
	LocksInfoPerObject []VolumeLockInfo `json:"locksInfoPerObject"`
}

// VolumeLockInfo - Structure to contain the lock state info for volume, path pair.
type VolumeLockInfo struct {
	Bucket string `json:"bucket"`
	Object string `json:"object"`

	// All locks blocked + running for given <volume,path> pair.
	LocksOnObject int64 `json:"-"`
	// Count of operations which has successfully acquired the lock
	// but hasn't unlocked yet( operation in progress).
	LocksAcquiredOnObject int64 `json:"-"`
	// Count of operations which are blocked waiting for the lock
	// to be released.
	TotalBlockedLocks int64 `json:"-"`

	// Count of all read locks
	TotalReadLocks int64 `json:"readLocks"`
	// Count of all write locks
	TotalWriteLocks int64 `json:"writeLocks"`
	// State information containing state of the locks for all operations
	// on given <volume,path> pair.
	LockDetailsOnObject []OpsLockState `json:"lockOwners"`
}

// OpsLockState - structure to fill in state information of the lock.
// structure to fill in status information for each operation with given operation ID.
type OpsLockState struct {
	OperationID string     `json:"id"`     // String containing operation ID.
	LockSource  string     `json:"source"` // Operation type (GetObject, PutObject...)
	LockType    lockType   `json:"type"`   // Lock type (RLock, WLock)
	Status      statusType `json:"status"` // Status can be Running/Ready/Blocked.
	Since       time.Time  `json:"since"`  // Time when the lock was initially held.
}

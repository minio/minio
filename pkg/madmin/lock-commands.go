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
 *
 */

package madmin

import (
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"time"
)

type statusType string

type lockType string

// OpsLockState - represents lock specific details.
type OpsLockState struct {
	OperationID string     `json:"id"`     // String containing operation ID.
	LockSource  string     `json:"source"` // Operation type (GetObject, PutObject...)
	LockType    lockType   `json:"type"`   // Lock type (RLock, WLock)
	Status      statusType `json:"status"` // Status can be Running/Ready/Blocked.
	Since       time.Time  `json:"since"`  // Time when the lock was initially held.
}

// VolumeLockInfo - represents summary and individual lock details of all
// locks held on a given bucket, object.
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

// getLockInfos - unmarshal []VolumeLockInfo from a reader.
func getLockInfos(body io.Reader) ([]VolumeLockInfo, error) {
	respBytes, err := ioutil.ReadAll(body)
	if err != nil {
		return nil, err
	}

	var lockInfos []VolumeLockInfo

	err = json.Unmarshal(respBytes, &lockInfos)
	if err != nil {
		return nil, err
	}

	return lockInfos, nil
}

// ListLocks - Calls List Locks Management API to fetch locks matching
// bucket, prefix and held before the duration supplied.
func (adm *AdminClient) ListLocks(bucket, prefix string,
	duration time.Duration) ([]VolumeLockInfo, error) {

	queryVal := make(url.Values)
	queryVal.Set("bucket", bucket)
	queryVal.Set("prefix", prefix)
	queryVal.Set("older-than", duration.String())

	// Execute GET on /minio/admin/v1/locks to list locks.
	resp, err := adm.executeMethod("GET", requestData{
		queryValues: queryVal,
		relPath:     "/v1/locks",
	})
	defer closeResponse(resp)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		return nil, httpRespToErrorResponse(resp)
	}

	return getLockInfos(resp.Body)
}

// ClearLocks - Calls Clear Locks Management API to clear locks held
// on bucket, matching prefix older than duration supplied.
func (adm *AdminClient) ClearLocks(bucket, prefix string,
	duration time.Duration) ([]VolumeLockInfo, error) {

	queryVal := make(url.Values)
	queryVal.Set("bucket", bucket)
	queryVal.Set("prefix", prefix)
	queryVal.Set("duration", duration.String())

	// Execute POST on /?lock to clear locks.
	resp, err := adm.executeMethod("DELETE", requestData{
		queryValues: queryVal,
		relPath:     "/v1/locks",
	})
	defer closeResponse(resp)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		return nil, httpRespToErrorResponse(resp)
	}

	return getLockInfos(resp.Body)
}

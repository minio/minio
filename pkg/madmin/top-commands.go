/*
 * MinIO Cloud Storage, (C) 2019 MinIO, Inc.
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
	"io/ioutil"
	"net/http"
	"time"
)

// LockEntry holds information about client requesting the lock,
// servers holding the lock, source on the client machine,
// ID, type(read or write) and time stamp.
type LockEntry struct {
	Timestamp  time.Time `json:"time"`       // Timestamp set at the time of initialization.
	Resource   string    `json:"resource"`   // Resource contains info like bucket, object etc
	Type       string    `json:"type"`       // Bool whether write or read lock.
	Source     string    `json:"source"`     // Source which created the lock
	ServerList []string  `json:"serverlist"` // RPC path of servers issuing the lock.
	Owner      string    `json:"owner"`      // RPC path of client claiming lock.
	ID         string    `json:"id"`         // UID to uniquely identify request of client.
}

// LockEntries - To sort the locks
type LockEntries []LockEntry

func (l LockEntries) Len() int {
	return len(l)
}

func (l LockEntries) Less(i, j int) bool {
	return l[i].Timestamp.Before(l[j].Timestamp)
}

func (l LockEntries) Swap(i, j int) {
	l[i], l[j] = l[j], l[i]
}

// TopLocks - returns the oldest locks in a minio setup.
func (adm *AdminClient) TopLocks() (LockEntries, error) {
	// Execute GET on /minio/admin/v1/top/locks
	// to get the oldest locks in a minio setup.
	resp, err := adm.executeMethod("GET",
		requestData{relPath: "/v1/top/locks"})
	defer closeResponse(resp)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		return nil, httpRespToErrorResponse(resp)
	}

	response, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return LockEntries{}, err
	}

	var lockEntries LockEntries
	err = json.Unmarshal(response, &lockEntries)
	return lockEntries, err
}

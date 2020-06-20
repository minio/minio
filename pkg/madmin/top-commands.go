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
	"context"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

// LockEntry holds information about client requesting the lock,
// servers holding the lock, source on the client machine,
// ID, type(read or write) and time stamp.
type LockEntry struct {
	Timestamp  time.Time `json:"time"`       // When the lock was first granted
	Resource   string    `json:"resource"`   // Resource contains info like bucket+object
	Type       string    `json:"type"`       // Type indicates if 'Write' or 'Read' lock
	Source     string    `json:"source"`     // Source at which lock was granted
	ServerList []string  `json:"serverlist"` // List of servers participating in the lock.
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

// TopNLocks - returns the count number of oldest locks currently active on the server.
func (adm *AdminClient) TopNLocks(ctx context.Context, count int) (LockEntries, error) {
	// Execute GET on /minio/admin/v3/top/locks?count=10
	// to get the 'count' number of oldest locks currently
	// active on the server.
	queryVals := make(url.Values)
	queryVals.Set("count", strconv.Itoa(count))
	resp, err := adm.executeMethod(ctx,
		http.MethodGet,
		requestData{
			relPath:     adminAPIPrefix + "/top/locks",
			queryValues: queryVals,
		},
	)
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

// TopLocks - returns top '10' oldest locks currently active on the server.
func (adm *AdminClient) TopLocks(ctx context.Context) (LockEntries, error) {
	return adm.TopNLocks(ctx, 10)
}

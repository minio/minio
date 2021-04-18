// Copyright (c) 2015-2021 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package madmin

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"strings"
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
	Owner      string    `json:"owner"`      // Owner UUID indicates server owns the lock.
	ID         string    `json:"id"`         // UID to uniquely identify request of client.
	// Represents quorum number of servers required to hold this lock, used to look for stale locks.
	Quorum int `json:"quorum"`
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

// TopLockOpts top lock options
type TopLockOpts struct {
	Count int
	Stale bool
}

// ForceUnlock force unlocks input paths...
func (adm *AdminClient) ForceUnlock(ctx context.Context, paths ...string) error {
	// Execute POST on /minio/admin/v3/force-unlock
	queryVals := make(url.Values)
	queryVals.Set("paths", strings.Join(paths, ","))
	resp, err := adm.executeMethod(ctx,
		http.MethodPost,
		requestData{
			relPath:     adminAPIPrefix + "/force-unlock",
			queryValues: queryVals,
		},
	)
	defer closeResponse(resp)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		return httpRespToErrorResponse(resp)
	}

	return nil
}

// TopLocksWithOpts - returns the count number of oldest locks currently active on the server.
// additionally we can also enable `stale` to get stale locks currently present on server.
func (adm *AdminClient) TopLocksWithOpts(ctx context.Context, opts TopLockOpts) (LockEntries, error) {
	// Execute GET on /minio/admin/v3/top/locks?count=10
	// to get the 'count' number of oldest locks currently
	// active on the server.
	queryVals := make(url.Values)
	queryVals.Set("count", strconv.Itoa(opts.Count))
	queryVals.Set("stale", strconv.FormatBool(opts.Stale))
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
	return adm.TopLocksWithOpts(ctx, TopLockOpts{Count: 10})
}

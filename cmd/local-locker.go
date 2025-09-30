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

package cmd

//go:generate msgp -file=$GOFILE -unexported

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/minio/minio/internal/dsync"
)

// Reject new lock requests immediately when this many are queued
// for the local lock mutex.
// We do not block unlocking or maintenance, but they add to the count.
// The limit is set to allow for bursty behavior,
// but prevent requests to overload the server completely.
// Rejected clients are expected to retry.
const lockMutexWaitLimit = 1000

// lockRequesterInfo stores various info from the client for each lock that is requested.
type lockRequesterInfo struct {
	Name            string // name of the resource lock was requested for
	Writer          bool   // Bool whether write or read lock.
	UID             string // UID to uniquely identify request of client.
	Timestamp       int64  // Timestamp set at the time of initialization.
	TimeLastRefresh int64  // Timestamp for last lock refresh.
	Source          string // Contains line, function and filename requesting the lock.
	Group           bool   // indicates if it was a group lock.
	Owner           string // Owner represents the UUID of the owner who originally requested the lock.
	Quorum          int    // Quorum represents the quorum required for this lock to be active.
	idx             int    `msg:"-"` // index of the lock in the lockMap.
}

// isWriteLock returns whether the lock is a write or read lock.
func isWriteLock(lri []lockRequesterInfo) bool {
	return len(lri) == 1 && lri[0].Writer
}

// localLocker implements Dsync.NetLocker
//
//msgp:ignore localLocker
type localLocker struct {
	mutex     sync.Mutex
	waitMutex atomic.Int32
	lockMap   map[string][]lockRequesterInfo
	lockUID   map[string]string // UUID -> resource map.

	// the following are updated on every cleanup defined in lockValidityDuration
	readers         atomic.Int32
	writers         atomic.Int32
	lastCleanup     atomic.Pointer[time.Time]
	locksOverloaded atomic.Int64
}

// getMutex will lock the mutex.
// Call the returned function to unlock.
func (l *localLocker) getMutex() func() {
	l.waitMutex.Add(1)
	l.mutex.Lock()
	l.waitMutex.Add(-1)
	return l.mutex.Unlock
}

func (l *localLocker) String() string {
	return globalEndpoints.Localhost()
}

func (l *localLocker) canTakeLock(resources ...string) bool {
	for _, resource := range resources {
		_, lockTaken := l.lockMap[resource]
		if lockTaken {
			return false
		}
	}
	return true
}

func (l *localLocker) Lock(ctx context.Context, args dsync.LockArgs) (reply bool, err error) {
	if len(args.Resources) > maxDeleteList {
		return false, fmt.Errorf("internal error: localLocker.Lock called with more than %d resources", maxDeleteList)
	}

	// If we have too many waiting, reject this at once.
	if l.waitMutex.Load() > lockMutexWaitLimit {
		l.locksOverloaded.Add(1)
		return false, nil
	}
	// Wait for mutex
	defer l.getMutex()()
	if ctx.Err() != nil {
		return false, ctx.Err()
	}
	if !l.canTakeLock(args.Resources...) {
		// Not all locks can be taken on resources,
		// reject it completely.
		return false, nil
	}

	// No locks held on the all resources, so claim write
	// lock on all resources at once.
	now := UTCNow()
	for i, resource := range args.Resources {
		l.lockMap[resource] = []lockRequesterInfo{
			{
				Name:            resource,
				Writer:          true,
				Source:          args.Source,
				Owner:           args.Owner,
				UID:             args.UID,
				Timestamp:       now.UnixNano(),
				TimeLastRefresh: now.UnixNano(),
				Group:           len(args.Resources) > 1,
				Quorum:          *args.Quorum,
				idx:             i,
			},
		}
		l.lockUID[formatUUID(args.UID, i)] = resource
	}
	return true, nil
}

func formatUUID(s string, idx int) string {
	return concat(s, strconv.Itoa(idx))
}

func (l *localLocker) Unlock(_ context.Context, args dsync.LockArgs) (reply bool, err error) {
	if len(args.Resources) > maxDeleteList {
		return false, fmt.Errorf("internal error: localLocker.Unlock called with more than %d resources", maxDeleteList)
	}

	defer l.getMutex()()

	for _, resource := range args.Resources {
		lri, ok := l.lockMap[resource]
		if ok && !isWriteLock(lri) {
			// Unless it is a write lock reject it.
			err = fmt.Errorf("unlock attempted on a read locked entity: %s", resource)
			continue
		}
		if ok {
			reply = l.removeEntry(resource, args, &lri) || reply
		}
	}
	return reply, err
}

// removeEntry based on the uid of the lock message, removes a single entry from the
// lockRequesterInfo array or the whole array from the map (in case of a write lock
// or last read lock)
// UID and optionally owner must match for entries to be deleted.
func (l *localLocker) removeEntry(name string, args dsync.LockArgs, lri *[]lockRequesterInfo) bool {
	// Find correct entry to remove based on uid.
	for index, entry := range *lri {
		if entry.UID == args.UID && (args.Owner == "" || entry.Owner == args.Owner) {
			if len(*lri) == 1 {
				// Remove the write lock.
				delete(l.lockMap, name)
			} else {
				// Remove the appropriate read lock.
				*lri = append((*lri)[:index], (*lri)[index+1:]...)
				l.lockMap[name] = *lri
			}
			delete(l.lockUID, formatUUID(args.UID, entry.idx))
			return true
		}
	}

	// None found return false, perhaps entry removed in previous run.
	return false
}

func (l *localLocker) RLock(ctx context.Context, args dsync.LockArgs) (reply bool, err error) {
	if len(args.Resources) != 1 {
		return false, fmt.Errorf("internal error: localLocker.RLock called with more than one resource")
	}
	// If we have too many waiting, reject this at once.
	if l.waitMutex.Load() > lockMutexWaitLimit {
		l.locksOverloaded.Add(1)
		return false, nil
	}

	// Wait for mutex
	defer l.getMutex()()
	if ctx.Err() != nil {
		return false, ctx.Err()
	}
	resource := args.Resources[0]
	now := UTCNow()
	lrInfo := lockRequesterInfo{
		Name:            resource,
		Writer:          false,
		Source:          args.Source,
		Owner:           args.Owner,
		UID:             args.UID,
		Timestamp:       now.UnixNano(),
		TimeLastRefresh: now.UnixNano(),
		Quorum:          *args.Quorum,
	}
	lri, ok := l.lockMap[resource]
	if ok {
		if reply = !isWriteLock(lri); reply {
			// Unless there is a write lock
			l.lockMap[resource] = append(l.lockMap[resource], lrInfo)
			l.lockUID[formatUUID(args.UID, 0)] = resource
		}
	} else {
		// No locks held on the given name, so claim (first) read lock
		l.lockMap[resource] = []lockRequesterInfo{lrInfo}
		l.lockUID[formatUUID(args.UID, 0)] = resource
		reply = true
	}
	return reply, nil
}

func (l *localLocker) RUnlock(_ context.Context, args dsync.LockArgs) (reply bool, err error) {
	if len(args.Resources) > 1 {
		return false, fmt.Errorf("internal error: localLocker.RUnlock called with more than one resource")
	}

	defer l.getMutex()()
	var lri []lockRequesterInfo

	resource := args.Resources[0]
	if lri, reply = l.lockMap[resource]; !reply {
		// No lock is held on the given name
		return true, nil
	}
	if isWriteLock(lri) {
		// A write-lock is held, cannot release a read lock
		return false, fmt.Errorf("RUnlock attempted on a write locked entity: %s", resource)
	}
	l.removeEntry(resource, args, &lri)
	return reply, nil
}

type lockStats struct {
	Total          int
	Writes         int
	Reads          int
	LockQueue      int
	LocksAbandoned int
	LastCleanup    *time.Time
}

func (l *localLocker) stats() lockStats {
	return lockStats{
		Total:       len(l.lockMap),
		Reads:       int(l.readers.Load()),
		Writes:      int(l.writers.Load()),
		LockQueue:   int(l.waitMutex.Load()),
		LastCleanup: l.lastCleanup.Load(),
	}
}

type localLockMap map[string][]lockRequesterInfo

func (l *localLocker) DupLockMap() localLockMap {
	defer l.getMutex()()

	lockCopy := make(map[string][]lockRequesterInfo, len(l.lockMap))
	for k, v := range l.lockMap {
		if len(v) == 0 {
			delete(l.lockMap, k)
			continue
		}
		lockCopy[k] = append(make([]lockRequesterInfo, 0, len(v)), v...)
	}
	return lockCopy
}

func (l *localLocker) Close() error {
	return nil
}

// IsOnline - local locker is always online.
func (l *localLocker) IsOnline() bool {
	return true
}

// IsLocal - local locker returns true.
func (l *localLocker) IsLocal() bool {
	return true
}

func (l *localLocker) ForceUnlock(ctx context.Context, args dsync.LockArgs) (reply bool, err error) {
	if ctx.Err() != nil {
		return false, ctx.Err()
	}

	defer l.getMutex()()
	if ctx.Err() != nil {
		return false, ctx.Err()
	}
	if len(args.UID) == 0 {
		for _, resource := range args.Resources {
			lris, ok := l.lockMap[resource]
			if !ok {
				continue
			}
			// Collect uids, so we don't mutate while we delete
			uids := make([]string, 0, len(lris))
			for _, lri := range lris {
				uids = append(uids, lri.UID)
			}

			// Delete collected uids:
			for _, uid := range uids {
				lris, ok := l.lockMap[resource]
				if !ok {
					// Just to be safe, delete uuids.
					for idx := range maxDeleteList {
						mapID := formatUUID(uid, idx)
						if _, ok := l.lockUID[mapID]; !ok {
							break
						}
						delete(l.lockUID, mapID)
					}
					continue
				}
				l.removeEntry(resource, dsync.LockArgs{UID: uid}, &lris)
			}
		}
		return true, nil
	}

	idx := 0
	for {
		mapID := formatUUID(args.UID, idx)
		resource, ok := l.lockUID[mapID]
		if !ok {
			return idx > 0, nil
		}
		lris, ok := l.lockMap[resource]
		if !ok {
			// Unexpected  inconsistency, delete.
			delete(l.lockUID, mapID)
			idx++
			continue
		}
		reply = true
		l.removeEntry(resource, dsync.LockArgs{UID: args.UID}, &lris)
		idx++
	}
}

func (l *localLocker) Refresh(ctx context.Context, args dsync.LockArgs) (refreshed bool, err error) {
	if ctx.Err() != nil {
		return false, ctx.Err()
	}

	defer l.getMutex()()
	if ctx.Err() != nil {
		return false, ctx.Err()
	}

	// Check whether uid is still active.
	resource, ok := l.lockUID[formatUUID(args.UID, 0)]
	if !ok {
		return false, nil
	}
	idx := 0
	for {
		lris, ok := l.lockMap[resource]
		if !ok {
			// Inconsistent. Delete UID.
			delete(l.lockUID, formatUUID(args.UID, idx))
			return idx > 0, nil
		}
		now := UTCNow()
		for i := range lris {
			if lris[i].UID == args.UID {
				lris[i].TimeLastRefresh = now.UnixNano()
			}
		}
		idx++
		resource, ok = l.lockUID[formatUUID(args.UID, idx)]
		if !ok {
			// No more resources for UID, but we did update at least one.
			return true, nil
		}
	}
}

// Similar to removeEntry but only removes an entry only if the lock entry exists in map.
// Caller must hold 'l.mutex' lock.
func (l *localLocker) expireOldLocks(interval time.Duration) {
	defer l.getMutex()()

	var readers, writers int32
	for k, lris := range l.lockMap {
		modified := false
		for i := 0; i < len(lris); {
			lri := &lris[i]
			if time.Since(time.Unix(0, lri.TimeLastRefresh)) > interval {
				delete(l.lockUID, formatUUID(lri.UID, lri.idx))
				if len(lris) == 1 {
					// Remove the write lock.
					delete(l.lockMap, lri.Name)
					modified = false
					break
				}
				modified = true
				// Remove the appropriate lock.
				lris = append(lris[:i], lris[i+1:]...)
				// Check same i
			} else {
				if lri.Writer {
					writers++
				} else {
					readers++
				}
				// Move to next
				i++
			}
		}
		if modified {
			l.lockMap[k] = lris
		}
	}
	t := time.Now()
	l.lastCleanup.Store(&t)
	l.readers.Store(readers)
	l.writers.Store(writers)
}

func newLocker() *localLocker {
	return &localLocker{
		lockMap: make(map[string][]lockRequesterInfo, 1000),
		lockUID: make(map[string]string, 1000),
	}
}

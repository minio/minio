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

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/minio/minio/internal/dsync"
)

// lockRequesterInfo stores various info from the client for each lock that is requested.
type lockRequesterInfo struct {
	Name            string    // name of the resource lock was requested for
	Writer          bool      // Bool whether write or read lock.
	UID             string    // UID to uniquely identify request of client.
	Timestamp       time.Time // Timestamp set at the time of initialization.
	TimeLastRefresh time.Time // Timestamp for last lock refresh.
	Source          string    // Contains line, function and filename reqesting the lock.
	Group           bool      // indicates if it was a group lock.
	// Owner represents the UUID of the owner who originally requested the lock
	// useful in expiry.
	Owner string
	// Quorum represents the quorum required for this lock to be active.
	Quorum int
}

// isWriteLock returns whether the lock is a write or read lock.
func isWriteLock(lri []lockRequesterInfo) bool {
	return len(lri) == 1 && lri[0].Writer
}

// localLocker implements Dsync.NetLocker
type localLocker struct {
	mutex   sync.Mutex
	lockMap map[string][]lockRequesterInfo
}

func (l *localLocker) String() string {
	return globalEndpoints.Localhost()
}

func (l *localLocker) canTakeUnlock(resources ...string) bool {
	var lkCnt int
	for _, resource := range resources {
		isWriteLockTaken := isWriteLock(l.lockMap[resource])
		if isWriteLockTaken {
			lkCnt++
		}
	}
	return lkCnt == len(resources)
}

func (l *localLocker) canTakeLock(resources ...string) bool {
	var noLkCnt int
	for _, resource := range resources {
		_, lockTaken := l.lockMap[resource]
		if !lockTaken {
			noLkCnt++
		}
	}
	return noLkCnt == len(resources)
}

func (l *localLocker) Lock(ctx context.Context, args dsync.LockArgs) (reply bool, err error) {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	if !l.canTakeLock(args.Resources...) {
		// Not all locks can be taken on resources,
		// reject it completely.
		return false, nil
	}

	// No locks held on the all resources, so claim write
	// lock on all resources at once.
	for _, resource := range args.Resources {
		l.lockMap[resource] = []lockRequesterInfo{
			{
				Name:            resource,
				Writer:          true,
				Source:          args.Source,
				Owner:           args.Owner,
				UID:             args.UID,
				Timestamp:       UTCNow(),
				TimeLastRefresh: UTCNow(),
				Group:           len(args.Resources) > 1,
				Quorum:          args.Quorum,
			},
		}
	}
	return true, nil
}

func (l *localLocker) Unlock(_ context.Context, args dsync.LockArgs) (reply bool, err error) {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	if !l.canTakeUnlock(args.Resources...) {
		// Unless it is a write lock reject it.
		return reply, fmt.Errorf("Unlock attempted on a read locked entity: %s", args.Resources)
	}
	for _, resource := range args.Resources {
		lri, ok := l.lockMap[resource]
		if ok {
			l.removeEntry(resource, args, &lri)
		}
	}
	return true, nil

}

// removeEntry based on the uid of the lock message, removes a single entry from the
// lockRequesterInfo array or the whole array from the map (in case of a write lock
// or last read lock)
func (l *localLocker) removeEntry(name string, args dsync.LockArgs, lri *[]lockRequesterInfo) bool {
	// Find correct entry to remove based on uid.
	for index, entry := range *lri {
		if entry.UID == args.UID && entry.Owner == args.Owner {
			if len(*lri) == 1 {
				// Remove the write lock.
				delete(l.lockMap, name)
			} else {
				// Remove the appropriate read lock.
				*lri = append((*lri)[:index], (*lri)[index+1:]...)
				l.lockMap[name] = *lri
			}
			return true
		}
	}

	// None found return false, perhaps entry removed in previous run.
	return false
}

func (l *localLocker) RLock(ctx context.Context, args dsync.LockArgs) (reply bool, err error) {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	resource := args.Resources[0]
	lrInfo := lockRequesterInfo{
		Name:            resource,
		Writer:          false,
		Source:          args.Source,
		Owner:           args.Owner,
		UID:             args.UID,
		Timestamp:       UTCNow(),
		TimeLastRefresh: UTCNow(),
		Quorum:          args.Quorum,
	}
	if lri, ok := l.lockMap[resource]; ok {
		if reply = !isWriteLock(lri); reply {
			// Unless there is a write lock
			l.lockMap[resource] = append(l.lockMap[resource], lrInfo)
		}
	} else {
		// No locks held on the given name, so claim (first) read lock
		l.lockMap[resource] = []lockRequesterInfo{lrInfo}
		reply = true
	}
	return reply, nil
}

func (l *localLocker) RUnlock(_ context.Context, args dsync.LockArgs) (reply bool, err error) {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	var lri []lockRequesterInfo

	resource := args.Resources[0]
	if lri, reply = l.lockMap[resource]; !reply {
		// No lock is held on the given name
		return true, nil
	}
	if reply = !isWriteLock(lri); !reply {
		// A write-lock is held, cannot release a read lock
		return reply, fmt.Errorf("RUnlock attempted on a write locked entity: %s", resource)
	}
	l.removeEntry(resource, args, &lri)
	return reply, nil
}

func (l *localLocker) DupLockMap() map[string][]lockRequesterInfo {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	lockCopy := map[string][]lockRequesterInfo{}
	for k, v := range l.lockMap {
		lockCopy[k] = append(lockCopy[k], v...)
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
	select {
	case <-ctx.Done():
		return false, ctx.Err()
	default:
		l.mutex.Lock()
		defer l.mutex.Unlock()
		if len(args.UID) != 0 {
			return false, fmt.Errorf("ForceUnlock called with non-empty UID: %s", args.UID)
		}
		for _, resource := range args.Resources {
			delete(l.lockMap, resource) // Remove the lock (irrespective of write or read lock)
		}
		return true, nil
	}
}

func (l *localLocker) Refresh(ctx context.Context, args dsync.LockArgs) (refreshed bool, err error) {
	select {
	case <-ctx.Done():
		return false, ctx.Err()
	default:
		l.mutex.Lock()
		defer l.mutex.Unlock()

		resource := args.Resources[0] // refresh check is always per resource.

		// Lock found, proceed to verify if belongs to given uid.
		lri, ok := l.lockMap[resource]
		if !ok {
			// lock doesn't exist yet, return false
			return false, nil
		}

		// Check whether uid is still active
		for i := range lri {
			if lri[i].UID == args.UID && lri[i].Owner == args.Owner {
				lri[i].TimeLastRefresh = UTCNow()
				return true, nil
			}
		}

		return false, nil
	}
}

// Similar to removeEntry but only removes an entry only if the lock entry exists in map.
// Caller must hold 'l.mutex' lock.
func (l *localLocker) expireOldLocks(interval time.Duration) {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	for _, lris := range l.lockMap {
		for _, lri := range lris {
			if time.Since(lri.TimeLastRefresh) > interval {
				l.removeEntry(lri.Name, dsync.LockArgs{Owner: lri.Owner, UID: lri.UID}, &lris)
			}
		}
	}
}

func newLocker() *localLocker {
	return &localLocker{
		lockMap: make(map[string][]lockRequesterInfo),
	}
}

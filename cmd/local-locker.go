/*
 * MinIO Cloud Storage, (C) 2018, 2019 MinIO, Inc.
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
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/minio/minio/pkg/dsync"
)

// lockRequesterInfo stores various info from the client for each lock that is requested.
type lockRequesterInfo struct {
	Writer        bool      // Bool whether write or read lock.
	UID           string    // UID to uniquely identify request of client.
	Timestamp     time.Time // Timestamp set at the time of initialization.
	TimeLastCheck time.Time // Timestamp for last check of validity of lock.
	Source        string    // Contains line, function and filename reqesting the lock.
	// Owner represents the UUID of the owner who originally requested the lock
	// useful in expiry.
	Owner string
}

// isWriteLock returns whether the lock is a write or read lock.
func isWriteLock(lri []lockRequesterInfo) bool {
	return len(lri) == 1 && lri[0].Writer
}

// localLocker implements Dsync.NetLocker
type localLocker struct {
	mutex    sync.Mutex
	endpoint Endpoint
	lockMap  map[string][]lockRequesterInfo
}

func (l *localLocker) String() string {
	return l.endpoint.String()
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
				Writer:        true,
				Source:        args.Source,
				Owner:         args.Owner,
				UID:           args.UID,
				Timestamp:     UTCNow(),
				TimeLastCheck: UTCNow(),
			},
		}
	}
	return true, nil
}

func (l *localLocker) Unlock(args dsync.LockArgs) (reply bool, err error) {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	if !l.canTakeUnlock(args.Resources...) {
		// Unless it is a write lock reject it.
		return reply, fmt.Errorf("Unlock attempted on a read locked entity: %s", args.Resources)
	}
	for _, resource := range args.Resources {
		lri := l.lockMap[resource]
		if !l.removeEntry(resource, args, &lri) {
			return false, fmt.Errorf("Unlock unable to find corresponding lock for uid: %s on resource %s", args.UID, resource)
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
	lrInfo := lockRequesterInfo{
		Writer:        false,
		Source:        args.Source,
		Owner:         args.Owner,
		UID:           args.UID,
		Timestamp:     UTCNow(),
		TimeLastCheck: UTCNow(),
	}
	resource := args.Resources[0]
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

func (l *localLocker) RUnlock(args dsync.LockArgs) (reply bool, err error) {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	var lri []lockRequesterInfo

	resource := args.Resources[0]
	if lri, reply = l.lockMap[resource]; !reply {
		// No lock is held on the given name
		return reply, fmt.Errorf("RUnlock attempted on an unlocked entity: %s", resource)
	}
	if reply = !isWriteLock(lri); !reply {
		// A write-lock is held, cannot release a read lock
		return reply, fmt.Errorf("RUnlock attempted on a write locked entity: %s", resource)
	}
	if !l.removeEntry(resource, args, &lri) {
		return false, fmt.Errorf("RUnlock unable to find corresponding read lock for uid: %s", args.UID)
	}
	return reply, nil
}

func (l *localLocker) DupLockMap() map[string][]lockRequesterInfo {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	lockCopy := make(map[string][]lockRequesterInfo)
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

func (l *localLocker) Expired(ctx context.Context, args dsync.LockArgs) (expired bool, err error) {
	select {
	case <-ctx.Done():
		return false, ctx.Err()
	default:
		l.mutex.Lock()
		defer l.mutex.Unlock()

		// Lock found, proceed to verify if belongs to given uid.
		for _, resource := range args.Resources {
			if lri, ok := l.lockMap[resource]; ok {
				// Check whether uid is still active
				for _, entry := range lri {
					if entry.UID == args.UID && entry.Owner == args.Owner {
						return false, nil
					}
				}
			}
		}
		return true, nil
	}
}

// Similar to removeEntry but only removes an entry only if the lock entry exists in map.
// Caller must hold 'l.mutex' lock.
func (l *localLocker) removeEntryIfExists(nlrip nameLockRequesterInfoPair) {
	// Check if entry is still in map (could have been removed altogether by 'concurrent' (R)Unlock of last entry)
	if lri, ok := l.lockMap[nlrip.name]; ok {
		// Even if the entry exists, it may not be the same entry which was
		// considered as expired, so we simply an attempt to remove it if its
		// not possible there is nothing we need to do.
		l.removeEntry(nlrip.name, dsync.LockArgs{Owner: nlrip.lri.Owner, UID: nlrip.lri.UID}, &lri)
	}
}

func newLocker(endpoint Endpoint) *localLocker {
	return &localLocker{
		endpoint: endpoint,
		lockMap:  make(map[string][]lockRequesterInfo),
	}
}

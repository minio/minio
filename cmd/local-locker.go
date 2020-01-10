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

func (l *localLocker) Lock(args dsync.LockArgs) (reply bool, err error) {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	_, isLockTaken := l.lockMap[args.Resource]
	if !isLockTaken { // No locks held on the given name, so claim write lock
		l.lockMap[args.Resource] = []lockRequesterInfo{
			{
				Writer:        true,
				Source:        args.Source,
				UID:           args.UID,
				Timestamp:     UTCNow(),
				TimeLastCheck: UTCNow(),
			},
		}
	}
	// return reply=true if lock was granted.
	return !isLockTaken, nil
}

func (l *localLocker) Unlock(args dsync.LockArgs) (reply bool, err error) {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	var lri []lockRequesterInfo
	if lri, reply = l.lockMap[args.Resource]; !reply {
		// No lock is held on the given name
		return reply, fmt.Errorf("Unlock attempted on an unlocked entity: %s", args.Resource)
	}
	if reply = isWriteLock(lri); !reply {
		// Unless it is a write lock
		return reply, fmt.Errorf("Unlock attempted on a read locked entity: %s (%d read locks active)", args.Resource, len(lri))
	}
	if !l.removeEntry(args.Resource, args.UID, &lri) {
		return false, fmt.Errorf("Unlock unable to find corresponding lock for uid: %s", args.UID)
	}
	return true, nil

}

// removeEntry based on the uid of the lock message, removes a single entry from the
// lockRequesterInfo array or the whole array from the map (in case of a write lock
// or last read lock)
func (l *localLocker) removeEntry(name, uid string, lri *[]lockRequesterInfo) bool {
	// Find correct entry to remove based on uid.
	for index, entry := range *lri {
		if entry.UID == uid {
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

func (l *localLocker) RLock(args dsync.LockArgs) (reply bool, err error) {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	lrInfo := lockRequesterInfo{
		Writer:        false,
		Source:        args.Source,
		UID:           args.UID,
		Timestamp:     UTCNow(),
		TimeLastCheck: UTCNow(),
	}
	if lri, ok := l.lockMap[args.Resource]; ok {
		if reply = !isWriteLock(lri); reply {
			// Unless there is a write lock
			l.lockMap[args.Resource] = append(l.lockMap[args.Resource], lrInfo)
		}
	} else {
		// No locks held on the given name, so claim (first) read lock
		l.lockMap[args.Resource] = []lockRequesterInfo{lrInfo}
		reply = true
	}
	return reply, nil
}

func (l *localLocker) RUnlock(args dsync.LockArgs) (reply bool, err error) {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	var lri []lockRequesterInfo
	if lri, reply = l.lockMap[args.Resource]; !reply {
		// No lock is held on the given name
		return reply, fmt.Errorf("RUnlock attempted on an unlocked entity: %s", args.Resource)
	}
	if reply = !isWriteLock(lri); !reply {
		// A write-lock is held, cannot release a read lock
		return reply, fmt.Errorf("RUnlock attempted on a write locked entity: %s", args.Resource)
	}
	if !l.removeEntry(args.Resource, args.UID, &lri) {
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

// Local locker is always online.
func (l *localLocker) IsOnline() bool {
	return true
}

func (l *localLocker) Expired(args dsync.LockArgs) (expired bool, err error) {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	// Lock found, proceed to verify if belongs to given uid.
	if lri, ok := l.lockMap[args.Resource]; ok {
		// Check whether uid is still active
		for _, entry := range lri {
			if entry.UID == args.UID {
				return false, nil
			}
		}
	}
	return true, nil
}

// Similar to removeEntry but only removes an entry only if the lock entry exists in map.
// Caller must hold 'l.mutex' lock.
func (l *localLocker) removeEntryIfExists(nlrip nameLockRequesterInfoPair) {
	// Check if entry is still in map (could have been removed altogether by 'concurrent' (R)Unlock of last entry)
	if lri, ok := l.lockMap[nlrip.name]; ok {
		// Even if the entry exists, it may not be the same entry which was
		// considered as expired, so we simply an attempt to remove it if its
		// not possible there is nothing we need to do.
		l.removeEntry(nlrip.name, nlrip.lri.UID, &lri)
	}
}

func newLocker(endpoint Endpoint) *localLocker {
	return &localLocker{
		endpoint: endpoint,
		lockMap:  make(map[string][]lockRequesterInfo),
	}
}

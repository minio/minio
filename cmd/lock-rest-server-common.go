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
 */

package cmd

import (
	"errors"
	"path"
	"time"
)

const lockRESTVersion = "v2"
const lockRESTPath = minioReservedBucketPath + "/lock/" + lockRESTVersion

var lockServicePath = path.Join(minioReservedBucketPath, lockServiceSubPath)

const (
	lockRESTMethodLock        = "lock"
	lockRESTMethodRLock       = "rlock"
	lockRESTMethodUnlock      = "unlock"
	lockRESTMethodRUnlock     = "runlock"
	lockRESTMethodForceUnlock = "forceunlock"
	lockRESTMethodExpired     = "expired"

	// Unique ID of lock/unlock request.
	lockRESTUID = "uid"
	// Source contains the line number, function and file name of the code
	// on the client node that requested the lock.
	lockRESTSource = "source"
	// Resource contains a entity to be locked/unlocked.
	lockRESTResource = "resource"
	// ServerAddr contains the address of the server who requested lock/unlock of the above resource.
	lockRESTServerAddr = "serverAddr"
	// ServiceEndpoint contains the network path of above server to do lock/unlock.
	lockRESTServerEndpoint = "serverEndpoint"
)

// nameLockRequesterInfoPair is a helper type for lock maintenance
type nameLockRequesterInfoPair struct {
	name string
	lri  lockRequesterInfo
}

var errLockConflict = errors.New("lock conflict")
var errLockNotExpired = errors.New("lock not expired")

// Similar to removeEntry but only removes an entry only if the lock entry exists in map.
func (l *localLocker) removeEntryIfExists(nlrip nameLockRequesterInfoPair) {
	// Check if entry is still in map (could have been removed altogether by 'concurrent' (R)Unlock of last entry)
	if lri, ok := l.lockMap[nlrip.name]; ok {
		// Even if the entry exists, it may not be the same entry which was
		// considered as expired, so we simply an attempt to remove it if its
		// not possible there is nothing we need to do.
		l.removeEntry(nlrip.name, nlrip.lri.UID, &lri)
	}
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

// getLongLivedLocks returns locks that are older than a certain time and
// have not been 'checked' for validity too soon enough
func getLongLivedLocks(m map[string][]lockRequesterInfo, interval time.Duration) []nameLockRequesterInfoPair {
	rslt := []nameLockRequesterInfoPair{}
	for name, lriArray := range m {
		for idx := range lriArray {
			// Check whether enough time has gone by since last check
			if time.Since(lriArray[idx].TimeLastCheck) >= interval {
				rslt = append(rslt, nameLockRequesterInfoPair{name: name, lri: lriArray[idx]})
				lriArray[idx].TimeLastCheck = UTCNow()
			}
		}
	}
	return rslt
}

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
	"context"
	"errors"
	"time"

	"github.com/minio/minio/cmd/logger"
)

// Similar to removeEntry but only removes an entry only if the lock entry exists in map.
func (l *localLocker) removeEntryIfExists(nlrip nameLockRequesterInfoPair) {
	// Check if entry is still in map (could have been removed altogether by 'concurrent' (R)Unlock of last entry)
	if lri, ok := l.lockMap[nlrip.name]; ok {
		if !l.removeEntry(nlrip.name, nlrip.lri.uid, &lri) {
			// Remove failed, in case it is a:
			if nlrip.lri.writer {
				// Writer: this should never happen as the whole (mapped) entry should have been deleted
				reqInfo := (&logger.ReqInfo{}).AppendTags("name", nlrip.name)
				reqInfo.AppendTags("uid", nlrip.lri.uid)
				ctx := logger.SetReqInfo(context.Background(), reqInfo)
				logger.LogIf(ctx, errors.New("Lock maintenance failed to remove entry for write lock (should never happen)"))
			} // Reader: this can happen if multiple read locks were active and
			// the one we are looking for has been released concurrently (so it is fine).
		} // Removal went okay, all is fine.
	}
}

// removeEntry either, based on the uid of the lock message, removes a single entry from the
// lockRequesterInfo array or the whole array from the map (in case of a write lock or last read lock)
func (l *localLocker) removeEntry(name, uid string, lri *[]lockRequesterInfo) bool {
	// Find correct entry to remove based on uid.
	for index, entry := range *lri {
		if entry.uid == uid {
			if len(*lri) == 1 {
				// Remove the (last) lock.
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
			if time.Since(lriArray[idx].timeLastCheck) >= interval {
				rslt = append(rslt, nameLockRequesterInfoPair{name: name, lri: lriArray[idx]})
				lriArray[idx].timeLastCheck = UTCNow()
			}
		}
	}
	return rslt
}

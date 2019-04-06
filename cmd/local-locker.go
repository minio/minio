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

	"github.com/minio/dsync"
)

// lockRequesterInfo stores various info from the client for each lock that is requested.
type lockRequesterInfo struct {
	Writer          bool      // Bool whether write or read lock.
	Node            string    // Network address of client claiming lock.
	ServiceEndpoint string    // RPC path of client claiming lock.
	UID             string    // UID to uniquely identify request of client.
	Timestamp       time.Time // Timestamp set at the time of initialization.
	TimeLastCheck   time.Time // Timestamp for last check of validity of lock.
	Source          string    // Contains line, function and filename reqesting the lock.
}

// isWriteLock returns whether the lock is a write or read lock.
func isWriteLock(lri []lockRequesterInfo) bool {
	return len(lri) == 1 && lri[0].Writer
}

// localLocker implements Dsync.NetLocker
type localLocker struct {
	mutex           sync.Mutex
	serviceEndpoint string
	serverAddr      string
	lockMap         map[string][]lockRequesterInfo
}

func (l *localLocker) ServerAddr() string {
	return l.serverAddr
}

func (l *localLocker) ServiceEndpoint() string {
	return l.serviceEndpoint
}

func (l *localLocker) Lock(args dsync.LockArgs) (reply bool, err error) {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	_, isLockTaken := l.lockMap[args.Resource]
	if !isLockTaken { // No locks held on the given name, so claim write lock
		l.lockMap[args.Resource] = []lockRequesterInfo{
			{
				Writer:          true,
				Node:            args.ServerAddr,
				ServiceEndpoint: args.ServiceEndpoint,
				Source:          args.Source,
				UID:             args.UID,
				Timestamp:       UTCNow(),
				TimeLastCheck:   UTCNow(),
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

func (l *localLocker) RLock(args dsync.LockArgs) (reply bool, err error) {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	lrInfo := lockRequesterInfo{
		Writer:          false,
		Node:            args.ServerAddr,
		ServiceEndpoint: args.ServiceEndpoint,
		Source:          args.Source,
		UID:             args.UID,
		Timestamp:       UTCNow(),
		TimeLastCheck:   UTCNow(),
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

func (l *localLocker) ForceUnlock(args dsync.LockArgs) (reply bool, err error) {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	if len(args.UID) != 0 {
		return false, fmt.Errorf("ForceUnlock called with non-empty UID: %s", args.UID)
	}
	// Only clear lock when it is taken
	// Remove the lock (irrespective of write or read lock)
	delete(l.lockMap, args.Resource)
	return true, nil
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

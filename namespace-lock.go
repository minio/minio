/*
 * Minio Cloud Storage, (C) 2016 Minio, Inc.
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

package main

import (
	"errors"
	"path"
	"strconv"
	"strings"
	"sync"

	"github.com/minio/dsync"
)

// Global name space lock.
var nsMutex *nsLockMap

// Initialize distributed locking only in case of distributed setup.
// Returns if the setup is distributed or not on success.
func initDsyncNodes(disks []string, port int) (bool, error) {
	var isDist = false
	var dsyncNodes []string
	var rpcPaths []string
	serverPort := strconv.Itoa(port)

	for _, disk := range disks {
		if idx := strings.LastIndex(disk, ":"); idx != -1 {
			dsyncNodes = append(dsyncNodes, disk[:idx]+":"+serverPort)
			rpcPaths = append(rpcPaths, path.Join(lockRPCPath, disk[idx+1:]))
		}
		if !isLocalStorage(disk) {
			// One or more disks supplied as arguments are remote.
			isDist = true
		}
	}
	if isDist {
		return isDist, dsync.SetNodesWithPath(dsyncNodes, rpcPaths)
	}
	return isDist, nil
}

// initNSLock - initialize name space lock map.
func initNSLock(isDist bool) {
	nsMutex = &nsLockMap{
		isDist:  isDist,
		lockMap: make(map[nsParam]*nsLock),
	}
}

// RWLocker - interface that any read-write locking library should implement.
type RWLocker interface {
	sync.Locker
	RLock()
	RUnlock()
}

// nsParam - carries name space resource.
type nsParam struct {
	volume string
	path   string
}

// nsLock - provides primitives for locking critical namespace regions.
type nsLock struct {
	rwlock RWLocker
	ref    uint
}

// nsLockMap - namespace lock map, provides primitives to Lock,
// Unlock, RLock and RUnlock.
type nsLockMap struct {
	isDist  bool
	lockMap map[nsParam]*nsLock
	mutex   sync.Mutex
}

// Lock the namespace resource.
func (n *nsLockMap) lock(volume, path string, readLock bool) {
	var nsLk *nsLock
	n.mutex.Lock()

	param := nsParam{volume, path}
	nsLk, found := n.lockMap[param]
	if !found {
		if n.isDist {
			nsLk = &nsLock{
				rwlock: dsync.NewDRWMutex(volume + path),
				ref:    0,
			}
		} else {
			nsLk = &nsLock{
				rwlock: &sync.RWMutex{},
				ref:    0,
			}
		}
		n.lockMap[param] = nsLk
	}
	nsLk.ref++ // Update ref count here to avoid multiple races.
	// Unlock map before Locking NS which might block.
	n.mutex.Unlock()

	// Locking here can block.
	if readLock {
		nsLk.rwlock.RLock()
	} else {
		nsLk.rwlock.Lock()
	}
}

// Unlock the namespace resource.
func (n *nsLockMap) unlock(volume, path string, readLock bool) {
	// nsLk.Unlock() will not block, hence locking the map for the entire function is fine.
	n.mutex.Lock()
	defer n.mutex.Unlock()

	param := nsParam{volume, path}
	if nsLk, found := n.lockMap[param]; found {
		if readLock {
			nsLk.rwlock.RUnlock()
		} else {
			nsLk.rwlock.Unlock()
		}
		if nsLk.ref == 0 {
			errorIf(errors.New("Namespace reference count cannot be 0."), "Invalid reference count detected.")
		}
		if nsLk.ref != 0 {
			nsLk.ref--
		}
		if nsLk.ref == 0 {
			// Remove from the map if there are no more references.
			delete(n.lockMap, param)
		}
	}
}

// Lock - locks the given resource for writes, using a previously
// allocated name space lock or initializing a new one.
func (n *nsLockMap) Lock(volume, path string) {
	readLock := false
	n.lock(volume, path, readLock)
}

// Unlock - unlocks any previously acquired write locks.
func (n *nsLockMap) Unlock(volume, path string) {
	readLock := false
	n.unlock(volume, path, readLock)
}

// RLock - locks any previously acquired read locks.
func (n *nsLockMap) RLock(volume, path string) {
	readLock := true
	n.lock(volume, path, readLock)
}

// RUnlock - unlocks any previously acquired read locks.
func (n *nsLockMap) RUnlock(volume, path string) {
	readLock := true
	n.unlock(volume, path, readLock)
}

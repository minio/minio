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

package cmd

import (
	"errors"
	"net/url"
	pathutil "path"
	"sync"

	"github.com/minio/dsync"
)

// Global name space lock.
var nsMutex *nsLockMap

// Initialize distributed locking only in case of distributed setup.
// Returns if the setup is distributed or not on success.
func initDsyncNodes(eps []*url.URL) error {
	cred := serverConfig.GetCredential()
	// Initialize rpc lock client information only if this instance is a distributed setup.
	clnts := make([]dsync.RPC, len(eps))
	myNode := -1
	for index, ep := range eps {
		if ep == nil {
			return errInvalidArgument
		}
		clnts[index] = newAuthClient(&authConfig{
			accessKey: cred.AccessKeyID,
			secretKey: cred.SecretAccessKey,
			// Construct a new dsync server addr.
			secureConn: isSSL(),
			address:    ep.Host,
			// Construct a new rpc path for the endpoint.
			path:        pathutil.Join(lockRPCPath, getPath(ep)),
			loginMethod: "Dsync.LoginHandler",
		})
		if isLocalStorage(ep) && myNode == -1 {
			myNode = index
		}
	}
	return dsync.SetNodesWithClients(clnts, myNode)
}

// initNSLock - initialize name space lock map.
func initNSLock(isDist bool) {
	nsMutex = &nsLockMap{
		isDist:  isDist,
		lockMap: make(map[nsParam]*nsLock),
	}

	// Initialize nsLockMap with entry for instrumentation information.
	// Entries of <volume,path> -> stateInfo of locks
	nsMutex.debugLockMap = make(map[nsParam]*debugLockInfoPerVolumePath)
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
	RWLocker
	ref uint
}

// nsLockMap - namespace lock map, provides primitives to Lock,
// Unlock, RLock and RUnlock.
type nsLockMap struct {
	// Lock counter used for lock debugging.
	globalLockCounter  int64                                   // Total locks held.
	blockedCounter     int64                                   // Total operations blocked waiting for locks.
	runningLockCounter int64                                   // Total locks held but not released yet.
	debugLockMap       map[nsParam]*debugLockInfoPerVolumePath // Info for instrumentation on locks.

	// Indicates whether the locking service is part
	// of a distributed setup or not.
	isDist       bool
	lockMap      map[nsParam]*nsLock
	lockMapMutex sync.Mutex
}

// Lock the namespace resource.
func (n *nsLockMap) lock(volume, path string, lockSource, opsID string, readLock bool) {
	var nsLk *nsLock
	n.lockMapMutex.Lock()

	param := nsParam{volume, path}
	nsLk, found := n.lockMap[param]
	if !found {
		nsLk = &nsLock{
			RWLocker: func() RWLocker {
				if n.isDist {
					return dsync.NewDRWMutex(pathutil.Join(volume, path))
				}
				return &sync.RWMutex{}
			}(),
			ref: 0,
		}
		n.lockMap[param] = nsLk
	}
	nsLk.ref++ // Update ref count here to avoid multiple races.

	// Change the state of the lock to be blocked for the given
	// pair of <volume, path> and <OperationID> till the lock
	// unblocks. The lock for accessing `nsMutex` is held inside
	// the function itself.
	if err := n.statusNoneToBlocked(param, lockSource, opsID, readLock); err != nil {
		errorIf(err, "Failed to set lock state to blocked")
	}

	// Unlock map before Locking NS which might block.
	n.lockMapMutex.Unlock()

	// Locking here can block.
	if readLock {
		nsLk.RLock()
	} else {
		nsLk.Lock()
	}

	// Changing the status of the operation from blocked to
	// running.  change the state of the lock to be running (from
	// blocked) for the given pair of <volume, path> and <OperationID>.
	if err := n.statusBlockedToRunning(param, lockSource, opsID, readLock); err != nil {
		errorIf(err, "Failed to set the lock state to running")
	}
}

// Unlock the namespace resource.
func (n *nsLockMap) unlock(volume, path, opsID string, readLock bool) {
	// nsLk.Unlock() will not block, hence locking the map for the
	// entire function is fine.
	n.lockMapMutex.Lock()
	defer n.lockMapMutex.Unlock()

	param := nsParam{volume, path}
	if nsLk, found := n.lockMap[param]; found {
		if readLock {
			nsLk.RUnlock()
		} else {
			nsLk.Unlock()
		}
		if nsLk.ref == 0 {
			errorIf(errors.New("Namespace reference count cannot be 0"),
				"Invalid reference count detected")
		}
		if nsLk.ref != 0 {
			nsLk.ref--

			// delete the lock state entry for given operation ID.
			err := n.deleteLockInfoEntryForOps(param, opsID)
			if err != nil {
				errorIf(err, "Failed to delete lock info entry")
			}
		}
		if nsLk.ref == 0 {
			// Remove from the map if there are no more references.
			delete(n.lockMap, param)

			// delete the lock state entry for given
			// <volume, path> pair.
			err := n.deleteLockInfoEntryForVolumePath(param)
			if err != nil {
				errorIf(err, "Failed to delete lock info entry")
			}
		}
	}
}

// Lock - locks the given resource for writes, using a previously
// allocated name space lock or initializing a new one.
func (n *nsLockMap) Lock(volume, path, opsID string) {
	readLock := false // This is a write lock.

	lockSource := callerSource() // Useful for debugging
	n.lock(volume, path, lockSource, opsID, readLock)
}

// Unlock - unlocks any previously acquired write locks.
func (n *nsLockMap) Unlock(volume, path, opsID string) {
	readLock := false
	n.unlock(volume, path, opsID, readLock)
}

// RLock - locks any previously acquired read locks.
func (n *nsLockMap) RLock(volume, path, opsID string) {
	readLock := true

	lockSource := callerSource() // Useful for debugging
	n.lock(volume, path, lockSource, opsID, readLock)
}

// RUnlock - unlocks any previously acquired read locks.
func (n *nsLockMap) RUnlock(volume, path, opsID string) {
	readLock := true
	n.unlock(volume, path, opsID, readLock)
}

// ForceUnlock - forcefully unlock a lock based on name.
func (n *nsLockMap) ForceUnlock(volume, path string) {
	n.lockMapMutex.Lock()
	defer n.lockMapMutex.Unlock()

	// Clarification on operation:
	// - In case of FS or XL we call ForceUnlock on the local nsMutex
	//   (since there is only a single server) which will cause the 'stuck'
	//   mutex to be removed from the map. Existing operations for this
	//   will continue to be blocked (and timeout). New operations on this
	//   resource will use a new mutex and proceed normally.
	//
	// - In case of Distributed setup (using dsync), there is no need to call
	//   ForceUnlock on the server where the lock was acquired and is presumably
	//   'stuck'. Instead dsync.ForceUnlock() will release the underlying locks
	//   that participated in granting the lock. Any pending dsync locks that
	//   are blocking can now proceed as normal and any new locks will also
	//   participate normally.

	if n.isDist { // For distributed mode, broadcast ForceUnlock message.
		dsync.NewDRWMutex(pathutil.Join(volume, path)).ForceUnlock()
	}

	param := nsParam{volume, path}
	if _, found := n.lockMap[param]; found {
		// Remove lock from the map.
		delete(n.lockMap, param)

		// delete the lock state entry for given
		// <volume, path> pair.
		err := n.deleteLockInfoEntryForVolumePath(param)
		if err != nil {
			errorIf(err, "Failed to delete lock info entry")
		}
	}
}

// lockInstance - frontend/top-level interface for namespace locks.
type lockInstance struct {
	n                   *nsLockMap
	volume, path, opsID string
}

// NewNSLock - returns a lock instance for a given volume and
// path. The returned lockInstance object encapsulates the nsLockMap,
// volume, path and operation ID.
func (n *nsLockMap) NewNSLock(volume, path string) *lockInstance {
	return &lockInstance{n, volume, path, getOpsID()}
}

// Lock - block until write lock is taken.
func (li *lockInstance) Lock() {
	lockSource := callerSource()
	readLock := false
	li.n.lock(li.volume, li.path, lockSource, li.opsID, readLock)
}

// Unlock - block until write lock is released.
func (li *lockInstance) Unlock() {
	readLock := false
	li.n.unlock(li.volume, li.path, li.opsID, readLock)
}

// RLock - block until read lock is taken.
func (li *lockInstance) RLock() {
	lockSource := callerSource()
	readLock := true
	li.n.lock(li.volume, li.path, lockSource, li.opsID, readLock)
}

// RUnlock - block until read lock is released.
func (li *lockInstance) RUnlock() {
	readLock := true
	li.n.unlock(li.volume, li.path, li.opsID, readLock)
}

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
	pathutil "path"
	"runtime"
	"strings"
	"sync"

	"fmt"
	"time"

	"github.com/minio/dsync"
	"github.com/minio/lsync"
	"github.com/minio/minio-go/pkg/set"
)

// Global name space lock.
var globalNSMutex *nsLockMap

// Global lock server one per server.
var globalLockServer *lockServer

// Instance of dsync for distributed clients.
var globalDsync *dsync.Dsync

// RWLocker - locker interface to introduce GetRLock, RUnlock.
type RWLocker interface {
	GetLock(timeout *dynamicTimeout) (timedOutErr error)
	Unlock()
	GetRLock(timeout *dynamicTimeout) (timedOutErr error)
	RUnlock()
}

// RWLockerSync - internal locker interface.
type RWLockerSync interface {
	GetLock(timeout time.Duration) bool
	Unlock()
	GetRLock(timeout time.Duration) bool
	RUnlock()
}

// Initialize distributed locking only in case of distributed setup.
// Returns lock clients and the node index for the current server.
func newDsyncNodes(endpoints EndpointList) (clnts []dsync.NetLocker, myNode int) {
	cred := globalServerConfig.GetCredential()
	myNode = -1
	seenHosts := set.NewStringSet()
	for _, endpoint := range endpoints {
		if seenHosts.Contains(endpoint.Host) {
			continue
		}
		seenHosts.Add(endpoint.Host)
		if !endpoint.IsLocal {
			// For a remote endpoints setup a lock RPC client.
			clnts = append(clnts, newLockRPCClient(authConfig{
				accessKey:       cred.AccessKey,
				secretKey:       cred.SecretKey,
				serverAddr:      endpoint.Host,
				secureConn:      globalIsSSL,
				serviceEndpoint: pathutil.Join(minioReservedBucketPath, lockServicePath),
				serviceName:     lockServiceName,
			}))
			continue
		}

		// Local endpoint
		myNode = len(clnts)

		// For a local endpoint, setup a local lock server to
		// avoid network requests.
		localLockServer := lockServer{
			AuthRPCServer: AuthRPCServer{},
			ll: localLocker{
				serverAddr:      endpoint.Host,
				serviceEndpoint: pathutil.Join(minioReservedBucketPath, lockServicePath),
				lockMap:         make(map[string][]lockRequesterInfo),
			},
		}
		globalLockServer = &localLockServer
		clnts = append(clnts, &(localLockServer.ll))
	}

	return clnts, myNode
}

// newNSLock - return a new name space lock map.
func newNSLock(isDistXL bool) *nsLockMap {
	nsMutex := nsLockMap{
		isDistXL: isDistXL,
		lockMap:  make(map[nsParam]*nsLock),
		counters: &lockStat{},
	}

	// Initialize nsLockMap with entry for instrumentation information.
	// Entries of <volume,path> -> stateInfo of locks
	nsMutex.debugLockMap = make(map[nsParam]*debugLockInfoPerVolumePath)
	return &nsMutex
}

// initNSLock - initialize name space lock map.
func initNSLock(isDistXL bool) {
	globalNSMutex = newNSLock(isDistXL)
}

// nsParam - carries name space resource.
type nsParam struct {
	volume string
	path   string
}

// nsLock - provides primitives for locking critical namespace regions.
type nsLock struct {
	RWLockerSync
	ref uint
}

// nsLockMap - namespace lock map, provides primitives to Lock,
// Unlock, RLock and RUnlock.
type nsLockMap struct {
	// Lock counter used for lock debugging.
	counters     *lockStat
	debugLockMap map[nsParam]*debugLockInfoPerVolumePath // Info for instrumentation on locks.

	// Indicates if namespace is part of a distributed setup.
	isDistXL     bool
	lockMap      map[nsParam]*nsLock
	lockMapMutex sync.Mutex
}

// Lock the namespace resource.
func (n *nsLockMap) lock(volume, path string, lockSource, opsID string, readLock bool, timeout time.Duration) (locked bool) {
	var nsLk *nsLock
	n.lockMapMutex.Lock()

	param := nsParam{volume, path}
	nsLk, found := n.lockMap[param]
	if !found {
		nsLk = &nsLock{
			RWLockerSync: func() RWLockerSync {
				if n.isDistXL {
					return dsync.NewDRWMutex(pathJoin(volume, path), globalDsync)
				}
				return &lsync.LRWMutex{}
			}(),
			ref: 0,
		}
		n.lockMap[param] = nsLk
	}
	nsLk.ref++ // Update ref count here to avoid multiple races.

	// Change the state of the lock to be blocked for the given
	// pair of <volume, path> and <OperationID> till the lock
	// unblocks. The lock for accessing `globalNSMutex` is held inside
	// the function itself.
	if err := n.statusNoneToBlocked(param, lockSource, opsID, readLock); err != nil {
		errorIf(err, fmt.Sprintf("Failed to set lock state to blocked (param = %v; opsID = %s)", param, opsID))
	}

	// Unlock map before Locking NS which might block.
	n.lockMapMutex.Unlock()

	// Locking here will block (until timeout).
	if readLock {
		locked = nsLk.GetRLock(timeout)
	} else {
		locked = nsLk.GetLock(timeout)
	}

	if !locked { // We failed to get the lock
		n.lockMapMutex.Lock()
		defer n.lockMapMutex.Unlock()
		// Changing the status of the operation from blocked to none
		if err := n.statusBlockedToNone(param, lockSource, opsID, readLock); err != nil {
			errorIf(err, fmt.Sprintf("Failed to clear the lock state (param = %v; opsID = %s)", param, opsID))
		}

		nsLk.ref-- // Decrement ref count since we failed to get the lock
		// delete the lock state entry for given operation ID.
		err := n.deleteLockInfoEntryForOps(param, opsID)
		if err != nil {
			errorIf(err, fmt.Sprintf("Failed to delete lock info entry (param = %v; opsID = %s)", param, opsID))
		}
		if nsLk.ref == 0 {
			// Remove from the map if there are no more references.
			delete(n.lockMap, param)

			// delete the lock state entry for given
			// <volume, path> pair.
			err := n.deleteLockInfoEntryForVolumePath(param)
			if err != nil {
				errorIf(err, fmt.Sprintf("Failed to delete lock info entry (param = %v)", param))
			}
		}
		return
	}

	// Changing the status of the operation from blocked to
	// running.  change the state of the lock to be running (from
	// blocked) for the given pair of <volume, path> and <OperationID>.
	if err := n.statusBlockedToRunning(param, lockSource, opsID, readLock); err != nil {
		errorIf(err, "Failed to set the lock state to running")
	}
	return
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
func (n *nsLockMap) Lock(volume, path, opsID string, timeout time.Duration) (locked bool) {
	readLock := false // This is a write lock.

	lockSource := getSource() // Useful for debugging
	return n.lock(volume, path, lockSource, opsID, readLock, timeout)
}

// Unlock - unlocks any previously acquired write locks.
func (n *nsLockMap) Unlock(volume, path, opsID string) {
	readLock := false
	n.unlock(volume, path, opsID, readLock)
}

// RLock - locks any previously acquired read locks.
func (n *nsLockMap) RLock(volume, path, opsID string, timeout time.Duration) (locked bool) {
	readLock := true

	lockSource := getSource() // Useful for debugging
	return n.lock(volume, path, lockSource, opsID, readLock, timeout)
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
	// - In case of FS or XL we call ForceUnlock on the local globalNSMutex
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
	if n.isDistXL { // For distributed mode, broadcast ForceUnlock message.
		dsync.NewDRWMutex(pathJoin(volume, path), globalDsync).ForceUnlock()
	}

	param := nsParam{volume, path}
	if _, found := n.lockMap[param]; found {
		// Remove lock from the map.
		delete(n.lockMap, param)
	}

	// delete the lock state entry for given
	// <volume, path> pair. Ignore error as there
	// is no way to report it back
	n.deleteLockInfoEntryForVolumePath(param)
}

// lockInstance - frontend/top-level interface for namespace locks.
type lockInstance struct {
	ns                  *nsLockMap
	volume, path, opsID string
}

// NewNSLock - returns a lock instance for a given volume and
// path. The returned lockInstance object encapsulates the nsLockMap,
// volume, path and operation ID.
func (n *nsLockMap) NewNSLock(volume, path string) RWLocker {
	return &lockInstance{n, volume, path, getOpsID()}
}

// Lock - block until write lock is taken or timeout has occurred.
func (li *lockInstance) GetLock(timeout *dynamicTimeout) (timedOutErr error) {
	lockSource := getSource()
	start := UTCNow()
	readLock := false
	if !li.ns.lock(li.volume, li.path, lockSource, li.opsID, readLock, timeout.Timeout()) {
		timeout.LogFailure()
		return OperationTimedOut{Path: li.path}
	}
	timeout.LogSuccess(UTCNow().Sub(start))
	return
}

// Unlock - block until write lock is released.
func (li *lockInstance) Unlock() {
	readLock := false
	li.ns.unlock(li.volume, li.path, li.opsID, readLock)
}

// RLock - block until read lock is taken or timeout has occurred.
func (li *lockInstance) GetRLock(timeout *dynamicTimeout) (timedOutErr error) {
	lockSource := getSource()
	start := UTCNow()
	readLock := true
	if !li.ns.lock(li.volume, li.path, lockSource, li.opsID, readLock, timeout.Timeout()) {
		timeout.LogFailure()
		return OperationTimedOut{Path: li.path}
	}
	timeout.LogSuccess(UTCNow().Sub(start))
	return
}

// RUnlock - block until read lock is released.
func (li *lockInstance) RUnlock() {
	readLock := true
	li.ns.unlock(li.volume, li.path, li.opsID, readLock)
}

func getSource() string {
	var funcName string
	pc, filename, lineNum, ok := runtime.Caller(2)
	if ok {
		filename = pathutil.Base(filename)
		funcName = strings.TrimPrefix(runtime.FuncForPC(pc).Name(),
			"github.com/minio/minio/cmd.")
	} else {
		filename = "<unknown>"
		lineNum = 0
	}

	return fmt.Sprintf("[%s:%d:%s()]", filename, lineNum, funcName)
}

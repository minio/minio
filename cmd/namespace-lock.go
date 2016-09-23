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
	"fmt"
	pathutil "path"
	"runtime"
	"strconv"
	"strings"
	"sync"

	"github.com/minio/dsync"
)

// Global name space lock.
var nsMutex *nsLockMap

// Initialize distributed locking only in case of distributed setup.
// Returns if the setup is distributed or not on success.
func initDsyncNodes(disks []string, port int) error {
	serverPort := strconv.Itoa(port)
	cred := serverConfig.GetCredential()
	// Initialize rpc lock client information only if this instance is a distributed setup.
	var clnts []dsync.RPC
	myNode := -1
	for _, disk := range disks {
		if idx := strings.LastIndex(disk, ":"); idx != -1 {
			clnts = append(clnts, newAuthClient(&authConfig{
				accessKey: cred.AccessKeyID,
				secretKey: cred.SecretAccessKey,
				// Construct a new dsync server addr.
				address: disk[:idx] + ":" + serverPort,
				// Construct a new rpc path for the disk.
				path:        pathutil.Join(lockRPCPath, disk[idx+1:]),
				loginMethod: "Dsync.LoginHandler",
			}))

			if isLocalStorage(disk) && myNode == -1 {
				myNode = len(clnts) - 1
			}
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
	if globalDebugLock {
		// lock Debugging enabed, initialize nsLockMap with entry for debugging information.
		// entries of <volume,path> -> stateInfo of locks, for instrumentation purpose.
		nsMutex.debugLockMap = make(map[nsParam]*debugLockInfoPerVolumePath)
	}
}

func (n *nsLockMap) initLockInfoForVolumePath(param nsParam) {
	n.debugLockMap[param] = newDebugLockInfoPerVolumePath()
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
	// lock counter used for lock debugging.
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
func (n *nsLockMap) lock(volume, path string, lockOrigin, opsID string, readLock bool) {
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

	if globalDebugLock {
		// Change the state of the lock to be blocked for the given pair of <volume, path>
		// and <OperationID> till the lock unblocks. The lock for accessing `nsMutex` is
		// held inside the function itself.
		if err := n.statusNoneToBlocked(param, lockOrigin, opsID, readLock); err != nil {
			errorIf(err, "Failed to set lock state to blocked.")
		}
	}

	// Unlock map before Locking NS which might block.
	n.lockMapMutex.Unlock()

	// Locking here can block.
	if readLock {
		nsLk.RLock()
	} else {
		nsLk.Lock()
	}

	// Check if lock debugging enabled.
	if globalDebugLock {
		// Changing the status of the operation from blocked to running.
		// change the state of the lock to be  running (from blocked) for
		// the given pair of <volume, path> and <OperationID>.
		if err := n.statusBlockedToRunning(param, lockOrigin, opsID, readLock); err != nil {
			errorIf(err, "Failed to set the lock state to running.")
		}
	}
}

// Unlock the namespace resource.
func (n *nsLockMap) unlock(volume, path, opsID string, readLock bool) {
	// nsLk.Unlock() will not block, hence locking the map for the entire function is fine.
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
			errorIf(errors.New("Namespace reference count cannot be 0."), "Invalid reference count detected.")
		}
		if nsLk.ref != 0 {
			nsLk.ref--
			// Locking debug enabled, delete the lock state entry for given operation ID.
			if globalDebugLock {
				err := n.deleteLockInfoEntryForOps(param, opsID)
				if err != nil {
					errorIf(err, "Failed to delete lock info entry.")
				}
			}
		}
		if nsLk.ref == 0 {
			// Remove from the map if there are no more references.
			delete(n.lockMap, param)

			// Locking debug enabled, delete the lock state entry for given <volume, path> pair.
			if globalDebugLock {
				err := n.deleteLockInfoEntryForVolumePath(param)
				if err != nil {
					errorIf(err, "Failed to delete lock info entry.")
				}
			}
		}
	}
}

// Lock - locks the given resource for writes, using a previously
// allocated name space lock or initializing a new one.
func (n *nsLockMap) Lock(volume, path, opsID string) {
	var lockOrigin string
	// Lock debugging enabled. The caller information of the lock held has
	// been obtained here before calling any other function.
	if globalDebugLock {
		// Fetching the package, function name and the line number of the caller from the runtime.
		// here is an example https://play.golang.org/p/perrmNRI9_ .
		pc, fn, line, success := runtime.Caller(1)
		if !success {
			errorIf(errors.New("Couldn't get caller info."), "Fetching caller info form runtime failed.")
		}
		lockOrigin = fmt.Sprintf("[lock held] in %s[%s:%d]", runtime.FuncForPC(pc).Name(), fn, line)
	}
	readLock := false
	n.lock(volume, path, lockOrigin, opsID, readLock)
}

// Unlock - unlocks any previously acquired write locks.
func (n *nsLockMap) Unlock(volume, path, opsID string) {
	readLock := false
	n.unlock(volume, path, opsID, readLock)
}

// RLock - locks any previously acquired read locks.
func (n *nsLockMap) RLock(volume, path, opsID string) {
	var lockOrigin string
	readLock := true
	// Lock debugging enabled. The caller information of the lock held has
	// been obtained here before calling any other function.
	if globalDebugLock {
		// Fetching the package, function name and the line number of the
		// caller from the runtime. Here is an example https://play.golang.org/p/perrmNRI9_ .
		pc, fn, line, success := runtime.Caller(1)
		if !success {
			errorIf(errors.New("Couldn't get caller info."), "Fetching caller info form runtime failed.")
		}
		lockOrigin = fmt.Sprintf("[lock held] in %s[%s:%d]", runtime.FuncForPC(pc).Name(), fn, line)
	}
	n.lock(volume, path, lockOrigin, opsID, readLock)
}

// RUnlock - unlocks any previously acquired read locks.
func (n *nsLockMap) RUnlock(volume, path, opsID string) {
	readLock := true
	n.unlock(volume, path, opsID, readLock)
}

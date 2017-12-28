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
	"fmt"
	"math/rand"
	"path"
	"sync"
	"time"

	router "github.com/gorilla/mux"
	"github.com/minio/dsync"
	"github.com/minio/minio/pkg/errors"
)

const (
	// Lock rpc server endpoint.
	lockServicePath = "/lock"

	// Lock rpc service name.
	lockServiceName = "Dsync"

	// Lock maintenance interval.
	lockMaintenanceInterval = 1 * time.Minute // 1 minute.

	// Lock validity check interval.
	lockValidityCheckInterval = 2 * time.Minute // 2 minutes.
)

// lockRequesterInfo stores various info from the client for each lock that is requested.
type lockRequesterInfo struct {
	writer          bool      // Bool whether write or read lock.
	node            string    // Network address of client claiming lock.
	serviceEndpoint string    // RPC path of client claiming lock.
	uid             string    // UID to uniquely identify request of client.
	timestamp       time.Time // Timestamp set at the time of initialization.
	timeLastCheck   time.Time // Timestamp for last check of validity of lock.
}

// isWriteLock returns whether the lock is a write or read lock.
func isWriteLock(lri []lockRequesterInfo) bool {
	return len(lri) == 1 && lri[0].writer
}

// lockServer is type for RPC handlers
type lockServer struct {
	AuthRPCServer
	ll localLocker
}

// Start lock maintenance from all lock servers.
func startLockMaintenance(lockServers []*lockServer) {
	for _, locker := range lockServers {
		// Start loop for stale lock maintenance
		go func(lk *lockServer) {
			// Initialize a new ticker with a minute between each ticks.
			ticker := time.NewTicker(lockMaintenanceInterval)

			// Start with random sleep time, so as to avoid "synchronous checks" between servers
			time.Sleep(time.Duration(rand.Float64() * float64(lockMaintenanceInterval)))
			for {
				// Verifies every minute for locks held more than 2minutes.
				select {
				case <-ticker.C:
					lk.lockMaintenance(lockValidityCheckInterval)
				case <-globalServiceDoneCh:
					// Stop the timer.
					ticker.Stop()
				}
			}
		}(locker)
	}
}

// Register distributed NS lock handlers.
func registerDistNSLockRouter(mux *router.Router, endpoints EndpointList) error {
	// Start lock maintenance from all lock servers.
	startLockMaintenance(globalLockServers)

	// Register initialized lock servers to their respective rpc endpoints.
	return registerStorageLockers(mux, globalLockServers)
}

// registerStorageLockers - register locker rpc handlers for net/rpc library clients
func registerStorageLockers(mux *router.Router, lockServers []*lockServer) error {
	for _, lockServer := range lockServers {
		lockRPCServer := newRPCServer()
		if err := lockRPCServer.RegisterName(lockServiceName, lockServer); err != nil {
			return errors.Trace(err)
		}
		lockRouter := mux.PathPrefix(minioReservedBucketPath).Subrouter()
		lockRouter.Path(path.Join(lockServicePath, lockServer.ll.serviceEndpoint)).Handler(lockRPCServer)
	}
	return nil
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
				writer:          true,
				node:            args.ServerAddr,
				serviceEndpoint: args.ServiceEndpoint,
				uid:             args.UID,
				timestamp:       UTCNow(),
				timeLastCheck:   UTCNow(),
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
		writer:          false,
		node:            args.ServerAddr,
		serviceEndpoint: args.ServiceEndpoint,
		uid:             args.UID,
		timestamp:       UTCNow(),
		timeLastCheck:   UTCNow(),
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
	if _, ok := l.lockMap[args.Resource]; ok {
		// Only clear lock when it is taken
		// Remove the lock (irrespective of write or read lock)
		delete(l.lockMap, args.Resource)
	}
	return true, nil
}

///  Distributed lock handlers

// Lock - rpc handler for (single) write lock operation.
func (l *lockServer) Lock(args *LockArgs, reply *bool) (err error) {
	if err = args.IsAuthenticated(); err != nil {
		return err
	}
	*reply, err = l.ll.Lock(args.LockArgs)
	return err
}

// Unlock - rpc handler for (single) write unlock operation.
func (l *lockServer) Unlock(args *LockArgs, reply *bool) (err error) {
	if err = args.IsAuthenticated(); err != nil {
		return err
	}
	*reply, err = l.ll.Unlock(args.LockArgs)
	return err
}

// RLock - rpc handler for read lock operation.
func (l *lockServer) RLock(args *LockArgs, reply *bool) (err error) {
	if err = args.IsAuthenticated(); err != nil {
		return err
	}
	*reply, err = l.ll.RLock(args.LockArgs)
	return err
}

// RUnlock - rpc handler for read unlock operation.
func (l *lockServer) RUnlock(args *LockArgs, reply *bool) (err error) {
	if err = args.IsAuthenticated(); err != nil {
		return err
	}
	*reply, err = l.ll.RUnlock(args.LockArgs)
	return err
}

// ForceUnlock - rpc handler for force unlock operation.
func (l *lockServer) ForceUnlock(args *LockArgs, reply *bool) (err error) {
	if err = args.IsAuthenticated(); err != nil {
		return err
	}
	*reply, err = l.ll.ForceUnlock(args.LockArgs)
	return err
}

// Expired - rpc handler for expired lock status.
func (l *lockServer) Expired(args *LockArgs, reply *bool) error {
	if err := args.IsAuthenticated(); err != nil {
		return err
	}
	l.ll.mutex.Lock()
	defer l.ll.mutex.Unlock()
	// Lock found, proceed to verify if belongs to given uid.
	if lri, ok := l.ll.lockMap[args.LockArgs.Resource]; ok {
		// Check whether uid is still active
		for _, entry := range lri {
			if entry.uid == args.LockArgs.UID {
				*reply = false // When uid found, lock is still active so return not expired.
				return nil     // When uid found *reply is set to true.
			}
		}
	}
	// When we get here lock is no longer active due to either args.LockArgs.Resource
	// being absent from map or uid not found for given args.LockArgs.Resource
	*reply = true
	return nil
}

// nameLockRequesterInfoPair is a helper type for lock maintenance
type nameLockRequesterInfoPair struct {
	name string
	lri  lockRequesterInfo
}

// lockMaintenance loops over locks that have been active for some time and checks back
// with the original server whether it is still alive or not
//
// Following logic inside ignores the errors generated for Dsync.Active operation.
// - server at client down
// - some network error (and server is up normally)
//
// We will ignore the error, and we will retry later to get a resolve on this lock
func (l *lockServer) lockMaintenance(interval time.Duration) {
	l.ll.mutex.Lock()
	// Get list of long lived locks to check for staleness.
	nlripLongLived := getLongLivedLocks(l.ll.lockMap, interval)
	l.ll.mutex.Unlock()

	serverCred := globalServerConfig.GetCredential()
	// Validate if long lived locks are indeed clean.
	for _, nlrip := range nlripLongLived {
		// Initialize client based on the long live locks.
		c := newLockRPCClient(authConfig{
			accessKey:       serverCred.AccessKey,
			secretKey:       serverCred.SecretKey,
			serverAddr:      nlrip.lri.node,
			secureConn:      globalIsSSL,
			serviceEndpoint: nlrip.lri.serviceEndpoint,
			serviceName:     lockServiceName,
		})

		// Call back to original server verify whether the lock is still active (based on name & uid)
		expired, _ := c.Expired(dsync.LockArgs{
			UID:      nlrip.lri.uid,
			Resource: nlrip.name,
		})

		// Close the connection regardless of the call response.
		c.rpcClient.Close()

		// For successful response, verify if lock is indeed active or stale.
		if expired {
			// The lock is no longer active at server that originated the lock
			// So remove the lock from the map.
			l.ll.mutex.Lock()
			l.ll.removeEntryIfExists(nlrip) // Purge the stale entry if it exists.
			l.ll.mutex.Unlock()
		}
	}
}

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
	"net/rpc"
	"path"
	"sync"
	"time"

	router "github.com/gorilla/mux"
	"github.com/minio/dsync"
)

const (
	// Lock rpc server endpoint.
	lockRPCPath = "/lock"

	// Lock maintenance interval.
	lockMaintenanceInterval = 1 * time.Minute // 1 minute.

	// Lock validity check interval.
	lockValidityCheckInterval = 2 * time.Minute // 2 minutes.
)

// lockRequesterInfo stores various info from the client for each lock that is requested
type lockRequesterInfo struct {
	writer        bool      // Bool whether write or read lock
	node          string    // Network address of client claiming lock
	rpcPath       string    // RPC path of client claiming lock
	uid           string    // Uid to uniquely identify request of client
	timestamp     time.Time // Timestamp set at the time of initialization
	timeLastCheck time.Time // Timestamp for last check of validity of lock
}

// isWriteLock returns whether the lock is a write or read lock
func isWriteLock(lri []lockRequesterInfo) bool {
	return len(lri) == 1 && lri[0].writer
}

// lockServer is type for RPC handlers
type lockServer struct {
	AuthRPCServer
	rpcPath string
	mutex   sync.Mutex
	lockMap map[string][]lockRequesterInfo
}

// Start lock maintenance from all lock servers.
func startLockMaintainence(lockServers []*lockServer) {
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
func registerDistNSLockRouter(mux *router.Router, serverConfig serverCmdConfig) error {
	// Initialize a new set of lock servers.
	lockServers := newLockServers(serverConfig)

	// Start lock maintenance from all lock servers.
	startLockMaintainence(lockServers)

	// Register initialized lock servers to their respective rpc endpoints.
	return registerStorageLockers(mux, lockServers)
}

// Create one lock server for every local storage rpc server.
func newLockServers(srvConfig serverCmdConfig) (lockServers []*lockServer) {
	for _, ep := range srvConfig.endpoints {
		// Initialize new lock server for each local node.
		if isLocalStorage(ep) {
			// Create handler for lock RPCs
			locker := &lockServer{
				rpcPath: getPath(ep),
				mutex:   sync.Mutex{},
				lockMap: make(map[string][]lockRequesterInfo),
			}
			lockServers = append(lockServers, locker)
		}
	}
	return lockServers
}

// registerStorageLockers - register locker rpc handlers for net/rpc library clients
func registerStorageLockers(mux *router.Router, lockServers []*lockServer) error {
	for _, lockServer := range lockServers {
		lockRPCServer := rpc.NewServer()
		if err := lockRPCServer.RegisterName("Dsync", lockServer); err != nil {
			return traceError(err)
		}
		lockRouter := mux.PathPrefix(minioReservedBucketPath).Subrouter()
		lockRouter.Path(path.Join(lockRPCPath, lockServer.rpcPath)).Handler(lockRPCServer)
	}
	return nil
}

///  Distributed lock handlers

// Lock - rpc handler for (single) write lock operation.
func (l *lockServer) Lock(args *LockArgs, reply *bool) error {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	if err := args.IsAuthenticated(); err != nil {
		return err
	}
	_, *reply = l.lockMap[args.LockArgs.Resource]
	if !*reply { // No locks held on the given name, so claim write lock
		l.lockMap[args.LockArgs.Resource] = []lockRequesterInfo{
			{
				writer:        true,
				node:          args.LockArgs.ServerAddr,
				rpcPath:       args.LockArgs.ServiceEndpoint,
				uid:           args.LockArgs.UID,
				timestamp:     UTCNow(),
				timeLastCheck: UTCNow(),
			},
		}
	}
	*reply = !*reply // Negate *reply to return true when lock is granted or false otherwise
	return nil
}

// Unlock - rpc handler for (single) write unlock operation.
func (l *lockServer) Unlock(args *LockArgs, reply *bool) error {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	if err := args.IsAuthenticated(); err != nil {
		return err
	}
	var lri []lockRequesterInfo
	if lri, *reply = l.lockMap[args.LockArgs.Resource]; !*reply { // No lock is held on the given name
		return fmt.Errorf("Unlock attempted on an unlocked entity: %s", args.LockArgs.Resource)
	}
	if *reply = isWriteLock(lri); !*reply { // Unless it is a write lock
		return fmt.Errorf("Unlock attempted on a read locked entity: %s (%d read locks active)", args.LockArgs.Resource, len(lri))
	}
	if !l.removeEntry(args.LockArgs.Resource, args.LockArgs.UID, &lri) {
		return fmt.Errorf("Unlock unable to find corresponding lock for uid: %s", args.LockArgs.UID)
	}
	return nil
}

// RLock - rpc handler for read lock operation.
func (l *lockServer) RLock(args *LockArgs, reply *bool) error {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	if err := args.IsAuthenticated(); err != nil {
		return err
	}
	lrInfo := lockRequesterInfo{
		writer:        false,
		node:          args.LockArgs.ServerAddr,
		rpcPath:       args.LockArgs.ServiceEndpoint,
		uid:           args.LockArgs.UID,
		timestamp:     UTCNow(),
		timeLastCheck: UTCNow(),
	}
	if lri, ok := l.lockMap[args.LockArgs.Resource]; ok {
		if *reply = !isWriteLock(lri); *reply { // Unless there is a write lock
			l.lockMap[args.LockArgs.Resource] = append(l.lockMap[args.LockArgs.Resource], lrInfo)
		}
	} else { // No locks held on the given name, so claim (first) read lock
		l.lockMap[args.LockArgs.Resource] = []lockRequesterInfo{lrInfo}
		*reply = true
	}
	return nil
}

// RUnlock - rpc handler for read unlock operation.
func (l *lockServer) RUnlock(args *LockArgs, reply *bool) error {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	if err := args.IsAuthenticated(); err != nil {
		return err
	}
	var lri []lockRequesterInfo
	if lri, *reply = l.lockMap[args.LockArgs.Resource]; !*reply { // No lock is held on the given name
		return fmt.Errorf("RUnlock attempted on an unlocked entity: %s", args.LockArgs.Resource)
	}
	if *reply = !isWriteLock(lri); !*reply { // A write-lock is held, cannot release a read lock
		return fmt.Errorf("RUnlock attempted on a write locked entity: %s", args.LockArgs.Resource)
	}
	if !l.removeEntry(args.LockArgs.Resource, args.LockArgs.UID, &lri) {
		return fmt.Errorf("RUnlock unable to find corresponding read lock for uid: %s", args.LockArgs.UID)
	}
	return nil
}

// ForceUnlock - rpc handler for force unlock operation.
func (l *lockServer) ForceUnlock(args *LockArgs, reply *bool) error {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	if err := args.IsAuthenticated(); err != nil {
		return err
	}
	if len(args.LockArgs.UID) != 0 {
		return fmt.Errorf("ForceUnlock called with non-empty UID: %s", args.LockArgs.UID)
	}
	if _, ok := l.lockMap[args.LockArgs.Resource]; ok { // Only clear lock when set
		delete(l.lockMap, args.LockArgs.Resource) // Remove the lock (irrespective of write or read lock)
	}
	*reply = true
	return nil
}

// Expired - rpc handler for expired lock status.
func (l *lockServer) Expired(args *LockArgs, reply *bool) error {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	if err := args.IsAuthenticated(); err != nil {
		return err
	}
	// Lock found, proceed to verify if belongs to given uid.
	if lri, ok := l.lockMap[args.LockArgs.Resource]; ok {
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
	l.mutex.Lock()
	// Get list of long lived locks to check for staleness.
	nlripLongLived := getLongLivedLocks(l.lockMap, interval)
	l.mutex.Unlock()

	serverCred := serverConfig.GetCredential()
	// Validate if long lived locks are indeed clean.
	for _, nlrip := range nlripLongLived {
		// Initialize client based on the long live locks.
		c := newLockRPCClient(authConfig{
			accessKey:       serverCred.AccessKey,
			secretKey:       serverCred.SecretKey,
			serverAddr:      nlrip.lri.node,
			serviceEndpoint: nlrip.lri.rpcPath,
			secureConn:      globalIsSSL,
			serviceName:     "Dsync",
		})

		// Call back to original server verify whether the lock is still active (based on name & uid)
		expired, _ := c.Expired(dsync.LockArgs{UID: nlrip.lri.uid, Resource: nlrip.name})

		// Close the connection regardless of the call response.
		c.rpcClient.Close()

		// For successful response, verify if lock is indeed active or stale.
		if expired {
			// The lock is no longer active at server that originated the lock
			// So remove the lock from the map.
			l.mutex.Lock()
			l.removeEntryIfExists(nlrip) // Purge the stale entry if it exists.
			l.mutex.Unlock()
		}
	}
}

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
	"fmt"
	"math/rand"
	"net/rpc"
	"path"
	"sync"
	"time"

	router "github.com/gorilla/mux"
)

const lockRPCPath = "/minio/lock"
const lockMaintenanceLoop = 1 * time.Minute
const lockCheckValidityInterval = 2 * time.Minute

// LockArgs besides lock name, holds Token and Timestamp for session
// authentication and validation server restart.
type LockArgs struct {
	Name      string
	Token     string
	Timestamp time.Time
	Node      string
	RPCPath   string
	UID       string
}

// SetToken - sets the token to the supplied value.
func (l *LockArgs) SetToken(token string) {
	l.Token = token
}

// SetTimestamp - sets the timestamp to the supplied value.
func (l *LockArgs) SetTimestamp(tstamp time.Time) {
	l.Timestamp = tstamp
}

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
	rpcPath string
	mutex   sync.Mutex
	lockMap map[string][]lockRequesterInfo
}

// Register distributed NS lock handlers.
func registerDistNSLockRouter(mux *router.Router, serverConfig serverCmdConfig) error {
	lockServers := newLockServers(serverConfig)
	return registerStorageLockers(mux, lockServers)
}

// Create one lock server for every local storage rpc server.
func newLockServers(srvConfig serverCmdConfig) (lockServers []*lockServer) {
	for _, ep := range srvConfig.endpoints {
		// Not local storage move to the next node.
		if !isLocalStorage(ep) {
			continue
		}

		// Create handler for lock RPCs
		locker := &lockServer{
			rpcPath: getPath(ep),
			mutex:   sync.Mutex{},
			lockMap: make(map[string][]lockRequesterInfo),
		}

		// Start loop for stale lock maintenance
		go func() {
			// Start with random sleep time, so as to avoid "synchronous checks" between servers
			time.Sleep(time.Duration(rand.Float64() * float64(lockMaintenanceLoop)))
			for {
				time.Sleep(lockMaintenanceLoop)
				locker.lockMaintenance(lockCheckValidityInterval)
			}
		}()
		lockServers = append(lockServers, locker)
	}
	return lockServers
}

// registerStorageLockers - register locker rpc handlers for net/rpc library clients
func registerStorageLockers(mux *router.Router, lockServers []*lockServer) error {
	for _, lockServer := range lockServers {
		lockRPCServer := rpc.NewServer()
		err := lockRPCServer.RegisterName("Dsync", lockServer)
		if err != nil {
			return traceError(err)
		}
		lockRouter := mux.PathPrefix(reservedBucket).Subrouter()
		lockRouter.Path(path.Join("/lock", lockServer.rpcPath)).Handler(lockRPCServer)
	}
	return nil
}

///  Distributed lock handlers

// LoginHandler - handles LoginHandler RPC call.
func (l *lockServer) LoginHandler(args *RPCLoginArgs, reply *RPCLoginReply) error {
	jwt, err := newJWT(defaultInterNodeJWTExpiry, serverConfig.GetCredential())
	if err != nil {
		return err
	}
	if err = jwt.Authenticate(args.Username, args.Password); err != nil {
		return err
	}
	token, err := jwt.GenerateToken(args.Username)
	if err != nil {
		return err
	}
	reply.Token = token
	reply.Timestamp = time.Now().UTC()
	reply.ServerVersion = Version
	return nil
}

// Lock - rpc handler for (single) write lock operation.
func (l *lockServer) Lock(args *LockArgs, reply *bool) error {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	if err := l.validateLockArgs(args); err != nil {
		return err
	}
	_, *reply = l.lockMap[args.Name]
	if !*reply { // No locks held on the given name, so claim write lock
		l.lockMap[args.Name] = []lockRequesterInfo{
			{
				writer:        true,
				node:          args.Node,
				rpcPath:       args.RPCPath,
				uid:           args.UID,
				timestamp:     time.Now().UTC(),
				timeLastCheck: time.Now().UTC(),
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
	if err := l.validateLockArgs(args); err != nil {
		return err
	}
	var lri []lockRequesterInfo
	if lri, *reply = l.lockMap[args.Name]; !*reply { // No lock is held on the given name
		return fmt.Errorf("Unlock attempted on an unlocked entity: %s", args.Name)
	}
	if *reply = isWriteLock(lri); !*reply { // Unless it is a write lock
		return fmt.Errorf("Unlock attempted on a read locked entity: %s (%d read locks active)", args.Name, len(lri))
	}
	if !l.removeEntry(args.Name, args.UID, &lri) {
		return fmt.Errorf("Unlock unable to find corresponding lock for uid: %s", args.UID)
	}
	return nil
}

// RLock - rpc handler for read lock operation.
func (l *lockServer) RLock(args *LockArgs, reply *bool) error {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	if err := l.validateLockArgs(args); err != nil {
		return err
	}
	lrInfo := lockRequesterInfo{
		writer:        false,
		node:          args.Node,
		rpcPath:       args.RPCPath,
		uid:           args.UID,
		timestamp:     time.Now().UTC(),
		timeLastCheck: time.Now().UTC(),
	}
	if lri, ok := l.lockMap[args.Name]; ok {
		if *reply = !isWriteLock(lri); *reply { // Unless there is a write lock
			l.lockMap[args.Name] = append(l.lockMap[args.Name], lrInfo)
		}
	} else { // No locks held on the given name, so claim (first) read lock
		l.lockMap[args.Name] = []lockRequesterInfo{lrInfo}
		*reply = true
	}
	return nil
}

// RUnlock - rpc handler for read unlock operation.
func (l *lockServer) RUnlock(args *LockArgs, reply *bool) error {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	if err := l.validateLockArgs(args); err != nil {
		return err
	}
	var lri []lockRequesterInfo
	if lri, *reply = l.lockMap[args.Name]; !*reply { // No lock is held on the given name
		return fmt.Errorf("RUnlock attempted on an unlocked entity: %s", args.Name)
	}
	if *reply = !isWriteLock(lri); !*reply { // A write-lock is held, cannot release a read lock
		return fmt.Errorf("RUnlock attempted on a write locked entity: %s", args.Name)
	}
	if !l.removeEntry(args.Name, args.UID, &lri) {
		return fmt.Errorf("RUnlock unable to find corresponding read lock for uid: %s", args.UID)
	}
	return nil
}

// ForceUnlock - rpc handler for force unlock operation.
func (l *lockServer) ForceUnlock(args *LockArgs, reply *bool) error {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	if err := l.validateLockArgs(args); err != nil {
		return err
	}
	if len(args.UID) != 0 {
		return fmt.Errorf("ForceUnlock called with non-empty UID: %s", args.UID)
	}
	if _, ok := l.lockMap[args.Name]; ok { // Only clear lock when set
		delete(l.lockMap, args.Name) // Remove the lock (irrespective of write or read lock)
	}
	*reply = true
	return nil
}

// Expired - rpc handler for expired lock status.
func (l *lockServer) Expired(args *LockArgs, reply *bool) error {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	if err := l.validateLockArgs(args); err != nil {
		return err
	}
	// Lock found, proceed to verify if belongs to given uid.
	if lri, ok := l.lockMap[args.Name]; ok {
		// Check whether uid is still active
		for _, entry := range lri {
			if entry.uid == args.UID {
				*reply = false // When uid found, lock is still active so return not expired.
				return nil     // When uid found *reply is set to true.
			}
		}
	}
	// When we get here lock is no longer active due to either args.Name
	// being absent from map or uid not found for given args.Name
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

	// Validate if long lived locks are indeed clean.
	for _, nlrip := range nlripLongLived {
		// Initialize client based on the long live locks.
		c := newClient(nlrip.lri.node, nlrip.lri.rpcPath, isSSL())

		var expired bool

		// Call back to original server verify whether the lock is still active (based on name & uid)
		c.Call("Dsync.Expired", &LockArgs{
			Name: nlrip.name,
			UID:  nlrip.lri.uid,
		}, &expired)
		c.Close() // Close the connection regardless of the call response.

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

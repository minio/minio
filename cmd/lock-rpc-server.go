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
	"strings"
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
	rpcPath   string
	mutex     sync.Mutex
	lockMap   map[string][]lockRequesterInfo
	timestamp time.Time // Timestamp set at the time of initialization. Resets naturally on minio server restart.
}

func (l *lockServer) verifyArgs(args *LockArgs) error {
	if !l.timestamp.Equal(args.Timestamp) {
		return errInvalidTimestamp
	}
	if !isRPCTokenValid(args.Token) {
		return errInvalidToken
	}
	return nil
}

///  Distributed lock handlers

// LoginHandler - handles LoginHandler RPC call.
func (l *lockServer) LoginHandler(args *RPCLoginArgs, reply *RPCLoginReply) error {
	jwt, err := newJWT(defaultTokenExpiry)
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
	reply.Timestamp = l.timestamp
	return nil
}

// Lock - rpc handler for (single) write lock operation.
func (l *lockServer) Lock(args *LockArgs, reply *bool) error {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	if err := l.verifyArgs(args); err != nil {
		return err
	}
	_, *reply = l.lockMap[args.Name]
	if !*reply { // No locks held on the given name, so claim write lock
		l.lockMap[args.Name] = []lockRequesterInfo{{writer: true, node: args.Node, rpcPath: args.RPCPath, uid: args.UID, timestamp: time.Now(), timeLastCheck: time.Now()}}
	}
	*reply = !*reply // Negate *reply to return true when lock is granted or false otherwise
	return nil
}

// Unlock - rpc handler for (single) write unlock operation.
func (l *lockServer) Unlock(args *LockArgs, reply *bool) error {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	if err := l.verifyArgs(args); err != nil {
		return err
	}
	var lri []lockRequesterInfo
	lri, *reply = l.lockMap[args.Name]
	if !*reply { // No lock is held on the given name
		return fmt.Errorf("Unlock attempted on an unlocked entity: %s", args.Name)
	}
	if *reply = isWriteLock(lri); !*reply { // Unless it is a write lock
		return fmt.Errorf("Unlock attempted on a read locked entity: %s (%d read locks active)", args.Name, len(lri))
	}
	if l.removeEntry(args.Name, args.UID, &lri) {
		return nil
	}
	return fmt.Errorf("Unlock unable to find corresponding lock for uid: %s", args.UID)
}

// RLock - rpc handler for read lock operation.
func (l *lockServer) RLock(args *LockArgs, reply *bool) error {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	if err := l.verifyArgs(args); err != nil {
		return err
	}
	var lri []lockRequesterInfo
	lri, *reply = l.lockMap[args.Name]
	if !*reply { // No locks held on the given name, so claim (first) read lock
		l.lockMap[args.Name] = []lockRequesterInfo{{writer: false, node: args.Node, rpcPath: args.RPCPath, uid: args.UID, timestamp: time.Now(), timeLastCheck: time.Now()}}
		*reply = true
	} else {
		if *reply = !isWriteLock(lri); *reply { // Unless there is a write lock
			l.lockMap[args.Name] = append(l.lockMap[args.Name], lockRequesterInfo{writer: false, node: args.Node, rpcPath: args.RPCPath, uid: args.UID, timestamp: time.Now(), timeLastCheck: time.Now()})
		}
	}
	return nil
}

// RUnlock - rpc handler for read unlock operation.
func (l *lockServer) RUnlock(args *LockArgs, reply *bool) error {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	if err := l.verifyArgs(args); err != nil {
		return err
	}
	var lri []lockRequesterInfo
	if lri, *reply = l.lockMap[args.Name]; !*reply { // No lock is held on the given name
		return fmt.Errorf("RUnlock attempted on an unlocked entity: %s", args.Name)
	}
	if *reply = !isWriteLock(lri); !*reply { // A write-lock is held, cannot release a read lock
		return fmt.Errorf("RUnlock attempted on a write locked entity: %s", args.Name)
	}
	if l.removeEntry(args.Name, args.UID, &lri) {
		return nil
	}
	return fmt.Errorf("RUnlock unable to find corresponding read lock for uid: %s", args.UID)
}

// Active - rpc handler for active lock status.
func (l *lockServer) Active(args *LockArgs, reply *bool) error {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	if err := l.verifyArgs(args); err != nil {
		return err
	}
	var lri []lockRequesterInfo
	if lri, *reply = l.lockMap[args.Name]; !*reply {
		return nil // No lock is held on the given name so return false
	}
	// Check whether uid is still active
	for _, entry := range lri {
		if *reply = entry.uid == args.UID; *reply {
			return nil // When uid found return true
		}
	}
	return nil // None found so return false
}

// removeEntry either, based on the uid of the lock message, removes a single entry from the
// lockRequesterInfo array or the whole array from the map (in case of a write lock or last read lock)
func (l *lockServer) removeEntry(name, uid string, lri *[]lockRequesterInfo) bool {
	// Find correct entry to remove based on uid
	for index, entry := range *lri {
		if entry.uid == uid {
			if len(*lri) == 1 {
				delete(l.lockMap, name) // Remove the (last) lock
			} else {
				// Remove the appropriate read lock
				*lri = append((*lri)[:index], (*lri)[index+1:]...)
				l.lockMap[name] = *lri
			}
			return true
		}
	}
	return false
}

// nameLockRequesterInfoPair is a helper type for lock maintenance
type nameLockRequesterInfoPair struct {
	name string
	lri  lockRequesterInfo
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
				lriArray[idx].timeLastCheck = time.Now()
			}
		}
	}

	return rslt
}

// lockMaintenance loops over locks that have been active for some time and checks back
// with the original server whether it is still alive or not
func (l *lockServer) lockMaintenance(interval time.Duration) {

	l.mutex.Lock()
	// get list of locks to check
	nlripLongLived := getLongLivedLocks(l.lockMap, interval)
	l.mutex.Unlock()

	for _, nlrip := range nlripLongLived {

		c := newClient(nlrip.lri.node, nlrip.lri.rpcPath)

		var active bool

		// Call back to original server verify whether the lock is still active (based on name & uid)
		if err := c.Call("Dsync.Active", &LockArgs{Name: nlrip.name, UID: nlrip.lri.uid}, &active); err != nil {
			// We failed to connect back to the server that originated the lock, this can either be due to
			// - server at client down
			// - some network error (and server is up normally)
			//
			// We will ignore the error, and we will retry later to get resolve on this lock
			c.Close()
		} else {
			c.Close()

			if !active { // The lock is no longer active at server that originated the lock
				// so remove the lock from the map
				l.mutex.Lock()
				// Check if entry is still in map (could have been removed altogether by 'concurrent' (R)Unlock of last entry)
				if lri, ok := l.lockMap[nlrip.name]; ok {
					if !l.removeEntry(nlrip.name, nlrip.lri.uid, &lri) {
						// Remove failed, in case it is a:
						if nlrip.lri.writer {
							// Writer: this should never happen as the whole (mapped) entry should have been deleted
							log.Errorln("Lock maintenance failed to remove entry for write lock (should never happen)", nlrip.name, nlrip.lri, lri)
						} else {
							// Reader: this can happen if multiple read locks were active and the one we are looking for
							// has been released concurrently (so it is fine)
						}
					} else {
						// remove went okay, all is fine
					}
				}
				l.mutex.Unlock()
			}
		}
	}
}

// Initialize distributed lock.
func initDistributedNSLock(mux *router.Router, serverConfig serverCmdConfig) {
	lockServers := newLockServers(serverConfig)
	registerStorageLockers(mux, lockServers)
}

// Create one lock server for every local storage rpc server.
func newLockServers(serverConfig serverCmdConfig) (lockServers []*lockServer) {
	// Initialize posix storage API.
	exports := serverConfig.disks
	ignoredExports := serverConfig.ignoredDisks

	// Save ignored disks in a map
	skipDisks := make(map[string]bool)
	for _, ignoredExport := range ignoredExports {
		skipDisks[ignoredExport] = true
	}
	for _, export := range exports {
		if skipDisks[export] {
			continue
		}
		if isLocalStorage(export) {
			if idx := strings.LastIndex(export, ":"); idx != -1 {
				export = export[idx+1:]
			}

			// Create handler for lock RPCs
			locker := &lockServer{
				rpcPath:   export,
				mutex:     sync.Mutex{},
				lockMap:   make(map[string][]lockRequesterInfo),
				timestamp: time.Now().UTC(),
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
	}
	return lockServers
}

// registerStorageLockers - register locker rpc handlers for net/rpc library clients
func registerStorageLockers(mux *router.Router, lockServers []*lockServer) {
	for _, lockServer := range lockServers {
		lockRPCServer := rpc.NewServer()
		lockRPCServer.RegisterName("Dsync", lockServer)
		lockRouter := mux.PathPrefix(reservedBucket).Subrouter()
		lockRouter.Path(path.Join("/lock", lockServer.rpcPath)).Handler(lockRPCServer)
	}
}

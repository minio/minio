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
	"net/rpc"
	"path"
	"strings"
	"sync"
	"time"

	router "github.com/gorilla/mux"
)

const lockRPCPath = "/minio/lock"

// LockArgs besides lock name, holds Token and Timestamp for session
// authentication and validation server restart.
type LockArgs struct {
	Name      string
	Token     string
	Timestamp time.Time
}

// SetToken - sets the token to the supplied value.
func (l *LockArgs) SetToken(token string) {
	l.Token = token
}

// SetTimestamp - sets the timestamp to the supplied value.
func (l *LockArgs) SetTimestamp(tstamp time.Time) {
	l.Timestamp = tstamp
}

type lockServer struct {
	rpcPath string
	mutex   sync.Mutex
	// e.g, when a Lock(name) is held, map[string][]bool{"name" : []bool{true}}
	// when one or more RLock() is held, map[string][]bool{"name" : []bool{false, false}}
	lockMap   map[string][]bool
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
	_, ok := l.lockMap[args.Name]
	// No locks held on the given name.
	if !ok {
		*reply = true
		l.lockMap[args.Name] = []bool{true}
	} else {
		// Either a read or write lock is held on the given name.
		*reply = false
	}
	return nil
}

// Unlock - rpc handler for (single) write unlock operation.
func (l *lockServer) Unlock(args *LockArgs, reply *bool) error {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	if err := l.verifyArgs(args); err != nil {
		return err
	}
	locksHeld, ok := l.lockMap[args.Name]
	// No lock is held on the given name, there must be some issue at the lock client side.
	if !ok {
		*reply = false
		return fmt.Errorf("Unlock attempted on an un-locked entity: %s", args.Name)
	} else if len(locksHeld) == 1 && locksHeld[0] == true {
		*reply = true
		delete(l.lockMap, args.Name)
		return nil
	} else {
		*reply = false
		return fmt.Errorf("Unlock attempted on a read locked entity: %s (%d read locks active)", args.Name, len(locksHeld))
	}
}

// RLock - rpc handler for read lock operation.
func (l *lockServer) RLock(args *LockArgs, reply *bool) error {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	if err := l.verifyArgs(args); err != nil {
		return err
	}
	locksHeld, ok := l.lockMap[args.Name]
	// No locks held on the given name.
	if !ok {
		// First read-lock to be held on *name.
		l.lockMap[args.Name] = []bool{false}
		*reply = true
	} else if len(locksHeld) == 1 && locksHeld[0] == true {
		// A write-lock is held, read lock can't be granted.
		*reply = false
	} else {
		// Add an entry for this read lock.
		l.lockMap[args.Name] = append(locksHeld, false)
		*reply = true
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
	locksHeld, ok := l.lockMap[args.Name]
	if !ok {
		*reply = false
		return fmt.Errorf("RUnlock attempted on an un-locked entity: %s", args.Name)
	} else if len(locksHeld) == 1 && locksHeld[0] == true {
		// A write-lock is held, cannot release a read lock
		*reply = false
		return fmt.Errorf("RUnlock attempted on a write locked entity: %s", args.Name)
	} else if len(locksHeld) > 1 {
		// Remove one of the read locks held.
		locksHeld = locksHeld[1:]
		l.lockMap[args.Name] = locksHeld
		*reply = true
	} else {
		// Delete the map entry since this is the last read lock held
		// on *name.
		delete(l.lockMap, args.Name)
		*reply = true
	}
	return nil
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
			lockServers = append(lockServers, &lockServer{
				rpcPath:   export,
				mutex:     sync.Mutex{},
				lockMap:   make(map[string][]bool),
				timestamp: time.Now().UTC(),
			})
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

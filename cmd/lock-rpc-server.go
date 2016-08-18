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

	router "github.com/gorilla/mux"
)

const lockRPCPath = "/minio/lock"

type lockServer struct {
	rpcPath string
	mutex   sync.Mutex
	// e.g, when a Lock(name) is held, map[string][]bool{"name" : []bool{true}}
	// when one or more RLock() is held, map[string][]bool{"name" : []bool{false, false}}
	lockMap map[string][]bool
}

///  Distributed lock handlers

// LockHandler - rpc handler for lock operation.
func (l *lockServer) Lock(name *string, reply *bool) error {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	_, ok := l.lockMap[*name]
	// No locks held on the given name.
	if !ok {
		*reply = true
		l.lockMap[*name] = []bool{true}
		return nil
	}
	// Either a read or write lock is held on the given name.
	*reply = false
	return nil
}

// UnlockHandler - rpc handler for unlock operation.
func (l *lockServer) Unlock(name *string, reply *bool) error {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	_, ok := l.lockMap[*name]
	// No lock is held on the given name, there must be some issue at the lock client side.
	if !ok {
		return fmt.Errorf("Unlock attempted on an un-locked entity: %s", *name)
	}
	*reply = true
	delete(l.lockMap, *name)
	return nil
}

func (l *lockServer) RLock(name *string, reply *bool) error {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	locksHeld, ok := l.lockMap[*name]
	// No locks held on the given name.
	if !ok {
		// First read-lock to be held on *name.
		l.lockMap[*name] = []bool{false}
		*reply = true
	} else if len(locksHeld) == 1 && locksHeld[0] == true {
		// A write-lock is held, read lock can't be granted.
		*reply = false
	} else {
		// Add an entry for this read lock.
		l.lockMap[*name] = append(locksHeld, false)
		*reply = true
	}

	return nil
}

func (l *lockServer) RUnlock(name *string, reply *bool) error {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	locksHeld, ok := l.lockMap[*name]
	if !ok {
		return fmt.Errorf("RUnlock attempted on an un-locked entity: %s", *name)
	}
	if len(locksHeld) > 1 {
		// Remove one of the read locks held.
		locksHeld = locksHeld[1:]
		l.lockMap[*name] = locksHeld
		*reply = true
	} else {
		// Delete the map entry since this is the last read lock held
		// on *name.
		delete(l.lockMap, *name)
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
				rpcPath: export,
				mutex:   sync.Mutex{},
				lockMap: make(map[string][]bool),
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

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
	"fmt"
	"net/rpc"
	"path"
	"strings"
	"sync"

	router "github.com/gorilla/mux"
)

const lockRPCPath = "/lock"

type lockServer struct {
	rpcPath string
	mutex   sync.Mutex
	lockMap map[string]struct{}
}

///  Distributed lock handlers

// LockHandler - rpc handler for lock operation.
func (l *lockServer) LockHandler(name *string, reply *bool) error {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	_, ok := l.lockMap[*name]
	if !ok {
		*reply = true
		l.lockMap[*name] = struct{}{}
		return nil
	}
	*reply = false
	return nil
}

// UnlockHandler - rpc handler for unlock operation.
func (l *lockServer) UnlockHandler(name *string, reply *bool) error {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	_, ok := l.lockMap[*name]
	if !ok {
		return fmt.Errorf("Unlock attempted on an un-locked entity: %s", *name)
	}
	*reply = true
	delete(l.lockMap, *name)
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
		if idx := strings.LastIndex(export, ":"); idx != -1 {
			export = export[idx+1:]
		}
		lockServers = append(lockServers, &lockServer{
			rpcPath: export,
			mutex:   sync.Mutex{},
			lockMap: make(map[string]struct{}),
		})
	}
	return lockServers
}

// registerStorageLockers - register locker rpc handlers for valyala/gorpc library clients
func registerStorageLockers(mux *router.Router, lockServers []*lockServer) {
	lockRPCServer := rpc.NewServer()
	for _, lockServer := range lockServers {
		lockRPCServer.RegisterName("Dsync", lockServer)
		lockRouter := mux.PathPrefix(reservedBucket).Subrouter()
		lockRouter.Path(path.Join("/lock", lockServer.rpcPath)).Handler(lockRPCServer)
	}
}

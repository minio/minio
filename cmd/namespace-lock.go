/*
 * MinIO Cloud Storage, (C) 2016, 2017, 2018, 2019 MinIO, Inc.
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
	"context"
	"errors"
	pathutil "path"
	"runtime"
	"sort"
	"strings"
	"sync"

	"fmt"
	"time"

	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/dsync"
	"github.com/minio/minio/pkg/lsync"
)

// local lock servers
var globalLockServers = make(map[Endpoint]*localLocker)

// RWLocker - locker interface to introduce GetRLock, RUnlock.
type RWLocker interface {
	GetLock(timeout *dynamicTimeout) (timedOutErr error)
	Unlock()
	GetRLock(timeout *dynamicTimeout) (timedOutErr error)
	RUnlock()
}

// newNSLock - return a new name space lock map.
func newNSLock(isDistErasure bool) *nsLockMap {
	nsMutex := nsLockMap{
		isDistErasure: isDistErasure,
	}
	if isDistErasure {
		return &nsMutex
	}
	nsMutex.lockMap = make(map[string]*nsLock)
	return &nsMutex
}

// nsLock - provides primitives for locking critical namespace regions.
type nsLock struct {
	ref int32
	*lsync.LRWMutex
}

// nsLockMap - namespace lock map, provides primitives to Lock,
// Unlock, RLock and RUnlock.
type nsLockMap struct {
	// Indicates if namespace is part of a distributed setup.
	isDistErasure bool
	lockMap       map[string]*nsLock
	lockMapMutex  sync.Mutex
}

// Lock the namespace resource.
func (n *nsLockMap) lock(ctx context.Context, volume string, path string, lockSource, opsID string, readLock bool, timeout time.Duration) (locked bool) {
	resource := pathJoin(volume, path)

	n.lockMapMutex.Lock()
	nsLk, found := n.lockMap[resource]
	if !found {
		nsLk = &nsLock{
			LRWMutex: lsync.NewLRWMutex(),
		}
		// Add a count to indicate that a parallel unlock doesn't clear this entry.
	}
	nsLk.ref++
	n.lockMap[resource] = nsLk
	n.lockMapMutex.Unlock()

	// Locking here will block (until timeout).
	if readLock {
		locked = nsLk.GetRLock(ctx, opsID, lockSource, timeout)
	} else {
		locked = nsLk.GetLock(ctx, opsID, lockSource, timeout)
	}

	if !locked { // We failed to get the lock
		// Decrement ref count since we failed to get the lock
		n.lockMapMutex.Lock()
		n.lockMap[resource].ref--
		if n.lockMap[resource].ref < 0 {
			logger.CriticalIf(GlobalContext, errors.New("resource reference count was lower than 0"))
		}
		if n.lockMap[resource].ref == 0 {
			// Remove from the map if there are no more references.
			delete(n.lockMap, resource)
		}
		n.lockMapMutex.Unlock()
	}

	return
}

// Unlock the namespace resource.
func (n *nsLockMap) unlock(volume string, path string, readLock bool) {
	resource := pathJoin(volume, path)

	n.lockMapMutex.Lock()
	defer n.lockMapMutex.Unlock()
	if _, found := n.lockMap[resource]; !found {
		return
	}
	if readLock {
		n.lockMap[resource].RUnlock()
	} else {
		n.lockMap[resource].Unlock()
	}
	n.lockMap[resource].ref--
	if n.lockMap[resource].ref < 0 {
		logger.CriticalIf(GlobalContext, errors.New("resource reference count was lower than 0"))
	}
	if n.lockMap[resource].ref == 0 {
		// Remove from the map if there are no more references.
		delete(n.lockMap, resource)
	}
}

// dsync's distributed lock instance.
type distLockInstance struct {
	rwMutex *dsync.DRWMutex
	opsID   string
	ctx     context.Context
}

// Lock - block until write lock is taken or timeout has occurred.
func (di *distLockInstance) GetLock(timeout *dynamicTimeout) (timedOutErr error) {
	lockSource := getSource(2)
	start := UTCNow()

	if !di.rwMutex.GetLock(di.ctx, di.opsID, lockSource, timeout.Timeout()) {
		timeout.LogFailure()
		return OperationTimedOut{}
	}
	timeout.LogSuccess(UTCNow().Sub(start))
	return nil
}

// Unlock - block until write lock is released.
func (di *distLockInstance) Unlock() {
	di.rwMutex.Unlock()
}

// RLock - block until read lock is taken or timeout has occurred.
func (di *distLockInstance) GetRLock(timeout *dynamicTimeout) (timedOutErr error) {
	lockSource := getSource(2)
	start := UTCNow()
	if !di.rwMutex.GetRLock(di.ctx, di.opsID, lockSource, timeout.Timeout()) {
		timeout.LogFailure()
		return OperationTimedOut{}
	}
	timeout.LogSuccess(UTCNow().Sub(start))
	return nil
}

// RUnlock - block until read lock is released.
func (di *distLockInstance) RUnlock() {
	di.rwMutex.RUnlock()
}

// localLockInstance - frontend/top-level interface for namespace locks.
type localLockInstance struct {
	ctx    context.Context
	ns     *nsLockMap
	volume string
	paths  []string
	opsID  string
}

// NewNSLock - returns a lock instance for a given volume and
// path. The returned lockInstance object encapsulates the nsLockMap,
// volume, path and operation ID.
func (n *nsLockMap) NewNSLock(ctx context.Context, lockersFn func() []dsync.NetLocker, volume string, paths ...string) RWLocker {
	opsID := mustGetUUID()
	if n.isDistErasure {
		drwmutex := dsync.NewDRWMutex(&dsync.Dsync{
			GetLockersFn: lockersFn,
		}, pathsJoinPrefix(volume, paths...)...)
		return &distLockInstance{drwmutex, opsID, ctx}
	}
	sort.Strings(paths)
	return &localLockInstance{ctx, n, volume, paths, opsID}
}

// Lock - block until write lock is taken or timeout has occurred.
func (li *localLockInstance) GetLock(timeout *dynamicTimeout) (timedOutErr error) {
	lockSource := getSource(2)
	start := UTCNow()
	readLock := false
	var success []int
	for i, path := range li.paths {
		if !li.ns.lock(li.ctx, li.volume, path, lockSource, li.opsID, readLock, timeout.Timeout()) {
			timeout.LogFailure()
			for _, sint := range success {
				li.ns.unlock(li.volume, li.paths[sint], readLock)
			}
			return OperationTimedOut{}
		}
		success = append(success, i)
	}
	timeout.LogSuccess(UTCNow().Sub(start))
	return
}

// Unlock - block until write lock is released.
func (li *localLockInstance) Unlock() {
	readLock := false
	for _, path := range li.paths {
		li.ns.unlock(li.volume, path, readLock)
	}
}

// RLock - block until read lock is taken or timeout has occurred.
func (li *localLockInstance) GetRLock(timeout *dynamicTimeout) (timedOutErr error) {
	lockSource := getSource(2)
	start := UTCNow()
	readLock := true
	var success []int
	for i, path := range li.paths {
		if !li.ns.lock(li.ctx, li.volume, path, lockSource, li.opsID, readLock, timeout.Timeout()) {
			timeout.LogFailure()
			for _, sint := range success {
				li.ns.unlock(li.volume, li.paths[sint], readLock)
			}
			return OperationTimedOut{}
		}
		success = append(success, i)
	}
	timeout.LogSuccess(UTCNow().Sub(start))
	return
}

// RUnlock - block until read lock is released.
func (li *localLockInstance) RUnlock() {
	readLock := true
	for _, path := range li.paths {
		li.ns.unlock(li.volume, path, readLock)
	}
}

func getSource(n int) string {
	var funcName string
	pc, filename, lineNum, ok := runtime.Caller(n)
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

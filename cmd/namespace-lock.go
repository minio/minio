// Copyright (c) 2015-2021 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package cmd

import (
	"context"
	"errors"
	"fmt"
	pathutil "path"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/minio/minio/internal/dsync"
	"github.com/minio/minio/internal/logger"
	"github.com/minio/minio/internal/lsync"
)

// local lock servers
var globalLockServer *localLocker

// RWLocker - locker interface to introduce GetRLock, RUnlock.
type RWLocker interface {
	GetLock(ctx context.Context, timeout *dynamicTimeout) (lkCtx LockContext, timedOutErr error)
	Unlock(lkCtx LockContext)
	GetRLock(ctx context.Context, timeout *dynamicTimeout) (lkCtx LockContext, timedOutErr error)
	RUnlock(lkCtx LockContext)
}

// LockContext lock context holds the lock backed context and canceler for the context.
type LockContext struct {
	ctx    context.Context
	cancel context.CancelFunc
}

// Context returns lock context
func (l LockContext) Context() context.Context {
	return l.ctx
}

// Cancel function calls cancel() function
func (l LockContext) Cancel() {
	if l.cancel != nil {
		l.cancel()
	}
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

	return locked
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
}

// Lock - block until write lock is taken or timeout has occurred.
func (di *distLockInstance) GetLock(ctx context.Context, timeout *dynamicTimeout) (LockContext, error) {
	lockSource := getSource(2)
	start := UTCNow()

	newCtx, cancel := context.WithCancel(ctx)
	if !di.rwMutex.GetLock(newCtx, cancel, di.opsID, lockSource, dsync.Options{
		Timeout:       timeout.Timeout(),
		RetryInterval: timeout.RetryInterval(),
	}) {
		timeout.LogFailure()
		defer cancel()
		if err := newCtx.Err(); err == context.Canceled {
			return LockContext{ctx: ctx, cancel: func() {}}, err
		}
		return LockContext{ctx: ctx, cancel: func() {}}, OperationTimedOut{}
	}
	timeout.LogSuccess(UTCNow().Sub(start))
	return LockContext{ctx: newCtx, cancel: cancel}, nil
}

// Unlock - block until write lock is released.
func (di *distLockInstance) Unlock(lc LockContext) {
	if lc.cancel != nil {
		lc.cancel()
	}
	di.rwMutex.Unlock(context.Background())
}

// RLock - block until read lock is taken or timeout has occurred.
func (di *distLockInstance) GetRLock(ctx context.Context, timeout *dynamicTimeout) (LockContext, error) {
	lockSource := getSource(2)
	start := UTCNow()

	newCtx, cancel := context.WithCancel(ctx)
	if !di.rwMutex.GetRLock(ctx, cancel, di.opsID, lockSource, dsync.Options{
		Timeout:       timeout.Timeout(),
		RetryInterval: timeout.RetryInterval(),
	}) {
		timeout.LogFailure()
		defer cancel()
		if errors.Is(newCtx.Err(), context.Canceled) {
			return LockContext{ctx: ctx, cancel: func() {}}, newCtx.Err()
		}
		return LockContext{ctx: ctx, cancel: func() {}}, OperationTimedOut{}
	}
	timeout.LogSuccess(UTCNow().Sub(start))
	return LockContext{ctx: newCtx, cancel: cancel}, nil
}

// RUnlock - block until read lock is released.
func (di *distLockInstance) RUnlock(lc LockContext) {
	if lc.cancel != nil {
		lc.cancel()
	}
	di.rwMutex.RUnlock(lc.ctx)
}

// localLockInstance - frontend/top-level interface for namespace locks.
type localLockInstance struct {
	ns     *nsLockMap
	volume string
	paths  []string
	opsID  string
}

// NewNSLock - returns a lock instance for a given volume and
// path. The returned lockInstance object encapsulates the nsLockMap,
// volume, path and operation ID.
func (n *nsLockMap) NewNSLock(lockers func() ([]dsync.NetLocker, string), volume string, paths ...string) RWLocker {
	sort.Strings(paths)
	opsID := mustGetUUID()
	if n.isDistErasure {
		drwmutex := dsync.NewDRWMutex(&dsync.Dsync{
			GetLockers: lockers,
			Timeouts:   dsync.DefaultTimeouts,
		}, pathsJoinPrefix(volume, paths...)...)
		return &distLockInstance{drwmutex, opsID}
	}
	return &localLockInstance{n, volume, paths, opsID}
}

// Lock - block until write lock is taken or timeout has occurred.
func (li *localLockInstance) GetLock(ctx context.Context, timeout *dynamicTimeout) (_ LockContext, timedOutErr error) {
	lockSource := getSource(2)
	start := UTCNow()
	const readLock = false
	success := make([]int, len(li.paths))
	for i, path := range li.paths {
		if !li.ns.lock(ctx, li.volume, path, lockSource, li.opsID, readLock, timeout.Timeout()) {
			timeout.LogFailure()
			for si, sint := range success {
				if sint == 1 {
					li.ns.unlock(li.volume, li.paths[si], readLock)
				}
			}
			if errors.Is(ctx.Err(), context.Canceled) {
				return LockContext{}, ctx.Err()
			}
			return LockContext{}, OperationTimedOut{}
		}
		success[i] = 1
	}
	timeout.LogSuccess(UTCNow().Sub(start))
	return LockContext{ctx: ctx, cancel: func() {}}, nil
}

// Unlock - block until write lock is released.
func (li *localLockInstance) Unlock(lc LockContext) {
	if lc.cancel != nil {
		lc.cancel()
	}
	const readLock = false
	for _, path := range li.paths {
		li.ns.unlock(li.volume, path, readLock)
	}
}

// RLock - block until read lock is taken or timeout has occurred.
func (li *localLockInstance) GetRLock(ctx context.Context, timeout *dynamicTimeout) (_ LockContext, timedOutErr error) {
	lockSource := getSource(2)
	start := UTCNow()
	const readLock = true
	success := make([]int, len(li.paths))
	for i, path := range li.paths {
		if !li.ns.lock(ctx, li.volume, path, lockSource, li.opsID, readLock, timeout.Timeout()) {
			timeout.LogFailure()
			for si, sint := range success {
				if sint == 1 {
					li.ns.unlock(li.volume, li.paths[si], readLock)
				}
			}
			if err := ctx.Err(); err == context.Canceled {
				return LockContext{}, err
			}
			return LockContext{}, OperationTimedOut{}
		}
		success[i] = 1
	}
	timeout.LogSuccess(UTCNow().Sub(start))
	return LockContext{ctx: ctx, cancel: func() {}}, nil
}

// RUnlock - block until read lock is released.
func (li *localLockInstance) RUnlock(lc LockContext) {
	if lc.cancel != nil {
		lc.cancel()
	}
	const readLock = true
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

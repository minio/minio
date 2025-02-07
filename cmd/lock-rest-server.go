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
	"time"

	"github.com/minio/minio/internal/dsync"
	"github.com/minio/minio/internal/grid"
	"github.com/minio/minio/internal/logger"
)

// To abstract a node over network.
type lockRESTServer struct {
	ll *localLocker
}

// RefreshHandler - refresh the current lock
func (l *lockRESTServer) RefreshHandler(args *dsync.LockArgs) (*dsync.LockResp, *grid.RemoteErr) {
	// Add a timeout similar to what we expect upstream.
	ctx, cancel := context.WithTimeout(context.Background(), dsync.DefaultTimeouts.RefreshCall)
	defer cancel()

	resp := lockRPCRefresh.NewResponse()
	refreshed, err := l.ll.Refresh(ctx, *args)
	if err != nil {
		return l.makeResp(resp, err)
	}
	if !refreshed {
		return l.makeResp(resp, errLockNotFound)
	}
	return l.makeResp(resp, err)
}

// LockHandler - Acquires a lock.
func (l *lockRESTServer) LockHandler(args *dsync.LockArgs) (*dsync.LockResp, *grid.RemoteErr) {
	// Add a timeout similar to what we expect upstream.
	ctx, cancel := context.WithTimeout(context.Background(), dsync.DefaultTimeouts.Acquire)
	defer cancel()
	resp := lockRPCLock.NewResponse()
	success, err := l.ll.Lock(ctx, *args)
	if err == nil && !success {
		return l.makeResp(resp, errLockConflict)
	}
	return l.makeResp(resp, err)
}

// UnlockHandler - releases the acquired lock.
func (l *lockRESTServer) UnlockHandler(args *dsync.LockArgs) (*dsync.LockResp, *grid.RemoteErr) {
	resp := lockRPCUnlock.NewResponse()
	_, err := l.ll.Unlock(context.Background(), *args)
	// Ignore the Unlock() "reply" return value because if err == nil, "reply" is always true
	// Consequently, if err != nil, reply is always false
	return l.makeResp(resp, err)
}

// RLockHandler - Acquires an RLock.
func (l *lockRESTServer) RLockHandler(args *dsync.LockArgs) (*dsync.LockResp, *grid.RemoteErr) {
	// Add a timeout similar to what we expect upstream.
	ctx, cancel := context.WithTimeout(context.Background(), dsync.DefaultTimeouts.Acquire)
	defer cancel()
	resp := lockRPCRLock.NewResponse()
	success, err := l.ll.RLock(ctx, *args)
	if err == nil && !success {
		err = errLockConflict
	}
	return l.makeResp(resp, err)
}

// RUnlockHandler - releases the acquired read lock.
func (l *lockRESTServer) RUnlockHandler(args *dsync.LockArgs) (*dsync.LockResp, *grid.RemoteErr) {
	resp := lockRPCRUnlock.NewResponse()

	// Ignore the RUnlock() "reply" return value because if err == nil, "reply" is always true.
	// Consequently, if err != nil, reply is always false
	_, err := l.ll.RUnlock(context.Background(), *args)
	return l.makeResp(resp, err)
}

// ForceUnlockHandler - query expired lock status.
func (l *lockRESTServer) ForceUnlockHandler(args *dsync.LockArgs) (*dsync.LockResp, *grid.RemoteErr) {
	resp := lockRPCForceUnlock.NewResponse()

	_, err := l.ll.ForceUnlock(context.Background(), *args)
	return l.makeResp(resp, err)
}

var (
	// Static lock handlers.
	// All have the same signature.
	lockRPCForceUnlock = newLockHandler(grid.HandlerLockForceUnlock)
	lockRPCRefresh     = newLockHandler(grid.HandlerLockRefresh)
	lockRPCLock        = newLockHandler(grid.HandlerLockLock)
	lockRPCUnlock      = newLockHandler(grid.HandlerLockUnlock)
	lockRPCRLock       = newLockHandler(grid.HandlerLockRLock)
	lockRPCRUnlock     = newLockHandler(grid.HandlerLockRUnlock)
)

func newLockHandler(h grid.HandlerID) *grid.SingleHandler[*dsync.LockArgs, *dsync.LockResp] {
	return grid.NewSingleHandler[*dsync.LockArgs, *dsync.LockResp](h, func() *dsync.LockArgs {
		return &dsync.LockArgs{}
	}, func() *dsync.LockResp {
		return &dsync.LockResp{}
	})
}

// registerLockRESTHandlers - register lock rest router.
func registerLockRESTHandlers(gm *grid.Manager) {
	lockServer := &lockRESTServer{
		ll: newLocker(),
	}

	logger.FatalIf(lockRPCForceUnlock.Register(gm, lockServer.ForceUnlockHandler), "unable to register handler")
	logger.FatalIf(lockRPCRefresh.Register(gm, lockServer.RefreshHandler), "unable to register handler")
	logger.FatalIf(lockRPCLock.Register(gm, lockServer.LockHandler), "unable to register handler")
	logger.FatalIf(lockRPCUnlock.Register(gm, lockServer.UnlockHandler), "unable to register handler")
	logger.FatalIf(lockRPCRLock.Register(gm, lockServer.RLockHandler), "unable to register handler")
	logger.FatalIf(lockRPCRUnlock.Register(gm, lockServer.RUnlockHandler), "unable to register handler")

	globalLockServer = lockServer.ll

	go lockMaintenance(GlobalContext)
}

func (l *lockRESTServer) makeResp(dst *dsync.LockResp, err error) (*dsync.LockResp, *grid.RemoteErr) {
	*dst = dsync.LockResp{Code: dsync.RespOK}
	switch err {
	case nil:
	case errLockNotInitialized:
		dst.Code = dsync.RespLockNotInitialized
	case errLockConflict:
		dst.Code = dsync.RespLockConflict
	case errLockNotFound:
		dst.Code = dsync.RespLockNotFound
	default:
		dst.Code = dsync.RespErr
		dst.Err = err.Error()
	}
	return dst, nil
}

const (
	// Lock maintenance interval.
	lockMaintenanceInterval = 1 * time.Minute

	// Lock validity duration
	lockValidityDuration = 1 * time.Minute
)

// lockMaintenance loops over all locks and discards locks
// that have not been refreshed for some time.
func lockMaintenance(ctx context.Context) {
	if !globalIsDistErasure {
		return
	}

	// Initialize a new ticker with 1 minute between each ticks.
	lkTimer := time.NewTimer(lockMaintenanceInterval)
	// Stop the timer upon returning.
	defer lkTimer.Stop()

	for {
		// Verifies every minute for locks held more than 2 minutes.
		select {
		case <-ctx.Done():
			return
		case <-lkTimer.C:
			globalLockServer.expireOldLocks(lockValidityDuration)

			// Reset the timer for next cycle.
			lkTimer.Reset(lockMaintenanceInterval)
		}
	}
}

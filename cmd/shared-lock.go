// Copyright (c) 2015-2022 MinIO, Inc.
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
)

var sharedLockTimeout = newDynamicTimeoutWithOpts(dynamicTimeoutOpts{
	timeout:       30 * time.Second,
	minimum:       10 * time.Second,
	retryInterval: time.Minute,
})

type sharedLock struct {
	lockContext chan LockContext
}

func (ld sharedLock) backgroundRoutine(ctx context.Context, objAPI ObjectLayer, lockName string) {
	for {
		locker := objAPI.NewNSLock(minioMetaBucket, lockName)
		lkctx, err := locker.GetLock(ctx, sharedLockTimeout)
		if err != nil {
			continue
		}

	keepLock:
		for {
			select {
			case <-ctx.Done():
				return
			case <-lkctx.Context().Done():
				// The context of the lock is canceled, this can happen
				// if one lock lost quorum due to cluster instability
				// in that case, try to lock again.
				break keepLock
			case ld.lockContext <- lkctx:
				// Send the lock context to anyone asking for it
			}
		}
	}
}

func mergeContext(ctx1, ctx2 context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		select {
		case <-ctx1.Done():
		case <-ctx2.Done():
		// The lock acquirer decides to cancel, exit this goroutine
		case <-ctx.Done():
		}

		cancel()
	}()
	return ctx, cancel
}

func (ld sharedLock) GetLock(ctx context.Context) (context.Context, context.CancelFunc) {
	l := <-ld.lockContext
	return mergeContext(l.Context(), ctx)
}

func newSharedLock(ctx context.Context, objAPI ObjectLayer, lockName string) *sharedLock {
	l := &sharedLock{
		lockContext: make(chan LockContext),
	}
	go l.backgroundRoutine(ctx, objAPI, lockName)

	return l
}

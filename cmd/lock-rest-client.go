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

	"github.com/minio/minio/internal/dsync"
	"github.com/minio/minio/internal/grid"
)

// lockRESTClient is authenticable lock REST client
type lockRESTClient struct {
	connection *grid.Connection
}

// IsOnline - returns whether REST client failed to connect or not.
func (c *lockRESTClient) IsOnline() bool {
	return c.connection.State() == grid.StateConnected
}

// Not a local locker
func (c *lockRESTClient) IsLocal() bool {
	return false
}

// Close - marks the client as closed.
func (c *lockRESTClient) Close() error {
	return nil
}

// String - returns the remote host of the connection.
func (c *lockRESTClient) String() string {
	return c.connection.Remote
}

func (c *lockRESTClient) call(ctx context.Context, h *grid.SingleHandler[*dsync.LockArgs, *dsync.LockResp], args *dsync.LockArgs) (ok bool, err error) {
	r, err := h.Call(ctx, c.connection, args)
	if err != nil {
		return false, err
	}
	defer h.PutResponse(r)
	ok = r.Code == dsync.RespOK
	switch r.Code {
	case dsync.RespLockConflict, dsync.RespLockNotFound, dsync.RespOK:
	// no error
	case dsync.RespLockNotInitialized:
		err = errLockNotInitialized
	default:
		err = errors.New(r.Err)
	}
	return ok, err
}

// RLock calls read lock REST API.
func (c *lockRESTClient) RLock(ctx context.Context, args dsync.LockArgs) (reply bool, err error) {
	return c.call(ctx, lockRPCRLock, &args)
}

// Lock calls lock REST API.
func (c *lockRESTClient) Lock(ctx context.Context, args dsync.LockArgs) (reply bool, err error) {
	return c.call(ctx, lockRPCLock, &args)
}

// RUnlock calls read unlock REST API.
func (c *lockRESTClient) RUnlock(ctx context.Context, args dsync.LockArgs) (reply bool, err error) {
	return c.call(ctx, lockRPCRUnlock, &args)
}

// Refresh calls Refresh REST API.
func (c *lockRESTClient) Refresh(ctx context.Context, args dsync.LockArgs) (reply bool, err error) {
	return c.call(ctx, lockRPCRefresh, &args)
}

// Unlock calls write unlock RPC.
func (c *lockRESTClient) Unlock(ctx context.Context, args dsync.LockArgs) (reply bool, err error) {
	return c.call(ctx, lockRPCUnlock, &args)
}

// ForceUnlock calls force unlock handler to forcibly unlock an active lock.
func (c *lockRESTClient) ForceUnlock(ctx context.Context, args dsync.LockArgs) (reply bool, err error) {
	return c.call(ctx, lockRPCForceUnlock, &args)
}

func newLockAPI(endpoint Endpoint) dsync.NetLocker {
	if endpoint.IsLocal {
		return globalLockServer
	}
	return newlockRESTClient(endpoint)
}

// Returns a lock rest client.
func newlockRESTClient(ep Endpoint) *lockRESTClient {
	return &lockRESTClient{globalLockGrid.Load().Connection(ep.GridHost())}
}

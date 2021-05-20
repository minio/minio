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

package dsync

import (
	"context"
	"net/rpc"
	"sync"
)

// ReconnectRPCClient is a wrapper type for rpc.Client which provides reconnect on first failure.
type ReconnectRPCClient struct {
	mutex    sync.Mutex
	rpc      *rpc.Client
	addr     string
	endpoint string
}

// newClient constructs a ReconnectRPCClient object with addr and endpoint initialized.
// It _doesn't_ connect to the remote endpoint. See Call method to see when the
// connect happens.
func newClient(addr, endpoint string) NetLocker {
	return &ReconnectRPCClient{
		addr:     addr,
		endpoint: endpoint,
	}
}

// Close closes the underlying socket file descriptor.
func (rpcClient *ReconnectRPCClient) IsOnline() bool {
	rpcClient.mutex.Lock()
	defer rpcClient.mutex.Unlock()
	// If rpc client has not connected yet there is nothing to close.
	return rpcClient.rpc != nil
}

func (rpcClient *ReconnectRPCClient) IsLocal() bool {
	return false
}

// Close closes the underlying socket file descriptor.
func (rpcClient *ReconnectRPCClient) Close() error {
	rpcClient.mutex.Lock()
	defer rpcClient.mutex.Unlock()
	// If rpc client has not connected yet there is nothing to close.
	if rpcClient.rpc == nil {
		return nil
	}
	// Reset rpcClient.rpc to allow for subsequent calls to use a new
	// (socket) connection.
	clnt := rpcClient.rpc
	rpcClient.rpc = nil
	return clnt.Close()
}

// Call makes a RPC call to the remote endpoint using the default codec, namely encoding/gob.
func (rpcClient *ReconnectRPCClient) Call(serviceMethod string, args interface{}, reply interface{}) (err error) {
	rpcClient.mutex.Lock()
	defer rpcClient.mutex.Unlock()
	dialCall := func() error {
		// If the rpc.Client is nil, we attempt to (re)connect with the remote endpoint.
		if rpcClient.rpc == nil {
			clnt, derr := rpc.DialHTTPPath("tcp", rpcClient.addr, rpcClient.endpoint)
			if derr != nil {
				return derr
			}
			rpcClient.rpc = clnt
		}
		// If the RPC fails due to a network-related error, then we reset
		// rpc.Client for a subsequent reconnect.
		return rpcClient.rpc.Call(serviceMethod, args, reply)
	}
	if err = dialCall(); err == rpc.ErrShutdown {
		rpcClient.rpc.Close()
		rpcClient.rpc = nil
		err = dialCall()
	}
	return err
}

func (rpcClient *ReconnectRPCClient) RLock(ctx context.Context, args LockArgs) (status bool, err error) {
	err = rpcClient.Call("Dsync.RLock", &args, &status)
	return status, err
}

func (rpcClient *ReconnectRPCClient) Lock(ctx context.Context, args LockArgs) (status bool, err error) {
	err = rpcClient.Call("Dsync.Lock", &args, &status)
	return status, err
}

func (rpcClient *ReconnectRPCClient) RUnlock(ctx context.Context, args LockArgs) (status bool, err error) {
	err = rpcClient.Call("Dsync.RUnlock", &args, &status)
	return status, err
}

func (rpcClient *ReconnectRPCClient) Unlock(ctx context.Context, args LockArgs) (status bool, err error) {
	err = rpcClient.Call("Dsync.Unlock", &args, &status)
	return status, err
}

func (rpcClient *ReconnectRPCClient) Refresh(ctx context.Context, args LockArgs) (refreshed bool, err error) {
	err = rpcClient.Call("Dsync.Refresh", &args, &refreshed)
	return refreshed, err
}

func (rpcClient *ReconnectRPCClient) ForceUnlock(ctx context.Context, args LockArgs) (reply bool, err error) {
	err = rpcClient.Call("Dsync.ForceUnlock", &args, &reply)
	return reply, err
}

func (rpcClient *ReconnectRPCClient) String() string {
	return "http://" + rpcClient.addr + "/" + rpcClient.endpoint
}

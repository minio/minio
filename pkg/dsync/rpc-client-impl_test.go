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

package dsync_test

import (
	"context"
	"net/rpc"
	"sync"

	. "github.com/minio/minio/pkg/dsync"
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

func (rpcClient *ReconnectRPCClient) RUnlock(args LockArgs) (status bool, err error) {
	err = rpcClient.Call("Dsync.RUnlock", &args, &status)
	return status, err
}

func (rpcClient *ReconnectRPCClient) Unlock(args LockArgs) (status bool, err error) {
	err = rpcClient.Call("Dsync.Unlock", &args, &status)
	return status, err
}

func (rpcClient *ReconnectRPCClient) Expired(ctx context.Context, args LockArgs) (expired bool, err error) {
	err = rpcClient.Call("Dsync.Expired", &args, &expired)
	return expired, err
}

func (rpcClient *ReconnectRPCClient) String() string {
	return "http://" + rpcClient.addr + "/" + rpcClient.endpoint
}

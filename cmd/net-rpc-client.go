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
	"errors"
	"net/rpc"
	"sync"
)

// RPCClient is a wrapper type for rpc.Client which provides reconnect on first failure.
type RPCClient struct {
	mu         sync.Mutex
	rpcPrivate *rpc.Client
	node       string
	rpcPath    string
}

// newClient constructs a RPCClient object with node and rpcPath initialized.
// It _doesn't_ connect to the remote endpoint. See Call method to see when the
// connect happens.
func newClient(node, rpcPath string) *RPCClient {
	return &RPCClient{
		node:    node,
		rpcPath: rpcPath,
	}
}

// clearRPCClient clears the pointer to the rpc.Client object in a safe manner
func (rpcClient *RPCClient) clearRPCClient() {
	rpcClient.mu.Lock()
	rpcClient.rpcPrivate = nil
	rpcClient.mu.Unlock()
}

// getRPCClient gets the pointer to the rpc.Client object in a safe manner
func (rpcClient *RPCClient) getRPCClient() *rpc.Client {
	rpcClient.mu.Lock()
	rpcLocalStack := rpcClient.rpcPrivate
	rpcClient.mu.Unlock()
	return rpcLocalStack
}

// dialRPCClient tries to establish a connection to the server in a safe manner
func (rpcClient *RPCClient) dialRPCClient() (*rpc.Client, error) {
	rpcClient.mu.Lock()
	defer rpcClient.mu.Unlock()
	// After acquiring lock, check whether another thread may not have already dialed and established connection
	if rpcClient.rpcPrivate != nil {
		return rpcClient.rpcPrivate, nil
	}
	rpc, err := rpc.DialHTTPPath("tcp", rpcClient.node, rpcClient.rpcPath)
	if err != nil {
		return nil, err
	} else if rpc == nil {
		return nil, errors.New("No valid RPC Client created after dial")
	}
	rpcClient.rpcPrivate = rpc
	return rpcClient.rpcPrivate, nil
}

// Call makes a RPC call to the remote endpoint using the default codec, namely encoding/gob.
func (rpcClient *RPCClient) Call(serviceMethod string, args interface{}, reply interface{}) error {
	// Make a copy below so that we can safely (continue to) work with the rpc.Client.
	// Even in the case the two threads would simultaneously find that the connection is not initialised,
	// they would both attempt to dial and only one of them would succeed in doing so.
	rpcLocalStack := rpcClient.getRPCClient()

	// If the rpc.Client is nil, we attempt to (re)connect with the remote endpoint.
	if rpcLocalStack == nil {
		var err error
		rpcLocalStack, err = rpcClient.dialRPCClient()
		if err != nil {
			return err
		}
	}

	// If the RPC fails due to a network-related error, then we reset
	// rpc.Client for a subsequent reconnect.
	err := rpcLocalStack.Call(serviceMethod, args, reply)
	if err != nil {
		if err.Error() == rpc.ErrShutdown.Error() {
			// Reset rpcClient.rpc to nil to trigger a reconnect in future
			// and close the underlying connection.
			rpcClient.clearRPCClient()

			// Close the underlying connection.
			rpcLocalStack.Close()

			// Set rpc error as rpc.ErrShutdown type.
			err = rpc.ErrShutdown
		}
	}
	return err
}

// Close closes the underlying socket file descriptor.
func (rpcClient *RPCClient) Close() error {
	// See comment above for making a copy on local stack
	rpcLocalStack := rpcClient.getRPCClient()

	// If rpc client has not connected yet there is nothing to close.
	if rpcLocalStack == nil {
		return nil
	}

	// Reset rpcClient.rpc to allow for subsequent calls to use a new
	// (socket) connection.
	rpcClient.clearRPCClient()
	return rpcLocalStack.Close()
}

// Node returns the node (network address) of the connection
func (rpcClient *RPCClient) Node() string {
	return rpcClient.node
}

// RPCPath returns the RPC path of the connection
func (rpcClient *RPCClient) RPCPath() string {
	return rpcClient.rpcPath
}

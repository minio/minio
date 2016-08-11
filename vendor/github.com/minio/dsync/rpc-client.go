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

package dsync

import (
	"net/rpc"
	"reflect"
	"sync"
)

// Wrapper type for rpc.Client that provides connection management like
// reconnect on first failure.
type RPCClient struct {
	sync.Mutex
	rpc     *rpc.Client
	node    string
	rpcPath string
}

func newClient(node, rpcPath string) *RPCClient {
	return &RPCClient{
		node:    node,
		rpcPath: rpcPath,
	}
}

func (rpcClient *RPCClient) SetRPC(rpc *rpc.Client) {
	rpcClient.Lock()
	defer rpcClient.Unlock()
	rpcClient.rpc = rpc
}
func (rpcClient *RPCClient) Close() error {
	rpcClient.Lock()
	defer rpcClient.Unlock()
	return rpcClient.rpc.Close()
}

func (rpcClient *RPCClient) Call(serviceMethod string, args interface{}, reply interface{}) error {
	rpcClient.Lock()
	defer rpcClient.Unlock()
	if rpcClient.rpc == nil {
		return rpc.ErrShutdown
	}
	err := rpcClient.rpc.Call(serviceMethod, args, reply)
	return err

}

func (rpcClient *RPCClient) Reconnect() error {
	rpcClient.Lock()
	defer rpcClient.Unlock()
	clnt, err := rpc.DialHTTPPath("tcp", rpcClient.node, rpcClient.rpcPath)
	if err != nil {
		return err
	}
	rpcClient.rpc = clnt
	return nil
}

func IsRPCError(err error) bool {
	// The following are net/rpc specific errors that indicate that
	// the connection may have been reset.
	if err == rpc.ErrShutdown ||
		reflect.TypeOf(err) == reflect.TypeOf((*rpc.ServerError)(nil)).Elem() {
		return true
	}
	return false
}

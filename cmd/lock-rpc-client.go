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
	"crypto/tls"

	"github.com/minio/dsync"
	xnet "github.com/minio/minio/pkg/net"
)

// LockRPCClient is authenticable lock RPC client compatible to dsync.NetLocker
type LockRPCClient struct {
	*RPCClient
}

// ServerAddr - dsync.NetLocker interface compatible method.
func (lockRPC *LockRPCClient) ServerAddr() string {
	url := lockRPC.ServiceURL()
	return url.Host
}

// ServiceEndpoint - dsync.NetLocker interface compatible method.
func (lockRPC *LockRPCClient) ServiceEndpoint() string {
	url := lockRPC.ServiceURL()
	return url.Path
}

// RLock calls read lock RPC.
func (lockRPC *LockRPCClient) RLock(args dsync.LockArgs) (reply bool, err error) {
	err = lockRPC.Call(lockServiceName+".RLock", &LockArgs{LockArgs: args}, &reply)
	return reply, err
}

// Lock calls write lock RPC.
func (lockRPC *LockRPCClient) Lock(args dsync.LockArgs) (reply bool, err error) {
	err = lockRPC.Call(lockServiceName+".Lock", &LockArgs{LockArgs: args}, &reply)
	return reply, err
}

// RUnlock calls read unlock RPC.
func (lockRPC *LockRPCClient) RUnlock(args dsync.LockArgs) (reply bool, err error) {
	err = lockRPC.Call(lockServiceName+".RUnlock", &LockArgs{LockArgs: args}, &reply)
	return reply, err
}

// Unlock calls write unlock RPC.
func (lockRPC *LockRPCClient) Unlock(args dsync.LockArgs) (reply bool, err error) {
	err = lockRPC.Call(lockServiceName+".Unlock", &LockArgs{LockArgs: args}, &reply)
	return reply, err
}

// ForceUnlock calls force unlock RPC.
func (lockRPC *LockRPCClient) ForceUnlock(args dsync.LockArgs) (reply bool, err error) {
	err = lockRPC.Call(lockServiceName+".ForceUnlock", &LockArgs{LockArgs: args}, &reply)
	return reply, err
}

// Expired calls expired RPC.
func (lockRPC *LockRPCClient) Expired(args dsync.LockArgs) (reply bool, err error) {
	err = lockRPC.Call(lockServiceName+".Expired", &LockArgs{LockArgs: args}, &reply)
	return reply, err
}

// NewLockRPCClient - returns new lock RPC client.
func NewLockRPCClient(host *xnet.Host) (*LockRPCClient, error) {
	scheme := "http"
	if globalIsSSL {
		scheme = "https"
	}

	serviceURL := &xnet.URL{
		Scheme: scheme,
		Host:   host.String(),
		Path:   lockServicePath,
	}

	var tlsConfig *tls.Config
	if globalIsSSL {
		tlsConfig = &tls.Config{
			ServerName: host.Name,
			RootCAs:    globalRootCAs,
		}
	}

	rpcClient, err := NewRPCClient(
		RPCClientArgs{
			NewAuthTokenFunc: newAuthToken,
			RPCVersion:       globalRPCAPIVersion,
			ServiceName:      lockServiceName,
			ServiceURL:       serviceURL,
			TLSConfig:        tlsConfig,
		},
	)
	if err != nil {
		return nil, err
	}

	return &LockRPCClient{rpcClient}, nil
}

/*
 * Minio Cloud Storage, (C) 2018 Minio, Inc.
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
	"fmt"
	"net"
	"net/url"
	"sync"
	"time"

	xrpc "github.com/minio/minio/cmd/rpc"
	xnet "github.com/minio/minio/pkg/net"
)

// DefaultSkewTime - skew time is 15 minutes between minio peers.
const DefaultSkewTime = 15 * time.Minute

var errRPCRetry = fmt.Errorf("rpc: retry error")

func isNetError(err error) bool {
	if err == nil {
		return false
	}

	if uerr, isURLError := err.(*url.Error); isURLError {
		if uerr.Timeout() {
			return true
		}

		err = uerr.Err
	}

	_, isNetOpError := err.(*net.OpError)
	return isNetOpError
}

// RPCVersion - RPC semantic version based on semver 2.0.0 https://semver.org/.
type RPCVersion struct {
	Major uint64
	Minor uint64
	Patch uint64
}

// Compare - compares given version with this version.
func (v RPCVersion) Compare(o RPCVersion) int {
	compare := func(v1, v2 uint64) int {
		if v1 == v2 {
			return 0
		}

		if v1 > v2 {
			return 1
		}

		return -1
	}

	if r := compare(v.Major, o.Major); r != 0 {
		return r
	}

	if r := compare(v.Minor, o.Minor); r != 0 {
		return r
	}

	return compare(v.Patch, o.Patch)
}

func (v RPCVersion) String() string {
	return fmt.Sprintf("%v.%v.%v", v.Major, v.Minor, v.Patch)
}

// AuthArgs - base argument for any RPC call for authentication.
type AuthArgs struct {
	Token       string
	RPCVersion  RPCVersion
	RequestTime time.Time
}

// Authenticate - checks if given arguments are valid to allow RPC call.
// This is xrpc.Authenticator and is called in RPC server.
func (args AuthArgs) Authenticate() error {
	// Check whether request time is within acceptable skew time.
	utcNow := time.Now().UTC()
	if args.RequestTime.Sub(utcNow) > DefaultSkewTime || utcNow.Sub(args.RequestTime) > DefaultSkewTime {
		return fmt.Errorf("client time %v is too apart with server time %v", args.RequestTime, utcNow)
	}

	if globalRPCAPIVersion.Compare(args.RPCVersion) != 0 {
		return fmt.Errorf("version mismatch. expected: %v, received: %v", globalRPCAPIVersion, args.RPCVersion)
	}

	if !isAuthTokenValid(args.Token) {
		return errAuthentication
	}

	return nil
}

// SetAuthArgs - sets given authentication arguments to this args. This is called in RPC client.
func (args *AuthArgs) SetAuthArgs(authArgs AuthArgs) {
	*args = authArgs
}

// VoidReply - void (empty) RPC reply.
type VoidReply struct{}

// RPCClientArgs - RPC client arguments.
type RPCClientArgs struct {
	NewAuthTokenFunc func() string
	RPCVersion       RPCVersion
	ServiceName      string
	ServiceURL       *xnet.URL
	TLSConfig        *tls.Config
}

// validate - checks whether given args are valid or not.
func (args RPCClientArgs) validate() error {
	if args.NewAuthTokenFunc == nil {
		return fmt.Errorf("NewAuthTokenFunc must not be empty")
	}

	if args.ServiceName == "" {
		return fmt.Errorf("ServiceName must not be empty")
	}

	if args.ServiceURL.Scheme != "http" && args.ServiceURL.Scheme != "https" {
		return fmt.Errorf("unknown RPC URL %v", args.ServiceURL)
	}

	if args.ServiceURL.User != nil || args.ServiceURL.ForceQuery || args.ServiceURL.RawQuery != "" || args.ServiceURL.Fragment != "" {
		return fmt.Errorf("unknown RPC URL %v", args.ServiceURL)
	}

	if args.ServiceURL.Scheme == "https" && args.TLSConfig == nil {
		return fmt.Errorf("tls configuration must not be empty for https url %v", args.ServiceURL)
	}

	return nil
}

// RPCClient - base RPC client.
type RPCClient struct {
	sync.RWMutex
	args        RPCClientArgs
	authToken   string
	rpcClient   *xrpc.Client
	retryTicker *time.Ticker
}

func (client *RPCClient) setRetryTicker(ticker *time.Ticker) {
	if ticker == nil {
		client.RLock()
		isNil := client.retryTicker == nil
		client.RUnlock()
		if isNil {
			return
		}
	}

	client.Lock()
	defer client.Unlock()

	if client.retryTicker != nil {
		client.retryTicker.Stop()
	}

	client.retryTicker = ticker
}

// Call - calls servicemethod on remote server.
func (client *RPCClient) Call(serviceMethod string, args interface {
	SetAuthArgs(args AuthArgs)
}, reply interface{}) (err error) {
	lockedCall := func() error {
		client.RLock()
		retryTicker := client.retryTicker
		client.RUnlock()
		if retryTicker != nil {
			select {
			case <-retryTicker.C:
			default:
				return errRPCRetry
			}
		}

		client.RLock()
		authToken := client.authToken
		client.RUnlock()

		// Make RPC call.
		args.SetAuthArgs(AuthArgs{authToken, client.args.RPCVersion, time.Now().UTC()})
		return client.rpcClient.Call(serviceMethod, args, reply)
	}

	call := func() error {
		err = lockedCall()

		if err == errRPCRetry {
			return err
		}

		if isNetError(err) {
			client.setRetryTicker(time.NewTicker(xrpc.DefaultRPCTimeout))
		} else {
			client.setRetryTicker(nil)
		}

		return err
	}

	// If authentication error is received, retry the same call only once
	// with new authentication token.
	if err = call(); err == nil {
		return nil
	}
	if err.Error() != errAuthentication.Error() {
		return err
	}

	client.Lock()
	client.authToken = client.args.NewAuthTokenFunc()
	client.Unlock()
	return call()
}

// Close - closes underneath RPC client.
func (client *RPCClient) Close() error {
	client.Lock()
	defer client.Unlock()

	client.authToken = ""
	return client.rpcClient.Close()
}

// ServiceURL - returns service URL used for RPC call.
func (client *RPCClient) ServiceURL() *xnet.URL {
	// Take copy of ServiceURL
	u := *(client.args.ServiceURL)
	return &u
}

// NewRPCClient - returns new RPC client.
func NewRPCClient(args RPCClientArgs) (*RPCClient, error) {
	if err := args.validate(); err != nil {
		return nil, err
	}

	return &RPCClient{
		args:      args,
		authToken: args.NewAuthTokenFunc(),
		rpcClient: xrpc.NewClient(args.ServiceURL, args.TLSConfig, xrpc.DefaultRPCTimeout),
	}, nil
}

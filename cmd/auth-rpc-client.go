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
	"net/rpc"
	"sync"
	"time"
)

// GenericReply represents any generic RPC reply.
type GenericReply struct{}

// GenericArgs represents any generic RPC arguments.
type GenericArgs struct {
	Token string // Used to authenticate every RPC call.
	// Used to verify if the RPC call was issued between
	// the same Login() and disconnect event pair.
	Timestamp time.Time

	// Indicates if args should be sent to remote peers as well.
	Remote bool
}

// SetToken - sets the token to the supplied value.
func (ga *GenericArgs) SetToken(token string) {
	ga.Token = token
}

// SetTimestamp - sets the timestamp to the supplied value.
func (ga *GenericArgs) SetTimestamp(tstamp time.Time) {
	ga.Timestamp = tstamp
}

// RPCLoginArgs - login username and password for RPC.
type RPCLoginArgs struct {
	Username string
	Password string
}

// RPCLoginReply - login reply provides generated token to be used
// with subsequent requests.
type RPCLoginReply struct {
	Token         string
	Timestamp     time.Time
	ServerVersion string
}

// Auth config represents authentication credentials and Login method name to be used
// for fetching JWT tokens from the RPC server.
type authConfig struct {
	accessKey   string // Username for the server.
	secretKey   string // Password for the server.
	secureConn  bool   // Ask for a secured connection
	address     string // Network address path of RPC server.
	path        string // Network path for HTTP dial.
	loginMethod string // RPC service name for authenticating using JWT
}

// AuthRPCClient is a wrapper type for RPCClient which provides JWT based authentication across reconnects.
type AuthRPCClient struct {
	mu            sync.Mutex
	config        *authConfig
	rpc           *RPCClient // reconnect'able rpc client built on top of net/rpc Client
	serverToken   string     // Disk rpc JWT based token.
	serverVersion string     // Server version exchanged by the RPC.
}

// newAuthClient - returns a jwt based authenticated (go) rpc client, which does automatic reconnect.
func newAuthClient(cfg *authConfig) *AuthRPCClient {
	return &AuthRPCClient{
		// Save the config.
		config: cfg,
		// Initialize a new reconnectable rpc client.
		rpc: newRPCClient(cfg.address, cfg.path, cfg.secureConn),
	}
}

// Close - closes underlying rpc connection.
func (authClient *AuthRPCClient) Close() error {
	authClient.mu.Lock()
	// reset token on closing a connection
	authClient.serverToken = ""
	authClient.mu.Unlock()
	return authClient.rpc.Close()
}

// Login - a jwt based authentication is performed with rpc server.
func (authClient *AuthRPCClient) Login() (err error) {
	authClient.mu.Lock()
	// As soon as the function returns unlock,
	defer authClient.mu.Unlock()

	// Return if already logged in.
	if authClient.serverToken != "" {
		return nil
	}

	reply := RPCLoginReply{}
	if err = authClient.rpc.Call(authClient.config.loginMethod, RPCLoginArgs{
		Username: authClient.config.accessKey,
		Password: authClient.config.secretKey,
	}, &reply); err != nil {
		return err
	}

	// Validate if version do indeed match.
	if reply.ServerVersion != Version {
		return errServerVersionMismatch
	}

	// Validate if server timestamp is skewed.
	curTime := time.Now().UTC()
	if curTime.Sub(reply.Timestamp) > globalMaxSkewTime {
		return errServerTimeMismatch
	}

	// Set token, time stamp as received from a successful login call.
	authClient.serverToken = reply.Token
	authClient.serverVersion = reply.ServerVersion
	return nil
}

// Call - If rpc connection isn't established yet since previous disconnect,
// connection is established, a jwt authenticated login is performed and then
// the call is performed.
func (authClient *AuthRPCClient) Call(serviceMethod string, args interface {
	SetToken(token string)
	SetTimestamp(tstamp time.Time)
}, reply interface{}) (err error) {
	loginAndCallFn := func() error {
		// On successful login, proceed to attempt the requested service method.
		if err = authClient.Login(); err == nil {
			// Set token and timestamp before the rpc call.
			args.SetToken(authClient.serverToken)
			args.SetTimestamp(time.Now().UTC())

			// Finally make the network call using net/rpc client.
			err = authClient.rpc.Call(serviceMethod, args, reply)
		}
		return err
	}
	doneCh := make(chan struct{})
	defer close(doneCh)
	for i := range newRetryTimer(time.Second, time.Second*30, MaxJitter, doneCh) {
		// Invalidate token, and mark it for re-login and
		// reconnect upon rpc shutdown.
		if err = loginAndCallFn(); err == rpc.ErrShutdown {
			// Close the underlying connection, and proceed to reconnect
			// if we haven't reached the retry threshold.
			authClient.Close()

			// No need to return error until the retry count threshold has reached.
			if i < globalMaxAuthRPCRetryThreshold {
				continue
			}
		}
		break
	}
	return err
}

// Node returns the node (network address) of the connection
func (authClient *AuthRPCClient) Node() (node string) {
	if authClient.rpc != nil {
		node = authClient.rpc.node
	}
	return node
}

// RPCPath returns the RPC path of the connection
func (authClient *AuthRPCClient) RPCPath() (rpcPath string) {
	if authClient.rpc != nil {
		rpcPath = authClient.rpc.rpcPath
	}
	return rpcPath
}

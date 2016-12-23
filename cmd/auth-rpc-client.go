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

// Attempt to retry only this many number of times before
// giving up on the remote RPC entirely.
const globalAuthRPCRetryThreshold = 1

// authConfig requires to make new AuthRPCClient.
type authConfig struct {
	accessKey        string // Access key (like username) for authentication.
	secretKey        string // Secret key (like Password) for authentication.
	serverAddr       string // RPC server address.
	serviceEndpoint  string // Endpoint on the server to make any RPC call.
	secureConn       bool   // Make TLS connection to RPC server or not.
	serviceName      string // Service name of auth server.
	disableReconnect bool   // Disable reconnect on failure or not.
}

// AuthRPCClient is a authenticated RPC client which does authentication before doing Call().
type AuthRPCClient struct {
	sync.Mutex            // Mutex to lock this object.
	rpcClient  *RPCClient // Reconnectable RPC client to make any RPC call.
	config     authConfig // Authentication configuration information.
	authToken  string     // Authentication token.
}

// newAuthRPCClient - returns a JWT based authenticated (go) rpc client, which does automatic reconnect.
func newAuthRPCClient(config authConfig) *AuthRPCClient {
	return &AuthRPCClient{
		rpcClient: newRPCClient(config.serverAddr, config.serviceEndpoint, config.secureConn),
		config:    config,
	}
}

// Login - a jwt based authentication is performed with rpc server.
func (authClient *AuthRPCClient) Login() (err error) {
	authClient.Lock()
	defer authClient.Unlock()

	// Return if already logged in.
	if authClient.authToken != "" {
		return nil
	}

	// Call login.
	args := LoginRPCArgs{
		Username:    authClient.config.accessKey,
		Password:    authClient.config.secretKey,
		Version:     Version,
		RequestTime: time.Now().UTC(),
	}

	reply := LoginRPCReply{}
	serviceMethod := authClient.config.serviceName + loginMethodName
	if err = authClient.rpcClient.Call(serviceMethod, &args, &reply); err != nil {
		return err
	}

	// Logged in successfully.
	authClient.authToken = reply.AuthToken

	return nil
}

// call makes a RPC call after logs into the server.
func (authClient *AuthRPCClient) call(serviceMethod string, args interface {
	SetAuthToken(authToken string)
	SetRequestTime(requestTime time.Time)
}, reply interface{}) (err error) {
	// On successful login, execute RPC call.
	if err = authClient.Login(); err == nil {
		// Set token and timestamp before the rpc call.
		args.SetAuthToken(authClient.authToken)
		args.SetRequestTime(time.Now().UTC())

		// Do RPC call.
		err = authClient.rpcClient.Call(serviceMethod, args, reply)
	}
	return err
}

// Call executes RPC call till success or globalAuthRPCRetryThreshold on ErrShutdown.
func (authClient *AuthRPCClient) Call(serviceMethod string, args interface {
	SetAuthToken(authToken string)
	SetRequestTime(requestTime time.Time)
}, reply interface{}) (err error) {
	doneCh := make(chan struct{})
	defer close(doneCh)
	for i := range newRetryTimer(time.Second, 30*time.Second, MaxJitter, doneCh) {
		if err = authClient.call(serviceMethod, args, reply); err == rpc.ErrShutdown {
			// As connection at server side is closed, close the rpc client.
			authClient.Close()

			// Retry if reconnect is not disabled.
			if !authClient.config.disableReconnect {
				// Retry until threshold reaches.
				if i < globalAuthRPCRetryThreshold {
					continue
				}
			}
		}
		break
	}
	return err
}

// Close closes underlying RPC Client.
func (authClient *AuthRPCClient) Close() error {
	authClient.Lock()
	defer authClient.Unlock()

	authClient.authToken = ""
	return authClient.rpcClient.Close()
}

// ServerAddr returns the serverAddr (network address) of the connection.
func (authClient *AuthRPCClient) ServerAddr() string {
	return authClient.config.serverAddr
}

// ServiceEndpoint returns the RPC service endpoint of the connection.
func (authClient *AuthRPCClient) ServiceEndpoint() string {
	return authClient.config.serviceEndpoint
}

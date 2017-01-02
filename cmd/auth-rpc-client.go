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

// authConfig requires to make new AuthRPCClient.
type authConfig struct {
	accessKey       string // Access key (like username) for authentication.
	secretKey       string // Secret key (like Password) for authentication.
	serverAddr      string // RPC server address.
	serviceEndpoint string // Endpoint on the server to make any RPC call.
	secureConn      bool   // Make TLS connection to RPC server or not.
	serviceName     string // Service name of auth server.
}

// retryConfig sets configuration parameters to control retry on RPC call failure.
type retryConfig struct {
	// isRetryableErr is called when RPC call is not successful.
	// If the function returns true, the RPC call is tried again.
	isRetryableErr func(error) bool

	// Maximum retry attempts to be made.
	maxAttempts int

	// RetryTimer parameters.
	timerUnit    time.Duration
	timerMaxTime time.Duration
}

// Default retry configuration.
var defaultRetryConfig = retryConfig{
	isRetryableErr: func(err error) bool { return err == rpc.ErrShutdown },
	maxAttempts:    1,
	timerUnit:      time.Second,
	timerMaxTime:   30 * time.Second,
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
	rpcClient := newRPCClient(config.serverAddr, config.serviceEndpoint, config.secureConn)
	return &AuthRPCClient{
		rpcClient: rpcClient,
		config:    config,
	}
}

// login - a jwt based authentication is performed with rpc server.
func (authClient *AuthRPCClient) login() (err error) {
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
}, reply interface{}, postLoginFunc func() error) (err error) {
	// On successful login, execute RPC call.
	if err = authClient.login(); err != nil {
		return err
	}

	// Call postLoginFunc() if available.
	if postLoginFunc != nil {
		if err = postLoginFunc(); err != nil {
			return err
		}
	}

	// Make RPC call on successful login.

	// Set token and timestamp before the rpc call.
	args.SetAuthToken(authClient.authToken)
	args.SetRequestTime(time.Now().UTC())

	// Do RPC call.
	return authClient.rpcClient.Call(serviceMethod, args, reply)
}

// Call executes RPC call till success or globalAuthRPCRetryThreshold on ErrShutdown.
func (authClient *AuthRPCClient) Call(serviceMethod string,
	args interface {
		SetAuthToken(authToken string)
		SetRequestTime(requestTime time.Time)
	},
	reply interface{},
	postLoginFunc func() error,
	retryCfg retryConfig,
) (err error) {

	// Allocate a done channel for managing timer routine.
	doneCh := make(chan struct{})
	defer close(doneCh)

	for i := range newRetryTimer(retryCfg.timerUnit, retryCfg.timerMaxTime, MaxJitter, doneCh) {
		if err = authClient.call(serviceMethod, args, reply, postLoginFunc); err == nil {
			// Upon success break out and return success.
			break
		}

		var retry bool
		// Call isRetryableErr if available.
		if retryCfg.isRetryableErr != nil {
			retry = retryCfg.isRetryableErr(err)
		}

		// Do not continue if not a retryable error.
		if !retry {
			break
		}

		// Close the rpc client so that we will
		// reconnect with fresh login with our next attempt.
		authClient.Close()

		// Do not continue if maximum retry attempts has reached.
		if i >= retryCfg.maxAttempts {
			break
		}
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

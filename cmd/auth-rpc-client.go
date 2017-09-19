/*
 * Minio Cloud Storage, (C) 2016, 2017 Minio, Inc.
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

	/// Retry configurable values.

	// Each retry unit multiplicative, measured in time.Duration.
	// This is the basic unit used for calculating backoffs.
	retryUnit time.Duration
	// Maximum retry duration i.e A caller would wait no more than this
	// duration to continue their loop.
	retryCap time.Duration

	// Maximum retries an call authRPC client would do for a failed
	// RPC call.
	retryAttemptThreshold int
}

// AuthRPCClient is a authenticated RPC client which does authentication before doing Call().
type AuthRPCClient struct {
	sync.RWMutex            // Mutex to lock this object.
	rpcClient    *RPCClient // Reconnectable RPC client to make any RPC call.
	config       authConfig // Authentication configuration information.
	authToken    string     // Authentication token.
}

// newAuthRPCClient - returns a JWT based authenticated (go) rpc client, which does automatic reconnect.
func newAuthRPCClient(config authConfig) *AuthRPCClient {
	// Check if retry params are set properly if not default them.
	emptyDuration := time.Duration(int64(0))
	if config.retryUnit == emptyDuration {
		config.retryUnit = defaultRetryUnit
	}
	if config.retryCap == emptyDuration {
		config.retryCap = defaultRetryCap
	}
	if config.retryAttemptThreshold == 0 {
		config.retryAttemptThreshold = globalAuthRPCRetryThreshold
	}

	return &AuthRPCClient{
		rpcClient: newRPCClient(config.serverAddr, config.serviceEndpoint, config.secureConn),
		config:    config,
	}
}

// Login a JWT based authentication is performed with rpc server.
func (authClient *AuthRPCClient) Login() (err error) {
	// Login should be attempted one at a time.
	//
	// The reason for large region lock here is
	// to avoid two simultaneous login attempts
	// racing over each other.
	//
	// #1 Login() gets the lock proceeds to login.
	// #2 Login() waits for the unlock to happen
	//    after login in #1.
	// #1 Successfully completes login saves the
	//    newly acquired token.
	// #2 Successfully gets the lock and proceeds,
	//    but since we have acquired the token
	//    already the call quickly returns.
	authClient.Lock()
	defer authClient.Unlock()

	// Attempt to login if not logged in already.
	if authClient.authToken == "" {
		authClient.authToken, err = authenticateNode(authClient.config.accessKey, authClient.config.secretKey)
		if err != nil {
			return err
		}
		// Login to authenticate your token.
		var (
			loginMethod = authClient.config.serviceName + loginMethodName
			loginArgs   = LoginRPCArgs{
				AuthToken:   authClient.authToken,
				Version:     Version,
				RequestTime: UTCNow(),
			}
		)
		if err = authClient.rpcClient.Call(loginMethod, &loginArgs, &LoginRPCReply{}); err != nil {
			return err
		}
	}
	return nil
}

// call makes a RPC call after logs into the server.
func (authClient *AuthRPCClient) call(serviceMethod string, args interface {
	SetAuthToken(authToken string)
}, reply interface{}) (err error) {
	if err = authClient.Login(); err != nil {
		return err
	} // On successful login, execute RPC call.

	authClient.RLock()
	// Set token before the rpc call.
	args.SetAuthToken(authClient.authToken)
	authClient.RUnlock()

	// Do an RPC call.
	return authClient.rpcClient.Call(serviceMethod, args, reply)
}

// Call executes RPC call till success or globalAuthRPCRetryThreshold on ErrShutdown.
func (authClient *AuthRPCClient) Call(serviceMethod string, args interface {
	SetAuthToken(authToken string)
}, reply interface{}) (err error) {

	// Done channel is used to close any lingering retry routine, as soon
	// as this function returns.
	doneCh := make(chan struct{})
	defer close(doneCh)

	for i := range newRetryTimer(authClient.config.retryUnit, authClient.config.retryCap, doneCh) {
		if err = authClient.call(serviceMethod, args, reply); err == rpc.ErrShutdown {
			// As connection at server side is closed, close the rpc client.
			authClient.Close()

			// Retry if reconnect is not disabled.
			if !authClient.config.disableReconnect {
				// Retry until threshold reaches.
				if i < authClient.config.retryAttemptThreshold {
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

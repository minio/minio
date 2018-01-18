/*
 * Minio Cloud Storage, (C) 2016, 2017, 2018 Minio, Inc.
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
	"bufio"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/rpc"
	"strings"
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
	sync.RWMutex             // Mutex to lock this object.
	rpcClient    *rpc.Client // RPC client to make any RPC call.
	config       authConfig  // Authentication configuration information.
	authToken    string      // Authentication token.
	version      semVersion  // RPC version.
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
		config:  config,
		version: globalRPCAPIVersion,
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
		var authToken string
		authToken, err = authenticateNode(authClient.config.accessKey, authClient.config.secretKey)
		if err != nil {
			return err
		}

		// Login to authenticate your token.
		var (
			loginMethod = authClient.config.serviceName + loginMethodName
			loginArgs   = LoginRPCArgs{
				AuthToken:   authToken,
				Version:     globalRPCAPIVersion,
				RequestTime: UTCNow(),
			}
		)

		// Re-dial after we have disconnected or if its a fresh run.
		var rpcClient *rpc.Client
		rpcClient, err = rpcDial(authClient.config.serverAddr, authClient.config.serviceEndpoint, authClient.config.secureConn)
		if err != nil {
			return err
		}

		if err = rpcClient.Call(loginMethod, &loginArgs, &LoginRPCReply{}); err != nil {
			// gob doesn't provide any typed errors for us to reflect
			// upon, this is the only way to return proper error.
			if strings.Contains(err.Error(), "gob: wrong type") {
				return errRPCAPIVersionUnsupported
			}
			return err
		}

		// Initialize rpc client and auth token after a successful login.
		authClient.authToken = authToken
		authClient.rpcClient = rpcClient
	}

	return nil
}

// call makes a RPC call after logs into the server.
func (authClient *AuthRPCClient) call(serviceMethod string, args interface {
	SetAuthToken(authToken string)
	SetRPCAPIVersion(version semVersion)
}, reply interface{}) (err error) {
	if err = authClient.Login(); err != nil {
		return err
	} // On successful login, execute RPC call.

	// Set token before the rpc call.
	authClient.RLock()
	defer authClient.RUnlock()
	args.SetAuthToken(authClient.authToken)
	args.SetRPCAPIVersion(authClient.version)

	// Do an RPC call.
	return authClient.rpcClient.Call(serviceMethod, args, reply)
}

// Call executes RPC call till success or globalAuthRPCRetryThreshold on ErrShutdown.
func (authClient *AuthRPCClient) Call(serviceMethod string, args interface {
	SetAuthToken(authToken string)
	SetRPCAPIVersion(version semVersion)
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
		// gob doesn't provide any typed errors for us to reflect
		// upon, this is the only way to return proper error.
		if err != nil && strings.Contains(err.Error(), "gob: wrong type") {
			err = errRPCAPIVersionUnsupported
		}
		break
	}
	return err
}

// Close closes underlying RPC Client.
func (authClient *AuthRPCClient) Close() error {
	authClient.Lock()
	defer authClient.Unlock()

	if authClient.rpcClient == nil {
		return nil
	}

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

// default Dial timeout for RPC connections.
const defaultDialTimeout = 3 * time.Second

// Connect success message required from rpc server.
const connectSuccessMessage = "200 Connected to Go RPC"

// dial tries to establish a connection to serverAddr in a safe manner.
// If there is a valid rpc.Cliemt, it returns that else creates a new one.
func rpcDial(serverAddr, serviceEndpoint string, secureConn bool) (netRPCClient *rpc.Client, err error) {
	if serverAddr == "" || serviceEndpoint == "" {
		return nil, errInvalidArgument
	}
	d := &net.Dialer{
		Timeout: defaultDialTimeout,
	}
	var conn net.Conn
	if secureConn {
		var hostname string
		if hostname, _, err = net.SplitHostPort(serverAddr); err != nil {
			return nil, &net.OpError{
				Op:   "dial-http",
				Net:  serverAddr + serviceEndpoint,
				Addr: nil,
				Err:  fmt.Errorf("Unable to parse server address <%s>: %s", serverAddr, err),
			}
		}
		// ServerName in tls.Config needs to be specified to support SNI certificates.
		conn, err = tls.DialWithDialer(d, "tcp", serverAddr, &tls.Config{
			ServerName: hostname,
			RootCAs:    globalRootCAs,
		})
	} else {
		conn, err = d.Dial("tcp", serverAddr)
	}

	if err != nil {
		// Print RPC connection errors that are worthy to display in log.
		switch err.(type) {
		case x509.HostnameError:
			errorIf(err, "Unable to establish secure connection to %s", serverAddr)
		}

		return nil, &net.OpError{
			Op:   "dial-http",
			Net:  serverAddr + serviceEndpoint,
			Addr: nil,
			Err:  err,
		}
	}

	// Check for network errors writing over the dialed conn.
	if _, err = io.WriteString(conn, "CONNECT "+serviceEndpoint+" HTTP/1.0\n\n"); err != nil {
		conn.Close()
		return nil, &net.OpError{
			Op:   "dial-http",
			Net:  serverAddr + serviceEndpoint,
			Addr: nil,
			Err:  err,
		}
	}

	// Attempt to read the HTTP response for the HTTP method CONNECT, upon
	// success return the RPC connection instance.
	resp, err := http.ReadResponse(bufio.NewReader(conn), &http.Request{
		Method: http.MethodConnect,
	})
	if err != nil {
		conn.Close()
		return nil, &net.OpError{
			Op:   "dial-http",
			Net:  serverAddr + serviceEndpoint,
			Addr: nil,
			Err:  err,
		}
	}
	if resp.Status != connectSuccessMessage {
		conn.Close()
		return nil, errors.New("unexpected HTTP response: " + resp.Status)
	}

	// Initialize rpc client.
	return rpc.NewClient(conn), nil
}

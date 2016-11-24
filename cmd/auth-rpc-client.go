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
	"fmt"
	"net/rpc"
	"sync"
	"time"

	jwtgo "github.com/dgrijalva/jwt-go"
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

// Validates if incoming token is valid.
func isRPCTokenValid(tokenStr string) bool {
	jwt, err := newJWT(defaultInterNodeJWTExpiry, serverConfig.GetCredential())
	if err != nil {
		errorIf(err, "Unable to initialize JWT")
		return false
	}
	token, err := jwtgo.Parse(tokenStr, func(token *jwtgo.Token) (interface{}, error) {
		if _, ok := token.Method.(*jwtgo.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("Unexpected signing method: %v", token.Header["alg"])
		}
		return []byte(jwt.SecretAccessKey), nil
	})
	if err != nil {
		errorIf(err, "Unable to parse JWT token string")
		return false
	}
	// Return if token is valid.
	return token.Valid
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
	isLoggedIn    bool       // Indicates if the auth client has been logged in and token is valid.
	serverToken   string     // Disk rpc JWT based token.
	serverVersion string     // Server version exchanged by the RPC.
}

// newAuthClient - returns a jwt based authenticated (go) rpc client, which does automatic reconnect.
func newAuthClient(cfg *authConfig) *AuthRPCClient {
	return &AuthRPCClient{
		// Save the config.
		config: cfg,
		// Initialize a new reconnectable rpc client.
		rpc: newClient(cfg.address, cfg.path, cfg.secureConn),
		// Allocated auth client not logged in yet.
		isLoggedIn: false,
	}
}

// Close - closes underlying rpc connection.
func (authClient *AuthRPCClient) Close() error {
	authClient.mu.Lock()
	// reset token on closing a connection
	authClient.isLoggedIn = false
	authClient.mu.Unlock()
	return authClient.rpc.Close()
}

// Login - a jwt based authentication is performed with rpc server.
func (authClient *AuthRPCClient) Login() (err error) {
	authClient.mu.Lock()
	// As soon as the function returns unlock,
	defer authClient.mu.Unlock()

	// Return if already logged in.
	if authClient.isLoggedIn {
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
	authClient.isLoggedIn = true
	return nil
}

// Call - If rpc connection isn't established yet since previous disconnect,
// connection is established, a jwt authenticated login is performed and then
// the call is performed.
func (authClient *AuthRPCClient) Call(serviceMethod string, args interface {
	SetToken(token string)
	SetTimestamp(tstamp time.Time)
}, reply interface{}) (err error) {
	// On successful login, attempt the call.
	if err = authClient.Login(); err == nil {
		// Set token and timestamp before the rpc call.
		args.SetToken(authClient.serverToken)
		args.SetTimestamp(time.Now().UTC())

		// Call the underlying rpc.
		err = authClient.rpc.Call(serviceMethod, args, reply)

		// Invalidate token, and mark it for re-login on subsequent reconnect.
		if err == rpc.ErrShutdown {
			authClient.mu.Lock()
			authClient.isLoggedIn = false
			authClient.mu.Unlock()
		}
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

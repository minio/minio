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
	"bufio"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/rpc"
	"sync"
	"time"
)

// RPCClient is a wrapper type for rpc.Client which provides reconnect on first failure.
type RPCClient struct {
	mu         sync.Mutex
	rpcPrivate *rpc.Client
	node       string
	rpcPath    string
	secureConn bool
}

// newClient constructs a RPCClient object with node and rpcPath initialized.
// It _doesn't_ connect to the remote endpoint. See Call method to see when the
// connect happens.
func newClient(node, rpcPath string, secureConn bool) *RPCClient {
	return &RPCClient{
		node:       node,
		rpcPath:    rpcPath,
		secureConn: secureConn,
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
	// After acquiring lock, check whether another thread may not have already dialed and established connection
	if rpcClient.rpcPrivate != nil {
		rpcClient.mu.Unlock()
		return rpcClient.rpcPrivate, nil
	}
	rpcClient.mu.Unlock()

	var err error
	var conn net.Conn

	if rpcClient.secureConn {
		hostname, _, splitErr := net.SplitHostPort(rpcClient.node)
		if splitErr != nil {
			err = errors.New("Unable to parse RPC address <" + rpcClient.node + "> : " + splitErr.Error())
			return nil, &net.OpError{
				Op:   "dial-http",
				Net:  rpcClient.node + " " + rpcClient.rpcPath,
				Addr: nil,
				Err:  err,
			}
		}
		// ServerName in tls.Config needs to be specified to support SNI certificates
		conn, err = tls.Dial("tcp", rpcClient.node, &tls.Config{ServerName: hostname, RootCAs: globalRootCAs})
	} else {
		// Have a dial timeout with 3 secs.
		conn, err = net.DialTimeout("tcp", rpcClient.node, 3*time.Second)
	}
	if err != nil {
		// Print RPC connection errors that are worthy to display in log
		switch err.(type) {
		case x509.HostnameError:
			errorIf(err, "Unable to establish RPC to %s", rpcClient.node)
		}
		return nil, &net.OpError{
			Op:   "dial-http",
			Net:  rpcClient.node + " " + rpcClient.rpcPath,
			Addr: nil,
			Err:  err,
		}
	}
	io.WriteString(conn, "CONNECT "+rpcClient.rpcPath+" HTTP/1.0\n\n")

	// Require successful HTTP response before switching to RPC protocol.
	resp, err := http.ReadResponse(bufio.NewReader(conn), &http.Request{Method: "CONNECT"})
	if err == nil && resp.Status == "200 Connected to Go RPC" {
		rpc := rpc.NewClient(conn)
		if rpc == nil {
			return nil, &net.OpError{
				Op:   "dial-http",
				Net:  rpcClient.node + " " + rpcClient.rpcPath,
				Addr: nil,
				Err:  fmt.Errorf("Unable to initialize new rpcClient, %s", errUnexpected),
			}
		}
		rpcClient.mu.Lock()
		rpcClient.rpcPrivate = rpc
		rpcClient.mu.Unlock()
		return rpc, nil
	}
	if err == nil {
		err = errors.New("unexpected HTTP response: " + resp.Status)
	}
	conn.Close()
	return nil, &net.OpError{
		Op:   "dial-http",
		Net:  rpcClient.node + " " + rpcClient.rpcPath,
		Addr: nil,
		Err:  err,
	}
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

	// If the RPC fails due to a network-related error
	return rpcLocalStack.Call(serviceMethod, args, reply)
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

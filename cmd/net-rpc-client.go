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

// defaultDialTimeout is used for non-secure connection.
const defaultDialTimeout = 3 * time.Second

// RPCClient is a wrapper type for rpc.Client which provides reconnect on first failure.
type RPCClient struct {
	mu           sync.Mutex
	netRPCClient *rpc.Client
	node         string
	rpcPath      string
	secureConn   bool
}

// newClient constructs a RPCClient object with node and rpcPath initialized.
// It does lazy connect to the remote endpoint on Call().
func newRPCClient(node, rpcPath string, secureConn bool) *RPCClient {
	return &RPCClient{
		node:       node,
		rpcPath:    rpcPath,
		secureConn: secureConn,
	}
}

// dial tries to establish a connection to the server in a safe manner.
// If there is a valid rpc.Cliemt, it returns that else creates a new one.
func (rpcClient *RPCClient) dial() (*rpc.Client, error) {
	rpcClient.mu.Lock()
	defer rpcClient.mu.Unlock()

	// Nothing to do as we already have valid connection.
	if rpcClient.netRPCClient != nil {
		return rpcClient.netRPCClient, nil
	}

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
		// Dial with 3 seconds timeout.
		conn, err = net.DialTimeout("tcp", rpcClient.node, defaultDialTimeout)
	}
	if err != nil {
		// Print RPC connection errors that are worthy to display in log
		switch err.(type) {
		case x509.HostnameError:
			errorIf(err, "Unable to establish secure connection to %s", rpcClient.node)
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
		netRPCClient := rpc.NewClient(conn)
		if netRPCClient == nil {
			return nil, &net.OpError{
				Op:   "dial-http",
				Net:  rpcClient.node + " " + rpcClient.rpcPath,
				Addr: nil,
				Err:  fmt.Errorf("Unable to initialize new rpc.Client, %s", errUnexpected),
			}
		}

		rpcClient.netRPCClient = netRPCClient

		return netRPCClient, nil
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
	// Get a new or existing rpc.Client.
	netRPCClient, err := rpcClient.dial()
	if err != nil {
		return err
	}

	return netRPCClient.Call(serviceMethod, args, reply)
}

// Close closes underlying rpc.Client.
func (rpcClient *RPCClient) Close() error {
	rpcClient.mu.Lock()

	if rpcClient.netRPCClient != nil {
		// We make a copy of rpc.Client and unlock it immediately so that another
		// goroutine could try to dial or close in parallel.
		netRPCClient := rpcClient.netRPCClient
		rpcClient.netRPCClient = nil
		rpcClient.mu.Unlock()

		return netRPCClient.Close()
	}

	rpcClient.mu.Unlock()
	return nil
}

// Node returns the node (network address) of the connection
func (rpcClient *RPCClient) Node() string {
	return rpcClient.node
}

// RPCPath returns the RPC path of the connection
func (rpcClient *RPCClient) RPCPath() string {
	return rpcClient.rpcPath
}

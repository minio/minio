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

// RPCClient is a reconnectable RPC client on Call().
type RPCClient struct {
	sync.Mutex                  // Mutex to lock net rpc client.
	netRPCClient    *rpc.Client // Base RPC client to make any RPC call.
	serverAddr      string      // RPC server address.
	serviceEndpoint string      // Endpoint on the server to make any RPC call.
	secureConn      bool        // Make TLS connection to RPC server or not.
}

// newRPCClient returns new RPCClient object with given serverAddr and serviceEndpoint.
// It does lazy connect to the remote endpoint on Call().
func newRPCClient(serverAddr, serviceEndpoint string, secureConn bool) *RPCClient {
	return &RPCClient{
		serverAddr:      serverAddr,
		serviceEndpoint: serviceEndpoint,
		secureConn:      secureConn,
	}
}

// dial tries to establish a connection to serverAddr in a safe manner.
// If there is a valid rpc.Cliemt, it returns that else creates a new one.
func (rpcClient *RPCClient) dial() (netRPCClient *rpc.Client, err error) {
	rpcClient.Lock()
	defer rpcClient.Unlock()

	// Nothing to do as we already have valid connection.
	if rpcClient.netRPCClient != nil {
		return rpcClient.netRPCClient, nil
	}

	var conn net.Conn
	if rpcClient.secureConn {
		var hostname string
		if hostname, _, err = net.SplitHostPort(rpcClient.serverAddr); err != nil {
			err = &net.OpError{
				Op:   "dial-http",
				Net:  rpcClient.serverAddr + rpcClient.serviceEndpoint,
				Addr: nil,
				Err:  fmt.Errorf("Unable to parse server address <%s>: %s", rpcClient.serverAddr, err.Error()),
			}

			return nil, err
		}

		// ServerName in tls.Config needs to be specified to support SNI certificates.
		conn, err = tls.Dial("tcp", rpcClient.serverAddr, &tls.Config{ServerName: hostname, RootCAs: globalRootCAs})
	} else {
		// Dial with a timeout.
		conn, err = net.DialTimeout("tcp", rpcClient.serverAddr, defaultDialTimeout)
	}

	if err != nil {
		// Print RPC connection errors that are worthy to display in log.
		switch err.(type) {
		case x509.HostnameError:
			errorIf(err, "Unable to establish secure connection to %s", rpcClient.serverAddr)
		}

		return nil, &net.OpError{
			Op:   "dial-http",
			Net:  rpcClient.serverAddr + rpcClient.serviceEndpoint,
			Addr: nil,
			Err:  err,
		}
	}

	io.WriteString(conn, "CONNECT "+rpcClient.serviceEndpoint+" HTTP/1.0\n\n")

	// Require successful HTTP response before switching to RPC protocol.
	resp, err := http.ReadResponse(bufio.NewReader(conn), &http.Request{Method: "CONNECT"})
	if err == nil && resp.Status == "200 Connected to Go RPC" {
		netRPCClient := rpc.NewClient(conn)

		if netRPCClient == nil {
			return nil, &net.OpError{
				Op:   "dial-http",
				Net:  rpcClient.serverAddr + rpcClient.serviceEndpoint,
				Addr: nil,
				Err:  fmt.Errorf("Unable to initialize new rpc.Client, %s", errUnexpected),
			}
		}

		rpcClient.netRPCClient = netRPCClient

		return netRPCClient, nil
	}

	conn.Close()

	if err == nil {
		err = errors.New("unexpected HTTP response: " + resp.Status)
	}

	return nil, &net.OpError{
		Op:   "dial-http",
		Net:  rpcClient.serverAddr + rpcClient.serviceEndpoint,
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
	rpcClient.Lock()

	if rpcClient.netRPCClient != nil {
		// We make a copy of rpc.Client and unlock it immediately so that another
		// goroutine could try to dial or close in parallel.
		netRPCClient := rpcClient.netRPCClient
		rpcClient.netRPCClient = nil
		rpcClient.Unlock()

		return netRPCClient.Close()
	}

	rpcClient.Unlock()
	return nil
}

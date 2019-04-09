/*
 * MinIO Cloud Storage, (C) 2018 MinIO, Inc.
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

package rpc

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/gob"
	"errors"
	"fmt"
	"net"
	"net/http"
	"reflect"
	"time"

	xhttp "github.com/minio/minio/cmd/http"
	xnet "github.com/minio/minio/pkg/net"
	"golang.org/x/net/http2"
)

// DefaultRPCTimeout - default RPC timeout is one minute.
const DefaultRPCTimeout = 1 * time.Minute

// Client - http based RPC client.
type Client struct {
	httpClient          *http.Client
	httpIdleConnsCloser func()
	serviceURL          *xnet.URL
}

// Call - calls service method on RPC server.
func (client *Client) Call(serviceMethod string, args, reply interface{}) error {
	replyKind := reflect.TypeOf(reply).Kind()
	if replyKind != reflect.Ptr {
		return fmt.Errorf("rpc reply must be a pointer type, but found %v", replyKind)
	}

	argBuf := bytes.NewBuffer(make([]byte, 0, 1024))

	if err := gobEncodeBuf(args, argBuf); err != nil {
		return err
	}

	callRequest := CallRequest{
		Method:   serviceMethod,
		ArgBytes: argBuf.Bytes(),
	}

	reqBuf := bytes.NewBuffer(make([]byte, 0, 1024))
	if err := gob.NewEncoder(reqBuf).Encode(callRequest); err != nil {
		return err
	}

	response, err := client.httpClient.Post(client.serviceURL.String(), "", reqBuf)
	if err != nil {
		return err
	}
	defer xhttp.DrainBody(response.Body)

	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("%v rpc call failed with error code %v", serviceMethod, response.StatusCode)
	}

	var callResponse CallResponse
	if err := gob.NewDecoder(response.Body).Decode(&callResponse); err != nil {
		return err
	}

	if callResponse.Error != "" {
		return errors.New(callResponse.Error)
	}

	return gobDecode(callResponse.ReplyBytes, reply)
}

// Close closes all idle connections of the underlying http client
func (client *Client) Close() error {
	if client.httpIdleConnsCloser != nil {
		client.httpIdleConnsCloser()
	}
	return nil
}

func newCustomDialContext(timeout time.Duration) func(ctx context.Context, network, addr string) (net.Conn, error) {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		dialer := &net.Dialer{
			Timeout:   timeout,
			KeepAlive: timeout,
			DualStack: true,
		}

		conn, err := dialer.DialContext(ctx, network, addr)
		if err != nil {
			return nil, err
		}

		return xhttp.NewTimeoutConn(conn, timeout, timeout), nil
	}
}

// NewClient - returns new RPC client.
func NewClient(serviceURL *xnet.URL, tlsConfig *tls.Config, timeout time.Duration) (*Client, error) {
	// Transport is exactly same as Go default in https://golang.org/pkg/net/http/#RoundTripper
	// except custom DialContext and TLSClientConfig.
	tr := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           newCustomDialContext(timeout),
		MaxIdleConnsPerHost:   4096,
		MaxIdleConns:          4096,
		IdleConnTimeout:       120 * time.Second,
		TLSHandshakeTimeout:   30 * time.Second,
		ExpectContinueTimeout: 10 * time.Second,
		TLSClientConfig:       tlsConfig,
		DisableCompression:    true,
	}
	if tlsConfig != nil {
		// If TLS is enabled configure http2
		if err := http2.ConfigureTransport(tr); err != nil {
			return nil, err
		}
	}
	return &Client{
		httpClient:          &http.Client{Transport: tr},
		httpIdleConnsCloser: tr.CloseIdleConnections,
		serviceURL:          serviceURL,
	}, nil
}

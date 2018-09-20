/*
 * Minio Cloud Storage, (C) 2018 Minio, Inc.
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
	"context"
	"crypto/tls"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"reflect"
	"time"

	xhttp "github.com/minio/minio/cmd/http"
	xnet "github.com/minio/minio/pkg/net"
)

// DefaultRPCTimeout - default RPC timeout is one minute.
const DefaultRPCTimeout = 1 * time.Minute

// Client - http based RPC client.
type Client struct {
	httpClient *http.Client
	serviceURL *xnet.URL
}

// closeResponse close non nil response with any response Body.
// convenient wrapper to drain any remaining data on response body.
//
// Subsequently this allows golang http RoundTripper
// to re-use the same connection for future requests.
func closeResponse(body io.ReadCloser) {
	// Callers should close resp.Body when done reading from it.
	// If resp.Body is not closed, the Client's underlying RoundTripper
	// (typically Transport) may not be able to re-use a persistent TCP
	// connection to the server for a subsequent "keep-alive" request.
	if body != nil {
		// Drain any remaining Body and then close the connection.
		// Without this closing connection would disallow re-using
		// the same connection for future uses.
		//  - http://stackoverflow.com/a/17961593/4465767
		bufp := b512pool.Get().(*[]byte)
		defer b512pool.Put(bufp)
		io.CopyBuffer(ioutil.Discard, body, *bufp)
		body.Close()
	}
}

// Call - calls service method on RPC server.
func (client *Client) Call(serviceMethod string, args, reply interface{}) error {
	replyKind := reflect.TypeOf(reply).Kind()
	if replyKind != reflect.Ptr {
		return fmt.Errorf("rpc reply must be a pointer type, but found %v", replyKind)
	}

	argBuf := bufPool.Get()
	defer bufPool.Put(argBuf)

	if err := gobEncodeBuf(args, argBuf); err != nil {
		return err
	}

	callRequest := CallRequest{
		Method:   serviceMethod,
		ArgBytes: argBuf.Bytes(),
	}

	reqBuf := bufPool.Get()
	defer bufPool.Put(reqBuf)
	if err := gob.NewEncoder(reqBuf).Encode(callRequest); err != nil {
		return err
	}

	response, err := client.httpClient.Post(client.serviceURL.String(), "", reqBuf)
	if err != nil {
		return err
	}
	defer closeResponse(response.Body)

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

// Close - does nothing and presents for interface compatibility.
func (client *Client) Close() error {
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
func NewClient(serviceURL *xnet.URL, tlsConfig *tls.Config, timeout time.Duration) *Client {
	return &Client{
		httpClient: &http.Client{
			// Transport is exactly same as Go default in https://golang.org/pkg/net/http/#RoundTripper
			// except custom DialContext and TLSClientConfig.
			Transport: &http.Transport{
				Proxy:                 http.ProxyFromEnvironment,
				DialContext:           newCustomDialContext(timeout),
				MaxIdleConnsPerHost:   4096,
				MaxIdleConns:          4096,
				IdleConnTimeout:       90 * time.Second,
				TLSHandshakeTimeout:   10 * time.Second,
				ExpectContinueTimeout: 1 * time.Second,
				TLSClientConfig:       tlsConfig,
				DisableCompression:    true,
			},
		},
		serviceURL: serviceURL,
	}
}

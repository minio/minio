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

package rest

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"time"

	xhttp "github.com/minio/minio/cmd/http"
)

// DefaultRESTTimeout - default RPC timeout is one minute.
const DefaultRESTTimeout = 1 * time.Minute

// Client - http based RPC client.
type Client struct {
	httpClient          *http.Client
	httpIdleConnsCloser func()
	url                 *url.URL
	newAuthToken        func() string
}

// Call - make a REST call.
func (c *Client) Call(method string, values url.Values, body io.Reader) (reply io.ReadCloser, err error) {
	req, err := http.NewRequest(http.MethodPost, c.url.String()+"/"+method+"?"+values.Encode(), body)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Authorization", "Bearer "+c.newAuthToken())
	req.Header.Set("X-Minio-Time", time.Now().UTC().Format(time.RFC3339))

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		// Limit the ReadAll(), just in case, because of a bug, the server responds with large data.
		r := io.LimitReader(resp.Body, 1024)
		b, err := ioutil.ReadAll(r)
		if err != nil {
			return nil, err
		}
		return nil, errors.New(string(b))
	}
	return resp.Body, nil
}

// Close closes all idle connections of the underlying http client
func (c *Client) Close() {
	if c.httpIdleConnsCloser != nil {
		c.httpIdleConnsCloser()
	}
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

// NewClient - returns new REST client.
func NewClient(url *url.URL, tlsConfig *tls.Config, timeout time.Duration, newAuthToken func() string) *Client {
	// Transport is exactly same as Go default in https://golang.org/pkg/net/http/#RoundTripper
	// except custom DialContext and TLSClientConfig.
	tr := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           newCustomDialContext(timeout),
		MaxIdleConnsPerHost:   4096,
		MaxIdleConns:          4096,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		TLSClientConfig:       tlsConfig,
		DisableCompression:    true,
	}

	return &Client{
		httpClient:          &http.Client{Transport: tr},
		httpIdleConnsCloser: tr.CloseIdleConnections,
		url:                 url,
		newAuthToken:        newAuthToken,
	}
}

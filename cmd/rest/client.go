/*
 * MinIO Cloud Storage, (C) 2018-2020 MinIO, Inc.
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
	"errors"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"sync/atomic"
	"time"

	xhttp "github.com/minio/minio/cmd/http"
)

// DefaultRESTTimeout - default RPC timeout is one minute.
const DefaultRESTTimeout = 1 * time.Minute

const (
	offline = iota
	online
	closed
)

// NetworkError - error type in case of errors related to http/transport
// for ex. connection refused, connection reset, dns resolution failure etc.
// All errors returned by storage-rest-server (ex errFileNotFound, errDiskNotFound) are not considered to be network errors.
type NetworkError struct {
	Err error
}

func (n *NetworkError) Error() string {
	return n.Err.Error()
}

// Unwrap returns the error wrapped in NetworkError.
func (n *NetworkError) Unwrap() error {
	return n.Err
}

// Client - http based RPC client.
type Client struct {
	// HealthCheckFn is the function set to test for health.
	// If not set the client will not keep track of health.
	// Calling this returns true or false if the target
	// is online or offline.
	HealthCheckFn func() bool

	// HealthCheckInterval will be the duration between re-connection attempts
	// when a call has failed with a network error.
	HealthCheckInterval time.Duration

	// HealthCheckTimeout determines timeout for each call.
	HealthCheckTimeout time.Duration

	// MaxErrResponseSize is the maximum expected response size.
	// Should only be modified before any calls are made.
	MaxErrResponseSize int64

	httpClient          *http.Client
	httpIdleConnsCloser func()
	url                 *url.URL
	newAuthToken        func(audience string) string
	connected           int32
}

// URL query separator constants
const (
	querySep = "?"
)

// CallWithContext - make a REST call with context.
func (c *Client) CallWithContext(ctx context.Context, method string, values url.Values, body io.Reader, length int64) (reply io.ReadCloser, err error) {
	if !c.IsOnline() {
		return nil, &NetworkError{Err: errors.New("remote server offline")}
	}
	req, err := http.NewRequest(http.MethodPost, c.url.String()+method+querySep+values.Encode(), body)
	if err != nil {
		return nil, &NetworkError{err}
	}
	req = req.WithContext(ctx)
	req.Header.Set("Authorization", "Bearer "+c.newAuthToken(req.URL.Query().Encode()))
	req.Header.Set("X-Minio-Time", time.Now().UTC().Format(time.RFC3339))
	if length > 0 {
		req.ContentLength = length
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		// A canceled context doesn't always mean a network problem.
		if !errors.Is(err, context.Canceled) {
			// We are safe from recursion
			c.MarkOffline()
		}
		return nil, &NetworkError{err}
	}

	final := resp.Trailer.Get("FinalStatus")
	if final != "" && final != "Success" {
		defer xhttp.DrainBody(resp.Body)
		return nil, errors.New(final)
	}

	if resp.StatusCode != http.StatusOK {
		// If server returns 412 pre-condition failed, it would
		// mean that authentication succeeded, but another
		// side-channel check has failed, we shall take
		// the client offline in such situations.
		// generally all implementations should simply return
		// 403, but in situations where there is a dependency
		// with the caller to take the client offline purpose
		// fully it should make sure to respond with '412'
		// instead, see cmd/storage-rest-server.go for ideas.
		if resp.StatusCode == http.StatusPreconditionFailed {
			c.MarkOffline()
		}
		defer xhttp.DrainBody(resp.Body)
		// Limit the ReadAll(), just in case, because of a bug, the server responds with large data.
		b, err := ioutil.ReadAll(io.LimitReader(resp.Body, c.MaxErrResponseSize))
		if err != nil {
			return nil, err
		}
		if len(b) > 0 {
			return nil, errors.New(string(b))
		}
		return nil, errors.New(resp.Status)
	}
	return resp.Body, nil
}

// Call - make a REST call.
func (c *Client) Call(method string, values url.Values, body io.Reader, length int64) (reply io.ReadCloser, err error) {
	ctx := context.Background()
	return c.CallWithContext(ctx, method, values, body, length)
}

// Close closes all idle connections of the underlying http client
func (c *Client) Close() {
	atomic.StoreInt32(&c.connected, closed)
	if c.httpIdleConnsCloser != nil {
		c.httpIdleConnsCloser()
	}
}

// NewClient - returns new REST client.
func NewClient(url *url.URL, newCustomTransport func() *http.Transport, newAuthToken func(aud string) string) (*Client, error) {
	// Transport is exactly same as Go default in https://golang.org/pkg/net/http/#RoundTripper
	// except custom DialContext and TLSClientConfig.
	tr := newCustomTransport()
	return &Client{
		httpClient:          &http.Client{Transport: tr},
		httpIdleConnsCloser: tr.CloseIdleConnections,
		url:                 url,
		newAuthToken:        newAuthToken,
		connected:           online,

		MaxErrResponseSize:  4096,
		HealthCheckInterval: 200 * time.Millisecond,
		HealthCheckTimeout:  time.Second,
	}, nil
}

// IsOnline returns whether the client is likely to be online.
func (c *Client) IsOnline() bool {
	return atomic.LoadInt32(&c.connected) == online
}

// MarkOffline - will mark a client as being offline and spawns
// a goroutine that will attempt to reconnect if HealthCheckFn is set.
func (c *Client) MarkOffline() {
	// Start goroutine that will attempt to reconnect.
	// If server is already trying to reconnect this will have no effect.
	if c.HealthCheckFn != nil && atomic.CompareAndSwapInt32(&c.connected, online, offline) {
		if c.httpIdleConnsCloser != nil {
			c.httpIdleConnsCloser()
		}
		go func() {
			ticker := time.NewTicker(c.HealthCheckInterval)
			defer ticker.Stop()
			for range ticker.C {
				if status := atomic.LoadInt32(&c.connected); status == closed {
					return
				}
				if c.HealthCheckFn() {
					atomic.CompareAndSwapInt32(&c.connected, offline, online)
					return
				}
			}
		}()
	}
}

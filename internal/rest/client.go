// Copyright (c) 2015-2021 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package rest

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"net/url"
	"sync/atomic"
	"time"

	xhttp "github.com/minio/minio/internal/http"
	"github.com/minio/minio/internal/logger"
	xnet "github.com/minio/pkg/net"
)

// DefaultTimeout - default REST timeout is 10 seconds.
const DefaultTimeout = 10 * time.Second

const (
	offline = iota
	online
	closed
)

// Hold the number of failed RPC calls due to networking errors
var networkErrsCounter uint64

// GetNetworkErrsCounter returns the number of failed RPC requests
func GetNetworkErrsCounter() uint64 {
	return atomic.LoadUint64(&networkErrsCounter)
}

// ResetNetworkErrsCounter resets the number of failed RPC requests
func ResetNetworkErrsCounter() {
	atomic.StoreUint64(&networkErrsCounter, 0)
}

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
	connected int32 // ref: https://golang.org/pkg/sync/atomic/#pkg-note-BUG
	_         int32 // For 64 bits alignment
	lastConn  int64

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

	// ExpectTimeouts indicates if context timeouts are expected.
	// This will not mark the client offline in these cases.
	ExpectTimeouts bool

	// Avoid metrics update if set to true
	NoMetrics bool

	httpClient   *http.Client
	url          *url.URL
	newAuthToken func(audience string) string
}

// URL query separator constants
const (
	querySep = "?"
)

type restError string

func (e restError) Error() string {
	return string(e)
}

func (e restError) Timeout() bool {
	return true
}

// Call - make a REST call with context.
func (c *Client) Call(ctx context.Context, method string, values url.Values, body io.Reader, length int64) (reply io.ReadCloser, err error) {
	if !c.IsOnline() {
		return nil, &NetworkError{Err: &url.Error{Op: method, URL: c.url.String(), Err: restError("remote server offline")}}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url.String()+method+querySep+values.Encode(), body)
	if err != nil {
		return nil, &NetworkError{err}
	}
	req.Header.Set("Authorization", "Bearer "+c.newAuthToken(req.URL.RawQuery))
	req.Header.Set("X-Minio-Time", time.Now().UTC().Format(time.RFC3339))
	req.Header.Set("Expect", "100-continue")
	if length > 0 {
		req.ContentLength = length
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		if xnet.IsNetworkOrHostDown(err, c.ExpectTimeouts) {
			if !c.NoMetrics {
				atomic.AddUint64(&networkErrsCounter, 1)
			}
			if c.MarkOffline() {
				logger.LogIf(ctx, fmt.Errorf("Marking %s temporary offline; caused by %w", c.url.String(), err))
			}
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
		if c.HealthCheckFn != nil && resp.StatusCode == http.StatusPreconditionFailed {
			logger.LogIf(ctx, fmt.Errorf("Marking %s temporary offline; caused by PreconditionFailed with disk ID mismatch", c.url.String()))
			c.MarkOffline()
		}
		defer xhttp.DrainBody(resp.Body)
		// Limit the ReadAll(), just in case, because of a bug, the server responds with large data.
		b, err := ioutil.ReadAll(io.LimitReader(resp.Body, c.MaxErrResponseSize))
		if err != nil {
			if xnet.IsNetworkOrHostDown(err, c.ExpectTimeouts) {
				if !c.NoMetrics {
					atomic.AddUint64(&networkErrsCounter, 1)
				}
				if c.MarkOffline() {
					logger.LogIf(ctx, fmt.Errorf("Marking %s temporary offline; caused by %w", c.url.String(), err))
				}
			}
			return nil, err
		}
		if len(b) > 0 {
			return nil, errors.New(string(b))
		}
		return nil, errors.New(resp.Status)
	}
	return resp.Body, nil
}

// Close closes all idle connections of the underlying http client
func (c *Client) Close() {
	atomic.StoreInt32(&c.connected, closed)
}

// NewClient - returns new REST client.
func NewClient(url *url.URL, tr http.RoundTripper, newAuthToken func(aud string) string) *Client {
	// Transport is exactly same as Go default in https://golang.org/pkg/net/http/#RoundTripper
	// except custom DialContext and TLSClientConfig.
	return &Client{
		httpClient:          &http.Client{Transport: tr},
		url:                 url,
		newAuthToken:        newAuthToken,
		connected:           online,
		lastConn:            time.Now().UnixNano(),
		MaxErrResponseSize:  4096,
		HealthCheckInterval: 200 * time.Millisecond,
		HealthCheckTimeout:  time.Second,
	}
}

// IsOnline returns whether the client is likely to be online.
func (c *Client) IsOnline() bool {
	return atomic.LoadInt32(&c.connected) == online
}

// LastConn returns when the disk was (re-)connected
func (c *Client) LastConn() time.Time {
	return time.Unix(0, atomic.LoadInt64(&c.lastConn))
}

// MarkOffline - will mark a client as being offline and spawns
// a goroutine that will attempt to reconnect if HealthCheckFn is set.
// returns true if the node changed state from online to offline
func (c *Client) MarkOffline() bool {
	// Start goroutine that will attempt to reconnect.
	// If server is already trying to reconnect this will have no effect.
	if c.HealthCheckFn != nil && atomic.CompareAndSwapInt32(&c.connected, online, offline) {
		r := rand.New(rand.NewSource(time.Now().UnixNano()))
		go func() {
			for {
				if atomic.LoadInt32(&c.connected) == closed {
					return
				}
				if c.HealthCheckFn() {
					if atomic.CompareAndSwapInt32(&c.connected, offline, online) {
						logger.Info("Client %s online", c.url.String())
						atomic.StoreInt64(&c.lastConn, time.Now().UnixNano())
					}
					return
				}
				time.Sleep(time.Duration(r.Float64() * float64(c.HealthCheckInterval)))
			}
		}()
		return true
	}
	return false
}

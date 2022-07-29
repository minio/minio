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
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"net/url"
	"path"
	"strings"
	"sync"
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

	sync.RWMutex // mutex for lastErr
	lastErr      error
}

type restError string

func (e restError) Error() string {
	return string(e)
}

func (e restError) Timeout() bool {
	return true
}

// Given a string of the form "host", "host:port", or "[ipv6::address]:port",
// return true if the string includes a port.
func hasPort(s string) bool { return strings.LastIndex(s, ":") > strings.LastIndex(s, "]") }

// removeEmptyPort strips the empty port in ":port" to ""
// as mandated by RFC 3986 Section 6.2.3.
func removeEmptyPort(host string) string {
	if hasPort(host) {
		return strings.TrimSuffix(host, ":")
	}
	return host
}

// Copied from http.NewRequest but implemented to ensure we re-use `url.URL` instance.
func (c *Client) newRequest(ctx context.Context, u *url.URL, body io.Reader) (*http.Request, error) {
	rc, ok := body.(io.ReadCloser)
	if !ok && body != nil {
		rc = io.NopCloser(body)
	}
	u.Host = removeEmptyPort(u.Host)
	// The host's colon:port should be normalized. See Issue 14836.
	req := &http.Request{
		Method:     http.MethodPost,
		URL:        u,
		Proto:      "HTTP/1.1",
		ProtoMajor: 1,
		ProtoMinor: 1,
		Header:     make(http.Header),
		Body:       rc,
		Host:       u.Host,
	}
	req = req.WithContext(ctx)
	if body != nil {
		switch v := body.(type) {
		case *bytes.Buffer:
			req.ContentLength = int64(v.Len())
			buf := v.Bytes()
			req.GetBody = func() (io.ReadCloser, error) {
				r := bytes.NewReader(buf)
				return io.NopCloser(r), nil
			}
		case *bytes.Reader:
			req.ContentLength = int64(v.Len())
			snapshot := *v
			req.GetBody = func() (io.ReadCloser, error) {
				r := snapshot
				return io.NopCloser(&r), nil
			}
		case *strings.Reader:
			req.ContentLength = int64(v.Len())
			snapshot := *v
			req.GetBody = func() (io.ReadCloser, error) {
				r := snapshot
				return io.NopCloser(&r), nil
			}
		default:
			// This is where we'd set it to -1 (at least
			// if body != NoBody) to mean unknown, but
			// that broke people during the Go 1.8 testing
			// period. People depend on it being 0 I
			// guess. Maybe retry later. See Issue 18117.
		}
		// For client requests, Request.ContentLength of 0
		// means either actually 0, or unknown. The only way
		// to explicitly say that the ContentLength is zero is
		// to set the Body to nil. But turns out too much code
		// depends on NewRequest returning a non-nil Body,
		// so we use a well-known ReadCloser variable instead
		// and have the http package also treat that sentinel
		// variable to mean explicitly zero.
		if req.GetBody != nil && req.ContentLength == 0 {
			req.Body = http.NoBody
			req.GetBody = func() (io.ReadCloser, error) { return http.NoBody, nil }
		}
	}

	if c.newAuthToken != nil {
		req.Header.Set("Authorization", "Bearer "+c.newAuthToken(u.RawQuery))
	}
	req.Header.Set("X-Minio-Time", time.Now().UTC().Format(time.RFC3339))
	if body != nil {
		req.Header.Set("Expect", "100-continue")
	}

	return req, nil
}

// Call - make a REST call with context.
func (c *Client) Call(ctx context.Context, method string, values url.Values, body io.Reader, length int64) (reply io.ReadCloser, err error) {
	urlStr := c.url.String()
	if !c.IsOnline() {
		return nil, &NetworkError{c.LastError()}
	}

	u, err := url.Parse(urlStr)
	if err != nil {
		return nil, &NetworkError{Err: &url.Error{Op: method, URL: urlStr, Err: err}}
	}

	u.Path = path.Join(u.Path, method)
	u.RawQuery = values.Encode()

	req, err := c.newRequest(ctx, u, body)
	if err != nil {
		return nil, &NetworkError{err}
	}
	if length > 0 {
		req.ContentLength = length
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		if xnet.IsNetworkOrHostDown(err, c.ExpectTimeouts) {
			if !c.NoMetrics {
				atomic.AddUint64(&networkErrsCounter, 1)
			}
			if c.MarkOffline(err) {
				logger.LogOnceIf(ctx, fmt.Errorf("Marking %s offline temporarily; caused by %w", c.url.Host, err), c.url.Host)
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
			err = fmt.Errorf("Marking %s offline temporarily; caused by PreconditionFailed with disk ID mismatch", c.url.Host)
			logger.LogOnceIf(ctx, err, c.url.Host)
			c.MarkOffline(err)
		}
		defer xhttp.DrainBody(resp.Body)
		// Limit the ReadAll(), just in case, because of a bug, the server responds with large data.
		b, err := ioutil.ReadAll(io.LimitReader(resp.Body, c.MaxErrResponseSize))
		if err != nil {
			if xnet.IsNetworkOrHostDown(err, c.ExpectTimeouts) {
				if !c.NoMetrics {
					atomic.AddUint64(&networkErrsCounter, 1)
				}
				if c.MarkOffline(err) {
					logger.LogOnceIf(ctx, fmt.Errorf("Marking %s offline temporarily; caused by %w", c.url.Host, err), c.url.Host)
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

// LastError returns previous error
func (c *Client) LastError() error {
	c.RLock()
	defer c.RUnlock()
	return c.lastErr
}

// MarkOffline - will mark a client as being offline and spawns
// a goroutine that will attempt to reconnect if HealthCheckFn is set.
// returns true if the node changed state from online to offline
func (c *Client) MarkOffline(err error) bool {
	c.Lock()
	c.lastErr = err
	c.Unlock()
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
						now := time.Now()
						disconnected := now.Sub(c.LastConn())
						logger.Info("Client '%s' re-connected in %s", c.url.String(), disconnected)
						atomic.StoreInt64(&c.lastConn, now.UnixNano())
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

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
	"math/rand"
	"net/http"
	"net/http/httputil"
	"net/url"
	"path"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	xhttp "github.com/minio/minio/internal/http"
	"github.com/minio/minio/internal/logger"
	"github.com/minio/minio/internal/mcontext"
	xnet "github.com/minio/pkg/v3/net"
)

const logSubsys = "internodes"

// DefaultTimeout - default REST timeout is 10 seconds.
const DefaultTimeout = 10 * time.Second

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
	connected int32 // ref: https://golang.org/pkg/sync/atomic/#pkg-note-BUG
	_         int32 // For 64 bits alignment
	lastConn  int64

	// HealthCheckFn is the function set to test for health.
	// If not set the client will not keep track of health.
	// Calling this returns true or false if the target
	// is online or offline.
	HealthCheckFn func() bool

	// HealthCheckRetryUnit will be used to calculate the exponential
	// backoff when trying to reconnect to an offline node
	HealthCheckReconnectUnit time.Duration

	// HealthCheckTimeout determines timeout for each call.
	HealthCheckTimeout time.Duration

	// MaxErrResponseSize is the maximum expected response size.
	// Should only be modified before any calls are made.
	MaxErrResponseSize int64

	// Avoid metrics update if set to true
	NoMetrics bool

	// TraceOutput will print debug information on non-200 calls if set.
	TraceOutput io.Writer // Debug trace output

	httpClient *http.Client
	url        *url.URL
	auth       func() string

	sync.RWMutex // mutex for lastErr
	lastErr      error
	lastErrTime  time.Time
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

// Copied from http.NewRequest but implemented to ensure we reuse `url.URL` instance.
func (c *Client) newRequest(ctx context.Context, method string, u url.URL, body io.Reader) (*http.Request, error) {
	rc, ok := body.(io.ReadCloser)
	if !ok && body != nil {
		rc = io.NopCloser(body)
	}
	req := &http.Request{
		Method:     method,
		URL:        &u,
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

	if c.auth != nil {
		req.Header.Set("Authorization", "Bearer "+c.auth())
	}
	req.Header.Set("X-Minio-Time", strconv.FormatInt(time.Now().UnixNano(), 10))

	if tc, ok := ctx.Value(mcontext.ContextTraceKey).(*mcontext.TraceCtxt); ok {
		req.Header.Set(xhttp.AmzRequestID, tc.AmzReqID)
	}

	return req, nil
}

type respBodyMonitor struct {
	io.ReadCloser
	expectTimeouts  bool
	errorStatusOnce sync.Once
}

func (r *respBodyMonitor) Read(p []byte) (n int, err error) {
	n, err = r.ReadCloser.Read(p)
	r.errorStatus(err)
	return n, err
}

func (r *respBodyMonitor) Close() (err error) {
	err = r.ReadCloser.Close()
	r.errorStatus(err)
	return err
}

func (r *respBodyMonitor) errorStatus(err error) {
	if xnet.IsNetworkOrHostDown(err, r.expectTimeouts) {
		r.errorStatusOnce.Do(func() {
			atomic.AddUint64(&globalStats.errs, 1)
		})
	}
}

// dumpHTTP - dump HTTP request and response.
func (c *Client) dumpHTTP(req *http.Request, resp *http.Response) {
	// Starts http dump.
	_, err := fmt.Fprintln(c.TraceOutput, "---------START-HTTP---------")
	if err != nil {
		return
	}

	// Filter out Signature field from Authorization header.
	origAuth := req.Header.Get("Authorization")
	if origAuth != "" {
		req.Header.Set("Authorization", "**REDACTED**")
	}

	// Only display request header.
	reqTrace, err := httputil.DumpRequestOut(req, false)
	if err != nil {
		return
	}

	// Write request to trace output.
	_, err = fmt.Fprint(c.TraceOutput, string(reqTrace))
	if err != nil {
		return
	}

	// Only display response header.
	var respTrace []byte

	// For errors we make sure to dump response body as well.
	if resp.StatusCode != http.StatusOK &&
		resp.StatusCode != http.StatusPartialContent &&
		resp.StatusCode != http.StatusNoContent {
		respTrace, err = httputil.DumpResponse(resp, true)
		if err != nil {
			return
		}
	} else {
		respTrace, err = httputil.DumpResponse(resp, false)
		if err != nil {
			return
		}
	}

	// Write response to trace output.
	_, err = fmt.Fprint(c.TraceOutput, strings.TrimSuffix(string(respTrace), "\r\n"))
	if err != nil {
		return
	}

	// Ends the http dump.
	_, err = fmt.Fprintln(c.TraceOutput, "---------END-HTTP---------")
	if err != nil {
		return
	}

	// Returns success.
}

// ErrClientClosed returned when *Client is closed.
var ErrClientClosed = errors.New("rest client is closed")

// CallWithHTTPMethod - make a REST call with context, using a custom HTTP method.
func (c *Client) CallWithHTTPMethod(ctx context.Context, httpMethod, rpcMethod string, values url.Values, body io.Reader, length int64) (reply io.ReadCloser, err error) {
	switch atomic.LoadInt32(&c.connected) {
	case closed:
		// client closed, this is usually a manual process
		// so return a local error as client is closed
		return nil, &NetworkError{Err: ErrClientClosed}
	case offline:
		// client offline, return last error captured.
		return nil, &NetworkError{Err: c.LastError()}
	}

	// client is still connected, attempt the request.

	// Shallow copy. We don't modify the *UserInfo, if set.
	// All other fields are copied.
	u := *c.url
	u.Path = path.Join(u.Path, rpcMethod)
	u.RawQuery = values.Encode()

	req, err := c.newRequest(ctx, httpMethod, u, body)
	if err != nil {
		return nil, &NetworkError{Err: err}
	}
	if length > 0 {
		req.ContentLength = length
	}

	_, expectTimeouts := ctx.Deadline()

	req, update := setupReqStatsUpdate(req)
	defer update()

	resp, err := c.httpClient.Do(req)
	if err != nil {
		if xnet.IsNetworkOrHostDown(err, expectTimeouts) {
			if !c.NoMetrics {
				atomic.AddUint64(&globalStats.errs, 1)
			}
			if c.MarkOffline(err) {
				logger.LogOnceIf(ctx, logSubsys, fmt.Errorf("Marking %s offline temporarily; caused by %w", c.url.Host, err), c.url.Host)
			}
		}
		return nil, &NetworkError{err}
	}

	// If trace is enabled, dump http request and response,
	// except when the traceErrorsOnly enabled and the response's status code is ok
	if c.TraceOutput != nil && resp.StatusCode != http.StatusOK {
		c.dumpHTTP(req, resp)
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
			err = fmt.Errorf("Marking %s offline temporarily; caused by PreconditionFailed with drive ID mismatch", c.url.Host)
			logger.LogOnceIf(ctx, logSubsys, err, c.url.Host)
			c.MarkOffline(err)
		}
		defer xhttp.DrainBody(resp.Body)
		// Limit the ReadAll(), just in case, because of a bug, the server responds with large data.
		b, err := io.ReadAll(io.LimitReader(resp.Body, c.MaxErrResponseSize))
		if err != nil {
			if xnet.IsNetworkOrHostDown(err, expectTimeouts) {
				if !c.NoMetrics {
					atomic.AddUint64(&globalStats.errs, 1)
				}
				if c.MarkOffline(err) {
					logger.LogOnceIf(ctx, logSubsys, fmt.Errorf("Marking %s offline temporarily; caused by %w", c.url.Host, err), c.url.Host)
				}
			}
			return nil, err
		}
		if len(b) > 0 {
			return nil, errors.New(string(b))
		}
		return nil, errors.New(resp.Status)
	}
	if !c.NoMetrics {
		resp.Body = &respBodyMonitor{ReadCloser: resp.Body, expectTimeouts: expectTimeouts}
	}
	return resp.Body, nil
}

// Call - make a REST call with context.
func (c *Client) Call(ctx context.Context, rpcMethod string, values url.Values, body io.Reader, length int64) (reply io.ReadCloser, err error) {
	return c.CallWithHTTPMethod(ctx, http.MethodPost, rpcMethod, values, body, length)
}

// Close closes all idle connections of the underlying http client
func (c *Client) Close() {
	atomic.StoreInt32(&c.connected, closed)
}

// NewClient - returns new REST client.
func NewClient(uu *url.URL, tr http.RoundTripper, auth func() string) *Client {
	connected := int32(online)
	urlStr := uu.String()
	u, err := url.Parse(urlStr)
	if err != nil {
		// Mark offline, with no reconnection attempts.
		connected = int32(offline)
		err = &url.Error{URL: urlStr, Err: err}
	}
	// The host's colon:port should be normalized. See Issue 14836.
	u.Host = removeEmptyPort(u.Host)

	// Transport is exactly same as Go default in https://golang.org/pkg/net/http/#RoundTripper
	// except custom DialContext and TLSClientConfig.
	clnt := &Client{
		httpClient:               &http.Client{Transport: tr},
		url:                      u,
		auth:                     auth,
		connected:                connected,
		lastConn:                 time.Now().UnixNano(),
		MaxErrResponseSize:       4096,
		HealthCheckReconnectUnit: 200 * time.Millisecond,
		HealthCheckTimeout:       time.Second,
	}
	if err != nil {
		clnt.lastErr = err
		clnt.lastErrTime = time.Now()
	}

	if clnt.HealthCheckFn != nil {
		// make connection pre-emptively.
		go clnt.HealthCheckFn()
	}
	return clnt
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
	return fmt.Errorf("[%s] %w", c.lastErrTime.Format(time.RFC3339), c.lastErr)
}

// computes the exponential backoff duration according to
// https://www.awsarchitectureblog.com/2015/03/backoff.html
func exponentialBackoffWait(r *rand.Rand, unit, maxSleep time.Duration) func(uint) time.Duration {
	if unit > time.Hour {
		// Protect against integer overflow
		panic("unit cannot exceed one hour")
	}
	return func(attempt uint) time.Duration {
		if attempt > 16 {
			// Protect against integer overflow
			attempt = 16
		}
		// sleep = random_between(unit, min(cap, base * 2 ** attempt))
		sleep := min(unit*time.Duration(1<<attempt), maxSleep)
		sleep -= time.Duration(r.Float64() * float64(sleep-unit))
		return sleep
	}
}

func (c *Client) runHealthCheck() bool {
	// Start goroutine that will attempt to reconnect.
	// If server is already trying to reconnect this will have no effect.
	if c.HealthCheckFn != nil && atomic.CompareAndSwapInt32(&c.connected, online, offline) {
		go func() {
			backOff := exponentialBackoffWait(
				rand.New(rand.NewSource(time.Now().UnixNano())),
				200*time.Millisecond,
				30*time.Second,
			)

			attempt := uint(0)
			for {
				if atomic.LoadInt32(&c.connected) == closed {
					return
				}
				if c.HealthCheckFn() {
					if atomic.CompareAndSwapInt32(&c.connected, offline, online) {
						now := time.Now()
						disconnected := now.Sub(c.LastConn())
						logger.Event(context.Background(), "healthcheck", "Client '%s' re-connected in %s", c.url.String(), disconnected)
						atomic.StoreInt64(&c.lastConn, now.UnixNano())
					}
					return
				}
				attempt++
				time.Sleep(backOff(attempt))
			}
		}()
		return true
	}
	return false
}

// MarkOffline - will mark a client as being offline and spawns
// a goroutine that will attempt to reconnect if HealthCheckFn is set.
// returns true if the node changed state from online to offline
func (c *Client) MarkOffline(err error) bool {
	c.Lock()
	c.lastErr = err
	c.lastErrTime = time.Now()
	atomic.StoreInt64(&c.lastConn, time.Now().UnixNano())
	c.Unlock()

	return c.runHealthCheck()
}

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

package rest

import (
	"context"
	"errors"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"time"

	xhttp "github.com/minio/minio/cmd/http"
)

// DefaultRESTTimeout - default RPC timeout is one minute.
const DefaultRESTTimeout = 1 * time.Minute

// NetworkError - error type in case of errors related to http/transport
// for ex. connection refused, connection reset, dns resolution failure etc.
// All errors returned by storage-rest-server (ex errFileNotFound, errDiskNotFound) are not considered to be network errors.
type NetworkError struct {
	Err error
}

func (n *NetworkError) Error() string {
	return n.Err.Error()
}

// Client - http based RPC client.
type Client struct {
	httpClient          *http.Client
	httpIdleConnsCloser func()
	url                 *url.URL
	newAuthToken        func(audience string) string
}

// URL query separator constants
const (
	querySep = "?"
)

// CallWithContext - make a REST call with context.
func (c *Client) CallWithContext(ctx context.Context, method string, values url.Values, body io.Reader, length int64) (reply io.ReadCloser, err error) {
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
		c.httpClient.CloseIdleConnections()
		return nil, &NetworkError{err}
	}

	final := resp.Trailer.Get("FinalStatus")
	if final != "" && final != "Success" {
		defer xhttp.DrainBody(resp.Body)
		return nil, errors.New(final)
	}

	if resp.StatusCode != http.StatusOK {
		defer xhttp.DrainBody(resp.Body)
		// Limit the ReadAll(), just in case, because of a bug, the server responds with large data.
		b, err := ioutil.ReadAll(io.LimitReader(resp.Body, 4096))
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
	}, nil
}

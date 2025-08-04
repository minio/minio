// Copyright (c) 2015-2022 MinIO, Inc.
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

package http

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net/http"
	"syscall"
	"time"

	"github.com/minio/pkg/v3/certs"
)

// tlsClientSessionCacheSize is the cache size for client sessions.
var tlsClientSessionCacheSize = 100

const (
	// WriteBufferSize 64KiB moving up from 4KiB default
	WriteBufferSize = 64 << 10

	// ReadBufferSize 64KiB moving up from 4KiB default
	ReadBufferSize = 64 << 10
)

// ConnSettings - contains connection settings.
type ConnSettings struct {
	DialContext DialContext // Custom dialContext, DialTimeout is ignored if this is already setup.
	LookupHost  LookupHost  // Custom lookupHost, is nil on containerized deployments.
	DialTimeout time.Duration

	// TLS Settings
	RootCAs          *x509.CertPool
	CipherSuites     []uint16
	CurvePreferences []tls.CurveID

	// HTTP2
	EnableHTTP2 bool

	// TCP Options
	TCPOptions TCPOptions
}

func (s ConnSettings) getDefaultTransport(maxIdleConnsPerHost int) *http.Transport {
	if maxIdleConnsPerHost <= 0 {
		maxIdleConnsPerHost = 1024
	}

	dialContext := s.DialContext
	if dialContext == nil {
		dialContext = DialContextWithLookupHost(s.LookupHost, NewInternodeDialContext(s.DialTimeout, s.TCPOptions))
	}

	tlsClientConfig := tls.Config{
		RootCAs:            s.RootCAs,
		CipherSuites:       s.CipherSuites,
		CurvePreferences:   s.CurvePreferences,
		ClientSessionCache: tls.NewLRUClientSessionCache(tlsClientSessionCacheSize),
	}

	// For more details about various values used here refer
	// https://golang.org/pkg/net/http/#Transport documentation
	tr := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           dialContext,
		MaxIdleConnsPerHost:   maxIdleConnsPerHost,
		WriteBufferSize:       WriteBufferSize,
		ReadBufferSize:        ReadBufferSize,
		IdleConnTimeout:       15 * time.Second,
		ResponseHeaderTimeout: 15 * time.Minute, // Conservative timeout is the default (for MinIO internode)
		TLSHandshakeTimeout:   10 * time.Second,
		TLSClientConfig:       &tlsClientConfig,
		ForceAttemptHTTP2:     s.EnableHTTP2,
		// Go net/http automatically unzip if content-type is
		// gzip disable this feature, as we are always interested
		// in raw stream.
		DisableCompression: true,
	}

	// https://github.com/golang/go/issues/23559
	// https://github.com/golang/go/issues/42534
	// https://github.com/golang/go/issues/43989
	// https://github.com/golang/go/issues/33425
	// https://github.com/golang/go/issues/29246
	// if tlsConfig != nil {
	// 	trhttp2, _ := http2.ConfigureTransports(tr)
	// 	if trhttp2 != nil {
	// 		// ReadIdleTimeout is the timeout after which a health check using ping
	// 		// frame will be carried out if no frame is received on the
	// 		// connection. 5 minutes is sufficient time for any idle connection.
	// 		trhttp2.ReadIdleTimeout = 5 * time.Minute
	// 		// PingTimeout is the timeout after which the connection will be closed
	// 		// if a response to Ping is not received.
	// 		trhttp2.PingTimeout = dialTimeout
	// 		// DisableCompression, if true, prevents the Transport from
	// 		// requesting compression with an "Accept-Encoding: gzip"
	// 		trhttp2.DisableCompression = true
	// 	}
	// }

	return tr
}

// NewInternodeHTTPTransport returns transport for internode MinIO connections.
func (s ConnSettings) NewInternodeHTTPTransport(maxIdleConnsPerHost int) func() http.RoundTripper {
	tr := s.getDefaultTransport(maxIdleConnsPerHost)

	// Settings specific to internode requests.
	tr.TLSHandshakeTimeout = 15 * time.Second

	return func() http.RoundTripper {
		return tr
	}
}

// NewCustomHTTPProxyTransport is used only for proxied requests, specifically
// only supports HTTP/1.1
func (s ConnSettings) NewCustomHTTPProxyTransport() func() *http.Transport {
	s.EnableHTTP2 = false
	tr := s.getDefaultTransport(0)

	// Settings specific to proxied requests.
	tr.ResponseHeaderTimeout = 30 * time.Minute

	return func() *http.Transport {
		return tr
	}
}

// NewHTTPTransportWithTimeout allows setting a timeout for response headers
func (s ConnSettings) NewHTTPTransportWithTimeout(timeout time.Duration) *http.Transport {
	tr := s.getDefaultTransport(0)

	// Settings specific to this transport.
	tr.ResponseHeaderTimeout = timeout
	return tr
}

// NewHTTPTransportWithClientCerts returns a new http configuration used for
// communicating with client cert authentication.
func (s ConnSettings) NewHTTPTransportWithClientCerts(ctx context.Context, clientCert, clientKey string) (*http.Transport, error) {
	transport := s.NewHTTPTransportWithTimeout(1 * time.Minute)
	if clientCert != "" && clientKey != "" {
		c, err := certs.NewManager(ctx, clientCert, clientKey, tls.LoadX509KeyPair)
		if err != nil {
			return nil, err
		}
		if c != nil {
			c.UpdateReloadDuration(10 * time.Second)
			c.ReloadOnSignal(syscall.SIGHUP) // allow reloads upon SIGHUP
			transport.TLSClientConfig.GetClientCertificate = c.GetClientCertificate
		}
	}
	return transport, nil
}

// NewRemoteTargetHTTPTransport returns a new http configuration
// used while communicating with the remote replication targets.
func (s ConnSettings) NewRemoteTargetHTTPTransport(insecure bool) func() *http.Transport {
	tr := s.getDefaultTransport(0)

	tr.TLSHandshakeTimeout = 10 * time.Second
	tr.ResponseHeaderTimeout = 0
	tr.TLSClientConfig.InsecureSkipVerify = insecure

	return func() *http.Transport {
		return tr
	}
}

// uaTransport - User-Agent  transport
type uaTransport struct {
	ua string
	rt http.RoundTripper
}

func (u *uaTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	req2 := req.Clone(req.Context())
	req2.Header.Set("User-Agent", u.ua)
	return u.rt.RoundTrip(req2)
}

// WithUserAgent wraps an existing transport with custom User-Agent
func WithUserAgent(rt http.RoundTripper, getUA func() string) http.RoundTripper {
	return &uaTransport{
		ua: getUA(),
		rt: rt,
	}
}

/*
 * MinIO Cloud Storage, (C) 2018-2019 MinIO, Inc.
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

package handlers

import (
	"context"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"time"
)

const defaultFlushInterval = time.Duration(100) * time.Millisecond

// Forwarder forwards all incoming HTTP requests to configured transport.
type Forwarder struct {
	RoundTripper http.RoundTripper
	PassHost     bool
	Logger       func(error)

	// internal variables
	rewriter *headerRewriter
}

// NewForwarder creates an instance of Forwarder based on the provided list of configuration options
func NewForwarder(f *Forwarder) *Forwarder {
	f.rewriter = &headerRewriter{}
	if f.RoundTripper == nil {
		f.RoundTripper = http.DefaultTransport
	}

	return f
}

// ServeHTTP forwards HTTP traffic using the configured transport
func (f *Forwarder) ServeHTTP(w http.ResponseWriter, inReq *http.Request) {
	outReq := new(http.Request)
	*outReq = *inReq // includes shallow copies of maps, but we handle this in Director

	revproxy := httputil.ReverseProxy{
		Director: func(req *http.Request) {
			f.modifyRequest(req, inReq.URL)
		},
		Transport:     f.RoundTripper,
		FlushInterval: defaultFlushInterval,
		ErrorHandler:  f.customErrHandler,
	}
	revproxy.ServeHTTP(w, outReq)
}

// customErrHandler is originally implemented to avoid having the following error
//    `http: proxy error: context canceled` printed by Golang
func (f *Forwarder) customErrHandler(w http.ResponseWriter, r *http.Request, err error) {
	if f.Logger != nil && err != context.Canceled {
		f.Logger(err)
	}
	w.WriteHeader(http.StatusBadGateway)
}

func (f *Forwarder) getURLFromRequest(req *http.Request) *url.URL {
	// If the Request was created by Go via a real HTTP request,  RequestURI will
	// contain the original query string. If the Request was created in code, RequestURI
	// will be empty, and we will use the URL object instead
	u := req.URL
	if req.RequestURI != "" {
		parsedURL, err := url.ParseRequestURI(req.RequestURI)
		if err == nil {
			u = parsedURL
		}
	}
	return u
}

// copyURL provides update safe copy by avoiding shallow copying User field
func copyURL(i *url.URL) *url.URL {
	out := *i
	if i.User != nil {
		u := *i.User
		out.User = &u
	}
	return &out
}

// Modify the request to handle the target URL
func (f *Forwarder) modifyRequest(outReq *http.Request, target *url.URL) {
	outReq.URL = copyURL(outReq.URL)
	outReq.URL.Scheme = target.Scheme
	outReq.URL.Host = target.Host

	u := f.getURLFromRequest(outReq)

	outReq.URL.Path = u.Path
	outReq.URL.RawPath = u.RawPath
	outReq.URL.RawQuery = u.RawQuery
	outReq.RequestURI = "" // Outgoing request should not have RequestURI

	// Do not pass client Host header unless requested.
	if !f.PassHost {
		outReq.Host = target.Host
	}

	// TODO: only supports HTTP 1.1 for now.
	outReq.Proto = "HTTP/1.1"
	outReq.ProtoMajor = 1
	outReq.ProtoMinor = 1

	f.rewriter.Rewrite(outReq)

	// Disable closeNotify when method GET for http pipelining
	if outReq.Method == http.MethodGet {
		quietReq := outReq.WithContext(context.Background())
		*outReq = *quietReq
	}
}

// headerRewriter is responsible for removing hop-by-hop headers and setting forwarding headers
type headerRewriter struct{}

// Clean up IP in case if it is ipv6 address and it has {zone} information in it, like
// "[fe80::d806:a55d:eb1b:49cc%vEthernet (vmxnet3 Ethernet Adapter - Virtual Switch)]:64692"
func ipv6fix(clientIP string) string {
	return strings.Split(clientIP, "%")[0]
}

func (rw *headerRewriter) Rewrite(req *http.Request) {
	if clientIP, _, err := net.SplitHostPort(req.RemoteAddr); err == nil {
		clientIP = ipv6fix(clientIP)
		if req.Header.Get(xRealIP) == "" {
			req.Header.Set(xRealIP, clientIP)
		}
	}

	xfProto := req.Header.Get(xForwardedProto)
	if xfProto == "" {
		if req.TLS != nil {
			req.Header.Set(xForwardedProto, "https")
		} else {
			req.Header.Set(xForwardedProto, "http")
		}
	}

	if xfPort := req.Header.Get(xForwardedPort); xfPort == "" {
		req.Header.Set(xForwardedPort, forwardedPort(req))
	}

	if xfHost := req.Header.Get(xForwardedHost); xfHost == "" && req.Host != "" {
		req.Header.Set(xForwardedHost, req.Host)
	}
}

func forwardedPort(req *http.Request) string {
	if req == nil {
		return ""
	}

	if _, port, err := net.SplitHostPort(req.Host); err == nil && port != "" {
		return port
	}

	if req.TLS != nil {
		return "443"
	}

	return "80"
}

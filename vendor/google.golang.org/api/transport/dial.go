// Copyright 2015 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package transport supports network connections to HTTP and GRPC servers.
// This package is not intended for use by end developers. Use the
// google.golang.org/api/option package to configure API clients.
package transport

import (
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"

	"golang.org/x/net/context"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/oauth"

	gtransport "google.golang.org/api/googleapi/transport"
	"google.golang.org/api/internal"
	"google.golang.org/api/option"
)

// NewHTTPClient returns an HTTP client for use communicating with a Google cloud
// service, configured with the given ClientOptions. It also returns the endpoint
// for the service as specified in the options.
func NewHTTPClient(ctx context.Context, opts ...option.ClientOption) (*http.Client, string, error) {
	var o internal.DialSettings
	for _, opt := range opts {
		opt.Apply(&o)
	}
	if o.GRPCConn != nil {
		return nil, "", errors.New("unsupported gRPC connection specified")
	}
	// TODO(cbro): consider injecting the User-Agent even if an explicit HTTP client is provided?
	if o.HTTPClient != nil {
		return o.HTTPClient, o.Endpoint, nil
	}
	if o.APIKey != "" {
		hc := &http.Client{
			Transport: &gtransport.APIKey{
				Key: o.APIKey,
				Transport: userAgentTransport{
					base:      baseTransport(ctx),
					userAgent: o.UserAgent,
				},
			},
		}
		return hc, o.Endpoint, nil
	}
	if o.ServiceAccountJSONFilename != "" {
		ts, err := serviceAcctTokenSource(ctx, o.ServiceAccountJSONFilename, o.Scopes...)
		if err != nil {
			return nil, "", err
		}
		o.TokenSource = ts
	}
	if o.TokenSource == nil {
		var err error
		o.TokenSource, err = google.DefaultTokenSource(ctx, o.Scopes...)
		if err != nil {
			return nil, "", fmt.Errorf("google.DefaultTokenSource: %v", err)
		}
	}
	hc := &http.Client{
		Transport: &oauth2.Transport{
			Source: o.TokenSource,
			Base: userAgentTransport{
				base:      baseTransport(ctx),
				userAgent: o.UserAgent,
			},
		},
	}
	return hc, o.Endpoint, nil
}

type userAgentTransport struct {
	userAgent string
	base      http.RoundTripper
}

func (t userAgentTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	rt := t.base
	if rt == nil {
		return nil, errors.New("transport: no Transport specified")
	}
	if t.userAgent == "" {
		return rt.RoundTrip(req)
	}
	newReq := *req
	newReq.Header = make(http.Header)
	for k, vv := range req.Header {
		newReq.Header[k] = vv
	}
	// TODO(cbro): append to existing User-Agent header?
	newReq.Header["User-Agent"] = []string{t.userAgent}
	return rt.RoundTrip(&newReq)
}

// Set at init time by dial_appengine.go. If nil, we're not on App Engine.
var appengineDialerHook func(context.Context) grpc.DialOption
var appengineUrlfetchHook func(context.Context) http.RoundTripper

// baseTransport returns the base HTTP transport.
// On App Engine, this is urlfetch.Transport, otherwise it's http.DefaultTransport.
func baseTransport(ctx context.Context) http.RoundTripper {
	if appengineUrlfetchHook != nil {
		return appengineUrlfetchHook(ctx)
	}
	return http.DefaultTransport
}

// DialGRPC returns a GRPC connection for use communicating with a Google cloud
// service, configured with the given ClientOptions.
func DialGRPC(ctx context.Context, opts ...option.ClientOption) (*grpc.ClientConn, error) {
	var o internal.DialSettings
	for _, opt := range opts {
		opt.Apply(&o)
	}
	if o.HTTPClient != nil {
		return nil, errors.New("unsupported HTTP client specified")
	}
	if o.GRPCConn != nil {
		return o.GRPCConn, nil
	}
	if o.ServiceAccountJSONFilename != "" {
		ts, err := serviceAcctTokenSource(ctx, o.ServiceAccountJSONFilename, o.Scopes...)
		if err != nil {
			return nil, err
		}
		o.TokenSource = ts
	}
	if o.TokenSource == nil {
		var err error
		o.TokenSource, err = google.DefaultTokenSource(ctx, o.Scopes...)
		if err != nil {
			return nil, fmt.Errorf("google.DefaultTokenSource: %v", err)
		}
	}
	grpcOpts := []grpc.DialOption{
		grpc.WithPerRPCCredentials(oauth.TokenSource{o.TokenSource}),
		grpc.WithTransportCredentials(credentials.NewClientTLSFromCert(nil, "")),
	}
	if appengineDialerHook != nil {
		// Use the Socket API on App Engine.
		grpcOpts = append(grpcOpts, appengineDialerHook(ctx))
	}
	grpcOpts = append(grpcOpts, o.GRPCDialOpts...)
	if o.UserAgent != "" {
		grpcOpts = append(grpcOpts, grpc.WithUserAgent(o.UserAgent))
	}
	return grpc.DialContext(ctx, o.Endpoint, grpcOpts...)
}

func serviceAcctTokenSource(ctx context.Context, filename string, scope ...string) (oauth2.TokenSource, error) {
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("cannot read service account file: %v", err)
	}
	cfg, err := google.JWTConfigFromJSON(data, scope...)
	if err != nil {
		return nil, fmt.Errorf("google.JWTConfigFromJSON: %v", err)
	}
	return cfg.TokenSource(ctx), nil
}

// DialGRPCInsecure returns an insecure GRPC connection for use communicating
// with fake or mock Google cloud service implementations, such as emulators.
// The connection is configured with the given ClientOptions.
func DialGRPCInsecure(ctx context.Context, opts ...option.ClientOption) (*grpc.ClientConn, error) {
	var o internal.DialSettings
	for _, opt := range opts {
		opt.Apply(&o)
	}
	if o.HTTPClient != nil {
		return nil, errors.New("unsupported HTTP client specified")
	}
	if o.GRPCConn != nil {
		return o.GRPCConn, nil
	}
	grpcOpts := []grpc.DialOption{grpc.WithInsecure()}
	grpcOpts = append(grpcOpts, o.GRPCDialOpts...)
	if o.UserAgent != "" {
		grpcOpts = append(grpcOpts, grpc.WithUserAgent(o.UserAgent))
	}
	return grpc.DialContext(ctx, o.Endpoint, grpcOpts...)
}

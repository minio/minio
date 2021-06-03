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

package http

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	xhttp "github.com/minio/minio/internal/http"
	"github.com/minio/minio/internal/logger"
)

// Timeout for the webhook http call
const webhookCallTimeout = 5 * time.Second

// Target implements logger.Target and sends the json
// format of a log entry to the configured http endpoint.
// An internal buffer of logs is maintained but when the
// buffer is full, new logs are just ignored and an error
// is returned to the caller.
type Target struct {
	// Channel of log entries
	logCh chan interface{}

	name string
	// HTTP(s) endpoint
	endpoint string
	// Authorization token for `endpoint`
	authToken string
	// User-Agent to be set on each log to `endpoint`
	userAgent string
	logKind   string
	client    http.Client
}

// Endpoint returns the backend endpoint
func (h *Target) Endpoint() string {
	return h.endpoint
}

func (h *Target) String() string {
	return h.name
}

// Validate validate the http target
func (h *Target) Validate() error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*webhookCallTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, h.endpoint, strings.NewReader(`{}`))
	if err != nil {
		return err
	}

	req.Header.Set(xhttp.ContentType, "application/json")

	// Set user-agent to indicate MinIO release
	// version to the configured log endpoint
	req.Header.Set("User-Agent", h.userAgent)

	if h.authToken != "" {
		req.Header.Set("Authorization", h.authToken)
	}

	resp, err := h.client.Do(req)
	if err != nil {
		return err
	}

	// Drain any response.
	xhttp.DrainBody(resp.Body)

	if resp.StatusCode != http.StatusOK {
		switch resp.StatusCode {
		case http.StatusForbidden:
			return fmt.Errorf("%s returned '%s', please check if your auth token is correctly set",
				h.endpoint, resp.Status)
		}
		return fmt.Errorf("%s returned '%s', please check your endpoint configuration",
			h.endpoint, resp.Status)
	}

	return nil
}

func (h *Target) startHTTPLogger() {
	// Create a routine which sends json logs received
	// from an internal channel.
	go func() {
		for entry := range h.logCh {
			logJSON, err := json.Marshal(&entry)
			if err != nil {
				continue
			}

			ctx, cancel := context.WithTimeout(context.Background(), webhookCallTimeout)
			req, err := http.NewRequestWithContext(ctx, http.MethodPost,
				h.endpoint, bytes.NewReader(logJSON))
			if err != nil {
				cancel()
				continue
			}
			req.Header.Set(xhttp.ContentType, "application/json")

			// Set user-agent to indicate MinIO release
			// version to the configured log endpoint
			req.Header.Set("User-Agent", h.userAgent)

			if h.authToken != "" {
				req.Header.Set("Authorization", h.authToken)
			}

			resp, err := h.client.Do(req)
			cancel()
			if err != nil {
				logger.LogOnceIf(ctx, fmt.Errorf("%s returned '%w', please check your endpoint configuration",
					h.endpoint, err), h.endpoint)
				continue
			}

			// Drain any response.
			xhttp.DrainBody(resp.Body)

			if resp.StatusCode != http.StatusOK {
				switch resp.StatusCode {
				case http.StatusForbidden:
					logger.LogOnceIf(ctx, fmt.Errorf("%s returned '%s', please check if your auth token is correctly set",
						h.endpoint, resp.Status), h.endpoint)
				default:
					logger.LogOnceIf(ctx, fmt.Errorf("%s returned '%s', please check your endpoint configuration",
						h.endpoint, resp.Status), h.endpoint)
				}
			}
		}
	}()
}

// Option is a function type that accepts a pointer Target
type Option func(*Target)

// WithTargetName target name
func WithTargetName(name string) Option {
	return func(t *Target) {
		t.name = name
	}
}

// WithEndpoint adds a new endpoint
func WithEndpoint(endpoint string) Option {
	return func(t *Target) {
		t.endpoint = endpoint
	}
}

// WithLogKind adds a log type for this target
func WithLogKind(logKind string) Option {
	return func(t *Target) {
		t.logKind = strings.ToUpper(logKind)
	}
}

// WithUserAgent adds a custom user-agent sent to the target.
func WithUserAgent(userAgent string) Option {
	return func(t *Target) {
		t.userAgent = userAgent
	}
}

// WithAuthToken adds a new authorization header to be sent to target.
func WithAuthToken(authToken string) Option {
	return func(t *Target) {
		t.authToken = authToken
	}
}

// WithTransport adds a custom transport with custom timeouts and tuning.
func WithTransport(transport *http.Transport) Option {
	return func(t *Target) {
		t.client = http.Client{
			Transport: transport,
		}
	}
}

// New initializes a new logger target which
// sends log over http to the specified endpoint
func New(opts ...Option) *Target {
	h := &Target{
		logCh: make(chan interface{}, 10000),
	}

	// Loop through each option
	for _, opt := range opts {
		// Call the option giving the instantiated
		// *Target as the argument
		opt(h)
	}

	h.startHTTPLogger()
	return h
}

// Send log message 'e' to http target.
func (h *Target) Send(entry interface{}, errKind string) error {
	if h.logKind != errKind && h.logKind != "ALL" {
		return nil
	}

	select {
	case h.logCh <- entry:
	default:
		// log channel is full, do not wait and return
		// an error immediately to the caller
		return errors.New("log buffer full")
	}

	return nil
}

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
	"sync"
	"sync/atomic"
	"time"

	xhttp "github.com/minio/minio/internal/http"
	"github.com/minio/minio/internal/logger/target/types"
)

const (
	// Timeout for the webhook http call
	webhookCallTimeout = 5 * time.Second

	// maxWorkers is the maximum number of concurrent operations.
	maxWorkers = 8
)

// Config http logger target
type Config struct {
	Enabled    bool              `json:"enabled"`
	Name       string            `json:"name"`
	UserAgent  string            `json:"userAgent"`
	Endpoint   string            `json:"endpoint"`
	AuthToken  string            `json:"authToken"`
	ClientCert string            `json:"clientCert"`
	ClientKey  string            `json:"clientKey"`
	QueueSize  int               `json:"queueSize"`
	Transport  http.RoundTripper `json:"-"`

	// Custom logger
	LogOnce func(ctx context.Context, err error, id string, errKind ...interface{}) `json:"-"`
}

// Target implements logger.Target and sends the json
// format of a log entry to the configured http endpoint.
// An internal buffer of logs is maintained but when the
// buffer is full, new logs are just ignored and an error
// is returned to the caller.
type Target struct {
	totalMessages  int64
	failedMessages int64

	// Worker control
	workers       int64
	workerStartMu sync.Mutex
	lastStarted   time.Time

	wg     sync.WaitGroup
	doneCh chan struct{}

	// Channel of log entries
	logCh chan interface{}

	// is the target online?
	online bool

	config Config
	client *http.Client
}

// Endpoint returns the backend endpoint
func (h *Target) Endpoint() string {
	return h.config.Endpoint
}

func (h *Target) String() string {
	return h.config.Name
}

// IsOnline returns true if the initialization was successful
func (h *Target) IsOnline() bool {
	return h.online
}

// Stats returns the target statistics.
func (h *Target) Stats() types.TargetStats {
	return types.TargetStats{
		TotalMessages:  atomic.LoadInt64(&h.totalMessages),
		FailedMessages: atomic.LoadInt64(&h.failedMessages),
		QueueLength:    len(h.logCh),
	}
}

// Init validate and initialize the http target
func (h *Target) Init() error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*webhookCallTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, h.config.Endpoint, strings.NewReader(`{}`))
	if err != nil {
		return err
	}

	req.Header.Set(xhttp.ContentType, "application/json")

	// Set user-agent to indicate MinIO release
	// version to the configured log endpoint
	req.Header.Set("User-Agent", h.config.UserAgent)

	if h.config.AuthToken != "" {
		req.Header.Set("Authorization", h.config.AuthToken)
	}

	client := http.Client{Transport: h.config.Transport}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	h.client = &client

	// Drain any response.
	xhttp.DrainBody(resp.Body)

	if !acceptedResponseStatusCode(resp.StatusCode) {
		switch resp.StatusCode {
		case http.StatusForbidden:
			return fmt.Errorf("%s returned '%s', please check if your auth token is correctly set",
				h.config.Endpoint, resp.Status)
		}
		return fmt.Errorf("%s returned '%s', please check your endpoint configuration",
			h.config.Endpoint, resp.Status)
	}

	h.lastStarted = time.Now()
	h.online = true
	atomic.AddInt64(&h.workers, 1)
	go h.startHTTPLogger()
	return nil
}

// Accepted HTTP Status Codes
var acceptedStatusCodeMap = map[int]bool{http.StatusOK: true, http.StatusCreated: true, http.StatusAccepted: true, http.StatusNoContent: true}

func acceptedResponseStatusCode(code int) bool {
	return acceptedStatusCodeMap[code]
}

func (h *Target) logEntry(entry interface{}) {
	logJSON, err := json.Marshal(&entry)
	if err != nil {
		atomic.AddInt64(&h.failedMessages, 1)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), webhookCallTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		h.config.Endpoint, bytes.NewReader(logJSON))
	if err != nil {
		h.config.LogOnce(ctx, fmt.Errorf("%s returned '%w', please check your endpoint configuration", h.config.Endpoint, err), h.config.Endpoint)
		atomic.AddInt64(&h.failedMessages, 1)
		return
	}
	req.Header.Set(xhttp.ContentType, "application/json")
	req.Header.Set(xhttp.MinIOVersion, xhttp.GlobalMinIOVersion)
	req.Header.Set(xhttp.MinioDeploymentID, xhttp.GlobalDeploymentID)

	// Set user-agent to indicate MinIO release
	// version to the configured log endpoint
	req.Header.Set("User-Agent", h.config.UserAgent)

	if h.config.AuthToken != "" {
		req.Header.Set("Authorization", h.config.AuthToken)
	}

	resp, err := h.client.Do(req)
	if err != nil {
		atomic.AddInt64(&h.failedMessages, 1)
		h.config.LogOnce(ctx, fmt.Errorf("%s returned '%w', please check your endpoint configuration", h.config.Endpoint, err), h.config.Endpoint)
		return
	}

	// Drain any response.
	xhttp.DrainBody(resp.Body)

	if !acceptedResponseStatusCode(resp.StatusCode) {
		atomic.AddInt64(&h.failedMessages, 1)
		switch resp.StatusCode {
		case http.StatusForbidden:
			h.config.LogOnce(ctx, fmt.Errorf("%s returned '%s', please check if your auth token is correctly set", h.config.Endpoint, resp.Status), h.config.Endpoint)
		default:
			h.config.LogOnce(ctx, fmt.Errorf("%s returned '%s', please check your endpoint configuration", h.config.Endpoint, resp.Status), h.config.Endpoint)
		}
	}
}

func (h *Target) startHTTPLogger() {
	// Create a routine which sends json logs received
	// from an internal channel.
	h.wg.Add(1)
	go func() {
		defer func() {
			h.wg.Done()
			atomic.AddInt64(&h.workers, -1)
		}()

		for {
			select {
			case entry := <-h.logCh:
				atomic.AddInt64(&h.totalMessages, 1)
				h.logEntry(entry)
			case <-h.doneCh:
				return
			}
		}
	}()
}

// New initializes a new logger target which
// sends log over http to the specified endpoint
func New(config Config) *Target {
	h := &Target{
		logCh:  make(chan interface{}, config.QueueSize),
		doneCh: make(chan struct{}),
		config: config,
		online: false,
	}

	return h
}

// Send log message 'e' to http target.
func (h *Target) Send(entry interface{}) error {
	if !h.online {
		return nil
	}

	select {
	case <-h.doneCh:
		return nil
	default:
	}

	select {
	case <-h.doneCh:
	case h.logCh <- entry:
	default:
		nWorkers := atomic.LoadInt64(&h.workers)
		if nWorkers < maxWorkers {
			// Only have one try to start at the same time.
			h.workerStartMu.Lock()
			defer h.workerStartMu.Unlock()
			// Start one max every second.
			if time.Since(h.lastStarted) > time.Second {
				if atomic.CompareAndSwapInt64(&h.workers, nWorkers, nWorkers+1) {
					// Start another logger.
					h.lastStarted = time.Now()
					go h.startHTTPLogger()
				}
			}
			// Block to send
			select {
			case <-h.doneCh:
			case h.logCh <- entry:
			}
			return nil
		}
		// log channel is full, do not wait and return
		// an error immediately to the caller
		atomic.AddInt64(&h.totalMessages, 1)
		atomic.AddInt64(&h.failedMessages, 1)
		return errors.New("log buffer full")
	}

	return nil
}

// Cancel - cancels the target
func (h *Target) Cancel() {
	close(h.doneCh)
	close(h.logCh)
	h.wg.Wait()
}

// Type - returns type of the target
func (h *Target) Type() types.TargetType {
	return types.TargetHTTP
}

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
	"net/url"
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
	maxWorkers = 16
)

const (
	statusOffline = iota
	statusOnline
	statusClosed
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
	Proxy      string            `json:"string"`
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
	status         int32

	// Worker control
	workers       int64
	workerStartMu sync.Mutex
	lastStarted   time.Time

	wg sync.WaitGroup

	// Channel of log entries.
	// Reading logCh must hold read lock on logChMu (to avoid read race)
	// Sending a value on logCh must hold read lock on logChMu (to avoid closing)
	logCh   chan interface{}
	logChMu sync.RWMutex

	// If the first init fails, this starts a goroutine that
	// will attempt to establish the connection.
	revive sync.Once

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
	return atomic.LoadInt32(&h.status) == statusOnline
}

// Stats returns the target statistics.
func (h *Target) Stats() types.TargetStats {
	h.logChMu.RLock()
	logCh := h.logCh
	h.logChMu.RUnlock()
	stats := types.TargetStats{
		TotalMessages:  atomic.LoadInt64(&h.totalMessages),
		FailedMessages: atomic.LoadInt64(&h.failedMessages),
		QueueLength:    len(logCh),
	}

	return stats
}

// Init validate and initialize the http target
func (h *Target) Init() (err error) {
	switch atomic.LoadInt32(&h.status) {
	case statusOnline:
		return nil
	case statusClosed:
		return errors.New("target is closed")
	}

	// This will check if we can reach the remote.
	checkAlive := func() error {
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

		resp, err := h.client.Do(req)
		if err != nil {
			return err
		}
		// Drain any response.
		xhttp.DrainBody(resp.Body)

		if !acceptedResponseStatusCode(resp.StatusCode) {
			if resp.StatusCode == http.StatusForbidden {
				return fmt.Errorf("%s returned '%s', please check if your auth token is correctly set",
					h.config.Endpoint, resp.Status)
			}
			return fmt.Errorf("%s returned '%s', please check your endpoint configuration",
				h.config.Endpoint, resp.Status)
		}
		return nil
	}

	err = checkAlive()
	if err != nil {
		// Start a goroutine that will continue to check if we can reach
		h.revive.Do(func() {
			go func() {
				t := time.NewTicker(time.Second)
				defer t.Stop()
				for range t.C {
					if atomic.LoadInt32(&h.status) != statusOffline {
						return
					}
					if err := checkAlive(); err == nil {
						// We are online.
						if atomic.CompareAndSwapInt32(&h.status, statusOffline, statusOnline) {
							h.workerStartMu.Lock()
							h.lastStarted = time.Now()
							h.workerStartMu.Unlock()
							atomic.AddInt64(&h.workers, 1)
							go h.startHTTPLogger()
						}
						return
					}
				}
			}()
		})
		return err
	}

	if atomic.CompareAndSwapInt32(&h.status, statusOffline, statusOnline) {
		h.workerStartMu.Lock()
		h.lastStarted = time.Now()
		h.workerStartMu.Unlock()
		go h.startHTTPLogger()
	}
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
	h.logChMu.RLock()
	logCh := h.logCh
	if logCh != nil {
		// We are not allowed to add when logCh is nil
		h.wg.Add(1)
		defer h.wg.Done()
	}
	h.logChMu.RUnlock()

	defer atomic.AddInt64(&h.workers, -1)

	if logCh == nil {
		return
	}
	// Send messages until channel is closed.
	for entry := range logCh {
		atomic.AddInt64(&h.totalMessages, 1)
		h.logEntry(entry)
	}
}

// New initializes a new logger target which
// sends log over http to the specified endpoint
func New(config Config) *Target {
	h := &Target{
		logCh:  make(chan interface{}, config.QueueSize),
		config: config,
		status: statusOffline,
	}

	// If proxy available, set the same
	if h.config.Proxy != "" {
		proxyURL, _ := url.Parse(h.config.Proxy)
		transport := h.config.Transport
		ctransport := transport.(*http.Transport).Clone()
		ctransport.Proxy = http.ProxyURL(proxyURL)
		h.config.Transport = ctransport
	}
	h.client = &http.Client{Transport: h.config.Transport}

	return h
}

// Send log message 'e' to http target.
// If servers are offline messages are queued until queue is full.
// If Cancel has been called the message is ignored.
func (h *Target) Send(entry interface{}) error {
	if atomic.LoadInt32(&h.status) == statusClosed {
		return nil
	}
	h.logChMu.RLock()
	defer h.logChMu.RUnlock()
	if h.logCh == nil {
		// We are closing...
		return nil
	}
	select {
	case h.logCh <- entry:
	default:
		// Drop messages until we are online.
		if !h.IsOnline() {
			return errors.New("log buffer full and remote offline")
		}
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
			h.logCh <- entry
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

// Cancel - cancels the target.
// All queued messages are flushed and the function returns afterwards.
// All messages sent to the target after this function has been called will be dropped.
func (h *Target) Cancel() {
	atomic.StoreInt32(&h.status, statusClosed)

	// Set logch to nil and close it.
	// This will block all Send operations,
	// and finish the existing ones.
	// All future ones will be discarded.
	h.logChMu.Lock()
	close(h.logCh)
	h.logCh = nil
	h.logChMu.Unlock()

	// Wait for messages to be sent...
	h.wg.Wait()
}

// Type - returns type of the target
func (h *Target) Type() types.TargetType {
	return types.TargetHTTP
}

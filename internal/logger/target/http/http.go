// Copyright (c) 2015-2023 MinIO, Inc.
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
	"math"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	xhttp "github.com/minio/minio/internal/http"
	"github.com/minio/minio/internal/logger/target/types"
	"github.com/minio/minio/internal/once"
	"github.com/minio/minio/internal/store"
	xnet "github.com/minio/pkg/v2/net"
)

const (
	// Timeout for the webhook http call
	webhookCallTimeout = 3 * time.Second

	// maxWorkers is the maximum number of concurrent http loggers
	maxWorkers = 16

	// the suffix for the configured queue dir where the logs will be persisted.
	httpLoggerExtension = ".http.log"
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
	Endpoint   *xnet.URL         `json:"endpoint"`
	AuthToken  string            `json:"authToken"`
	ClientCert string            `json:"clientCert"`
	ClientKey  string            `json:"clientKey"`
	QueueSize  int               `json:"queueSize"`
	QueueDir   string            `json:"queueDir"`
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

	// store to persist and replay the logs to the target
	// to avoid missing events when the target is down.
	store          store.Store[interface{}]
	storeCtxCancel context.CancelFunc

	initQueueStoreOnce once.Init

	config Config
	client *http.Client
}

// Name returns the name of the target
func (h *Target) Name() string {
	return "minio-http-" + h.config.Name
}

// Endpoint returns the backend endpoint
func (h *Target) Endpoint() string {
	return h.config.Endpoint.String()
}

func (h *Target) String() string {
	return h.config.Name
}

// IsOnline returns true if the target is reachable using a cached value
func (h *Target) IsOnline(ctx context.Context) bool {
	return atomic.LoadInt32(&h.status) == statusOnline
}

// ping returns true if the target is reachable.
func (h *Target) ping(ctx context.Context) bool {
	if err := h.send(ctx, []byte(`{}`), webhookCallTimeout); err != nil {
		return !xnet.IsNetworkOrHostDown(err, false) && !xnet.IsConnRefusedErr(err)
	}
	go h.startHTTPLogger(ctx)
	return true
}

// Stats returns the target statistics.
func (h *Target) Stats() types.TargetStats {
	h.logChMu.RLock()
	queueLength := len(h.logCh)
	h.logChMu.RUnlock()
	stats := types.TargetStats{
		TotalMessages:  atomic.LoadInt64(&h.totalMessages),
		FailedMessages: atomic.LoadInt64(&h.failedMessages),
		QueueLength:    queueLength,
	}

	return stats
}

// Init validate and initialize the http target
func (h *Target) Init(ctx context.Context) (err error) {
	if h.config.QueueDir != "" {
		return h.initQueueStoreOnce.DoWithContext(ctx, h.initQueueStore)
	}
	return h.init(ctx)
}

func (h *Target) initQueueStore(ctx context.Context) (err error) {
	var queueStore store.Store[interface{}]
	queueDir := filepath.Join(h.config.QueueDir, h.Name())
	queueStore = store.NewQueueStore[interface{}](queueDir, uint64(h.config.QueueSize), httpLoggerExtension)
	if err = queueStore.Open(); err != nil {
		return fmt.Errorf("unable to initialize the queue store of %s webhook: %w", h.Name(), err)
	}
	ctx, cancel := context.WithCancel(ctx)
	h.store = queueStore
	h.storeCtxCancel = cancel
	store.StreamItems(h.store, h, ctx.Done(), h.config.LogOnce)
	return
}

func (h *Target) init(ctx context.Context) (err error) {
	switch atomic.LoadInt32(&h.status) {
	case statusOnline:
		return nil
	case statusClosed:
		return errors.New("target is closed")
	}

	if !h.ping(ctx) {
		// Start a goroutine that will continue to check if we can reach
		h.revive.Do(func() {
			go func() {
				// Avoid stamping herd, add jitter.
				t := time.NewTicker(time.Second + time.Duration(rand.Int63n(int64(5*time.Second))))
				defer t.Stop()

				for range t.C {
					if atomic.LoadInt32(&h.status) != statusOffline {
						return
					}
					if h.ping(ctx) {
						// We are online.
						if atomic.CompareAndSwapInt32(&h.status, statusOffline, statusOnline) {
							h.workerStartMu.Lock()
							h.lastStarted = time.Now()
							h.workerStartMu.Unlock()
							atomic.AddInt64(&h.workers, 1)
							go h.startHTTPLogger(ctx)
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
		atomic.AddInt64(&h.workers, 1)
		go h.startHTTPLogger(ctx)
	}
	return nil
}

func (h *Target) send(ctx context.Context, payload []byte, timeout time.Duration) (err error) {
	defer func() {
		if err != nil {
			atomic.StoreInt32(&h.status, statusOffline)
		} else {
			atomic.StoreInt32(&h.status, statusOnline)
		}
	}()

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		h.Endpoint(), bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("invalid configuration for '%s'; %v", h.Endpoint(), err)
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
		return fmt.Errorf("%s returned '%w', please check your endpoint configuration", h.Endpoint(), err)
	}

	// Drain any response.
	xhttp.DrainBody(resp.Body)

	switch resp.StatusCode {
	case http.StatusOK, http.StatusCreated, http.StatusAccepted, http.StatusNoContent:
		// accepted HTTP status codes.
		return nil
	case http.StatusForbidden:
		return fmt.Errorf("%s returned '%s', please check if your auth token is correctly set", h.Endpoint(), resp.Status)
	default:
		return fmt.Errorf("%s returned '%s', please check your endpoint configuration", h.Endpoint(), resp.Status)
	}
}

func (h *Target) logEntry(ctx context.Context, entry interface{}) {
	logJSON, err := json.Marshal(&entry)
	if err != nil {
		atomic.AddInt64(&h.failedMessages, 1)
		return
	}

	const maxTries = 3
	tries := 0
	for tries < maxTries {
		if atomic.LoadInt32(&h.status) == statusClosed {
			// Don't retry when closing...
			return
		}
		// sleep = (tries+2) ^ 2 milliseconds.
		sleep := time.Duration(math.Pow(float64(tries+2), 2)) * time.Millisecond
		if sleep > time.Second {
			sleep = time.Second
		}
		time.Sleep(sleep)
		tries++
		err := h.send(ctx, logJSON, webhookCallTimeout)
		if err == nil {
			return
		}
		h.config.LogOnce(ctx, err, h.Endpoint())
	}
	if tries == maxTries {
		// Even with multiple retries, count failed messages as only one.
		atomic.AddInt64(&h.failedMessages, 1)
	}
}

func (h *Target) startHTTPLogger(ctx context.Context) {
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
		h.logEntry(ctx, entry)
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

// SendFromStore - reads the log from store and sends it to webhook.
func (h *Target) SendFromStore(key string) (err error) {
	var eventData interface{}
	eventData, err = h.store.Get(key)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	atomic.AddInt64(&h.totalMessages, 1)
	logJSON, err := json.Marshal(&eventData)
	if err != nil {
		atomic.AddInt64(&h.failedMessages, 1)
		return
	}
	if err := h.send(context.Background(), logJSON, webhookCallTimeout); err != nil {
		atomic.AddInt64(&h.failedMessages, 1)
		if xnet.IsNetworkOrHostDown(err, true) {
			return store.ErrNotConnected
		}
		return err
	}
	// Delete the event from store.
	return h.store.Del(key)
}

// Send the log message 'entry' to the http target.
// Messages are queued in the disk if the store is enabled
// If Cancel has been called the message is ignored.
func (h *Target) Send(ctx context.Context, entry interface{}) error {
	if atomic.LoadInt32(&h.status) == statusClosed {
		return nil
	}
	if h.store != nil {
		// save the entry to the queue store which will be replayed to the target.
		return h.store.Put(entry)
	}
	h.logChMu.RLock()
	defer h.logChMu.RUnlock()
	if h.logCh == nil {
		// We are closing...
		return nil
	}

	select {
	case h.logCh <- entry:
	case <-ctx.Done():
		// return error only for context timedout.
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return ctx.Err()
		}
		return nil
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
					go h.startHTTPLogger(ctx)
				}
			}
			h.logCh <- entry
			return nil
		}
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

	// If queuestore is configured, cancel it's context to
	// stop the replay go-routine.
	if h.store != nil {
		h.storeCtxCancel()
	}

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

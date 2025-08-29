// Copyright (c) 2015-2024 MinIO, Inc.
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
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	jsoniter "github.com/json-iterator/go"
	xhttp "github.com/minio/minio/internal/http"
	xioutil "github.com/minio/minio/internal/ioutil"
	types "github.com/minio/minio/internal/logger/target/loggertypes"
	"github.com/minio/minio/internal/once"
	"github.com/minio/minio/internal/store"
	xnet "github.com/minio/pkg/v3/net"
	"github.com/valyala/bytebufferpool"
)

const (

	// maxWorkers is the maximum number of concurrent http loggers
	maxWorkers = 16

	// maxWorkers is the maximum number of concurrent batch http loggers
	maxWorkersWithBatchEvents = 4

	// the suffix for the configured queue dir where the logs will be persisted.
	httpLoggerExtension = ".http.log"
)

const (
	statusOffline = iota
	statusOnline
	statusClosed
)

var (
	logChBuffers = make(map[string]chan any)
	logChLock    = sync.Mutex{}
)

// Config http logger target
type Config struct {
	Enabled     bool              `json:"enabled"`
	Name        string            `json:"name"`
	UserAgent   string            `json:"userAgent"`
	Endpoint    *xnet.URL         `json:"endpoint"`
	AuthToken   string            `json:"authToken"`
	ClientCert  string            `json:"clientCert"`
	ClientKey   string            `json:"clientKey"`
	BatchSize   int               `json:"batchSize"`
	QueueSize   int               `json:"queueSize"`
	QueueDir    string            `json:"queueDir"`
	MaxRetry    int               `json:"maxRetry"`
	RetryIntvl  time.Duration     `json:"retryInterval"`
	Proxy       string            `json:"string"`
	Transport   http.RoundTripper `json:"-"`
	HTTPTimeout time.Duration     `json:"httpTimeout"`

	// Custom logger
	LogOnceIf func(ctx context.Context, err error, id string, errKind ...any) `json:"-"`
}

// Target implements logger.Target and sends the json
// format of a log entry to the configured http endpoint.
// An internal buffer of logs is maintained but when the
// buffer is full, new logs are just ignored and an error
// is returned to the caller.
type Target struct {
	totalMessages  atomic.Int64
	failedMessages atomic.Int64
	status         atomic.Int32

	// Worker control
	workers    atomic.Int64
	maxWorkers int64

	// workerStartMu sync.Mutex
	lastStarted time.Time

	wg sync.WaitGroup

	// Channel of log entries.
	// Reading logCh must hold read lock on logChMu (to avoid read race)
	// Sending a value on logCh must hold read lock on logChMu (to avoid closing)
	logCh   chan any
	logChMu sync.RWMutex

	// If this webhook is being re-configured we will
	// assign the new webhook target to this field.
	// The Send() method will then re-direct entries
	// to the new target when the current one
	// has been set to status "statusClosed".
	// Once the glogal target slice has been migrated
	// the current target will stop receiving entries.
	migrateTarget *Target

	// Number of events per HTTP send to webhook target
	// this is ideally useful only if your endpoint can
	// support reading multiple events on a stream for example
	// like : Splunk HTTP Event collector, if you are unsure
	// set this to '1'.
	batchSize   int
	payloadType string

	// store to persist and replay the logs to the target
	// to avoid missing events when the target is down.
	store          store.Store[any]
	storeCtxCancel context.CancelFunc

	initQueueOnce once.Init

	config      Config
	client      *http.Client
	httpTimeout time.Duration
}

// Name returns the name of the target
func (h *Target) Name() string {
	return "minio-http-" + h.config.Name
}

// Type - returns type of the target
func (h *Target) Type() types.TargetType {
	return types.TargetHTTP
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
	return h.status.Load() == statusOnline
}

// Stats returns the target statistics.
func (h *Target) Stats() types.TargetStats {
	h.logChMu.RLock()
	queueLength := len(h.logCh)
	h.logChMu.RUnlock()
	stats := types.TargetStats{
		TotalMessages:  h.totalMessages.Load(),
		FailedMessages: h.failedMessages.Load(),
		QueueLength:    queueLength,
	}

	return stats
}

// AssignMigrateTarget assigns a target
// which will eventually replace the current target.
func (h *Target) AssignMigrateTarget(migrateTgt *Target) {
	h.migrateTarget = migrateTgt
}

// Init validate and initialize the http target
func (h *Target) Init(ctx context.Context) (err error) {
	if h.config.QueueDir != "" {
		return h.initQueueOnce.DoWithContext(ctx, h.initDiskStore)
	}
	return h.initQueueOnce.DoWithContext(ctx, h.initMemoryStore)
}

func (h *Target) initDiskStore(ctx context.Context) (err error) {
	ctx, cancel := context.WithCancel(ctx)
	h.storeCtxCancel = cancel
	h.lastStarted = time.Now()
	go h.startQueueProcessor(ctx, true)

	queueStore := store.NewQueueStore[any](
		filepath.Join(h.config.QueueDir, h.Name()),
		uint64(h.config.QueueSize),
		httpLoggerExtension,
	)

	if err := queueStore.Open(); err != nil {
		return fmt.Errorf("unable to initialize the queue store of %s webhook: %w", h.Name(), err)
	}

	h.store = queueStore
	store.StreamItems(h.store, h, ctx.Done(), h.config.LogOnceIf)

	return nil
}

func (h *Target) initMemoryStore(ctx context.Context) (err error) {
	ctx, cancel := context.WithCancel(ctx)
	h.storeCtxCancel = cancel
	h.lastStarted = time.Now()
	go h.startQueueProcessor(ctx, true)
	return nil
}

func (h *Target) send(ctx context.Context, payload []byte, payloadCount int, payloadType string, timeout time.Duration) (err error) {
	defer func() {
		if err != nil {
			if xnet.IsNetworkOrHostDown(err, false) {
				h.status.Store(statusOffline)
			}
			h.failedMessages.Add(int64(payloadCount))
		} else {
			h.status.Store(statusOnline)
		}
	}()

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		h.Endpoint(), bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("invalid configuration for '%s'; %v", h.Endpoint(), err)
	}
	if payloadType != "" {
		req.Header.Set(xhttp.ContentType, payloadType)
	}
	req.Header.Set(xhttp.WebhookEventPayloadCount, strconv.Itoa(payloadCount))
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

	if resp.StatusCode >= 200 && resp.StatusCode <= 299 {
		// accepted HTTP status codes.
		return nil
	} else if resp.StatusCode == http.StatusForbidden {
		return fmt.Errorf("%s returned '%s', please check if your auth token is correctly set", h.Endpoint(), resp.Status)
	}
	return fmt.Errorf("%s returned '%s', please check your endpoint configuration", h.Endpoint(), resp.Status)
}

func (h *Target) startQueueProcessor(ctx context.Context, mainWorker bool) {
	h.logChMu.RLock()
	if h.logCh == nil {
		h.logChMu.RUnlock()
		return
	}
	h.logChMu.RUnlock()

	h.workers.Add(1)
	defer h.workers.Add(-1)

	h.wg.Add(1)
	defer h.wg.Done()

	entries := make([]any, 0)
	name := h.Name()

	defer func() {
		// re-load the global buffer pointer
		// in case it was modified by a new target.
		logChLock.Lock()
		currentGlobalBuffer, ok := logChBuffers[name]
		logChLock.Unlock()
		if !ok {
			return
		}

		for _, v := range entries {
			select {
			case currentGlobalBuffer <- v:
			default:
			}
		}

		if mainWorker {
		drain:
			for {
				select {
				case v, ok := <-h.logCh:
					if !ok {
						break drain
					}

					currentGlobalBuffer <- v
				default:
					break drain
				}
			}
		}
	}()

	lastBatchProcess := time.Now()

	buf := bytebufferpool.Get()
	enc := jsoniter.ConfigCompatibleWithStandardLibrary.NewEncoder(buf)
	defer bytebufferpool.Put(buf)

	isDirQueue := h.config.QueueDir != ""

	// globalBuffer is always created or adjusted
	// before this method is launched.
	logChLock.Lock()
	globalBuffer := logChBuffers[name]
	logChLock.Unlock()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	var count int
	for {
		var (
			ok    bool
			entry any
		)

		if count < h.batchSize {
			tickered := false
			select {
			case <-ticker.C:
				tickered = true
			case entry = <-globalBuffer:
			case entry, ok = <-h.logCh:
				if !ok {
					return
				}
			case <-ctx.Done():
				return
			}

			if !tickered {
				h.totalMessages.Add(1)
				if !isDirQueue {
					if err := enc.Encode(&entry); err != nil {
						h.config.LogOnceIf(
							ctx,
							fmt.Errorf("unable to encode webhook log entry, err  '%w' entry: %v\n", err, entry),
							h.Name(),
						)
						h.failedMessages.Add(1)
						continue
					}
				} else {
					entries = append(entries, entry)
				}
				count++
			}

			if len(h.logCh) > 0 || len(globalBuffer) > 0 || count == 0 {
				// there is something in the log queue
				// process it first, even if we tickered
				// first, or we have not received any events
				// yet, still wait on it.
				continue
			}

			// If we are doing batching, we should wait
			// at least for a second, before sending.
			// Even if there is nothing in the queue.
			if h.batchSize > 1 && time.Since(lastBatchProcess) < time.Second {
				continue
			}
		}

		// if we have reached the count send at once
		// or we have crossed last second before batch was sent, send at once
		lastBatchProcess = time.Now()

		var retries int
		retryIntvl := h.config.RetryIntvl
		if retryIntvl <= 0 {
			retryIntvl = 3 * time.Second
		}

		maxRetries := h.config.MaxRetry

	retry:
		// If the channel reaches above half capacity
		// we spawn more workers. The workers spawned
		// from this main worker routine will exit
		// once the channel drops below half capacity
		// and when it's been at least 30 seconds since
		// we launched a new worker.
		if mainWorker && len(h.logCh) > cap(h.logCh)/2 {
			nWorkers := h.workers.Load()
			if nWorkers < h.maxWorkers {
				if time.Since(h.lastStarted).Milliseconds() > 10 {
					h.lastStarted = time.Now()
					go h.startQueueProcessor(ctx, false)
				}
			}
		}

		var err error
		if !isDirQueue {
			err = h.send(ctx, buf.Bytes(), count, h.payloadType, h.httpTimeout)
		} else {
			_, err = h.store.PutMultiple(entries)
		}

		if err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}

			h.config.LogOnceIf(
				context.Background(),
				fmt.Errorf("unable to send audit/log entry(s) to '%s' err '%w': %d", name, err, count),
				name,
			)

			time.Sleep(retryIntvl)
			if maxRetries == 0 {
				goto retry
			}
			retries++
			if retries <= maxRetries {
				goto retry
			}
		}

		entries = make([]any, 0)
		count = 0
		if !isDirQueue {
			buf.Reset()
		}

		if !mainWorker && len(h.logCh) < cap(h.logCh)/2 {
			if time.Since(h.lastStarted).Seconds() > 30 {
				return
			}
		}
	}
}

// CreateOrAdjustGlobalBuffer will create or adjust the global log entry buffers
// which are used to migrate log entries between old and new targets.
func CreateOrAdjustGlobalBuffer(currentTgt *Target, newTgt *Target) {
	logChLock.Lock()
	defer logChLock.Unlock()

	requiredCap := currentTgt.config.QueueSize + (currentTgt.config.BatchSize * int(currentTgt.maxWorkers))
	currentCap := 0
	name := newTgt.Name()

	currentBuff, ok := logChBuffers[name]
	if !ok {
		logChBuffers[name] = make(chan any, requiredCap)
		currentCap = requiredCap
	} else {
		currentCap = cap(currentBuff)
		requiredCap += len(currentBuff)
	}

	if requiredCap > currentCap {
		logChBuffers[name] = make(chan any, requiredCap)

		if len(currentBuff) > 0 {
		drain:
			for {
				select {
				case v, ok := <-currentBuff:
					if !ok {
						break drain
					}
					logChBuffers[newTgt.Name()] <- v
				default:
					break drain
				}
			}
		}
	}
}

// New initializes a new logger target which
// sends log over http to the specified endpoint
func New(config Config) (*Target, error) {
	maxWorkers := maxWorkers
	if config.BatchSize > 100 {
		maxWorkers = maxWorkersWithBatchEvents
	} else if config.BatchSize <= 0 {
		config.BatchSize = 1
	}

	h := &Target{
		logCh:       make(chan any, config.QueueSize),
		config:      config,
		batchSize:   config.BatchSize,
		maxWorkers:  int64(maxWorkers),
		httpTimeout: config.HTTPTimeout,
	}
	h.status.Store(statusOffline)

	if config.BatchSize > 1 {
		h.payloadType = ""
	} else {
		h.payloadType = "application/json"
	}

	// If proxy available, set the same
	if h.config.Proxy != "" {
		proxyURL, _ := url.Parse(h.config.Proxy)
		transport := h.config.Transport
		if tr, ok := transport.(*http.Transport); ok {
			ctransport := tr.Clone()
			ctransport.Proxy = http.ProxyURL(proxyURL)
			h.config.Transport = ctransport
		}
	}

	h.client = &http.Client{Transport: h.config.Transport}
	return h, nil
}

// SendFromStore - reads the log from store and sends it to webhook.
func (h *Target) SendFromStore(key store.Key) (err error) {
	var eventData []byte
	eventData, err = h.store.GetRaw(key)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	count := 1
	v := strings.Split(key.Name, ":")
	if len(v) == 2 {
		count, err = strconv.Atoi(v[0])
		if err != nil {
			return err
		}
	}

	if err := h.send(context.Background(), eventData, count, h.payloadType, h.httpTimeout); err != nil {
		return err
	}

	// Delete the event from store.
	return h.store.Del(key)
}

// Send the log message 'entry' to the http target.
// Messages are queued in the disk if the store is enabled
// If Cancel has been called the message is ignored.
func (h *Target) Send(ctx context.Context, entry any) error {
	if h.status.Load() == statusClosed {
		if h.migrateTarget != nil {
			return h.migrateTarget.Send(ctx, entry)
		}
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
		h.totalMessages.Add(1)
	case <-ctx.Done():
		// return error only for context timedout.
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return ctx.Err()
		}
		return nil
	default:
		h.totalMessages.Add(1)
		h.failedMessages.Add(1)
		return errors.New("log buffer full")
	}

	return nil
}

// Cancel - cancels the target.
// All queued messages are flushed and the function returns afterwards.
// All messages sent to the target after this function has been called will be dropped.
func (h *Target) Cancel() {
	h.status.Store(statusClosed)
	h.storeCtxCancel()

	// Wait for messages to be sent...
	h.wg.Wait()

	// Set logch to nil and close it.
	// This will block all Send operations,
	// and finish the existing ones.
	// All future ones will be discarded.
	h.logChMu.Lock()
	xioutil.SafeClose(h.logCh)
	h.logCh = nil
	h.logChMu.Unlock()
}

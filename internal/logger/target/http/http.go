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
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	jsoniter "github.com/json-iterator/go"
	xhttp "github.com/minio/minio/internal/http"
	xioutil "github.com/minio/minio/internal/ioutil"
	"github.com/minio/minio/internal/logger/target/types"
	"github.com/minio/minio/internal/once"
	"github.com/minio/minio/internal/store"
	xnet "github.com/minio/pkg/v2/net"
	"github.com/valyala/bytebufferpool"
)

const (
	// Timeout for the webhook http call
	webhookCallTimeout = 3 * time.Second

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

// Config http logger target
type Config struct {
	Enabled    bool              `json:"enabled"`
	Name       string            `json:"name"`
	UserAgent  string            `json:"userAgent"`
	Endpoint   *xnet.URL         `json:"endpoint"`
	AuthToken  string            `json:"authToken"`
	ClientCert string            `json:"clientCert"`
	ClientKey  string            `json:"clientKey"`
	BatchSize  int               `json:"batchSize"`
	QueueSize  int               `json:"queueSize"`
	QueueDir   string            `json:"queueDir"`
	Proxy      string            `json:"string"`
	Transport  http.RoundTripper `json:"-"`

	// Custom logger
	LogOnce func(ctx context.Context, err error, id string, errKind ...interface{}) `json:"-"`
	Error   func(msg string, data ...interface{})                                   `json:"-"`
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
	workers    int64
	maxWorkers int64
	// workerStartMu sync.Mutex
	lastStarted time.Time

	wg sync.WaitGroup

	// Channel of log entries.
	// Reading logCh must hold read lock on logChMu (to avoid read race)
	// Sending a value on logCh must hold read lock on logChMu (to avoid closing)
	logCh   chan interface{}
	logChMu sync.RWMutex

	// If this webhook is being re-configured we will
	// assign the new webhook target to this field.
	// The Send() method will then re-direct entries
	// to the new target when the current on has
	// been set to status "statusClosed".
	// Once the global webhook slice has been update
	// the current target will stop receiving entries.
	migrateTarget *Target

	// This channel is used when migrating between webhook
	// targets when targets are re-configured.
	// If the context is canceled during a transport or we exit out of the processor
	// before those entries are sent, they will be drained into this channel.
	// This will then be forwarded to the new target as logMigrateCh.
	logDrainInto atomic.Pointer[chan chan interface{}]

	// logMigrateChCh will send any migration channels to be picked up by HTTP sender.
	// Each channel will be drained when sent to the new target.
	// All added channels must be closed before adding.
	logMigrateChCh atomic.Pointer[chan chan interface{}]

	// Number of events per HTTP send to webhook target
	// this is ideally useful only if your endpoint can
	// support reading multiple events on a stream for example
	// like : Splunk HTTP Event collector, if you are unsure
	// set this to '1'.
	batchSize int

	// store to persist and replay the logs to the target
	// to avoid missing events when the target is down.
	store          store.Store[interface{}]
	storeCtxCancel context.CancelFunc

	initQueueOnce once.Init

	config Config
	client *http.Client
}

// Name returns the name of the target
func (h *Target) Name() string {
	return "minio-http-" + h.config.Name
}

// Type - returns type of the target
func (h *Target) Type() types.TargetType {
	return types.TargetHTTP
}

// Migrate moveds the logCh from the current target
// to a new one and then closes the current target.
func (h *Target) Migrate(newTgt *Target) {
	existing := newTgt.logMigrateChCh.Load()
	if h.migrateTarget != nil || existing != nil {
		// Already migrating or something else that is unexpected.
		return
	}

	var lastMigration chan chan interface{}
	if v := h.logDrainInto.Load(); v != nil {
		lastMigration = *v
	}
	h.migrateTarget = newTgt
	// Create a new channel for the new target.
	x := make(chan chan interface{}, maxWorkers+len(lastMigration))
	h.logDrainInto.Store(&x)

	atomic.StoreInt32(&h.status, statusClosed)
	h.storeCtxCancel()
	h.logChMu.Lock()
	xioutil.SafeClose(h.logCh)
	h.logChMu.Unlock()

	// Wait for workers to exit.
	// When we are done or channels are no longer changing.
	h.wg.Wait()
	if lastMigration != nil {
		xioutil.SafeClose(lastMigration)
	}

	// Neither of these channels are being read/written to anymore.
	newLength := len(x) + len(lastMigration) + 3

	// Migrate to the channel we created above.
	// It should have enough capacity, but just in case we check.
	migrateTo := x
	if cap(x) < newLength-len(x) {
		migrateTo = make(chan chan interface{}, newLength)
		for v := range x {
			migrateTo <- v
		}
	}

	// Add anything from previous migration that may be unsent.
	if lastMigration != nil {
		for v := range lastMigration {
			migrateTo <- v
		}
	}

	// Add queued entries.
	if h.logCh != nil {
		migrateTo <- h.logCh
	}

	// Close the migration channel - it now has everything.
	newTgt.logChMu.Lock()
	newTgt.logMigrateChCh.Store(&migrateTo)
	newTgt.logChMu.Unlock()
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
func (h *Target) ping(ctx context.Context) error {
	return h.send(ctx, []byte(`{}`), "application/json", webhookCallTimeout)
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

// // InitDiskStore initializes the disk storage option
// func (h *Target) InitDiskStore(ctx context.Context) (err error) {
// 	return h.initQueueOnce.DoWithContext(ctx, h.initDiskStore)
// }
//
// // InitMemoryStore initializes the channel storage option
// func (h *Target) InitMemoryStore(ctx context.Context) (err error) {
// 	return h.initQueueOnce.DoWithContext(ctx, h.initMemoryStore)
// }

// Init validate and initialize the http target
func (h *Target) Init(ctx context.Context) (err error) {
	if h.config.QueueDir != "" {
		return h.initQueueOnce.DoWithContext(ctx, h.initDiskStore)
	}
	return h.initQueueOnce.DoWithContext(ctx, h.initMemoryStore)
}

func (h *Target) initDiskStore(ctx context.Context) (err error) {
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

func (h *Target) initMemoryStore(ctx context.Context) (err error) {
	pingErr := h.ping(ctx)
	if pingErr != nil {
		log.Println(
			fmt.Sprintf("unable to ping webhook target %s, url: %s", h.Name(), h.Endpoint()),
			pingErr,
		)
	}

	ctx, cancel := context.WithCancel(ctx)
	h.storeCtxCancel = cancel
	h.lastStarted = time.Now()
	go h.startHTTPLogger(ctx, true)
	return nil
}

func (h *Target) send(ctx context.Context, payload []byte, payloadType string, timeout time.Duration) (err error) {
	defer func() {
		// fmt.Println(string(debug.Stack()))
		if err != nil {
			// fmt.Println("ERROR SENDING TO:", h.Endpoint(), h.Type(), h.Name(), err)
			atomic.StoreInt32(&h.status, statusOffline)
		} else {
			// fmt.Println("SENDING TO:", h.Endpoint(), h.Type(), h.Name(), err)
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
	if payloadType != "" {
		req.Header.Set(xhttp.ContentType, payloadType)
	}
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

func (h *Target) startHTTPLogger(ctx context.Context, mainWorker bool) {
	atomic.AddInt64(&h.workers, 1)
	defer atomic.AddInt64(&h.workers, -1)

	h.logChMu.RLock()
	if h.logCh != nil {
		// We are not allowed to add when logCh is nil
		h.wg.Add(1)
		defer h.wg.Done()
	} else {
		h.logChMu.RUnlock()
		return
	}
	logCh := h.logCh
	h.logChMu.RUnlock()

	buf := bytebufferpool.Get()
	defer bytebufferpool.Put(buf)

	json := jsoniter.ConfigCompatibleWithStandardLibrary
	enc := json.NewEncoder(buf)
	batchSize := h.batchSize
	if batchSize <= 0 {
		batchSize = 1
	}

	payloadType := "application/json"
	if batchSize > 1 {
		payloadType = ""
	}

	entries := make([]interface{}, 0, batchSize)
	readFrom := logCh
	defer func() {
		if h.logCh == nil {
			return
		}
		migrate := h.logDrainInto.Load()
		if migrate == nil {
			return
		}
		if len(entries) > 0 {
			tmp := make(chan interface{}, len(entries))
			for _, v := range entries {
				tmp <- v
			}
			close(tmp)
			*migrate <- tmp
		}
		if readFrom != logCh {
			*migrate <- readFrom
		}
	}()

	var entry interface{}
	var ok bool
	var migrate chan chan interface{}
	for {
		// Do not attempt to reload if already set.
		if migrate == nil {
			if v := h.logMigrateChCh.Load(); v != nil {
				migrate = *v
			}
		}
		select {
		case readFrom = <-migrate:
			continue
		case entry, ok = <-readFrom:
			if !ok {
				if readFrom == logCh {
					return
				}
				readFrom = logCh
				continue
			}
		case <-ctx.Done():
			return
		}

		atomic.AddInt64(&h.totalMessages, 1)

		// If the channel reaches above half capacity
		// we spawn more workers. The workers spawned
		// from this main worker routine will exit
		// once the channel drops below half capacity
		// and it's been at least 30 seconds since we
		// launched a new worker.
		if mainWorker && len(logCh) > cap(logCh)/2 {
			nWorkers := atomic.LoadInt64(&h.workers)
			if nWorkers < h.maxWorkers {
				if time.Since(h.lastStarted).Milliseconds() > 100 {
					h.lastStarted = time.Now()
					go h.startHTTPLogger(ctx, false)
				}
			}
		}

		if err := enc.Encode(&entry); err != nil {
			h.config.LogOnce(
				ctx,
				fmt.Errorf("unable to decode webhook log entry, err:  %s, entry: %s\n", err, entry),
				"webhook_entry_encode_error",
			)
			atomic.AddInt64(&h.failedMessages, 1)
			continue
		}

		entries = append(entries, entry)
		if len(entries) != batchSize {
			if len(readFrom) > 0 {
				continue
			}
		}

	retry:
		err := h.send(ctx, buf.Bytes(), payloadType, webhookCallTimeout)
		if err == nil {
			entries = entries[:0]
		}

		select {
		case <-ctx.Done():
			return
		default:
		}

		if err != nil {
			if !xnet.IsNetworkOrHostDown(err, false) && !xnet.IsConnResetErr(err) {
				h.config.Error(fmt.Sprintf("unable to send log entry: %s", err))
			}
			time.Sleep(time.Second)
			goto retry
		}

		buf.Reset()

		if !mainWorker && len(logCh) < cap(logCh)/2 {
			if time.Since(h.lastStarted).Seconds() > 30 {
				return
			}
		}
	}
}

// New initializes a new logger target which
// sends log over http to the specified endpoint
func New(config Config) *Target {
	h := &Target{
		logCh:     make(chan interface{}, config.QueueSize),
		config:    config,
		status:    statusOffline,
		batchSize: config.BatchSize,
	}

	h.maxWorkers = maxWorkers
	if h.batchSize > 100 {
		h.maxWorkers = maxWorkersWithBatchEvents
	}

	// If proxy available, set the same
	if h.config.Proxy != "" {
		proxyURL, _ := url.Parse(h.config.Proxy)
		transport := h.config.Transport
		ctransport := transport.(*http.Transport).Clone()
		ctransport.Proxy = http.ProxyURL(proxyURL)
		h.config.Transport = ctransport
	}
	ctransport := h.config.Transport.(*http.Transport).Clone()
	ctransport.TLSClientConfig.InsecureSkipVerify = true
	h.client = &http.Client{Transport: ctransport}

	return h
}

// SendFromStore - reads the log from store and sends it to webhook.
func (h *Target) SendFromStore(key store.Key) (err error) {
	var eventData interface{}
	eventData, err = h.store.Get(key.Name)
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
	if err := h.send(context.Background(), logJSON, "application/json", webhookCallTimeout); err != nil {
		atomic.AddInt64(&h.failedMessages, 1)
		if xnet.IsNetworkOrHostDown(err, true) {
			return store.ErrNotConnected
		}
		return err
	}
	// Delete the event from store.
	return h.store.Del(key.Name)
}

// Send the log message 'entry' to the http target.
// Messages are queued in the disk if the store is enabled
// If Cancel has been called the message is ignored.
func (h *Target) Send(ctx context.Context, entry interface{}) error {
	if atomic.LoadInt32(&h.status) == statusClosed {
		if h.migrateTarget != nil {
			return h.migrateTarget.Send(ctx, entry)
		}
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

retry:
	select {
	case h.logCh <- entry:
		atomic.AddInt64(&h.totalMessages, 1)
	case <-ctx.Done():
		// return error only for context timedout.
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return ctx.Err()
		}
		return nil
	default:
		if h.workers < h.maxWorkers {
			goto retry
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
	h.storeCtxCancel()

	// Set logch to nil and close it.
	// This will block all Send operations,
	// and finish the existing ones.
	// All future ones will be discarded.
	h.logChMu.Lock()
	xioutil.SafeClose(h.logCh)
	h.logCh = nil
	h.logChMu.Unlock()

	// Wait for messages to be sent...
	h.wg.Wait()
}

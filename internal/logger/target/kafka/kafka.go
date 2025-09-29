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

package kafka

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/IBM/sarama"
	saramatls "github.com/IBM/sarama/tools/tls"

	xioutil "github.com/minio/minio/internal/ioutil"
	types "github.com/minio/minio/internal/logger/target/loggertypes"
	"github.com/minio/minio/internal/once"
	"github.com/minio/minio/internal/store"
	xnet "github.com/minio/pkg/v3/net"
)

// the suffix for the configured queue dir where the logs will be persisted.
const kafkaLoggerExtension = ".kafka.log"

const (
	statusClosed = iota
	statusOffline
	statusOnline
)

// Config - kafka target arguments.
type Config struct {
	Enabled bool        `json:"enable"`
	Brokers []xnet.Host `json:"brokers"`
	Topic   string      `json:"topic"`
	Version string      `json:"version"`
	TLS     struct {
		Enable        bool               `json:"enable"`
		RootCAs       *x509.CertPool     `json:"-"`
		SkipVerify    bool               `json:"skipVerify"`
		ClientAuth    tls.ClientAuthType `json:"clientAuth"`
		ClientTLSCert string             `json:"clientTLSCert"`
		ClientTLSKey  string             `json:"clientTLSKey"`
	} `json:"tls"`
	SASL struct {
		Enable    bool   `json:"enable"`
		User      string `json:"username"`
		Password  string `json:"password"`
		Mechanism string `json:"mechanism"`
	} `json:"sasl"`
	// Queue store
	QueueSize int    `json:"queueSize"`
	QueueDir  string `json:"queueDir"`

	// Custom logger
	LogOnce func(ctx context.Context, err error, id string, errKind ...any) `json:"-"`
}

// Target - Kafka target.
type Target struct {
	status int32

	totalMessages  int64
	failedMessages int64

	wg sync.WaitGroup

	// Channel of log entries.
	// Reading logCh must hold read lock on logChMu (to avoid read race)
	// Sending a value on logCh must hold read lock on logChMu (to avoid closing)
	logCh   chan any
	logChMu sync.RWMutex

	// store to persist and replay the logs to the target
	// to avoid missing events when the target is down.
	store          store.Store[any]
	storeCtxCancel context.CancelFunc

	initKafkaOnce      once.Init
	initQueueStoreOnce once.Init

	client   sarama.Client
	producer sarama.SyncProducer
	kconfig  Config
	config   *sarama.Config
}

func (h *Target) validate() error {
	if len(h.kconfig.Brokers) == 0 {
		return errors.New("no broker address found")
	}
	for _, b := range h.kconfig.Brokers {
		if _, err := xnet.ParseHost(b.String()); err != nil {
			return err
		}
	}
	return nil
}

// Name returns the name of the target
func (h *Target) Name() string {
	return "minio-kafka-audit"
}

// Endpoint - return kafka target
func (h *Target) Endpoint() string {
	return "kafka"
}

// String - kafka string
func (h *Target) String() string {
	return "kafka"
}

// Stats returns the target statistics.
func (h *Target) Stats() types.TargetStats {
	h.logChMu.RLock()
	queueLength := len(h.logCh)
	h.logChMu.RUnlock()

	return types.TargetStats{
		TotalMessages:  atomic.LoadInt64(&h.totalMessages),
		FailedMessages: atomic.LoadInt64(&h.failedMessages),
		QueueLength:    queueLength,
	}
}

// Init initialize kafka target
func (h *Target) Init(ctx context.Context) error {
	if !h.kconfig.Enabled {
		return nil
	}
	if err := h.validate(); err != nil {
		return err
	}
	if h.kconfig.QueueDir != "" {
		if err := h.initQueueStoreOnce.DoWithContext(ctx, h.initQueueStore); err != nil {
			return err
		}
		return h.initKafkaOnce.Do(h.init)
	}
	if err := h.init(); err != nil {
		return err
	}
	go h.startKafkaLogger()
	return nil
}

func (h *Target) initQueueStore(ctx context.Context) (err error) {
	queueDir := filepath.Join(h.kconfig.QueueDir, h.Name())
	queueStore := store.NewQueueStore[any](queueDir, uint64(h.kconfig.QueueSize), kafkaLoggerExtension)
	if err = queueStore.Open(); err != nil {
		return fmt.Errorf("unable to initialize the queue store of %s webhook: %w", h.Name(), err)
	}
	ctx, cancel := context.WithCancel(ctx)
	h.store = queueStore
	h.storeCtxCancel = cancel
	store.StreamItems(h.store, h, ctx.Done(), h.kconfig.LogOnce)
	return err
}

func (h *Target) startKafkaLogger() {
	h.logChMu.RLock()
	logCh := h.logCh
	if logCh != nil {
		// We are not allowed to add when logCh is nil
		h.wg.Add(1)
		defer h.wg.Done()
	}
	h.logChMu.RUnlock()

	if logCh == nil {
		return
	}

	// Create a routine which sends json logs received
	// from an internal channel.
	for entry := range logCh {
		h.logEntry(entry)
	}
}

func (h *Target) logEntry(entry any) {
	atomic.AddInt64(&h.totalMessages, 1)
	if err := h.send(entry); err != nil {
		atomic.AddInt64(&h.failedMessages, 1)
		h.kconfig.LogOnce(context.Background(), err, h.kconfig.Topic)
	}
}

func (h *Target) send(entry any) error {
	if err := h.initKafkaOnce.Do(h.init); err != nil {
		return err
	}
	logJSON, err := json.Marshal(&entry)
	if err != nil {
		return err
	}
	msg := sarama.ProducerMessage{
		Topic: h.kconfig.Topic,
		Value: sarama.ByteEncoder(logJSON),
	}
	_, _, err = h.producer.SendMessage(&msg)
	if err != nil {
		atomic.StoreInt32(&h.status, statusOffline)
	} else {
		atomic.StoreInt32(&h.status, statusOnline)
	}
	return err
}

// Init initialize kafka target
func (h *Target) init() error {
	if os.Getenv("_MINIO_KAFKA_DEBUG") != "" {
		sarama.DebugLogger = log.Default()
	}

	sconfig := sarama.NewConfig()
	if h.kconfig.Version != "" {
		kafkaVersion, err := sarama.ParseKafkaVersion(h.kconfig.Version)
		if err != nil {
			return err
		}
		sconfig.Version = kafkaVersion
	}

	sconfig.Net.KeepAlive = 60 * time.Second
	sconfig.Net.SASL.User = h.kconfig.SASL.User
	sconfig.Net.SASL.Password = h.kconfig.SASL.Password
	initScramClient(h.kconfig, sconfig) // initializes configured scram client.
	sconfig.Net.SASL.Enable = h.kconfig.SASL.Enable

	tlsConfig, err := saramatls.NewConfig(h.kconfig.TLS.ClientTLSCert, h.kconfig.TLS.ClientTLSKey)
	if err != nil {
		return err
	}

	sconfig.Net.TLS.Enable = h.kconfig.TLS.Enable
	sconfig.Net.TLS.Config = tlsConfig
	sconfig.Net.TLS.Config.InsecureSkipVerify = h.kconfig.TLS.SkipVerify
	sconfig.Net.TLS.Config.ClientAuth = h.kconfig.TLS.ClientAuth
	sconfig.Net.TLS.Config.RootCAs = h.kconfig.TLS.RootCAs

	// These settings are needed to ensure that kafka client doesn't hang on brokers
	// refer https://github.com/IBM/sarama/issues/765#issuecomment-254333355
	sconfig.Producer.Retry.Max = 2
	sconfig.Producer.Retry.Backoff = (10 * time.Second)
	sconfig.Producer.Return.Successes = true
	sconfig.Producer.Return.Errors = true
	sconfig.Producer.RequiredAcks = 1
	sconfig.Producer.Timeout = (10 * time.Second)
	sconfig.Net.ReadTimeout = (10 * time.Second)
	sconfig.Net.DialTimeout = (10 * time.Second)
	sconfig.Net.WriteTimeout = (10 * time.Second)
	sconfig.Metadata.Retry.Max = 1
	sconfig.Metadata.Retry.Backoff = (10 * time.Second)
	sconfig.Metadata.RefreshFrequency = (15 * time.Minute)

	h.config = sconfig

	var brokers []string
	for _, broker := range h.kconfig.Brokers {
		brokers = append(brokers, broker.String())
	}

	client, err := sarama.NewClient(brokers, sconfig)
	if err != nil {
		return err
	}

	producer, err := sarama.NewSyncProducerFromClient(client)
	if err != nil {
		return err
	}
	h.client = client
	h.producer = producer

	if len(h.client.Brokers()) > 0 {
		// Refer https://github.com/IBM/sarama/issues/1341
		atomic.StoreInt32(&h.status, statusOnline)
	}

	return nil
}

// IsOnline returns true if the target is online.
func (h *Target) IsOnline(_ context.Context) bool {
	return atomic.LoadInt32(&h.status) == statusOnline
}

// Send log message 'e' to kafka target.
func (h *Target) Send(ctx context.Context, entry any) error {
	if h.store != nil {
		// save the entry to the queue store which will be replayed to the target.
		_, err := h.store.Put(entry)
		return err
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
		// log channel is full, do not wait and return
		// an error immediately to the caller
		atomic.AddInt64(&h.totalMessages, 1)
		atomic.AddInt64(&h.failedMessages, 1)
		return errors.New("log buffer full")
	}
	return nil
}

// SendFromStore - reads the log from store and sends it to kafka.
func (h *Target) SendFromStore(key store.Key) (err error) {
	auditEntry, err := h.store.Get(key)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	atomic.AddInt64(&h.totalMessages, 1)
	err = h.send(auditEntry)
	if err != nil {
		atomic.AddInt64(&h.failedMessages, 1)
		return err
	}
	// Delete the event from store.
	return h.store.Del(key)
}

// Cancel - cancels the target
func (h *Target) Cancel() {
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
	xioutil.SafeClose(h.logCh)
	h.logCh = nil
	h.logChMu.Unlock()

	if h.producer != nil {
		h.producer.Close()
		h.client.Close()
	}

	// Wait for messages to be sent...
	h.wg.Wait()
}

// New initializes a new logger target which
// sends log over http to the specified endpoint
func New(config Config) *Target {
	target := &Target{
		logCh:   make(chan any, config.QueueSize),
		kconfig: config,
		status:  statusOffline,
	}
	return target
}

// Type - returns type of the target
func (h *Target) Type() types.TargetType {
	return types.TargetKafka
}

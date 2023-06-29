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
	"net"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Shopify/sarama"
	saramatls "github.com/Shopify/sarama/tools/tls"
	"github.com/tidwall/gjson"

	"github.com/minio/minio/internal/logger/target/types"
	"github.com/minio/minio/internal/once"
	"github.com/minio/minio/internal/store"
	xnet "github.com/minio/pkg/v2/net"
)

// the suffix for the configured queue dir where the logs will be persisted.
const kafkaLoggerExtension = ".kafka.log"

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
	LogOnce func(ctx context.Context, err error, id string, errKind ...interface{}) `json:"-"`
}

// Check if atleast one broker in cluster is active
func (k Config) pingBrokers() (err error) {
	d := net.Dialer{Timeout: 1 * time.Second}

	errs := make([]error, len(k.Brokers))
	var wg sync.WaitGroup
	for idx, broker := range k.Brokers {
		broker := broker
		idx := idx
		wg.Add(1)
		go func(broker xnet.Host, idx int) {
			defer wg.Done()

			_, errs[idx] = d.Dial("tcp", broker.String())
		}(broker, idx)
	}
	wg.Wait()

	var retErr error
	for _, err := range errs {
		if err == nil {
			// if one broker is online its enough
			return nil
		}
		retErr = err
	}
	return retErr
}

// Target - Kafka target.
type Target struct {
	totalMessages  int64
	failedMessages int64

	wg sync.WaitGroup

	// Channel of log entries.
	// Reading logCh must hold read lock on logChMu (to avoid read race)
	// Sending a value on logCh must hold read lock on logChMu (to avoid closing)
	logCh   chan interface{}
	logChMu sync.RWMutex

	// store to persist and replay the logs to the target
	// to avoid missing events when the target is down.
	store          store.Store[interface{}]
	storeCtxCancel context.CancelFunc

	initKafkaOnce      once.Init
	initQueueStoreOnce once.Init

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
	go h.startKakfaLogger()
	return nil
}

func (h *Target) initQueueStore(ctx context.Context) (err error) {
	var queueStore store.Store[interface{}]
	queueDir := filepath.Join(h.kconfig.QueueDir, h.Name())
	queueStore = store.NewQueueStore[interface{}](queueDir, uint64(h.kconfig.QueueSize), kafkaLoggerExtension)
	if err = queueStore.Open(); err != nil {
		return fmt.Errorf("unable to initialize the queue store of %s webhook: %w", h.Name(), err)
	}
	ctx, cancel := context.WithCancel(ctx)
	h.store = queueStore
	h.storeCtxCancel = cancel
	store.StreamItems(h.store, h, ctx.Done(), h.kconfig.LogOnce)
	return
}

func (h *Target) startKakfaLogger() {
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

func (h *Target) logEntry(entry interface{}) {
	atomic.AddInt64(&h.totalMessages, 1)
	if err := h.send(entry); err != nil {
		atomic.AddInt64(&h.failedMessages, 1)
		h.kconfig.LogOnce(context.Background(), err, h.kconfig.Topic)
	}
}

func (h *Target) send(entry interface{}) error {
	if err := h.initKafkaOnce.Do(h.init); err != nil {
		return err
	}
	logJSON, err := json.Marshal(&entry)
	if err != nil {
		return err
	}
	requestID := gjson.GetBytes(logJSON, "requestID").Str
	if requestID == "" {
		// unsupported data structure
		return fmt.Errorf("unsupported data structure: %s must be either audit.Entry or log.Entry", reflect.TypeOf(entry))
	}
	msg := sarama.ProducerMessage{
		Topic: h.kconfig.Topic,
		Key:   sarama.StringEncoder(requestID),
		Value: sarama.ByteEncoder(logJSON),
	}
	_, _, err = h.producer.SendMessage(&msg)
	return err
}

// Init initialize kafka target
func (h *Target) init() error {
	if err := h.kconfig.pingBrokers(); err != nil {
		return err
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

	sconfig.Producer.RequiredAcks = sarama.WaitForAll
	sconfig.Producer.Retry.Max = 10
	sconfig.Producer.Return.Successes = true

	h.config = sconfig

	var brokers []string
	for _, broker := range h.kconfig.Brokers {
		brokers = append(brokers, broker.String())
	}

	producer, err := sarama.NewSyncProducer(brokers, sconfig)
	if err != nil {
		return err
	}

	h.producer = producer
	return nil
}

// IsOnline returns true if the target is online.
func (h *Target) IsOnline(_ context.Context) bool {
	if err := h.initKafkaOnce.Do(h.init); err != nil {
		return false
	}
	return h.kconfig.pingBrokers() == nil
}

// Send log message 'e' to kafka target.
func (h *Target) Send(ctx context.Context, entry interface{}) error {
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
func (h *Target) SendFromStore(key string) (err error) {
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
		return
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
	close(h.logCh)
	h.logCh = nil
	h.logChMu.Unlock()

	if h.producer != nil {
		h.producer.Close()
	}

	// Wait for messages to be sent...
	h.wg.Wait()
}

// New initializes a new logger target which
// sends log over http to the specified endpoint
func New(config Config) *Target {
	target := &Target{
		logCh:   make(chan interface{}, config.QueueSize),
		kconfig: config,
	}
	return target
}

// Type - returns type of the target
func (h *Target) Type() types.TargetType {
	return types.TargetKafka
}

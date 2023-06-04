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
	"sync"
	"sync/atomic"
	"time"

	"github.com/minio/pkg/logger/message/audit"

	"github.com/Shopify/sarama"
	saramatls "github.com/Shopify/sarama/tools/tls"

	"github.com/minio/minio/internal/logger/target/types"
	"github.com/minio/minio/internal/once"
	"github.com/minio/minio/internal/store"
	xnet "github.com/minio/pkg/net"
)

// the suffix for the configured queue dir where the logs will be persisted.
const kafkaLoggerExtension = ".kafka.log"

// Config - kafka target arguments.
type Config struct {

	// Custom logger
	LogOnce func(ctx context.Context, err error, id string, errKind ...interface{}) `json:"-"`
	TLS     struct {
		RootCAs       *x509.CertPool     `json:"-"`
		ClientTLSCert string             `json:"clientTLSCert"`
		ClientTLSKey  string             `json:"clientTLSKey"`
		ClientAuth    tls.ClientAuthType `json:"clientAuth"`
		Enable        bool               `json:"enable"`
		SkipVerify    bool               `json:"skipVerify"`
	} `json:"tls"`
	SASL struct {
		User      string `json:"username"`
		Password  string `json:"password"`
		Mechanism string `json:"mechanism"`
		Enable    bool   `json:"enable"`
	} `json:"sasl"`
	Topic    string `json:"topic"`
	Version  string `json:"version"`
	QueueDir string `json:"queueDir"`

	Brokers []xnet.Host `json:"brokers"`
	// Queue store
	QueueSize int  `json:"queueSize"`
	Enabled   bool `json:"enable"`
}

// Check if atleast one broker in cluster is active
func (k Config) pingBrokers() (err error) {
	d := net.Dialer{Timeout: 60 * time.Second}

	for _, broker := range k.Brokers {
		_, err = d.Dial("tcp", broker.String())
		if err != nil {
			return err
		}
	}
	return nil
}

// Target - Kafka target.
type Target struct {
	kconfig Config

	// store to persist and replay the logs to the target
	// to avoid missing events when the target is down.
	store store.Store[audit.Entry]

	producer sarama.SyncProducer
	doneCh   chan struct{}

	// Channel of log entries
	logCh chan audit.Entry

	storeCtxCancel context.CancelFunc
	config         *sarama.Config

	wg             sync.WaitGroup
	totalMessages  int64
	failedMessages int64

	initKafkaOnce      once.Init
	initQueueStoreOnce once.Init
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
	return types.TargetStats{
		TotalMessages:  atomic.LoadInt64(&h.totalMessages),
		FailedMessages: atomic.LoadInt64(&h.failedMessages),
		QueueLength:    len(h.logCh),
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
	var queueStore store.Store[audit.Entry]
	queueDir := filepath.Join(h.kconfig.QueueDir, h.Name())
	queueStore = store.NewQueueStore[audit.Entry](queueDir, uint64(h.kconfig.QueueSize), kafkaLoggerExtension)
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
	// Create a routine which sends json logs received
	// from an internal channel.
	h.wg.Add(1)
	go func() {
		defer h.wg.Done()

		for {
			select {
			case entry := <-h.logCh:
				h.logEntry(entry)
			case <-h.doneCh:
				return
			}
		}
	}()
}

func (h *Target) logEntry(entry audit.Entry) {
	atomic.AddInt64(&h.totalMessages, 1)
	if err := h.send(entry); err != nil {
		atomic.AddInt64(&h.failedMessages, 1)
		h.kconfig.LogOnce(context.Background(), err, h.kconfig.Topic)
	}
}

func (h *Target) send(entry audit.Entry) error {
	if err := h.initKafkaOnce.Do(h.init); err != nil {
		return err
	}
	logJSON, err := json.Marshal(&entry)
	if err != nil {
		return err
	}
	msg := sarama.ProducerMessage{
		Topic: h.kconfig.Topic,
		Key:   sarama.StringEncoder(entry.RequestID),
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
	if auditEntry, ok := entry.(audit.Entry); ok {
		if h.store != nil {
			// save the entry to the queue store which will be replayed to the target.
			return h.store.Put(auditEntry)
		}
		if err := h.initKafkaOnce.Do(h.init); err != nil {
			return err
		}
		select {
		case <-h.doneCh:
		case h.logCh <- auditEntry:
		default:
			// log channel is full, do not wait and return
			// an error immediately to the caller
			atomic.AddInt64(&h.totalMessages, 1)
			atomic.AddInt64(&h.failedMessages, 1)
			return errors.New("log buffer full")
		}
	}
	return nil
}

// SendFromStore - reads the log from store and sends it to kafka.
func (h *Target) SendFromStore(key string) (err error) {
	var auditEntry audit.Entry
	auditEntry, err = h.store.Get(key)
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
	close(h.doneCh)
	close(h.logCh)
	// If queuestore is configured, cancel it's context to
	// stop the replay go-routine.
	if h.store != nil {
		h.storeCtxCancel()
	}
	if h.producer != nil {
		h.producer.Close()
	}
	h.wg.Wait()
}

// New initializes a new logger target which
// sends log over http to the specified endpoint
func New(config Config) *Target {
	target := &Target{
		logCh:   make(chan audit.Entry, 10000),
		doneCh:  make(chan struct{}),
		kconfig: config,
	}
	return target
}

// Type - returns type of the target
func (h *Target) Type() types.TargetType {
	return types.TargetKafka
}

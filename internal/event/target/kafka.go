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

package target

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/minio/minio/internal/event"
	"github.com/minio/minio/internal/logger"
	"github.com/minio/minio/internal/once"
	"github.com/minio/minio/internal/store"
	xnet "github.com/minio/pkg/v3/net"

	"github.com/IBM/sarama"
	saramatls "github.com/IBM/sarama/tools/tls"
)

// Kafka input constants
const (
	KafkaBrokers            = "brokers"
	KafkaTopic              = "topic"
	KafkaQueueDir           = "queue_dir"
	KafkaQueueLimit         = "queue_limit"
	KafkaTLS                = "tls"
	KafkaTLSSkipVerify      = "tls_skip_verify"
	KafkaTLSClientAuth      = "tls_client_auth"
	KafkaSASL               = "sasl"
	KafkaSASLUsername       = "sasl_username"
	KafkaSASLPassword       = "sasl_password"
	KafkaSASLMechanism      = "sasl_mechanism"
	KafkaClientTLSCert      = "client_tls_cert"
	KafkaClientTLSKey       = "client_tls_key"
	KafkaVersion            = "version"
	KafkaBatchSize          = "batch_size"
	KafkaBatchCommitTimeout = "batch_commit_timeout"
	KafkaCompressionCodec   = "compression_codec"
	KafkaCompressionLevel   = "compression_level"

	EnvKafkaEnable                   = "MINIO_NOTIFY_KAFKA_ENABLE"
	EnvKafkaBrokers                  = "MINIO_NOTIFY_KAFKA_BROKERS"
	EnvKafkaTopic                    = "MINIO_NOTIFY_KAFKA_TOPIC"
	EnvKafkaQueueDir                 = "MINIO_NOTIFY_KAFKA_QUEUE_DIR"
	EnvKafkaQueueLimit               = "MINIO_NOTIFY_KAFKA_QUEUE_LIMIT"
	EnvKafkaTLS                      = "MINIO_NOTIFY_KAFKA_TLS"
	EnvKafkaTLSSkipVerify            = "MINIO_NOTIFY_KAFKA_TLS_SKIP_VERIFY"
	EnvKafkaTLSClientAuth            = "MINIO_NOTIFY_KAFKA_TLS_CLIENT_AUTH"
	EnvKafkaSASLEnable               = "MINIO_NOTIFY_KAFKA_SASL"
	EnvKafkaSASLUsername             = "MINIO_NOTIFY_KAFKA_SASL_USERNAME"
	EnvKafkaSASLPassword             = "MINIO_NOTIFY_KAFKA_SASL_PASSWORD"
	EnvKafkaSASLMechanism            = "MINIO_NOTIFY_KAFKA_SASL_MECHANISM"
	EnvKafkaClientTLSCert            = "MINIO_NOTIFY_KAFKA_CLIENT_TLS_CERT"
	EnvKafkaClientTLSKey             = "MINIO_NOTIFY_KAFKA_CLIENT_TLS_KEY"
	EnvKafkaVersion                  = "MINIO_NOTIFY_KAFKA_VERSION"
	EnvKafkaBatchSize                = "MINIO_NOTIFY_KAFKA_BATCH_SIZE"
	EnvKafkaBatchCommitTimeout       = "MINIO_NOTIFY_KAFKA_BATCH_COMMIT_TIMEOUT"
	EnvKafkaProducerCompressionCodec = "MINIO_NOTIFY_KAFKA_PRODUCER_COMPRESSION_CODEC"
	EnvKafkaProducerCompressionLevel = "MINIO_NOTIFY_KAFKA_PRODUCER_COMPRESSION_LEVEL"
)

var codecs = map[string]sarama.CompressionCodec{
	"none":   sarama.CompressionNone,
	"gzip":   sarama.CompressionGZIP,
	"snappy": sarama.CompressionSnappy,
	"lz4":    sarama.CompressionLZ4,
	"zstd":   sarama.CompressionZSTD,
}

// KafkaArgs - Kafka target arguments.
type KafkaArgs struct {
	Enable             bool          `json:"enable"`
	Brokers            []xnet.Host   `json:"brokers"`
	Topic              string        `json:"topic"`
	QueueDir           string        `json:"queueDir"`
	QueueLimit         uint64        `json:"queueLimit"`
	Version            string        `json:"version"`
	BatchSize          uint32        `json:"batchSize"`
	BatchCommitTimeout time.Duration `json:"batchCommitTimeout"`
	TLS                struct {
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
	Producer struct {
		Compression      string `json:"compression"`
		CompressionLevel int    `json:"compressionLevel"`
	} `json:"producer"`
}

// Validate KafkaArgs fields
func (k KafkaArgs) Validate() error {
	if !k.Enable {
		return nil
	}
	if len(k.Brokers) == 0 {
		return errors.New("no broker address found")
	}
	for _, b := range k.Brokers {
		if _, err := xnet.ParseHost(b.String()); err != nil {
			return err
		}
	}
	if k.QueueDir != "" {
		if !filepath.IsAbs(k.QueueDir) {
			return errors.New("queueDir path should be absolute")
		}
	}
	if k.Version != "" {
		if _, err := sarama.ParseKafkaVersion(k.Version); err != nil {
			return err
		}
	}
	if k.BatchSize > 1 {
		if k.QueueDir == "" {
			return errors.New("batch should be enabled only if queue dir is enabled")
		}
	}
	if k.BatchCommitTimeout > 0 {
		if k.QueueDir == "" || k.BatchSize <= 1 {
			return errors.New("batch commit timeout should be set only if queue dir is enabled and batch size > 1")
		}
	}
	return nil
}

// KafkaTarget - Kafka target.
type KafkaTarget struct {
	initOnce once.Init

	id         event.TargetID
	args       KafkaArgs
	client     sarama.Client
	producer   sarama.SyncProducer
	config     *sarama.Config
	store      store.Store[event.Event]
	batch      *store.Batch[event.Event]
	loggerOnce logger.LogOnce
	quitCh     chan struct{}
}

// ID - returns target ID.
func (target *KafkaTarget) ID() event.TargetID {
	return target.id
}

// Name - returns the Name of the target.
func (target *KafkaTarget) Name() string {
	return target.ID().String()
}

// Store returns any underlying store if set.
func (target *KafkaTarget) Store() event.TargetStore {
	return target.store
}

// IsActive - Return true if target is up and active
func (target *KafkaTarget) IsActive() (bool, error) {
	if err := target.init(); err != nil {
		return false, err
	}
	return target.isActive()
}

func (target *KafkaTarget) isActive() (bool, error) {
	// Refer https://github.com/IBM/sarama/issues/1341
	brokers := target.client.Brokers()
	if len(brokers) == 0 {
		return false, store.ErrNotConnected
	}
	return true, nil
}

// Save - saves the events to the store which will be replayed when the Kafka connection is active.
func (target *KafkaTarget) Save(eventData event.Event) error {
	if target.store != nil {
		if target.batch != nil {
			return target.batch.Add(eventData)
		}
		_, err := target.store.Put(eventData)
		return err
	}
	if err := target.init(); err != nil {
		return err
	}
	return target.send(eventData)
}

// send - sends an event to the kafka.
func (target *KafkaTarget) send(eventData event.Event) error {
	if target.producer == nil {
		return store.ErrNotConnected
	}
	msg, err := target.toProducerMessage(eventData)
	if err != nil {
		return err
	}
	_, _, err = target.producer.SendMessage(msg)
	return err
}

// sendMultiple sends multiple messages to the kafka.
func (target *KafkaTarget) sendMultiple(events []event.Event) error {
	if target.producer == nil {
		return store.ErrNotConnected
	}
	var msgs []*sarama.ProducerMessage
	for _, event := range events {
		msg, err := target.toProducerMessage(event)
		if err != nil {
			return err
		}
		msgs = append(msgs, msg)
	}
	return target.producer.SendMessages(msgs)
}

// SendFromStore - reads an event from store and sends it to Kafka.
func (target *KafkaTarget) SendFromStore(key store.Key) (err error) {
	if err = target.init(); err != nil {
		return err
	}
	switch {
	case key.ItemCount == 1:
		var event event.Event
		event, err = target.store.Get(key)
		if err != nil {
			// The last event key in a successful batch will be sent in the channel atmost once by the replayEvents()
			// Such events will not exist and wouldve been already been sent successfully.
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		err = target.send(event)
	case key.ItemCount > 1:
		var events []event.Event
		events, err = target.store.GetMultiple(key)
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		err = target.sendMultiple(events)
	}
	if err != nil {
		if isKafkaConnErr(err) {
			return store.ErrNotConnected
		}
		return err
	}
	// Delete the event from store.
	return target.store.Del(key)
}

func (target *KafkaTarget) toProducerMessage(eventData event.Event) (*sarama.ProducerMessage, error) {
	objectName, err := url.QueryUnescape(eventData.S3.Object.Key)
	if err != nil {
		return nil, err
	}

	key := eventData.S3.Bucket.Name + "/" + objectName
	data, err := json.Marshal(event.Log{EventName: eventData.EventName, Key: key, Records: []event.Event{eventData}})
	if err != nil {
		return nil, err
	}

	return &sarama.ProducerMessage{
		Topic: target.args.Topic,
		Key:   sarama.StringEncoder(key),
		Value: sarama.ByteEncoder(data),
	}, nil
}

// Close - closes underneath kafka connection.
func (target *KafkaTarget) Close() error {
	close(target.quitCh)

	if target.batch != nil {
		target.batch.Close()
	}

	if target.producer != nil {
		if target.store != nil {
			// It is safe to abort the current transaction if
			// queue_dir is configured
			target.producer.AbortTxn()
		} else {
			target.producer.CommitTxn()
		}
		target.producer.Close()
		return target.client.Close()
	}

	return nil
}

func (target *KafkaTarget) init() error {
	return target.initOnce.Do(target.initKafka)
}

func (target *KafkaTarget) initKafka() error {
	if os.Getenv("_MINIO_KAFKA_DEBUG") != "" {
		sarama.DebugLogger = log.Default()
	}

	args := target.args

	config := sarama.NewConfig()
	if args.Version != "" {
		kafkaVersion, err := sarama.ParseKafkaVersion(args.Version)
		if err != nil {
			target.loggerOnce(context.Background(), err, target.ID().String())
			return err
		}
		config.Version = kafkaVersion
	}

	config.Net.KeepAlive = 60 * time.Second
	config.Net.SASL.User = args.SASL.User
	config.Net.SASL.Password = args.SASL.Password
	initScramClient(args, config) // initializes configured scram client.
	config.Net.SASL.Enable = args.SASL.Enable

	tlsConfig, err := saramatls.NewConfig(args.TLS.ClientTLSCert, args.TLS.ClientTLSKey)
	if err != nil {
		target.loggerOnce(context.Background(), err, target.ID().String())
		return err
	}

	config.Net.TLS.Enable = args.TLS.Enable
	config.Net.TLS.Config = tlsConfig
	config.Net.TLS.Config.InsecureSkipVerify = args.TLS.SkipVerify
	config.Net.TLS.Config.ClientAuth = args.TLS.ClientAuth
	config.Net.TLS.Config.RootCAs = args.TLS.RootCAs

	// These settings are needed to ensure that kafka client doesn't hang on brokers
	// refer https://github.com/IBM/sarama/issues/765#issuecomment-254333355
	config.Producer.Retry.Max = 2
	config.Producer.Retry.Backoff = (1 * time.Second)
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	config.Producer.RequiredAcks = 1
	config.Producer.Timeout = (5 * time.Second)
	// Set Producer Compression
	cc, ok := codecs[strings.ToLower(args.Producer.Compression)]
	if ok {
		config.Producer.Compression = cc
		config.Producer.CompressionLevel = args.Producer.CompressionLevel
	}

	config.Net.ReadTimeout = (5 * time.Second)
	config.Net.DialTimeout = (5 * time.Second)
	config.Net.WriteTimeout = (5 * time.Second)
	config.Metadata.Retry.Max = 1
	config.Metadata.Retry.Backoff = (1 * time.Second)
	config.Metadata.RefreshFrequency = (15 * time.Minute)

	target.config = config

	brokers := []string{}
	for _, broker := range args.Brokers {
		brokers = append(brokers, broker.String())
	}

	client, err := sarama.NewClient(brokers, config)
	if err != nil {
		if !errors.Is(err, sarama.ErrOutOfBrokers) {
			target.loggerOnce(context.Background(), err, target.ID().String())
		}
		return err
	}

	producer, err := sarama.NewSyncProducerFromClient(client)
	if err != nil {
		if !errors.Is(err, sarama.ErrOutOfBrokers) {
			target.loggerOnce(context.Background(), err, target.ID().String())
		}
		return err
	}
	target.client = client
	target.producer = producer

	yes, err := target.isActive()
	if err != nil {
		return err
	}
	if !yes {
		return store.ErrNotConnected
	}

	return nil
}

// NewKafkaTarget - creates new Kafka target with auth credentials.
func NewKafkaTarget(id string, args KafkaArgs, loggerOnce logger.LogOnce) (*KafkaTarget, error) {
	var queueStore store.Store[event.Event]
	if args.QueueDir != "" {
		queueDir := filepath.Join(args.QueueDir, storePrefix+"-kafka-"+id)
		queueStore = store.NewQueueStore[event.Event](queueDir, args.QueueLimit, event.StoreExtension)
		if err := queueStore.Open(); err != nil {
			return nil, fmt.Errorf("unable to initialize the queue store of Kafka `%s`: %w", id, err)
		}
	}

	target := &KafkaTarget{
		id:         event.TargetID{ID: id, Name: "kafka"},
		args:       args,
		store:      queueStore,
		loggerOnce: loggerOnce,
		quitCh:     make(chan struct{}),
	}
	if target.store != nil {
		if args.BatchSize > 1 {
			target.batch = store.NewBatch[event.Event](store.BatchConfig[event.Event]{
				Limit:         args.BatchSize,
				Log:           loggerOnce,
				Store:         queueStore,
				CommitTimeout: args.BatchCommitTimeout,
			})
		}
		store.StreamItems(target.store, target, target.quitCh, target.loggerOnce)
	}

	return target, nil
}

func isKafkaConnErr(err error) bool {
	// Sarama opens the circuit breaker after 3 consecutive connection failures.
	return err == sarama.ErrLeaderNotAvailable || err.Error() == "circuit breaker is open"
}

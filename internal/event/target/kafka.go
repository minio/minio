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
	"net"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/minio/minio/internal/event"
	"github.com/minio/minio/internal/logger"
	"github.com/minio/minio/internal/once"
	"github.com/minio/minio/internal/store"
	xnet "github.com/minio/pkg/v2/net"

	"github.com/Shopify/sarama"
	saramatls "github.com/Shopify/sarama/tools/tls"
)

// Kafka input constants
const (
	KafkaBrokers       = "brokers"
	KafkaTopic         = "topic"
	KafkaQueueDir      = "queue_dir"
	KafkaQueueLimit    = "queue_limit"
	KafkaTLS           = "tls"
	KafkaTLSSkipVerify = "tls_skip_verify"
	KafkaTLSClientAuth = "tls_client_auth"
	KafkaSASL          = "sasl"
	KafkaSASLUsername  = "sasl_username"
	KafkaSASLPassword  = "sasl_password"
	KafkaSASLMechanism = "sasl_mechanism"
	KafkaClientTLSCert = "client_tls_cert"
	KafkaClientTLSKey  = "client_tls_key"
	KafkaVersion       = "version"

	EnvKafkaEnable        = "MINIO_NOTIFY_KAFKA_ENABLE"
	EnvKafkaBrokers       = "MINIO_NOTIFY_KAFKA_BROKERS"
	EnvKafkaTopic         = "MINIO_NOTIFY_KAFKA_TOPIC"
	EnvKafkaQueueDir      = "MINIO_NOTIFY_KAFKA_QUEUE_DIR"
	EnvKafkaQueueLimit    = "MINIO_NOTIFY_KAFKA_QUEUE_LIMIT"
	EnvKafkaTLS           = "MINIO_NOTIFY_KAFKA_TLS"
	EnvKafkaTLSSkipVerify = "MINIO_NOTIFY_KAFKA_TLS_SKIP_VERIFY"
	EnvKafkaTLSClientAuth = "MINIO_NOTIFY_KAFKA_TLS_CLIENT_AUTH"
	EnvKafkaSASLEnable    = "MINIO_NOTIFY_KAFKA_SASL"
	EnvKafkaSASLUsername  = "MINIO_NOTIFY_KAFKA_SASL_USERNAME"
	EnvKafkaSASLPassword  = "MINIO_NOTIFY_KAFKA_SASL_PASSWORD"
	EnvKafkaSASLMechanism = "MINIO_NOTIFY_KAFKA_SASL_MECHANISM"
	EnvKafkaClientTLSCert = "MINIO_NOTIFY_KAFKA_CLIENT_TLS_CERT"
	EnvKafkaClientTLSKey  = "MINIO_NOTIFY_KAFKA_CLIENT_TLS_KEY"
	EnvKafkaVersion       = "MINIO_NOTIFY_KAFKA_VERSION"
)

// KafkaArgs - Kafka target arguments.
type KafkaArgs struct {
	Enable     bool        `json:"enable"`
	Brokers    []xnet.Host `json:"brokers"`
	Topic      string      `json:"topic"`
	QueueDir   string      `json:"queueDir"`
	QueueLimit uint64      `json:"queueLimit"`
	Version    string      `json:"version"`
	TLS        struct {
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
	return nil
}

// KafkaTarget - Kafka target.
type KafkaTarget struct {
	initOnce once.Init

	id         event.TargetID
	args       KafkaArgs
	producer   sarama.SyncProducer
	config     *sarama.Config
	store      store.Store[event.Event]
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
	if err := target.args.pingBrokers(); err != nil {
		return false, store.ErrNotConnected
	}
	return true, nil
}

// Save - saves the events to the store which will be replayed when the Kafka connection is active.
func (target *KafkaTarget) Save(eventData event.Event) error {
	if target.store != nil {
		return target.store.Put(eventData)
	}
	if err := target.init(); err != nil {
		return err
	}
	_, err := target.isActive()
	if err != nil {
		return err
	}
	return target.send(eventData)
}

// send - sends an event to the kafka.
func (target *KafkaTarget) send(eventData event.Event) error {
	if target.producer == nil {
		return store.ErrNotConnected
	}
	objectName, err := url.QueryUnescape(eventData.S3.Object.Key)
	if err != nil {
		return err
	}
	key := eventData.S3.Bucket.Name + "/" + objectName

	data, err := json.Marshal(event.Log{EventName: eventData.EventName, Key: key, Records: []event.Event{eventData}})
	if err != nil {
		return err
	}

	msg := sarama.ProducerMessage{
		Topic: target.args.Topic,
		Key:   sarama.StringEncoder(key),
		Value: sarama.ByteEncoder(data),
	}

	_, _, err = target.producer.SendMessage(&msg)

	return err
}

// SendFromStore - reads an event from store and sends it to Kafka.
func (target *KafkaTarget) SendFromStore(eventKey string) error {
	if err := target.init(); err != nil {
		return err
	}

	var err error
	_, err = target.isActive()
	if err != nil {
		return err
	}

	if target.producer == nil {
		brokers := []string{}
		for _, broker := range target.args.Brokers {
			brokers = append(brokers, broker.String())
		}
		target.producer, err = sarama.NewSyncProducer(brokers, target.config)
		if err != nil {
			if err != sarama.ErrOutOfBrokers {
				return err
			}
			return store.ErrNotConnected
		}
	}

	eventData, eErr := target.store.Get(eventKey)
	if eErr != nil {
		// The last event key in a successful batch will be sent in the channel atmost once by the replayEvents()
		// Such events will not exist and wouldve been already been sent successfully.
		if os.IsNotExist(eErr) {
			return nil
		}
		return eErr
	}

	err = target.send(eventData)
	if err != nil {
		// Sarama opens the ciruit breaker after 3 consecutive connection failures.
		if err == sarama.ErrLeaderNotAvailable || err.Error() == "circuit breaker is open" {
			return store.ErrNotConnected
		}
		return err
	}

	// Delete the event from store.
	return target.store.Del(eventKey)
}

// Close - closes underneath kafka connection.
func (target *KafkaTarget) Close() error {
	close(target.quitCh)
	if target.producer != nil {
		return target.producer.Close()
	}
	return nil
}

// Check if atleast one broker in cluster is active
func (k KafkaArgs) pingBrokers() (err error) {
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
			// if one of them is active we are good.
			return nil
		}
		retErr = err
	}
	return retErr
}

func (target *KafkaTarget) init() error {
	return target.initOnce.Do(target.initKafka)
}

func (target *KafkaTarget) initKafka() error {
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

	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 10
	config.Producer.Return.Successes = true

	target.config = config

	brokers := []string{}
	for _, broker := range args.Brokers {
		brokers = append(brokers, broker.String())
	}

	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		if err != sarama.ErrOutOfBrokers {
			target.loggerOnce(context.Background(), err, target.ID().String())
		}
		return err
	}
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
		store.StreamItems(target.store, target, target.quitCh, target.loggerOnce)
	}

	return target, nil
}

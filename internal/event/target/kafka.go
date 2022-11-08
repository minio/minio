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

	"github.com/minio/minio/internal/event"
	"github.com/minio/minio/internal/logger"
	xnet "github.com/minio/pkg/net"

	sarama "github.com/Shopify/sarama"
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
	lazyInit lazyInit

	id         event.TargetID
	args       KafkaArgs
	producer   sarama.SyncProducer
	config     *sarama.Config
	store      Store
	loggerOnce logger.LogOnce
	quitCh     chan struct{}
}

// ID - returns target ID.
func (target *KafkaTarget) ID() event.TargetID {
	return target.id
}

// IsActive - Return true if target is up and active
func (target *KafkaTarget) IsActive() (bool, error) {
	if err := target.init(); err != nil {
		return false, err
	}
	return target.isActive()
}

func (target *KafkaTarget) isActive() (bool, error) {
	if !target.args.pingBrokers() {
		return false, errNotConnected
	}
	return true, nil
}

// Save - saves the events to the store which will be replayed when the Kafka connection is active.
func (target *KafkaTarget) Save(eventData event.Event) error {
	if err := target.init(); err != nil {
		return err
	}

	if target.store != nil {
		return target.store.Put(eventData)
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
		return errNotConnected
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

// Send - reads an event from store and sends it to Kafka.
func (target *KafkaTarget) Send(eventKey string) error {
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
			return errNotConnected
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
			return errNotConnected
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
func (k KafkaArgs) pingBrokers() bool {
	for _, broker := range k.Brokers {
		_, dErr := net.Dial("tcp", broker.String())
		if dErr == nil {
			return true
		}
	}
	return false
}

func (target *KafkaTarget) init() error {
	return target.lazyInit.Do(target.initKafka)
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
		target.producer.Close()
		return err
	}
	target.producer = producer

	yes, err := target.isActive()
	if err != nil {
		return err
	}
	if !yes {
		return errNotConnected
	}

	return nil
}

// NewKafkaTarget - creates new Kafka target with auth credentials.
func NewKafkaTarget(id string, args KafkaArgs, loggerOnce logger.LogOnce) (*KafkaTarget, error) {
	var store Store
	if args.QueueDir != "" {
		queueDir := filepath.Join(args.QueueDir, storePrefix+"-kafka-"+id)
		store = NewQueueStore(queueDir, args.QueueLimit)
		if err := store.Open(); err != nil {
			return nil, fmt.Errorf("unable to initialize the queue store of Kafka `%s`: %w", id, err)
		}
	}

	target := &KafkaTarget{
		id:         event.TargetID{ID: id, Name: "kafka"},
		args:       args,
		store:      store,
		loggerOnce: loggerOnce,
		quitCh:     make(chan struct{}),
	}

	if target.store != nil {
		streamEventsFromStore(target.store, target, target.quitCh, target.loggerOnce)
	}

	return target, nil
}

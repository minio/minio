/*
 * MinIO Cloud Storage, (C) 2018 MinIO, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package target

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"net"
	"net/url"
	"os"
	"path/filepath"

	"github.com/minio/minio/pkg/event"
	xnet "github.com/minio/minio/pkg/net"

	sarama "gopkg.in/Shopify/sarama.v1"
)

// MQTT input constants
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

	EnvKafkaState         = "MINIO_NOTIFY_KAFKA_STATE"
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
)

// KafkaArgs - Kafka target arguments.
type KafkaArgs struct {
	Enable     bool        `json:"enable"`
	Brokers    []xnet.Host `json:"brokers"`
	Topic      string      `json:"topic"`
	QueueDir   string      `json:"queueDir"`
	QueueLimit uint64      `json:"queueLimit"`
	TLS        struct {
		Enable     bool               `json:"enable"`
		RootCAs    *x509.CertPool     `json:"-"`
		SkipVerify bool               `json:"skipVerify"`
		ClientAuth tls.ClientAuthType `json:"clientAuth"`
	} `json:"tls"`
	SASL struct {
		Enable   bool   `json:"enable"`
		User     string `json:"username"`
		Password string `json:"password"`
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
	if k.QueueLimit > 10000 {
		return errors.New("queueLimit should not exceed 10000")
	}
	return nil
}

// KafkaTarget - Kafka target.
type KafkaTarget struct {
	id       event.TargetID
	args     KafkaArgs
	producer sarama.SyncProducer
	config   *sarama.Config
	store    Store
}

// ID - returns target ID.
func (target *KafkaTarget) ID() event.TargetID {
	return target.id
}

// Save - saves the events to the store which will be replayed when the Kafka connection is active.
func (target *KafkaTarget) Save(eventData event.Event) error {
	if target.store != nil {
		return target.store.Put(eventData)
	}
	if !target.args.pingBrokers() {
		return errNotConnected
	}
	return target.send(eventData)
}

// send - sends an event to the kafka.
func (target *KafkaTarget) send(eventData event.Event) error {
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
	var err error

	if !target.args.pingBrokers() {
		return errNotConnected
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

// NewKafkaTarget - creates new Kafka target with auth credentials.
func NewKafkaTarget(id string, args KafkaArgs, doneCh <-chan struct{}, loggerOnce func(ctx context.Context, err error, id interface{}, kind ...interface{})) (*KafkaTarget, error) {
	config := sarama.NewConfig()

	config.Net.SASL.User = args.SASL.User
	config.Net.SASL.Password = args.SASL.Password
	config.Net.SASL.Enable = args.SASL.Enable

	config.Net.TLS.Enable = args.TLS.Enable
	tlsConfig := &tls.Config{
		ClientAuth:         args.TLS.ClientAuth,
		InsecureSkipVerify: args.TLS.SkipVerify,
		RootCAs:            args.TLS.RootCAs,
	}
	config.Net.TLS.Config = tlsConfig

	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 10
	config.Producer.Return.Successes = true

	brokers := []string{}
	for _, broker := range args.Brokers {
		brokers = append(brokers, broker.String())
	}

	var store Store

	if args.QueueDir != "" {
		queueDir := filepath.Join(args.QueueDir, storePrefix+"-kafka-"+id)
		store = NewQueueStore(queueDir, args.QueueLimit)
		if oErr := store.Open(); oErr != nil {
			return nil, oErr
		}
	}

	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		if store == nil || err != sarama.ErrOutOfBrokers {
			return nil, err
		}
	}

	target := &KafkaTarget{
		id:       event.TargetID{ID: id, Name: "kafka"},
		args:     args,
		producer: producer,
		config:   config,
		store:    store,
	}

	if target.store != nil {
		// Replays the events from the store.
		eventKeyCh := replayEvents(target.store, doneCh, loggerOnce, target.ID())
		// Start replaying events from the store.
		go sendEvents(target, eventKeyCh, doneCh, loggerOnce)
	}

	return target, nil
}

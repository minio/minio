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
	id         event.TargetID
	args       KafkaArgs
	producer   sarama.SyncProducer
	config     *sarama.Config
	store      Store
	loggerOnce func(ctx context.Context, err error, id interface{}, errKind ...interface{})
}

// ID - returns target ID.
func (target *KafkaTarget) ID() event.TargetID {
	return target.id
}

// HasQueueStore - Checks if the queueStore has been configured for the target
func (target *KafkaTarget) HasQueueStore() bool {
	return target.store != nil
}

// IsActive - Return true if target is up and active
func (target *KafkaTarget) IsActive() (bool, error) {
	if !target.args.pingBrokers() {
		return false, errNotConnected
	}
	return true, nil
}

// Save - saves the events to the store which will be replayed when the Kafka connection is active.
func (target *KafkaTarget) Save(eventData event.Event) error {
	if target.store != nil {
		return target.store.Put(eventData)
	}
	_, err := target.IsActive()
	if err != nil {
		return err
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
	_, err = target.IsActive()
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
func NewKafkaTarget(id string, args KafkaArgs, doneCh <-chan struct{}, loggerOnce func(ctx context.Context, err error, id interface{}, kind ...interface{}), test bool) (*KafkaTarget, error) {
	config := sarama.NewConfig()

	target := &KafkaTarget{
		id:         event.TargetID{ID: id, Name: "kafka"},
		args:       args,
		loggerOnce: loggerOnce,
	}

	if args.Version != "" {
		kafkaVersion, err := sarama.ParseKafkaVersion(args.Version)
		if err != nil {
			target.loggerOnce(context.Background(), err, target.ID())
			return target, err
		}
		config.Version = kafkaVersion
	}

	config.Net.SASL.User = args.SASL.User
	config.Net.SASL.Password = args.SASL.Password
	if args.SASL.Mechanism == "sha512" {
		config.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &XDGSCRAMClient{HashGeneratorFcn: KafkaSHA512} }
		config.Net.SASL.Mechanism = sarama.SASLMechanism(sarama.SASLTypeSCRAMSHA512)
	} else if args.SASL.Mechanism == "sha256" {
		config.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &XDGSCRAMClient{HashGeneratorFcn: KafkaSHA256} }
		config.Net.SASL.Mechanism = sarama.SASLMechanism(sarama.SASLTypeSCRAMSHA256)
	} else {
		// default to PLAIN
		config.Net.SASL.Mechanism = sarama.SASLMechanism(sarama.SASLTypePlaintext)
	}
	config.Net.SASL.Enable = args.SASL.Enable

	tlsConfig, err := saramatls.NewConfig(args.TLS.ClientTLSCert, args.TLS.ClientTLSKey)

	if err != nil {
		target.loggerOnce(context.Background(), err, target.ID())
		return target, err
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

	var store Store

	if args.QueueDir != "" {
		queueDir := filepath.Join(args.QueueDir, storePrefix+"-kafka-"+id)
		store = NewQueueStore(queueDir, args.QueueLimit)
		if oErr := store.Open(); oErr != nil {
			target.loggerOnce(context.Background(), oErr, target.ID())
			return target, oErr
		}
		target.store = store
	}

	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		if store == nil || err != sarama.ErrOutOfBrokers {
			target.loggerOnce(context.Background(), err, target.ID())
			return target, err
		}
	}
	target.producer = producer

	if target.store != nil && !test {
		// Replays the events from the store.
		eventKeyCh := replayEvents(target.store, doneCh, target.loggerOnce, target.ID())
		// Start replaying events from the store.
		go sendEvents(target, eventKeyCh, doneCh, target.loggerOnce)
	}

	return target, nil
}

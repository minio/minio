/*
 * Minio Cloud Storage, (C) 2018 Minio, Inc.
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
	"crypto/tls"
	"encoding/json"
	"errors"
	"net/url"

	"github.com/minio/minio/pkg/event"
	xnet "github.com/minio/minio/pkg/net"

	sarama "gopkg.in/Shopify/sarama.v1"
)

// KafkaArgs - Kafka target arguments.
type KafkaArgs struct {
	Enable  bool        `json:"enable"`
	Brokers []xnet.Host `json:"brokers"`
	Topic   string      `json:"topic"`
	TLS     struct {
		Enable     bool               `json:"enable"`
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
	return nil
}

// KafkaTarget - Kafka target.
type KafkaTarget struct {
	id       event.TargetID
	args     KafkaArgs
	producer sarama.SyncProducer
}

// ID - returns target ID.
func (target *KafkaTarget) ID() event.TargetID {
	return target.id
}

// Send - sends event to Kafka.
func (target *KafkaTarget) Send(eventData event.Event) error {
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

// Close - closes underneath kafka connection.
func (target *KafkaTarget) Close() error {
	return target.producer.Close()
}

// NewKafkaTarget - creates new Kafka target with auth credentials.
func NewKafkaTarget(id string, args KafkaArgs) (*KafkaTarget, error) {
	config := sarama.NewConfig()

	config.Net.SASL.User = args.SASL.User
	config.Net.SASL.Password = args.SASL.Password
	config.Net.SASL.Enable = args.SASL.Enable

	config.Net.TLS.Enable = args.TLS.Enable
	tlsConfig := &tls.Config{
		ClientAuth: args.TLS.ClientAuth,
	}
	config.Net.TLS.Config = tlsConfig

	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 10
	config.Producer.Return.Successes = true

	brokers := []string{}
	for _, broker := range args.Brokers {
		brokers = append(brokers, broker.String())
	}
	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		return nil, err
	}

	return &KafkaTarget{
		id:       event.TargetID{ID: id, Name: "kafka"},
		args:     args,
		producer: producer,
	}, nil
}

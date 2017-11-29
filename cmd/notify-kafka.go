/*
 * Minio Cloud Storage, (C) 2014-2016 Minio, Inc.
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

package cmd

import (
	"io/ioutil"
	"net"

	"github.com/Sirupsen/logrus"

	sarama "gopkg.in/Shopify/sarama.v1"
)

var (
	kkErrFunc = newNotificationErrorFactory("Kafka")
)

// kafkaNotify holds the configuration of the Kafka server/cluster to
// send notifications to.
type kafkaNotify struct {
	// Flag to enable/disable this notification from configuration
	// file.
	Enable bool `json:"enable"`

	// List of Kafka brokers in `addr:host` format.
	Brokers []string `json:"brokers"`

	// Topic to which event notifications should be sent.
	Topic string `json:"topic"`
}

func (k *kafkaNotify) Validate() error {
	if !k.Enable {
		return nil
	}
	if len(k.Brokers) == 0 {
		return kkErrFunc("No broker(s) specified.")
	}
	// Validate all specified brokers.
	for _, brokerAddr := range k.Brokers {
		if _, _, err := net.SplitHostPort(brokerAddr); err != nil {
			return err
		}
	}
	return nil
}

// kafkaConn contains the active connection to the Kafka cluster and
// the topic to send event notifications to.
type kafkaConn struct {
	producer sarama.SyncProducer
	topic    string
}

func dialKafka(kn kafkaNotify) (kc kafkaConn, e error) {
	if !kn.Enable {
		return kc, errNotifyNotEnabled
	}

	if kn.Topic == "" {
		return kc, kkErrFunc(
			"Topic was not specified in configuration")
	}

	config := sarama.NewConfig()
	// Wait for all in-sync replicas to ack the message
	config.Producer.RequiredAcks = sarama.WaitForAll
	// Retry up to 10 times to produce the message
	config.Producer.Retry.Max = 10
	config.Producer.Return.Successes = true

	p, err := sarama.NewSyncProducer(kn.Brokers, config)
	if err != nil {
		return kc, kkErrFunc("Failed to start producer: %v", err)
	}

	return kafkaConn{p, kn.Topic}, nil
}

func newKafkaNotify(accountID string) (*logrus.Logger, error) {
	kafkaNotifyCfg := globalServerConfig.Notify.GetKafkaByID(accountID)

	// Try connecting to the configured Kafka broker(s).
	kc, err := dialKafka(kafkaNotifyCfg)
	if err != nil {
		return nil, err
	}

	// Configure kafkaConn object as a Hook in logrus.
	kafkaLog := logrus.New()
	kafkaLog.Out = ioutil.Discard
	kafkaLog.Formatter = new(logrus.JSONFormatter)
	kafkaLog.Hooks.Add(kc)

	return kafkaLog, nil
}

func (kC kafkaConn) Close() {
	_ = kC.producer.Close()
}

// Fire - to implement logrus.Hook interface
func (kC kafkaConn) Fire(entry *logrus.Entry) error {
	body, err := entry.Reader()
	if err != nil {
		return err
	}

	// Extract the key of the event as a string
	keyStr, ok := entry.Data["Key"].(string)
	if !ok {
		return kkErrFunc("Unable to convert event key %v to string.",
			entry.Data["Key"])
	}

	// Construct message to send to Kafka
	msg := sarama.ProducerMessage{
		Topic: kC.topic,
		Key:   sarama.StringEncoder(keyStr),
		Value: sarama.ByteEncoder(body.Bytes()),
	}

	// Attempt sending the message to Kafka
	_, _, err = kC.producer.SendMessage(&msg)
	if err != nil {
		return kkErrFunc("Error sending event to Kafka - %v", err)
	}
	return nil
}

// Levels - to implement logrus.Hook interface
func (kC kafkaConn) Levels() []logrus.Level {
	return []logrus.Level{
		logrus.InfoLevel,
	}
}

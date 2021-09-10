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

package kafka

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"net"

	sarama "github.com/Shopify/sarama"
	saramatls "github.com/Shopify/sarama/tools/tls"

	"github.com/minio/minio/internal/logger/message/audit"
	xnet "github.com/minio/pkg/net"
)

// Target - Kafka target.
type Target struct {
	// Channel of log entries
	logCh chan interface{}

	producer sarama.SyncProducer
	kconfig  Config
	config   *sarama.Config
}

// Send log message 'e' to kafka target.
func (h *Target) Send(entry interface{}, errKind string) error {
	select {
	case h.logCh <- entry:
	default:
		// log channel is full, do not wait and return
		// an error immediately to the caller
		return errors.New("log buffer full")
	}

	return nil
}

func (h *Target) startKakfaLogger() {
	// Create a routine which sends json logs received
	// from an internal channel.
	go func() {
		for entry := range h.logCh {
			logJSON, err := json.Marshal(&entry)
			if err != nil {
				continue
			}

			ae, ok := entry.(audit.Entry)
			if ok {
				msg := sarama.ProducerMessage{
					Topic: h.kconfig.Topic,
					Key:   sarama.StringEncoder(ae.RequestID),
					Value: sarama.ByteEncoder(logJSON),
				}

				_, _, err = h.producer.SendMessage(&msg)
				if err != nil {
					h.kconfig.LogOnce(context.Background(), err, h.kconfig.Topic)
					continue
				}
			}
		}
	}()
}

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

	// Custom logger
	LogOnce func(ctx context.Context, err error, id interface{}, errKind ...interface{}) `json:"-"`
}

// Check if atleast one broker in cluster is active
func (k Config) pingBrokers() error {
	var err error
	for _, broker := range k.Brokers {
		_, err1 := net.Dial("tcp", broker.String())
		if err1 != nil {
			if err == nil {
				// Set first error
				err = err1
			}
		}
	}
	return err
}

// Endpoint - return kafka target
func (h *Target) Endpoint() string {
	return "kafka"
}

// String - kafka string
func (h *Target) String() string {
	return "kafka"
}

// Init initialize kafka target
func (h *Target) Init() error {
	if !h.kconfig.Enabled {
		return nil
	}
	if len(h.kconfig.Brokers) == 0 {
		return errors.New("no broker address found")
	}
	for _, b := range h.kconfig.Brokers {
		if _, err := xnet.ParseHost(b.String()); err != nil {
			return err
		}
	}
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

	go h.startKakfaLogger()
	return nil
}

// New initializes a new logger target which
// sends log over http to the specified endpoint
func New(config Config) *Target {
	target := &Target{
		logCh:   make(chan interface{}, 10000),
		kconfig: config,
	}
	return target
}

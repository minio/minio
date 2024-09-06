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
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/minio/minio/internal/event"
	"github.com/minio/minio/internal/logger"
	"github.com/minio/minio/internal/once"
	"github.com/minio/minio/internal/store"
	xnet "github.com/minio/pkg/v3/net"
)

const (
	reconnectInterval = 5 * time.Second
	storePrefix       = "minio"
)

// MQTT input constants
const (
	MqttBroker            = "broker"
	MqttTopic             = "topic"
	MqttQoS               = "qos"
	MqttUsername          = "username"
	MqttPassword          = "password"
	MqttReconnectInterval = "reconnect_interval"
	MqttKeepAliveInterval = "keep_alive_interval"
	MqttQueueDir          = "queue_dir"
	MqttQueueLimit        = "queue_limit"

	EnvMQTTEnable            = "MINIO_NOTIFY_MQTT_ENABLE"
	EnvMQTTBroker            = "MINIO_NOTIFY_MQTT_BROKER"
	EnvMQTTTopic             = "MINIO_NOTIFY_MQTT_TOPIC"
	EnvMQTTQoS               = "MINIO_NOTIFY_MQTT_QOS"
	EnvMQTTUsername          = "MINIO_NOTIFY_MQTT_USERNAME"
	EnvMQTTPassword          = "MINIO_NOTIFY_MQTT_PASSWORD"
	EnvMQTTReconnectInterval = "MINIO_NOTIFY_MQTT_RECONNECT_INTERVAL"
	EnvMQTTKeepAliveInterval = "MINIO_NOTIFY_MQTT_KEEP_ALIVE_INTERVAL"
	EnvMQTTQueueDir          = "MINIO_NOTIFY_MQTT_QUEUE_DIR"
	EnvMQTTQueueLimit        = "MINIO_NOTIFY_MQTT_QUEUE_LIMIT"
)

// MQTTArgs - MQTT target arguments.
type MQTTArgs struct {
	Enable               bool           `json:"enable"`
	Broker               xnet.URL       `json:"broker"`
	Topic                string         `json:"topic"`
	QoS                  byte           `json:"qos"`
	User                 string         `json:"username"`
	Password             string         `json:"password"`
	MaxReconnectInterval time.Duration  `json:"reconnectInterval"`
	KeepAlive            time.Duration  `json:"keepAliveInterval"`
	RootCAs              *x509.CertPool `json:"-"`
	QueueDir             string         `json:"queueDir"`
	QueueLimit           uint64         `json:"queueLimit"`
}

// Validate MQTTArgs fields
func (m MQTTArgs) Validate() error {
	if !m.Enable {
		return nil
	}
	u, err := xnet.ParseURL(m.Broker.String())
	if err != nil {
		return err
	}
	switch u.Scheme {
	case "ws", "wss", "tcp", "ssl", "tls", "tcps":
	default:
		return errors.New("unknown protocol in broker address")
	}
	if m.QueueDir != "" {
		if !filepath.IsAbs(m.QueueDir) {
			return errors.New("queueDir path should be absolute")
		}
		if m.QoS == 0 {
			return errors.New("qos should be set to 1 or 2 if queueDir is set")
		}
	}

	return nil
}

// MQTTTarget - MQTT target.
type MQTTTarget struct {
	initOnce once.Init

	id         event.TargetID
	args       MQTTArgs
	client     mqtt.Client
	store      store.Store[event.Event]
	quitCh     chan struct{}
	loggerOnce logger.LogOnce
}

// ID - returns target ID.
func (target *MQTTTarget) ID() event.TargetID {
	return target.id
}

// Name - returns the Name of the target.
func (target *MQTTTarget) Name() string {
	return target.ID().String()
}

// Store returns any underlying store if set.
func (target *MQTTTarget) Store() event.TargetStore {
	return target.store
}

// IsActive - Return true if target is up and active
func (target *MQTTTarget) IsActive() (bool, error) {
	if err := target.init(); err != nil {
		return false, err
	}
	return target.isActive()
}

func (target *MQTTTarget) isActive() (bool, error) {
	if !target.client.IsConnectionOpen() {
		return false, store.ErrNotConnected
	}
	return true, nil
}

// send - sends an event to the mqtt.
func (target *MQTTTarget) send(eventData event.Event) error {
	objectName, err := url.QueryUnescape(eventData.S3.Object.Key)
	if err != nil {
		return err
	}
	key := eventData.S3.Bucket.Name + "/" + objectName

	data, err := json.Marshal(event.Log{EventName: eventData.EventName, Key: key, Records: []event.Event{eventData}})
	if err != nil {
		return err
	}

	token := target.client.Publish(target.args.Topic, target.args.QoS, false, string(data))
	if !token.WaitTimeout(reconnectInterval) {
		return store.ErrNotConnected
	}
	return token.Error()
}

// SendFromStore - reads an event from store and sends it to MQTT.
func (target *MQTTTarget) SendFromStore(key store.Key) error {
	if err := target.init(); err != nil {
		return err
	}

	// Do not send if the connection is not active.
	_, err := target.isActive()
	if err != nil {
		return err
	}

	eventData, err := target.store.Get(key)
	if err != nil {
		// The last event key in a successful batch will be sent in the channel atmost once by the replayEvents()
		// Such events will not exist and wouldve been already been sent successfully.
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	if err = target.send(eventData); err != nil {
		return err
	}

	// Delete the event from store.
	return target.store.Del(key)
}

// Save - saves the events to the store if queuestore is configured, which will
// be replayed when the mqtt connection is active.
func (target *MQTTTarget) Save(eventData event.Event) error {
	if target.store != nil {
		_, err := target.store.Put(eventData)
		return err
	}
	if err := target.init(); err != nil {
		return err
	}

	// Do not send if the connection is not active.
	_, err := target.isActive()
	if err != nil {
		return err
	}

	return target.send(eventData)
}

// Close - does nothing and available for interface compatibility.
func (target *MQTTTarget) Close() error {
	if target.client != nil {
		target.client.Disconnect(100)
	}
	close(target.quitCh)
	return nil
}

func (target *MQTTTarget) init() error {
	return target.initOnce.Do(target.initMQTT)
}

func (target *MQTTTarget) initMQTT() error {
	args := target.args

	// Using hex here, to make sure we avoid 23
	// character limit on client_id according to
	// MQTT spec.
	clientID := fmt.Sprintf("%x", time.Now().UnixNano())

	options := mqtt.NewClientOptions().
		SetClientID(clientID).
		SetCleanSession(true).
		SetUsername(args.User).
		SetPassword(args.Password).
		SetMaxReconnectInterval(args.MaxReconnectInterval).
		SetKeepAlive(args.KeepAlive).
		SetTLSConfig(&tls.Config{RootCAs: args.RootCAs}).
		AddBroker(args.Broker.String())

	target.client = mqtt.NewClient(options)

	token := target.client.Connect()
	ok := token.WaitTimeout(reconnectInterval)
	if !ok {
		return store.ErrNotConnected
	}
	if token.Error() != nil {
		return token.Error()
	}

	yes, err := target.isActive()
	if err != nil {
		return err
	}
	if !yes {
		return store.ErrNotConnected
	}

	return nil
}

// NewMQTTTarget - creates new MQTT target.
func NewMQTTTarget(id string, args MQTTArgs, loggerOnce logger.LogOnce) (*MQTTTarget, error) {
	if args.MaxReconnectInterval == 0 {
		// Default interval
		// https://github.com/eclipse/paho.mqtt.golang/blob/master/options.go#L115
		args.MaxReconnectInterval = 10 * time.Minute
	}

	if args.KeepAlive == 0 {
		args.KeepAlive = 10 * time.Second
	}

	var queueStore store.Store[event.Event]
	if args.QueueDir != "" {
		queueDir := filepath.Join(args.QueueDir, storePrefix+"-mqtt-"+id)
		queueStore = store.NewQueueStore[event.Event](queueDir, args.QueueLimit, event.StoreExtension)
		if err := queueStore.Open(); err != nil {
			return nil, fmt.Errorf("unable to initialize the queue store of MQTT `%s`: %w", id, err)
		}
	}

	target := &MQTTTarget{
		id:         event.TargetID{ID: id, Name: "mqtt"},
		args:       args,
		store:      queueStore,
		quitCh:     make(chan struct{}),
		loggerOnce: loggerOnce,
	}

	if target.store != nil {
		store.StreamItems(target.store, target, target.quitCh, target.loggerOnce)
	}

	return target, nil
}

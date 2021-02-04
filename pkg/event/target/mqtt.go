/*
 * MinIO Cloud Storage, (C) 2018-2019 MinIO, Inc.
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
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/minio/minio/pkg/event"
	xnet "github.com/minio/minio/pkg/net"
)

const (
	reconnectInterval = 5 // In Seconds
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
	id         event.TargetID
	args       MQTTArgs
	client     mqtt.Client
	store      Store
	quitCh     chan struct{}
	loggerOnce func(ctx context.Context, err error, id interface{}, kind ...interface{})
}

// ID - returns target ID.
func (target *MQTTTarget) ID() event.TargetID {
	return target.id
}

// HasQueueStore - Checks if the queueStore has been configured for the target
func (target *MQTTTarget) HasQueueStore() bool {
	return target.store != nil
}

// IsActive - Return true if target is up and active
func (target *MQTTTarget) IsActive() (bool, error) {
	if !target.client.IsConnectionOpen() {
		return false, errNotConnected
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
	if !token.WaitTimeout(reconnectInterval * time.Second) {
		return errNotConnected
	}
	return token.Error()
}

// Send - reads an event from store and sends it to MQTT.
func (target *MQTTTarget) Send(eventKey string) error {
	// Do not send if the connection is not active.
	_, err := target.IsActive()
	if err != nil {
		return err
	}

	eventData, err := target.store.Get(eventKey)
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
	return target.store.Del(eventKey)
}

// Save - saves the events to the store if queuestore is configured, which will
// be replayed when the mqtt connection is active.
func (target *MQTTTarget) Save(eventData event.Event) error {
	if target.store != nil {
		return target.store.Put(eventData)
	}

	// Do not send if the connection is not active.
	_, err := target.IsActive()
	if err != nil {
		return err
	}

	return target.send(eventData)
}

// Close - does nothing and available for interface compatibility.
func (target *MQTTTarget) Close() error {
	target.client.Disconnect(100)
	close(target.quitCh)
	return nil
}

// NewMQTTTarget - creates new MQTT target.
func NewMQTTTarget(id string, args MQTTArgs, doneCh <-chan struct{}, loggerOnce func(ctx context.Context, err error, id interface{}, kind ...interface{}), test bool) (*MQTTTarget, error) {
	if args.MaxReconnectInterval == 0 {
		// Default interval
		// https://github.com/eclipse/paho.mqtt.golang/blob/master/options.go#L115
		args.MaxReconnectInterval = 10 * time.Minute
	}

	options := mqtt.NewClientOptions().
		SetClientID("").
		SetCleanSession(true).
		SetUsername(args.User).
		SetPassword(args.Password).
		SetMaxReconnectInterval(args.MaxReconnectInterval).
		SetKeepAlive(args.KeepAlive).
		SetTLSConfig(&tls.Config{RootCAs: args.RootCAs}).
		AddBroker(args.Broker.String())

	client := mqtt.NewClient(options)

	target := &MQTTTarget{
		id:         event.TargetID{ID: id, Name: "mqtt"},
		args:       args,
		client:     client,
		quitCh:     make(chan struct{}),
		loggerOnce: loggerOnce,
	}

	token := client.Connect()
	retryRegister := func() {
		for {
		retry:
			select {
			case <-doneCh:
				return
			case <-target.quitCh:
				return
			default:
				ok := token.WaitTimeout(reconnectInterval * time.Second)
				if ok && token.Error() != nil {
					target.loggerOnce(context.Background(),
						fmt.Errorf("Previous connect failed with %w attempting a reconnect",
							token.Error()),
						target.ID())
					time.Sleep(reconnectInterval * time.Second)
					token = client.Connect()
					goto retry
				}
				if ok {
					// Successfully connected.
					return
				}
			}
		}
	}

	if args.QueueDir != "" {
		queueDir := filepath.Join(args.QueueDir, storePrefix+"-mqtt-"+id)
		target.store = NewQueueStore(queueDir, args.QueueLimit)
		if err := target.store.Open(); err != nil {
			target.loggerOnce(context.Background(), err, target.ID())
			return target, err
		}

		if !test {
			go retryRegister()
			// Replays the events from the store.
			eventKeyCh := replayEvents(target.store, doneCh, target.loggerOnce, target.ID())
			// Start replaying events from the store.
			go sendEvents(target, eventKeyCh, doneCh, target.loggerOnce)
		}
	} else {
		if token.Wait() && token.Error() != nil {
			return target, token.Error()
		}
	}
	return target, nil
}

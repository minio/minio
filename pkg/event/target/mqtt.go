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
	"crypto/x509"
	"encoding/json"
	"errors"
	"net/url"
	"path/filepath"
	"time"

	"github.com/eclipse/paho.mqtt.golang"
	"github.com/minio/minio/pkg/event"
	xnet "github.com/minio/minio/pkg/net"
)

const (
	retryInterval     = 3 // In Seconds
	reconnectInterval = 5 // In Seconds
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
	QueueLimit           uint16         `json:"queueLimit"`
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
	id     event.TargetID
	args   MQTTArgs
	client mqtt.Client
	store  Store
	reconn bool
}

// ID - returns target ID.
func (target *MQTTTarget) ID() event.TargetID {
	return target.id
}

// Reads persisted events from the store and re-plays.
func (target *MQTTTarget) retry() {
	target.reconn = true
	events := target.store.ListAll()
	for len(events) != 0 {
		for _, key := range events {
			event, eErr := target.store.Get(key)
			if eErr != nil {
				continue
			}
			for !target.client.IsConnectionOpen() {
				time.Sleep(retryInterval * time.Second)
			}
			// The connection is open.
			if err := target.send(event); err != nil {
				continue
			}
			// Delete after a successful publish.
			target.store.Del(key)
		}
		events = target.store.ListAll()
	}
	// Release the reconn state.
	target.reconn = false
}

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
	token.Wait()
	return token.Error()

}

// Send - sends event to MQTT when the connection is active.
func (target *MQTTTarget) Send(eventData event.Event) error {
	// Persist the events if the connection is not active.
	if !target.client.IsConnectionOpen() {
		if err := target.store.Put(eventData); err != nil {
			return err
		}
		// Ignore if retry is triggered already.
		if !target.reconn {
			go target.retry()
		}
		return nil
	}

	// Publishes to the broker as the connection is active.
	return target.send(eventData)
}

// Close - does nothing and available for interface compatibility.
func (target *MQTTTarget) Close() error {
	return nil
}

// NewMQTTTarget - creates new MQTT target.
func NewMQTTTarget(id string, args MQTTArgs) (*MQTTTarget, error) {
	options := mqtt.NewClientOptions().
		SetClientID("").
		SetCleanSession(true).
		SetUsername(args.User).
		SetPassword(args.Password).
		SetMaxReconnectInterval(args.MaxReconnectInterval).
		SetKeepAlive(args.KeepAlive).
		SetTLSConfig(&tls.Config{RootCAs: args.RootCAs}).
		AddBroker(args.Broker.String())

	var store Store

	if args.QueueDir != "" {
		store = NewQueueStore(args.QueueDir, args.QueueLimit)
		if oErr := store.Open(); oErr != nil {
			return nil, oErr
		}
	} else {
		store = NewMemoryStore(args.QueueLimit)
	}

	client := mqtt.NewClient(options)

	// The client should establish a first time connection.
	// Connect() should be successful atleast once to publish events.
	token := client.Connect()

	go func() {
		// Repeat the pings until the client registers the clientId and receives a token.
		for {
			if token.Wait() && token.Error() == nil {
				// Connected
				break
			}
			// Reconnecting
			time.Sleep(reconnectInterval * time.Second)
			token = client.Connect()
		}
	}()

	target := &MQTTTarget{
		id:     event.TargetID{ID: id, Name: "mqtt"},
		args:   args,
		client: client,
		store:  store,
		reconn: false,
	}

	// Replay any previously persisted events in the store.
	if len(target.store.ListAll()) != 0 {
		go target.retry()

	}

	return target, nil
}

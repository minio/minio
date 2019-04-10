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
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"net/url"
	"os"
	"path/filepath"
	"time"

	"github.com/eclipse/paho.mqtt.golang"
	"github.com/minio/minio/pkg/event"
	xnet "github.com/minio/minio/pkg/net"
)

const (
	reconnectInterval = 5 // In Seconds
	storePrefix       = "minio"
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
}

// ID - returns target ID.
func (target *MQTTTarget) ID() event.TargetID {
	return target.id
}

// Send - sends event to MQTT.
func (target *MQTTTarget) Send(eventKey string) error {

	if !target.client.IsConnectionOpen() {
		return errNotConnected
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
	if token.Error() != nil {
		return token.Error()
	}

	// Delete the event from store.
	return target.store.Del(eventKey)
}

// Save - saves the events to the store which will be replayed when the mqtt connection is active.
func (target *MQTTTarget) Save(eventData event.Event) error {
	return target.store.Put(eventData)
}

// Close - does nothing and available for interface compatibility.
func (target *MQTTTarget) Close() error {
	return nil
}

// NewMQTTTarget - creates new MQTT target.
func NewMQTTTarget(id string, args MQTTArgs, doneCh <-chan struct{}) (*MQTTTarget, error) {
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
		queueDir := filepath.Join(args.QueueDir, storePrefix+"-mqtt-"+id)
		store = NewQueueStore(queueDir, args.QueueLimit)
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
	}

	// Replays the events from the store.
	eventKeyCh := replayEvents(target.store, doneCh)

	// Start replaying events from the store.
	go sendEvents(target, eventKeyCh, doneCh)

	return target, nil
}

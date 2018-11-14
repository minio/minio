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
	"time"

	"github.com/eclipse/paho.mqtt.golang"
	"github.com/minio/minio/pkg/event"
	xnet "github.com/minio/minio/pkg/net"
)

// MQTTArgs - MQTT target arguments.
type MQTTArgs struct {
	Enable               bool           `json:"enable"`
	Broker               xnet.URL       `json:"broker"`
	Topic                string         `json:"topic"`
	QoS                  byte           `json:"qos"`
	ClientID             string         `json:"clientId"`
	User                 string         `json:"username"`
	Password             string         `json:"password"`
	MaxReconnectInterval time.Duration  `json:"reconnectInterval"`
	KeepAlive            time.Duration  `json:"keepAliveInterval"`
	RootCAs              *x509.CertPool `json:"-"`
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
	return nil
}

// MQTTTarget - MQTT target.
type MQTTTarget struct {
	id     event.TargetID
	args   MQTTArgs
	client mqtt.Client
}

// ID - returns target ID.
func (target *MQTTTarget) ID() event.TargetID {
	return target.id
}

// Send - sends event to MQTT.
func (target *MQTTTarget) Send(eventData event.Event) error {
	if !target.client.IsConnected() {
		token := target.client.Connect()
		if token.Wait() {
			if err := token.Error(); err != nil {
				return err
			}
		}
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

	if token.Wait() {
		return token.Error()
	}

	return nil
}

// Close - does nothing and available for interface compatibility.
func (target *MQTTTarget) Close() error {
	return nil
}

// NewMQTTTarget - creates new MQTT target.
func NewMQTTTarget(id string, args MQTTArgs) (*MQTTTarget, error) {
	options := mqtt.NewClientOptions().
		SetClientID(args.ClientID).
		SetCleanSession(true).
		SetUsername(args.User).
		SetPassword(args.Password).
		SetMaxReconnectInterval(args.MaxReconnectInterval).
		SetKeepAlive(args.KeepAlive).
		SetTLSConfig(&tls.Config{RootCAs: args.RootCAs}).
		AddBroker(args.Broker.String())

	client := mqtt.NewClient(options)
	token := client.Connect()
	if token.Wait() && token.Error() != nil {
		return nil, token.Error()
	}

	return &MQTTTarget{
		id:     event.TargetID{ID: id, Name: "mqtt"},
		args:   args,
		client: client,
	}, nil
}

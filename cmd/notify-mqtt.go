/*
 * Minio Cloud Storage, (C) 2016 Minio, Inc.
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
	"crypto/tls"
	"io/ioutil"
	"time"

	"github.com/Sirupsen/logrus"
	MQTT "github.com/eclipse/paho.mqtt.golang"
)

type mqttNotify struct {
	Enable   bool   `json:"enable"`
	Broker   string `json:"broker"`
	Topic    string `json:"topic"`
	QoS      int    `json:"qos"`
	ClientID string `json:"clientId"`
	User     string `json:"username"`
	Password string `json:"password"`
}

func (m *mqttNotify) Validate() error {
	if !m.Enable {
		return nil
	}
	if _, err := checkURL(m.Broker); err != nil {
		return err
	}
	return nil
}

type mqttConn struct {
	params mqttNotify
	Client MQTT.Client
}

func dialMQTT(mqttL mqttNotify) (mc mqttConn, e error) {
	if !mqttL.Enable {
		return mc, errNotifyNotEnabled
	}
	connOpts := &MQTT.ClientOptions{
		ClientID:             mqttL.ClientID,
		CleanSession:         true,
		Username:             mqttL.User,
		Password:             mqttL.Password,
		MaxReconnectInterval: 1 * time.Second,
		KeepAlive:            30 * time.Second,
		TLSConfig:            tls.Config{RootCAs: globalRootCAs},
	}
	connOpts.AddBroker(mqttL.Broker)
	client := MQTT.NewClient(connOpts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		return mc, token.Error()
	}
	return mqttConn{Client: client, params: mqttL}, nil
}

func newMQTTNotify(accountID string) (*logrus.Logger, error) {
	mqttL := globalServerConfig.Notify.GetMQTTByID(accountID)

	//connect to MQTT Server
	mqttC, err := dialMQTT(mqttL)
	if err != nil {
		return nil, err
	}

	mqttLog := logrus.New()

	// Disable writing to console.
	mqttLog.Out = ioutil.Discard

	// Add a mqtt hook.
	mqttLog.Hooks.Add(mqttC)

	// Set default JSON formatter
	mqttLog.Formatter = new(logrus.JSONFormatter)

	// successfully enabled all MQTTs
	return mqttLog, nil
}

// Fire if called when an event should be sent to the message broker.
func (q mqttConn) Fire(entry *logrus.Entry) error {
	body, err := entry.String()
	if err != nil {
		return err
	}

	if !q.Client.IsConnected() {
		if token := q.Client.Connect(); token.Wait() && token.Error() != nil {
			return token.Error()
		}
	}
	token := q.Client.Publish(q.params.Topic, byte(q.params.QoS), false, body)
	if token.Wait() && token.Error() != nil {
		return token.Error()
	}

	return nil
}

// Levels is available logging levels.
func (q mqttConn) Levels() []logrus.Level {
	return []logrus.Level{
		logrus.InfoLevel,
	}
}

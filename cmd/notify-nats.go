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
	"io/ioutil"

	"github.com/Sirupsen/logrus"
	"github.com/nats-io/nats"
)

// natsNotify - represents logrus compatible NATS hook.
// All fields represent NATS configuration details.
type natsNotify struct {
	Enable       bool   `json:"enable"`
	Address      string `json:"address"`
	Subject      string `json:"subject"`
	Username     string `json:"username"`
	Password     string `json:"password"`
	Token        string `json:"token"`
	Secure       bool   `json:"secure"`
	PingInterval int64  `json:"pingInterval"`
}

type natsConn struct {
	params natsNotify
	*nats.Conn
}

// dialNATS - dials and returns an natsConn instance,
// for sending notifications. Returns error if nats logger
// is not enabled.
func dialNATS(natsL natsNotify) (natsConn, error) {
	if !natsL.Enable {
		return natsConn{}, errNotifyNotEnabled
	}
	// Configure and connect to NATS server
	natsC := nats.DefaultOptions
	natsC.Url = "nats://" + natsL.Address
	natsC.User = natsL.Username
	natsC.Password = natsL.Password
	natsC.Token = natsL.Token
	natsC.Secure = natsL.Secure
	conn, err := natsC.Connect()
	if err != nil {
		return natsConn{}, err
	}
	return natsConn{Conn: conn, params: natsL}, nil
}

func newNATSNotify(accountID string) (*logrus.Logger, error) {
	natsL := serverConfig.GetNATSNotifyByID(accountID)

	// Connect to nats server.
	natsC, err := dialNATS(natsL)
	if err != nil {
		return nil, err
	}

	natsLog := logrus.New()

	// Disable writing to console.
	natsLog.Out = ioutil.Discard

	// Add a nats hook.
	natsLog.Hooks.Add(natsC)

	// Set default JSON formatter.
	natsLog.Formatter = new(logrus.JSONFormatter)

	// Successfully enabled all NATSs.
	return natsLog, nil
}

// Fire is called when an event should be sent to the message broker
func (n natsConn) Fire(entry *logrus.Entry) error {
	ch := n.Conn
	body, err := entry.Reader()
	if err != nil {
		return err
	}
	err = ch.Publish(n.params.Subject, body.Bytes())
	if err != nil {
		return err
	}
	return nil
}

// Levels is available logging levels.
func (n natsConn) Levels() []logrus.Level {
	return []logrus.Level{
		logrus.InfoLevel,
	}
}

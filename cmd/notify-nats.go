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
	"net"

	"github.com/Sirupsen/logrus"
	"github.com/nats-io/go-nats-streaming"
	"github.com/nats-io/nats"
)

// natsNotifyStreaming contains specific options related to connection
// to a NATS streaming server
type natsNotifyStreaming struct {
	Enable             bool   `json:"enable"`
	ClusterID          string `json:"clusterID"`
	ClientID           string `json:"clientID"`
	Async              bool   `json:"async"`
	MaxPubAcksInflight int    `json:"maxPubAcksInflight"`
}

// natsNotify - represents logrus compatible NATS hook.
// All fields represent NATS configuration details.
type natsNotify struct {
	Enable       bool                `json:"enable"`
	Address      string              `json:"address"`
	Subject      string              `json:"subject"`
	Username     string              `json:"username"`
	Password     string              `json:"password"`
	Token        string              `json:"token"`
	Secure       bool                `json:"secure"`
	PingInterval int64               `json:"pingInterval"`
	Streaming    natsNotifyStreaming `json:"streaming"`
}

func (n *natsNotify) Validate() error {
	if !n.Enable {
		return nil
	}
	if _, _, err := net.SplitHostPort(n.Address); err != nil {
		return err
	}
	return nil
}

// natsIOConn abstracts connection to any type of NATS server
type natsIOConn struct {
	params   natsNotify
	natsConn *nats.Conn
	stanConn stan.Conn
}

// dialNATS - dials and returns an natsIOConn instance,
// for sending notifications. Returns error if nats logger
// is not enabled.
func dialNATS(natsL natsNotify, testDial bool) (nioc natsIOConn, e error) {
	if !natsL.Enable {
		return nioc, errNotifyNotEnabled
	}

	// Construct natsIOConn which holds all NATS connection information
	conn := natsIOConn{params: natsL}

	if natsL.Streaming.Enable {
		// Construct scheme to differentiate between clear and TLS connections
		scheme := "nats"
		if natsL.Secure {
			scheme = "tls"
		}
		// Construct address URL
		addressURL := scheme + "://" + natsL.Username + ":" + natsL.Password + "@" + natsL.Address
		// Fetch the user-supplied client ID and provide a random one if not provided
		clientID := natsL.Streaming.ClientID
		if clientID == "" {
			clientID = mustGetUUID()
		}
		// Add test suffix to clientID to avoid clientID already registered error
		if testDial {
			clientID += "-test"
		}
		connOpts := []stan.Option{
			stan.NatsURL(addressURL),
		}
		// Setup MaxPubAcksInflight parameter
		if natsL.Streaming.MaxPubAcksInflight > 0 {
			connOpts = append(connOpts,
				stan.MaxPubAcksInflight(natsL.Streaming.MaxPubAcksInflight))
		}
		// Do the real connection to the NATS server
		sc, err := stan.Connect(natsL.Streaming.ClusterID, clientID, connOpts...)
		if err != nil {
			return nioc, err
		}
		// Save the created connection
		conn.stanConn = sc
	} else {
		// Configure and connect to NATS server
		natsC := nats.DefaultOptions
		natsC.Url = "nats://" + natsL.Address
		natsC.User = natsL.Username
		natsC.Password = natsL.Password
		natsC.Token = natsL.Token
		natsC.Secure = natsL.Secure
		// Do the real connection
		nc, err := natsC.Connect()
		if err != nil {
			return nioc, err
		}
		// Save the created connection
		conn.natsConn = nc
	}
	return conn, nil
}

// closeNATS - close the underlying NATS connection
func closeNATS(conn natsIOConn) {
	if conn.params.Streaming.Enable {
		conn.stanConn.Close()
	} else {
		conn.natsConn.Close()
	}
}

func newNATSNotify(accountID string) (*logrus.Logger, error) {
	natsL := globalServerConfig.Notify.GetNATSByID(accountID)

	// Connect to nats server.
	natsC, err := dialNATS(natsL, false)
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
func (n natsIOConn) Fire(entry *logrus.Entry) error {
	body, err := entry.Reader()
	if err != nil {
		return err
	}
	if n.params.Streaming.Enable {
		// Streaming flag is enabled, publish the log synchronously or asynchronously
		// depending on the user supplied parameter
		if n.params.Streaming.Async {
			_, err = n.stanConn.PublishAsync(n.params.Subject, body.Bytes(), nil)
		} else {
			err = n.stanConn.Publish(n.params.Subject, body.Bytes())
		}
		if err != nil {
			return err
		}
	} else {
		// Publish the log
		err = n.natsConn.Publish(n.params.Subject, body.Bytes())
		if err != nil {
			return err
		}
	}
	return nil
}

// Levels is available logging levels.
func (n natsIOConn) Levels() []logrus.Level {
	return []logrus.Level{
		logrus.InfoLevel,
	}
}

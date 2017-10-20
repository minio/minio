/*
 * Minio Cloud Storage, (C) 2016, 2017 Minio, Inc.
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
	"sync"

	"github.com/Sirupsen/logrus"
	"github.com/streadway/amqp"
)

// amqpNotify - represents logrus compatible AMQP hook.
// All fields represent AMQP configuration details.
type amqpNotify struct {
	Enable       bool   `json:"enable"`
	URL          string `json:"url"`
	Exchange     string `json:"exchange"`
	RoutingKey   string `json:"routingKey"`
	ExchangeType string `json:"exchangeType"`
	DeliveryMode uint8  `json:"deliveryMode"`
	Mandatory    bool   `json:"mandatory"`
	Immediate    bool   `json:"immediate"`
	Durable      bool   `json:"durable"`
	Internal     bool   `json:"internal"`
	NoWait       bool   `json:"noWait"`
	AutoDeleted  bool   `json:"autoDeleted"`
}

func (a *amqpNotify) Validate() error {
	if !a.Enable {
		return nil
	}
	if _, err := checkURL(a.URL); err != nil {
		return err
	}
	return nil
}

// amqpConn implements a reconnecting amqp conn extending *amqp.Connection,
// also provides additional protection for such a mutation.
type amqpConn struct {
	sync.Mutex
	conn   *amqp.Connection
	params amqpNotify
}

// dialAMQP - dials and returns an amqpConnection instance,
// for sending notifications. Returns error if amqp logger
// is not enabled.
func dialAMQP(amqpL amqpNotify) (*amqpConn, error) {
	if !amqpL.Enable {
		return nil, errNotifyNotEnabled
	}
	conn, err := amqp.Dial(amqpL.URL)
	if err != nil {
		return nil, err
	}
	return &amqpConn{
		conn:   conn,
		params: amqpL,
	}, nil
}

func newAMQPNotify(accountID string) (*logrus.Logger, error) {
	amqpL := serverConfig.Notify.GetAMQPByID(accountID)

	// Connect to amqp server.
	amqpC, err := dialAMQP(amqpL)
	if err != nil {
		return nil, err
	}

	amqpLog := logrus.New()

	// Disable writing to console.
	amqpLog.Out = ioutil.Discard

	// Add a amqp hook.
	amqpLog.Hooks.Add(amqpC)

	// Set default JSON formatter.
	amqpLog.Formatter = new(logrus.JSONFormatter)

	// Successfully enabled all AMQPs.
	return amqpLog, nil
}

// Returns true if the error represents a closed
// network error.
func isAMQPClosedNetworkErr(err error) bool {
	// Any other error other than connection closed, return.
	if neterr, ok := err.(*net.OpError); ok &&
		neterr.Err.Error() == "use of closed network connection" {
		return true
	} else if err == amqp.ErrClosed {
		return true
	}
	return false
}

// Channel is a wrapper implementation of amqp.Connection.Channel()
// which implements transparent reconnection.
func (q *amqpConn) Channel() (*amqp.Channel, error) {
	q.Lock()
	ch, err := q.conn.Channel()
	q.Unlock()
	if err != nil {
		if !isAMQPClosedNetworkErr(err) {
			return nil, err
		}
		// Attempt to connect again.
		var conn *amqp.Connection
		conn, err = amqp.Dial(q.params.URL)
		if err != nil {
			return nil, err
		}
		ch, err = conn.Channel()
		if err != nil {
			return nil, err
		}
		q.Lock()
		q.conn = conn
		q.Unlock()
	}
	return ch, nil
}

// Fire is called when an event should be sent to the message broker.
func (q *amqpConn) Fire(entry *logrus.Entry) error {
	ch, err := q.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	err = ch.ExchangeDeclare(
		q.params.Exchange,
		q.params.ExchangeType,
		q.params.Durable,
		q.params.AutoDeleted,
		q.params.Internal,
		q.params.NoWait,
		nil,
	)
	if err != nil {
		return err
	}

	body, err := entry.String()
	if err != nil {
		return err
	}

	err = ch.Publish(
		q.params.Exchange,
		q.params.RoutingKey,
		q.params.Mandatory,
		q.params.Immediate,
		amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: q.params.DeliveryMode,
			Body:         []byte(body),
		})
	if err != nil {
		return err
	}

	return nil
}

// Levels is available logging levels.
func (q *amqpConn) Levels() []logrus.Level {
	return []logrus.Level{
		logrus.InfoLevel,
	}
}

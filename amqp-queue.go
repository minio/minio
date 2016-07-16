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

package main

import (
	"github.com/Sirupsen/logrus"
	"github.com/streadway/amqp"
)

const (
	arnAmqpQueue = "1:amqp:"
)

// Returns true if queueArn is for an AMQP queue.
func isAMQPQueue(sqsArn minioSqsArn) bool {
	if sqsArn.sqsType == arnAmqpQueue {
		// Sets to 'true' if total fields available are equal to known value.
		return queueInputFields[arnAmqpQueue] == len(sqsArn.inputs)
	}
	return false
}

// amqpQueue - represents logrus compatible AMQP hook.
// All fields represent AMQP configuration details.
type amqpQueue struct {
	amqpConn     *amqp.Connection
	amqpURL      string
	exchange     string
	routingKey   string
	exchangeType string
	mandatory    bool
	immediate    bool
	durable      bool
	internal     bool
	noWait       bool
	autoDeleted  bool
}

func connectAMQPQueue(amqpParams []string) (*amqp.Connection, string, error) {
	host := amqpParams[0]
	port := amqpParams[1]
	server := host + ":" + port
	username := amqpParams[2]
	password := amqpParams[3]

	dialURL := "amqp://" + username + ":" + password + "@" + server + "/"
	conn, err := amqp.Dial(dialURL)
	if err != nil {
		return nil, "", err
	}
	return conn, dialURL, nil
}

func enableAMQPQueue(amqpParams []string) error {
	exchange := amqpParams[4]
	routingKey := amqpParams[5]

	conn, dialURL, err := connectAMQPQueue(amqpParams)
	if err != nil {
		return err
	}

	// Initialize amqp queue.
	amqpQ := &amqpQueue{
		amqpConn:   conn,
		amqpURL:    dialURL,
		exchange:   exchange,
		routingKey: routingKey,
	}

	// Add a amqp hook.
	log.Hooks.Add(amqpQ)

	// Set default JSON formatter.
	log.Formatter = new(logrus.JSONFormatter)

	// Set default log level to info.
	log.Level = logrus.InfoLevel

	// Successfully enabled.
	return nil
}

// Fire is called when an event should be sent to the message broker.
func (q *amqpQueue) Fire(entry *logrus.Entry) error {
	q.durable = true
	q.exchangeType = "direct"

	ch, err := q.amqpConn.Channel()
	if err != nil {
		// Any other error other than connection closed, return.
		if err != amqp.ErrClosed {
			return err
		}
		// Attempt to connect again.
		var conn *amqp.Connection
		conn, err = amqp.Dial(q.amqpURL)
		if err != nil {
			return err
		}
		ch, err = conn.Channel()
		if err != nil {
			return err
		}
	}
	defer ch.Close()

	err = ch.ExchangeDeclare(
		q.exchange,
		q.exchangeType,
		q.durable,
		q.autoDeleted,
		q.internal,
		q.noWait,
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
		q.exchange,
		q.routingKey,
		q.mandatory,
		q.immediate,
		amqp.Publishing{
			ContentType: "application/json",
			Body:        []byte(body),
		})
	if err != nil {
		return err
	}

	return nil
}

// Levels is available logging levels.
func (q amqpQueue) Levels() []logrus.Level {
	return []logrus.Level{
		logrus.PanicLevel,
		logrus.FatalLevel,
		logrus.ErrorLevel,
		logrus.WarnLevel,
		logrus.InfoLevel,
		logrus.DebugLevel,
	}
}

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
	"errors"

	"github.com/Sirupsen/logrus"
	"github.com/streadway/amqp"
)

// amqpLogger - represents logrus compatible AMQP hook.
// All fields represent AMQP configuration details.
type amqpLogger struct {
	Enable       bool   `json:"enable"`
	Level        string `json:"level"`
	URL          string `json:"url"`
	Exchange     string `json:"exchange"`
	RoutingKey   string `json:"routineKey"`
	ExchangeType string `json:"exchangeType"`
	Mandatory    bool   `json:"mandatory"`
	Immediate    bool   `json:"immediate"`
	Durable      bool   `json:"durable"`
	Internal     bool   `json:"internal"`
	NoWait       bool   `json:"noWait"`
	AutoDeleted  bool   `json:"autoDeleted"`
}

type amqpConn struct {
	params amqpLogger
	*amqp.Connection
}

func dialAMQP(amqpL amqpLogger) (amqpConn, error) {
	conn, err := amqp.Dial(amqpL.URL)
	if err != nil {
		return amqpConn{}, err
	}
	return amqpConn{Connection: conn, params: amqpL}, nil
}

var errLoggerNotEnabled = errors.New("logger type not enabled")

func enableAMQPLogger() error {
	amqpL := serverConfig.GetAMQPLogger()
	if !amqpL.Enable {
		return errLoggerNotEnabled
	}

	// Connect to amqp server.
	amqpC, err := dialAMQP(amqpL)
	if err != nil {
		return err
	}

	lvl, err := logrus.ParseLevel(amqpL.Level)
	fatalIf(err, "Unknown log level found in the config file.")

	// Add a amqp hook.
	log.Hooks.Add(amqpC)

	// Set default JSON formatter.
	log.Formatter = new(logrus.JSONFormatter)

	// Set default log level to info.
	log.Level = lvl

	// Successfully enabled.
	return nil
}

// Fire is called when an event should be sent to the message broker.
func (q amqpConn) Fire(entry *logrus.Entry) error {
	ch, err := q.Connection.Channel()
	if err != nil {
		// Any other error other than connection closed, return.
		if err != amqp.ErrClosed {
			return err
		}
		// Attempt to connect again.
		var conn *amqp.Connection
		conn, err = amqp.Dial(q.params.URL)
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
			ContentType: "application/json",
			Body:        []byte(body),
		})
	if err != nil {
		return err
	}

	return nil
}

// Levels is available logging levels.
func (q amqpConn) Levels() []logrus.Level {
	return []logrus.Level{
		logrus.PanicLevel,
		logrus.FatalLevel,
		logrus.ErrorLevel,
		logrus.WarnLevel,
		logrus.InfoLevel,
		logrus.DebugLevel,
	}
}

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
	"encoding/json"

	"github.com/streadway/amqp"
)

// AmqpQueue is a queue redirecting to go's log facility
type AmqpQueue struct {
	Type  string `json:"type"`
	URL   string `json:"url"`
	Queue string `json:"queue"`
	conn  *amqp.Connection
	ch    *amqp.Channel
}

// NewAmqpQueue returns a new AmqpQueue
func NewAmqpQueue() AmqpQueue {
	return AmqpQueue{Type: "amqp"}
}

// Post posts a message to a log as a Go term or json
func (q AmqpQueue) Post(message interface{}) {
	if q.conn == nil {
		conn, err := amqp.Dial(q.URL)
		errorIf(err, "Can't connect to AMQP at %s: %v", q.URL, err)
		if err != nil {
			return
		}
		q.conn = conn
		ch, err := conn.Channel()
		errorIf(err, "Can't open AMQP channel %v", err)
		if err != nil {
			return
		}
		q.ch = ch
		_, err = ch.QueueDeclare(
			q.Queue, // name
			true,    // durable
			false,   // delete when unused
			false,   // exclusive
			false,   // no-wait
			nil,     // arguments
		)
		errorIf(err, "Can't declare AMQP queue %s %v", q.Queue, err)
		if err != nil {
			return
		}
	}
	json, err := json.Marshal(message)
	if err != nil {
		errorIf(err, "Error while generating JSON for %+v: %v", message, err)
	} else {
		err := q.ch.Publish(
			"",      // exchange
			q.Queue, // routing key
			false,   // mandatory
			false,   // immediate
			amqp.Publishing{
				ContentType: "application/json",
				Body:        json,
			})
		errorIf(err, "Can't publish an AMQP message %v", err)
	}
}

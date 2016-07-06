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
	"fmt"
)

// Queue is an interface for queues
type Queue interface {
	Post(message interface{})
}

// Queues is a list of queues
type Queues struct {
	Queues []Queue
}

// Post posts a message to all queues
func (q Queues) Post(message interface{}) {
	for _, queue := range q.Queues {
		queue.Post(message)
	}
}

// MarshalJSON encodes JSON config for Queues
func (q *Queues) MarshalJSON() ([]byte, error) {
	bytes, err := json.Marshal(q.Queues)
	if err != nil {
		return nil, err
	}
	return bytes, nil
}

// UnmarshalJSON decodes JSON config for Queues, for example
// [{type: "log", json: true}]
func (q *Queues) UnmarshalJSON(data []byte) error {
	q.Queues = make([]Queue, 0)
	var aux []map[string]interface{}
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}
	for _, item := range aux {
		var queue Queue
		itemJSON, err := json.Marshal(item)
		if err != nil {
			return err
		}
		switch item["type"] {
		case "nil":
			q := NewNilQueue()
			err = json.Unmarshal(itemJSON, &q)
			queue = q
		case "log":
			q := NewLogQueue()
			err = json.Unmarshal(itemJSON, &q)
			queue = q
		case "amqp":
			q := NewAmqpQueue()
			err = json.Unmarshal(itemJSON, &q)
			queue = q
		default:
			return fmt.Errorf("wrong log type %s, allowed types: nil, log, amqp", item["type"])
		}
		if err != nil {
			return err
		}
		q.Queues = append(q.Queues, queue)
	}
	return nil
}

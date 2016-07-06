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
	l "log"
)

// LogQueue is a queue redirecting to go's log facility
type LogQueue struct {
	Type string `json:"type"`
}

// NewLogQueue returns a new LogQueue
func NewLogQueue() LogQueue {
	return LogQueue{Type: "log"}
}

// Post posts a message to a log as a Go term or json
func (q LogQueue) Post(message interface{}) {
	json, err := json.Marshal(message)
	if err != nil {
		l.Fatalf("Error while generating JSON for %+v: %v", message, err)
	} else {
		l.Printf("S3 Notification Event: %s", string(json))
	}
}

/*
 * MinIO Cloud Storage, (C) 2018, 2019 MinIO, Inc.
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

package stdoutlog

import (
	"errors"
	"github.com/minio/minio/cmd/logger/message/audit"
	"log"
	"os"
)

// Target implements logger.Target and sends the json
// format of a log entry to stdout.
// An internal buffer of logs is maintained but when the
// buffer is full, new logs are just ignored and an error
// is returned to the caller.
type Target struct {
	logger *log.Logger
	logCh chan interface{}
}

// Endpoint returns the backend endpoint
func (h *Target) Endpoint() string {
	return ""
}

func (h *Target) String() string {
	return "stdout"
}

// Validate validate the http target
func (h *Target) Validate() error {
	return nil
}

// StartStdoutLogger StarS stdout logging coroutine
func (h *Target) StartStdoutLogger() {
	for entry := range h.logCh {
		var auditEntry, ok = entry.(audit.Entry)
		if ok {
			h.logger.Println(
				auditEntry.API.Name, auditEntry.API.Bucket, auditEntry.API.StatusCode, auditEntry.API.Status)
		} else {
			h.logger.Println(entry)
		}

	}
}

// New initializes a new stdout logger target
func New(prefix string) *Target {
	h := &Target{
		logCh: make(chan interface{}, 10000),
		logger : log.New(os.Stdout, prefix, log.LstdFlags),
	}

	go h.StartStdoutLogger()
	return h
}

// Send log message 'e' to stdout.
func (h *Target) Send(entry interface{}, errKind string) error {
	select {
	case h.logCh <- entry:
	default:
		// log channel is full, do not wait and return
		// an error immediately to the caller
		return errors.New("log buffer full")
	}

	return nil
}

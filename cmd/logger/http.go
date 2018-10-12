/*
 * Minio Cloud Storage, (C) 2018 Minio, Inc.
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

package logger

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"io/ioutil"
	"net/http"
)

// HTTPTarget implements loggerTarget and sends the json
// format of a log entry to the configured http endpoint.
// An internal buffer of logs is maintained but when the
// buffer is full, new logs are just ignored and an error
// is returned to the caller.
type HTTPTarget struct {
	// Channel of log entries
	logCh chan interface{}

	// HTTP(s) endpoint
	endpoint string
	client   http.Client
}

func (h *HTTPTarget) startHTTPLogger() {
	// Create a routine which sends json logs received
	// from an internal channel.
	go func() {
		for entry := range h.logCh {
			logJSON, err := json.Marshal(&entry)
			if err != nil {
				continue
			}

			req, err := http.NewRequest("POST", h.endpoint, bytes.NewBuffer(logJSON))
			req.Header.Set("Content-Type", "application/json")

			resp, err := h.client.Do(req)
			if err != nil {
				continue
			}
			if resp.Body != nil {
				buf := make([]byte, 512)
				io.CopyBuffer(ioutil.Discard, resp.Body, buf)
				resp.Body.Close()
			}
		}
	}()
}

// NewHTTP initializes a new logger target which
// sends log over http to the specified endpoint
func NewHTTP(endpoint string, transport *http.Transport) LoggingTarget {
	h := HTTPTarget{
		endpoint: endpoint,
		client: http.Client{
			Transport: transport,
		},
		logCh: make(chan interface{}, 10000),
	}

	h.startHTTPLogger()
	return &h
}

func (h *HTTPTarget) send(entry interface{}) error {
	select {
	case h.logCh <- entry:
	default:
		// log channel is full, do not wait and return
		// an error immediately to the caller
		return errors.New("log buffer full")
	}

	return nil
}

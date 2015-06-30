/*
 * Minimalist Object Storage, (C) 2015 Minio, Inc.
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

package api

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/minio/minio/pkg/iodine"
	"github.com/minio/minio/pkg/utils/log"
)

type logHandler struct {
	handler http.Handler
	logger  chan<- []byte
}

// logMessage is a serializable json log message
type logMessage struct {
	StartTime     time.Time
	Duration      time.Duration
	StatusMessage string // human readable http status message
	ContentLength string // human readable content length

	// HTTP detailed message
	HTTP struct {
		ResponseHeaders http.Header
		Request         *http.Request
	}
}

// logWriter is used to capture status for log messages
type logWriter struct {
	responseWriter http.ResponseWriter
	logMessage     *logMessage
}

// WriteHeader writes headers and stores status in LogMessage
func (w *logWriter) WriteHeader(status int) {
	w.logMessage.StatusMessage = http.StatusText(status)
	w.responseWriter.WriteHeader(status)
}

// Header Dummy wrapper for LogWriter
func (w *logWriter) Header() http.Header {
	return w.responseWriter.Header()
}

// Write Dummy wrapper for LogWriter
func (w *logWriter) Write(data []byte) (int, error) {
	return w.responseWriter.Write(data)
}

func (h *logHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	logMessage := &logMessage{
		StartTime: time.Now().UTC(),
	}
	logWriter := &logWriter{responseWriter: w, logMessage: logMessage}
	h.handler.ServeHTTP(logWriter, req)
	h.logger <- getLogMessage(logMessage, w, req)
}

func getLogMessage(logMessage *logMessage, w http.ResponseWriter, req *http.Request) []byte {
	// store lower level details
	logMessage.HTTP.ResponseHeaders = w.Header()
	logMessage.HTTP.Request = req

	// humanize content-length to be printed in logs
	contentLength, _ := strconv.Atoi(logMessage.HTTP.ResponseHeaders.Get("Content-Length"))
	logMessage.ContentLength = humanize.IBytes(uint64(contentLength))
	logMessage.Duration = time.Now().UTC().Sub(logMessage.StartTime)
	js, _ := json.Marshal(logMessage)
	js = append(js, byte('\n')) // append a new line
	return js
}

// loggingHandler logs requests
func loggingHandler(h http.Handler) http.Handler {
	logger, _ := fileLogger("access.log")
	return &logHandler{handler: h, logger: logger}
}

// fileLogger returns a channel that is used to write to the logger
func fileLogger(filename string) (chan<- []byte, error) {
	ch := make(chan []byte)
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return nil, iodine.New(err, map[string]string{"logfile": filename})
	}
	go func() {
		for message := range ch {
			if _, err := io.Copy(file, bytes.NewBuffer(message)); err != nil {
				log.Println(iodine.New(err, nil))
			}
		}
	}()
	return ch, nil
}

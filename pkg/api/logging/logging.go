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

package logging

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/minio-io/minio/pkg/iodine"
	"github.com/minio-io/minio/pkg/utils/log"
)

type logHandler struct {
	http.Handler
	Logger chan<- string
}

// LogMessage is a serializable json log message
type LogMessage struct {
	Request         *http.Request
	StartTime       time.Time
	Duration        time.Duration
	Status          int
	ResponseHeaders http.Header
}

// LogWriter is used to capture status for log messages
type LogWriter struct {
	http.ResponseWriter
	LogMessage *LogMessage
}

// WriteHeader writes headers and stores status in LogMessage
func (w *LogWriter) WriteHeader(status int) {
	w.LogMessage.Status = status
	w.ResponseWriter.WriteHeader(status)
	w.ResponseWriter.Header()
}

func (h *logHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	logMessage := &LogMessage{
		StartTime: time.Now().UTC(),
	}
	logWriter := &LogWriter{ResponseWriter: w, LogMessage: logMessage}
	h.Handler.ServeHTTP(logWriter, req)
	logMessage.ResponseHeaders = w.Header()
	logMessage.Request = req
	logMessage.Duration = time.Now().UTC().Sub(logMessage.StartTime)
	js, _ := json.Marshal(logMessage)
	h.Logger <- string(js)
}

// LogHandler logs requests
func LogHandler(h http.Handler) http.Handler {
	logger, _ := FileLogger("access.log")
	return &logHandler{Handler: h, Logger: logger}
}

// FileLogger returns a channel that is used to write to the logger
func FileLogger(filename string) (chan<- string, error) {
	ch := make(chan string)
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return nil, iodine.New(err, map[string]string{"logfile": filename})
	}
	go func() {
		for message := range ch {
			if _, err := io.Copy(file, bytes.NewBufferString(message+"\n")); err != nil {
				log.Println(iodine.New(err, nil))
			}
		}
	}()
	return ch, nil
}

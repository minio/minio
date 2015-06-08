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
	"strconv"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/minio/minio/pkg/iodine"
	"github.com/minio/minio/pkg/utils/log"
)

type logHandler struct {
	http.Handler
	Logger chan<- []byte
}

// LogMessage is a serializable json log message
type LogMessage struct {
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

// LogWriter is used to capture status for log messages
type LogWriter struct {
	ResponseWriter http.ResponseWriter
	LogMessage     *LogMessage
}

// WriteHeader writes headers and stores status in LogMessage
func (w *LogWriter) WriteHeader(status int) {
	w.LogMessage.StatusMessage = http.StatusText(status)
	w.ResponseWriter.WriteHeader(status)
}

// Header Dummy wrapper for LogWriter
func (w *LogWriter) Header() http.Header {
	return w.ResponseWriter.Header()
}

// Write Dummy wrapper for LogWriter
func (w *LogWriter) Write(data []byte) (int, error) {
	return w.ResponseWriter.Write(data)
}

func (h *logHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	logMessage := &LogMessage{
		StartTime: time.Now().UTC(),
	}
	logWriter := &LogWriter{ResponseWriter: w, LogMessage: logMessage}
	h.Handler.ServeHTTP(logWriter, req)
	h.Logger <- getLogMessage(logMessage, w, req)
}

func getLogMessage(logMessage *LogMessage, w http.ResponseWriter, req *http.Request) []byte {
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

// LogHandler logs requests
func LogHandler(h http.Handler) http.Handler {
	logger, _ := FileLogger("access.log")
	return &logHandler{Handler: h, Logger: logger}
}

// FileLogger returns a channel that is used to write to the logger
func FileLogger(filename string) (chan<- []byte, error) {
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

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

package main

import (
	"encoding/json"
	"net/http"
	"net/url"
	"os"
	"time"
)

type accessLogHandler struct {
	http.Handler
	accessLogFile *os.File
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
		Request         struct {
			Method     string
			URL        *url.URL
			Proto      string // "HTTP/1.0"
			ProtoMajor int    // 1
			ProtoMinor int    // 0
			Header     http.Header
			Host       string
			Form       url.Values
			PostForm   url.Values
			Trailer    http.Header
			RemoteAddr string
			RequestURI string
		}
	}
}

func (h *accessLogHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	message, err := getLogMessage(w, req)
	fatalIf(err, "Unable to parse HTTP request and response fields.")
	_, err = h.accessLogFile.Write(message)
	fatalIf(err, "Unable to log HTTP access.")

	h.Handler.ServeHTTP(w, req)
}

func getLogMessage(w http.ResponseWriter, req *http.Request) ([]byte, error) {
	logMessage := &LogMessage{
		StartTime: time.Now().UTC(),
	}
	// store lower level details
	logMessage.HTTP.ResponseHeaders = w.Header()
	logMessage.HTTP.Request = struct {
		Method     string
		URL        *url.URL
		Proto      string // "HTTP/1.0"
		ProtoMajor int    // 1
		ProtoMinor int    // 0
		Header     http.Header
		Host       string
		Form       url.Values
		PostForm   url.Values
		Trailer    http.Header
		RemoteAddr string
		RequestURI string
	}{
		Method:     req.Method,
		URL:        req.URL,
		Proto:      req.Proto,
		ProtoMajor: req.ProtoMajor,
		ProtoMinor: req.ProtoMinor,
		Header:     req.Header,
		Host:       req.Host,
		Form:       req.Form,
		PostForm:   req.PostForm,
		Trailer:    req.Header,
		RemoteAddr: req.RemoteAddr,
		RequestURI: req.RequestURI,
	}

	// logMessage.HTTP.Request = req
	logMessage.Duration = time.Now().UTC().Sub(logMessage.StartTime)
	js, err := json.Marshal(logMessage)
	if err != nil {
		return nil, err
	}
	js = append(js, byte('\n')) // append a new line
	return js, nil
}

// setAccessLogHandler logs requests
func setAccessLogHandler(h http.Handler) http.Handler {
	file, err := os.OpenFile("access.log", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	fatalIf(err, "Failed top open access log.")

	return &accessLogHandler{Handler: h, accessLogFile: file}
}

/*
 * Minio Cloud Storage, (C) 2015 Minio, Inc.
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
	"net/http"
	"strings"
	"time"

	router "github.com/gorilla/mux"
	"github.com/rs/cors"
)

// MiddlewareHandler - useful to chain different middleware http.Handler
type MiddlewareHandler func(http.Handler) http.Handler

func registerCustomMiddleware(mux *router.Router, mwHandlers ...MiddlewareHandler) http.Handler {
	var f http.Handler
	f = mux
	for _, mw := range mwHandlers {
		f = mw(f)
	}
	return f
}

type timeHandler struct {
	handler http.Handler
}

type resourceHandler struct {
	handler http.Handler
}

func parseDate(req *http.Request) (time.Time, error) {
	amzDate := req.Header.Get(http.CanonicalHeaderKey("x-amz-date"))
	switch {
	case amzDate != "":
		if _, err := time.Parse(time.RFC1123, amzDate); err == nil {
			return time.Parse(time.RFC1123, amzDate)
		}
		if _, err := time.Parse(time.RFC1123Z, amzDate); err == nil {
			return time.Parse(time.RFC1123Z, amzDate)
		}
		if _, err := time.Parse(iso8601Format, amzDate); err == nil {
			return time.Parse(iso8601Format, amzDate)
		}
	}
	minioDate := req.Header.Get(http.CanonicalHeaderKey("x-minio-date"))
	switch {
	case minioDate != "":
		if _, err := time.Parse(time.RFC1123, minioDate); err == nil {
			return time.Parse(time.RFC1123, minioDate)
		}
		if _, err := time.Parse(time.RFC1123Z, minioDate); err == nil {
			return time.Parse(time.RFC1123Z, minioDate)
		}
		if _, err := time.Parse(iso8601Format, minioDate); err == nil {
			return time.Parse(iso8601Format, minioDate)
		}
	}
	date := req.Header.Get("Date")
	switch {
	case date != "":
		if _, err := time.Parse(time.RFC1123, date); err == nil {
			return time.Parse(time.RFC1123, date)
		}
		if _, err := time.Parse(time.RFC1123Z, date); err == nil {
			return time.Parse(time.RFC1123Z, date)
		}
		if _, err := time.Parse(iso8601Format, amzDate); err == nil {
			return time.Parse(iso8601Format, amzDate)
		}
	}
	return time.Time{}, errors.New("invalid request")
}

// TimeValidityHandler to validate parsable time over http header
func TimeValidityHandler(h http.Handler) http.Handler {
	return timeHandler{h}
}

func (h timeHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Verify if date headers are set, if not reject the request
	if r.Header.Get("Authorization") != "" {
		if r.Header.Get(http.CanonicalHeaderKey("x-amz-date")) == "" && r.Header.Get(http.CanonicalHeaderKey("x-minio-date")) == "" && r.Header.Get("Date") == "" {
			// there is no way to knowing if this is a valid request, could be a attack reject such clients
			writeErrorResponse(w, r, RequestTimeTooSkewed, r.URL.Path)
			return
		}
		date, err := parseDate(r)
		if err != nil {
			// there is no way to knowing if this is a valid request, could be a attack reject such clients
			writeErrorResponse(w, r, RequestTimeTooSkewed, r.URL.Path)
			return
		}
		duration := time.Since(date)
		minutes := time.Duration(5) * time.Minute
		if duration.Minutes() > minutes.Minutes() {
			writeErrorResponse(w, r, RequestTimeTooSkewed, r.URL.Path)
			return
		}
	}
	h.handler.ServeHTTP(w, r)
}

// CorsHandler handler for CORS (Cross Origin Resource Sharing)
func CorsHandler(h http.Handler) http.Handler {
	return cors.Default().Handler(h)
}

// IgnoreResourcesHandler -
// Ignore resources handler is wrapper handler used for API request resource validation
// Since we do not support all the S3 queries, it is necessary for us to throw back a
// valid error message indicating such a feature is not implemented.
func IgnoreResourcesHandler(h http.Handler) http.Handler {
	return resourceHandler{h}
}

// Resource handler ServeHTTP() wrapper
func (h resourceHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	splits := strings.SplitN(r.URL.Path, "/", 3)
	switch len(splits) {
	// bucket exists
	case 2:
		if ignoreNotImplementedBucketResources(r) {
			writeErrorResponse(w, r, NotImplemented, r.URL.Path)
			return
		}
	// object exists
	case 3:
		if ignoreNotImplementedObjectResources(r) {
			writeErrorResponse(w, r, NotImplemented, r.URL.Path)
			return
		}
	}
	h.handler.ServeHTTP(w, r)
}

//// helpers

// Checks requests for not implemented Bucket resources
func ignoreNotImplementedBucketResources(req *http.Request) bool {
	q := req.URL.Query()
	for name := range q {
		if notimplementedBucketResourceNames[name] {
			return true
		}
	}
	return false
}

// Checks requests for not implemented Object resources
func ignoreNotImplementedObjectResources(req *http.Request) bool {
	q := req.URL.Query()
	for name := range q {
		if notimplementedObjectResourceNames[name] {
			return true
		}
	}
	return false
}

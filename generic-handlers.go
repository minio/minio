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

// HandlerFunc - useful to chain different middleware http.Handler
type HandlerFunc func(http.Handler) http.Handler

func registerHandlers(mux *router.Router, handlerFns ...HandlerFunc) http.Handler {
	var f http.Handler
	f = mux
	for _, hFn := range handlerFns {
		f = hFn(f)
	}
	return f
}

type timeHandler struct {
	handler http.Handler
}

type resourceHandler struct {
	handler http.Handler
}

type ignoreSignatureV2RequestHandler struct {
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

// Adds Cache-Control header
type cacheControlHandler struct {
	handler http.Handler
}

func setCacheControlHandler(h http.Handler) http.Handler {
	return cacheControlHandler{h}
}

func (h cacheControlHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method == "GET" {
		// expire the cache in one week
		w.Header().Set("Cache-Control", "public, max-age=604800")
	}
	h.handler.ServeHTTP(w, r)
}

// setTimeValidityHandler to validate parsable time over http header
func setTimeValidityHandler(h http.Handler) http.Handler {
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

// setCorsHandler handler for CORS (Cross Origin Resource Sharing)
func setCorsHandler(h http.Handler) http.Handler {
	c := cors.New(cors.Options{
		AllowedOrigins: []string{"*"},
		AllowedMethods: []string{"GET", "HEAD", "POST", "PUT"},
		AllowedHeaders: []string{"*"},
	})
	return c.Handler(h)
}

// setIgnoreSignatureV2RequestHandler -
// Verify if authorization header has signature version '2', reject it cleanly.
func setIgnoreSignatureV2RequestHandler(h http.Handler) http.Handler {
	return ignoreSignatureV2RequestHandler{h}
}

// Ignore signature version '2' ServerHTTP() wrapper.
func (h ignoreSignatureV2RequestHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if _, ok := r.Header["Authorization"]; ok {
		if !strings.HasPrefix(r.Header.Get("Authorization"), authHeaderPrefix) {
			writeErrorResponse(w, r, SignatureVersionNotSupported, r.URL.Path)
			return
		}
	}
	h.handler.ServeHTTP(w, r)
}

// setIgnoreResourcesHandler -
// Ignore resources handler is wrapper handler used for API request resource validation
// Since we do not support all the S3 queries, it is necessary for us to throw back a
// valid error message indicating such a feature is not implemented.
func setIgnoreResourcesHandler(h http.Handler) http.Handler {
	return resourceHandler{h}
}

// Resource handler ServeHTTP() wrapper
func (h resourceHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Skip the first element which is usually '/' and split the rest.
	splits := strings.SplitN(r.URL.Path[1:], "/", 2)

	// Save bucketName and objectName extracted from url Path.
	var bucketName, objectName string
	if len(splits) == 1 {
		bucketName = splits[0]
	}
	if len(splits) == 2 {
		bucketName = splits[0]
		objectName = splits[1]
	}
	// If bucketName is present and not objectName check for bucket
	// level resource queries.
	if bucketName != "" && objectName == "" {
		if ignoreNotImplementedBucketResources(r) {
			writeErrorResponse(w, r, NotImplemented, r.URL.Path)
			return
		}
	}
	// If bucketName and objectName are present check for its resource queries.
	if bucketName != "" && objectName != "" {
		if ignoreNotImplementedObjectResources(r) {
			writeErrorResponse(w, r, NotImplemented, r.URL.Path)
			return
		}
	}
	// A put method on path "/" doesn't make sense, ignore it.
	if r.Method == "PUT" && r.URL.Path == "/" {
		writeErrorResponse(w, r, NotImplemented, r.URL.Path)
		return
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

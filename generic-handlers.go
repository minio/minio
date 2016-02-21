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
	"path"
	"path/filepath"
	"strings"
	"time"

	router "github.com/gorilla/mux"
	"github.com/rs/cors"
)

const (
	iso8601Format = "20060102T150405Z"
	privateBucket = "minio"
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

// Attempts to parse date string into known date layouts. Date layouts
// currently supported are ``time.RFC1123``, ``time.RFC1123Z`` and
// special ``iso8601Format``.
func parseKnownLayouts(date string) (time.Time, error) {
	parsedTime, e := time.Parse(time.RFC1123, date)
	if e == nil {
		return parsedTime, nil
	}
	parsedTime, e = time.Parse(time.RFC1123Z, date)
	if e == nil {
		return parsedTime, nil
	}
	parsedTime, e = time.Parse(iso8601Format, date)
	if e == nil {
		return parsedTime, nil
	}
	return time.Time{}, e
}

// Parse date string from incoming header, current supports and verifies
// follow HTTP headers.
//
//  - X-Amz-Date
//  - X-Minio-Date
//  - Date
//
// In following time layouts ``time.RFC1123``, ``time.RFC1123Z`` and ``iso8601Format``.
func parseDateHeader(req *http.Request) (time.Time, error) {
	amzDate := req.Header.Get(http.CanonicalHeaderKey("x-amz-date"))
	if amzDate != "" {
		return parseKnownLayouts(amzDate)
	}
	minioDate := req.Header.Get(http.CanonicalHeaderKey("x-minio-date"))
	if minioDate != "" {
		return parseKnownLayouts(minioDate)
	}
	genericDate := req.Header.Get("Date")
	if genericDate != "" {
		return parseKnownLayouts(genericDate)
	}
	return time.Time{}, errors.New("Date header missing, invalid request.")
}

// Adds redirect rules for incoming requests.
type redirectHandler struct {
	handler        http.Handler
	locationPrefix string
}

func setBrowserRedirectHandler(h http.Handler) http.Handler {
	return redirectHandler{handler: h, locationPrefix: privateBucket}
}

func (h redirectHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Re-direction handled specifically for browsers.
	if strings.Contains(r.Header.Get("User-Agent"), "Mozilla") {
		// Following re-direction code handles redirects only for
		// these specific incoming URLs.
		// '/' is redirected to '/locationPrefix'
		// '/rpc' is redirected to '/locationPrefix/rpc'
		// '/login' is redirected to '/locationPrefix/login'
		switch r.URL.Path {
		case "/", "/rpc", "/login":
			location := path.Join(h.locationPrefix, r.URL.Path)
			// Redirect to new location.
			http.Redirect(w, r, location, http.StatusTemporaryRedirect)
			return
		}
	}
	h.handler.ServeHTTP(w, r)
}

// Adds Cache-Control header
type cacheControlHandler struct {
	handler http.Handler
}

func setBrowserCacheControlHandler(h http.Handler) http.Handler {
	return cacheControlHandler{h}
}

func (h cacheControlHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method == "GET" && strings.Contains(r.Header.Get("User-Agent"), "Mozilla") {
		// Expire cache in one hour for all browser requests.
		w.Header().Set("Cache-Control", "public, max-age=3600")
	}
	h.handler.ServeHTTP(w, r)
}

// Adds verification for incoming paths.
type minioPrivateBucketHandler struct {
	handler       http.Handler
	privateBucket string
}

func setPrivateBucketHandler(h http.Handler) http.Handler {
	return minioPrivateBucketHandler{handler: h, privateBucket: privateBucket}
}

func (h minioPrivateBucketHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// For all non browser requests, reject access to 'privateBucket'.
	if !strings.Contains(r.Header.Get("User-Agent"), "Mozilla") && filepath.Base(r.URL.Path) == privateBucket {
		writeErrorResponse(w, r, AllAccessDisabled, r.URL.Path)
		return
	}
	h.handler.ServeHTTP(w, r)
}

type timeHandler struct {
	handler http.Handler
}

// setTimeValidityHandler to validate parsable time over http header
func setTimeValidityHandler(h http.Handler) http.Handler {
	return timeHandler{h}
}

func (h timeHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Verify if date headers are set, if not reject the request
	if r.Header.Get("Authorization") != "" {
		date, e := parseDateHeader(r)
		if e != nil {
			// All our internal APIs are sensitive towards Date
			// header, for all requests where Date header is not
			// present we will reject such clients.
			writeErrorResponse(w, r, RequestTimeTooSkewed, r.URL.Path)
			return
		}
		duration := time.Since(date)
		minutes := time.Duration(5) * time.Minute
		// Verify if the request date header is more than 5minutes
		// late, reject such clients.
		if duration.Minutes() > minutes.Minutes() {
			writeErrorResponse(w, r, RequestTimeTooSkewed, r.URL.Path)
			return
		}
	}
	h.handler.ServeHTTP(w, r)
}

type resourceHandler struct {
	handler http.Handler
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

// setIgnoreResourcesHandler -
// Ignore resources handler is wrapper handler used for API request resource validation
// Since we do not support all the S3 queries, it is necessary for us to throw back a
// valid error message indicating that requested feature is not implemented.
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

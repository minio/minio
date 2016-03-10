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
	"regexp"
	"strings"
	"time"

	router "github.com/gorilla/mux"
	"github.com/rs/cors"
)

const (
	iso8601Format = "20060102T150405Z"
	privateBucket = "/minio"
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
		// '/' is redirected to 'locationPrefix/'
		// '/rpc' is redirected to 'locationPrefix/rpc'
		// '/login' is redirected to 'locationPrefix/login'
		switch r.URL.Path {
		case "/", "/rpc", "/login", "/favicon.ico":
			location := h.locationPrefix + r.URL.Path
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
		// For all browser requests set appropriate Cache-Control policies
		match, e := regexp.MatchString(privateBucket+`/([^/]+\.js|favicon.ico)`, r.URL.Path)
		if e != nil {
			writeErrorResponse(w, r, ErrInternalError, r.URL.Path)
			return
		}
		if match {
			// For assets set cache expiry of one year. For each release, the name
			// of the asset name will change and hence it can not be served from cache.
			w.Header().Set("Cache-Control", "max-age=31536000")
		} else if strings.HasPrefix(r.URL.Path, privateBucket+"/") {
			// For non asset requests we serve index.html which will never be cached.
			w.Header().Set("Cache-Control", "no-store")
		}
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
	if !strings.Contains(r.Header.Get("User-Agent"), "Mozilla") && path.Clean(r.URL.Path) == privateBucket {
		writeErrorResponse(w, r, ErrAllAccessDisabled, r.URL.Path)
		return
	}
	h.handler.ServeHTTP(w, r)
}

// Supported incoming date formats.
var timeFormats = []string{
	time.RFC1123,
	time.RFC1123Z,
	iso8601Format,
}

// Attempts to parse date string into known date layouts. Date layouts
// currently supported are
//  - ``time.RFC1123``
//  - ``time.RFC1123Z``
//  - ``iso8601Format``
func parseDate(date string) (parsedTime time.Time, e error) {
	for _, layout := range timeFormats {
		parsedTime, e = time.Parse(layout, date)
		if e == nil {
			return parsedTime, nil
		}
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
// In following time layouts ``time.RFC1123``, ``time.RFC1123Z`` and
// ``iso8601Format``.
var dateHeaders = []string{
	"x-amz-date",
	"x-minio-date",
	"date",
}

func parseDateHeader(req *http.Request) (time.Time, error) {
	for _, dateHeader := range dateHeaders {
		date := req.Header.Get(http.CanonicalHeaderKey(dateHeader))
		if date != "" {
			return parseDate(date)
		}
	}
	return time.Time{}, errors.New("Date header missing, invalid request.")
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
			writeErrorResponse(w, r, ErrRequestTimeTooSkewed, r.URL.Path)
			return
		}
		// Verify if the request date header is more than 5minutes
		// late, reject such clients.
		if time.Now().UTC().Sub(date)/time.Minute > time.Duration(5)*time.Minute {
			writeErrorResponse(w, r, ErrRequestTimeTooSkewed, r.URL.Path)
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
			writeErrorResponse(w, r, ErrNotImplemented, r.URL.Path)
			return
		}
	}
	// If bucketName and objectName are present check for its resource queries.
	if bucketName != "" && objectName != "" {
		if ignoreNotImplementedObjectResources(r) {
			writeErrorResponse(w, r, ErrNotImplemented, r.URL.Path)
			return
		}
	}
	// A put method on path "/" doesn't make sense, ignore it.
	if r.Method == "PUT" && r.URL.Path == "/" {
		writeErrorResponse(w, r, ErrNotImplemented, r.URL.Path)
		return
	}
	h.handler.ServeHTTP(w, r)
}

//// helpers

// Checks requests for not implemented Bucket resources
func ignoreNotImplementedBucketResources(req *http.Request) bool {
	for name := range req.URL.Query() {
		if notimplementedBucketResourceNames[name] {
			return true
		}
	}
	return false
}

// Checks requests for not implemented Object resources
func ignoreNotImplementedObjectResources(req *http.Request) bool {
	for name := range req.URL.Query() {
		if notimplementedObjectResourceNames[name] {
			return true
		}
	}
	return false
}

// List of not implemented bucket queries
var notimplementedBucketResourceNames = map[string]bool{
	"acl":            true,
	"cors":           true,
	"lifecycle":      true,
	"logging":        true,
	"notification":   true,
	"replication":    true,
	"tagging":        true,
	"versions":       true,
	"requestPayment": true,
	"versioning":     true,
	"website":        true,
}

// List of not implemented object queries
var notimplementedObjectResourceNames = map[string]bool{
	"torrent": true,
	"acl":     true,
	"policy":  true,
}

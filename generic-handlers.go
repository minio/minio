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
	iso8601Format    = "20060102T150405Z"
	restrictedBucket = "/minio"
)

// handlerFunc - useful to chain different middleware http.Handler
type handlerFunc func(http.Handler) http.Handler

// Registers all handlers to an instantiated router.
func registerHandlers(mux *router.Router, handlerFns ...handlerFunc) http.Handler {
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
	return redirectHandler{handler: h, locationPrefix: webBrowserPrefix}
}

// Redirect handler ServeHTTP wrapper.
func (h redirectHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Re-direction handled specifically for browsers.
	if strings.Contains(r.Header.Get("User-Agent"), "Mozilla") {
		// '/' is redirected to 'locationPrefix/'
		// '/favicon.ico' is redirected to 'locationPrefix/favicon.ico'
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

// Cache control handler ServeHTTP wrapper.
func (h cacheControlHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method == "GET" && strings.Contains(r.Header.Get("User-Agent"), "Mozilla") {
		// For all browser requests set appropriate Cache-Control policies
		match, e := regexp.MatchString(webBrowserPrefix+`/([^/]+\.js|favicon.ico)`, r.URL.Path)
		if e != nil {
			writeErrorResponse(w, r, InternalError, r.URL.Path)
			return
		}
		if match {
			// For assets set cache expiry of one year. For each release, the name
			// of the asset name will change and hence it can not be served from cache.
			w.Header().Set("Cache-Control", "max-age=31536000")
		} else if strings.HasPrefix(r.URL.Path, webBrowserPrefix+"/") {
			// For non asset requests we serve index.html which will never be cached.
			w.Header().Set("Cache-Control", "no-store")
		}
	}
	h.handler.ServeHTTP(w, r)
}

// Adds verification for incoming paths and returns error if they are
// for restricted buckets.
type minioRestrictedBucketHandler struct {
	handler          http.Handler
	restrictedBucket string
}

func setRestrictedBucketHandler(h http.Handler) http.Handler {
	return minioRestrictedBucketHandler{handler: h, restrictedBucket: restrictedBucket}
}

// Restricted bucket handler ServeHTTP wrapper.
func (h minioRestrictedBucketHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// For all non browser requests, reject access to 'restrictedBucket'.
	if !strings.Contains(r.Header.Get("User-Agent"), "Mozilla") && path.Clean(r.URL.Path) == restrictedBucket {
		writeErrorResponse(w, r, AllAccessDisabled, r.URL.Path)
		return
	}
	h.handler.ServeHTTP(w, r)
}

// Adds verification for incoming time string when authorization
// header is set.
type timeHandler struct {
	handler http.Handler
}

// setTimeValidityHandler to validate parsable time over http header
func setTimeValidityHandler(h http.Handler) http.Handler {
	return timeHandler{h}
}

// timeHandler ServeHTTP wrapper.
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

// setCorsHandler handler for CORS (Cross Origin Resource Sharing)
func setCorsHandler(h http.Handler) http.Handler {
	c := cors.New(cors.Options{
		AllowedOrigins: []string{"*"},
		AllowedMethods: []string{"GET", "HEAD", "POST", "PUT"},
		AllowedHeaders: []string{"*"},
	})
	return c.Handler(h)
}

// Adds verification for incoming URL path query resources, returns
// valid http error if an unsupported resource is found in the request.
type resourceHandler struct {
	handler http.Handler
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
	// If bucketName is present.
	if bucketName != "" {
		// and objectName is empty, check for bucket level resource queries.
		if objectName == "" {
			if ignoreNotImplementedBucketResources(r) {
				writeErrorResponse(w, r, NotImplemented, r.URL.Path)
				return
			}
		} else {
			// and objectName is present, check for its object level
			// resource queries.
			if ignoreNotImplementedObjectResources(r) {
				writeErrorResponse(w, r, NotImplemented, r.URL.Path)
				return
			}
		}
	}

	// A put method on path "/" doesn't make sense, ignore it.
	if r.Method == "PUT" && r.URL.Path == "/" {
		writeErrorResponse(w, r, NotImplemented, r.URL.Path)
		return
	}

	// Request supported, top level caller now handles it appropriately.
	h.handler.ServeHTTP(w, r)
}

//// helpers

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
	"policy":         true,
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
	"delete":         true,
}

// List of not implemented object queries
var notimplementedObjectResourceNames = map[string]bool{
	"torrent": true,
	"acl":     true,
	"policy":  true,
}

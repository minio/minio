/*
 * Minio Cloud Storage, (C) 2015, 2016 Minio, Inc.
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

package cmd

import (
	"net/http"
	"path"
	"regexp"
	"strings"
	"time"

	humanize "github.com/dustin/go-humanize"
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

// Adds limiting body size middleware

// Maximum allowed form data field values. 64MiB is a guessed practical value
// which is more than enough to accommodate any form data fields and headers.
const requestFormDataSize = 64 * humanize.MiByte

// For any HTTP request, request body should be not more than 5GiB + requestFormDataSize
// where, 5GiB is the maximum allowed object size for object upload.
const requestMaxBodySize = 5*humanize.GiByte + requestFormDataSize

type requestSizeLimitHandler struct {
	handler     http.Handler
	maxBodySize int64
}

func setRequestSizeLimitHandler(h http.Handler) http.Handler {
	return requestSizeLimitHandler{handler: h, maxBodySize: requestMaxBodySize}
}

func (h requestSizeLimitHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Restricting read data to a given maximum length
	r.Body = http.MaxBytesReader(w, r.Body, h.maxBodySize)
	h.handler.ServeHTTP(w, r)
}

// Reserved bucket.
const (
	reservedBucket = "/minio"
)

// Adds redirect rules for incoming requests.
type redirectHandler struct {
	handler http.Handler
}

func setBrowserRedirectHandler(h http.Handler) http.Handler {
	return redirectHandler{handler: h}
}

// Fetch redirect location if urlPath satisfies certain
// criteria. Some special names are considered to be
// redirectable, this is purely internal function and
// serves only limited purpose on redirect-handler for
// browser requests.
func getRedirectLocation(urlPath string) (rLocation string) {
	if urlPath == reservedBucket {
		rLocation = reservedBucket + "/"
	}
	if contains([]string{
		"/",
		"/webrpc",
		"/login",
		"/favicon.ico",
	}, urlPath) {
		rLocation = reservedBucket + urlPath
	}
	return rLocation
}

// guessIsBrowserReq - returns true if the request is browser.
// This implementation just validates user-agent and
// looks for "Mozilla" string. This is no way certifiable
// way to know if the request really came from a browser
// since User-Agent's can be arbitrary. But this is just
// a best effort function.
func guessIsBrowserReq(req *http.Request) bool {
	if req == nil {
		return false
	}
	return strings.Contains(req.Header.Get("User-Agent"), "Mozilla")
}

func (h redirectHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	aType := getRequestAuthType(r)
	// Re-direct only for JWT and anonymous requests from browser.
	if aType == authTypeJWT || aType == authTypeAnonymous {
		// Re-direction is handled specifically for browser requests.
		if guessIsBrowserReq(r) && globalIsBrowserEnabled {
			// Fetch the redirect location if any.
			redirectLocation := getRedirectLocation(r.URL.Path)
			if redirectLocation != "" {
				// Employ a temporary re-direct.
				http.Redirect(w, r, redirectLocation, http.StatusTemporaryRedirect)
				return
			}
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
	if r.Method == "GET" && guessIsBrowserReq(r) && globalIsBrowserEnabled {
		// For all browser requests set appropriate Cache-Control policies
		match, err := regexp.Match(reservedBucket+`/([^/]+\.js|favicon.ico)`, []byte(r.URL.Path))
		if err != nil {
			errorIf(err, "Unable to match incoming URL %s", r.URL)
			writeErrorResponse(w, r, ErrInternalError, r.URL.Path)
			return
		}
		if match {
			// For assets set cache expiry of one year. For each release, the name
			// of the asset name will change and hence it can not be served from cache.
			w.Header().Set("Cache-Control", "max-age=31536000")
		} else if strings.HasPrefix(r.URL.Path, reservedBucket+"/") {
			// For non asset requests we serve index.html which will never be cached.
			w.Header().Set("Cache-Control", "no-store")
		}
	}
	h.handler.ServeHTTP(w, r)
}

// Adds verification for incoming paths.
type minioPrivateBucketHandler struct {
	handler        http.Handler
	reservedBucket string
}

func setPrivateBucketHandler(h http.Handler) http.Handler {
	return minioPrivateBucketHandler{handler: h, reservedBucket: reservedBucket}
}

func (h minioPrivateBucketHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// For all non browser requests, reject access to 'reservedBucket'.
	if !guessIsBrowserReq(r) && path.Clean(r.URL.Path) == reservedBucket {
		writeErrorResponse(w, r, ErrAllAccessDisabled, r.URL.Path)
		return
	}
	h.handler.ServeHTTP(w, r)
}

type timeValidityHandler struct {
	handler http.Handler
}

// setTimeValidityHandler to validate parsable time over http header
func setTimeValidityHandler(h http.Handler) http.Handler {
	return timeValidityHandler{h}
}

// Supported Amz date formats.
var amzDateFormats = []string{
	time.RFC1123,
	time.RFC1123Z,
	iso8601Format,
	// Add new AMZ date formats here.
}

// Supported Amz date headers.
var amzDateHeaders = []string{
	"x-amz-date",
	"date",
}

// parseAmzDate - parses date string into supported amz date formats.
func parseAmzDate(amzDateStr string) (amzDate time.Time, apiErr APIErrorCode) {
	for _, dateFormat := range amzDateFormats {
		amzDate, err := time.Parse(dateFormat, amzDateStr)
		if err == nil {
			return amzDate, ErrNone
		}
	}
	return time.Time{}, ErrMalformedDate
}

// parseAmzDateHeader - parses supported amz date headers, in
// supported amz date formats.
func parseAmzDateHeader(req *http.Request) (time.Time, APIErrorCode) {
	for _, amzDateHeader := range amzDateHeaders {
		amzDateStr := req.Header.Get(http.CanonicalHeaderKey(amzDateHeader))
		if amzDateStr != "" {
			return parseAmzDate(amzDateStr)
		}
	}
	// Date header missing.
	return time.Time{}, ErrMissingDateHeader
}

func (h timeValidityHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	aType := getRequestAuthType(r)
	if aType == authTypeSigned || aType == authTypeSignedV2 || aType == authTypeStreamingSigned {
		// Verify if date headers are set, if not reject the request
		amzDate, apiErr := parseAmzDateHeader(r)
		if apiErr != ErrNone {
			// All our internal APIs are sensitive towards Date
			// header, for all requests where Date header is not
			// present we will reject such clients.
			writeErrorResponse(w, r, apiErr, r.URL.Path)
			return
		}
		// Verify if the request date header is shifted by less than globalMaxSkewTime parameter in the past
		// or in the future, reject request otherwise.
		curTime := time.Now().UTC()
		if curTime.Sub(amzDate) > globalMaxSkewTime || amzDate.Sub(curTime) > globalMaxSkewTime {
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
		ExposedHeaders: []string{"ETag"},
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

	// If bucketName is present and not objectName check for bucket level resource queries.
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

	// Serve HTTP.
	h.handler.ServeHTTP(w, r)
}

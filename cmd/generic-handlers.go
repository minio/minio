/*
 * Minio Cloud Storage, (C) 2015, 2016, 2017 Minio, Inc.
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
	"bufio"
	"context"
	"net"
	"net/http"
	"strings"
	"time"

	humanize "github.com/dustin/go-humanize"
	"github.com/minio/minio/pkg/sys"
	"github.com/rs/cors"
	"golang.org/x/time/rate"
)

// HandlerFunc - useful to chain different middleware http.Handler
type HandlerFunc func(http.Handler) http.Handler

func registerHandlers(h http.Handler, handlerFns ...HandlerFunc) http.Handler {
	for _, hFn := range handlerFns {
		h = hFn(h)
	}
	return h
}

// Adds limiting body size middleware

// Maximum allowed form data field values. 64MiB is a guessed practical value
// which is more than enough to accommodate any form data fields and headers.
const requestFormDataSize = 64 * humanize.MiByte

// For any HTTP request, request body should be not more than 16GiB + requestFormDataSize
// where, 16GiB is the maximum allowed object size for object upload.
const requestMaxBodySize = globalMaxObjectSize + requestFormDataSize

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

const (
	// Maximum size for http headers - See: https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingMetadata.html
	maxHeaderSize = 8 * 1024
	// Maximum size for user-defined metadata - See: https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingMetadata.html
	maxUserDataSize = 2 * 1024
)

type requestHeaderSizeLimitHandler struct {
	http.Handler
}

func setRequestHeaderSizeLimitHandler(h http.Handler) http.Handler {
	return requestHeaderSizeLimitHandler{h}
}

// ServeHTTP restricts the size of the http header to 8 KB and the size
// of the user-defined metadata to 2 KB.
func (h requestHeaderSizeLimitHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if isHTTPHeaderSizeTooLarge(r.Header) {
		writeErrorResponse(w, ErrMetadataTooLarge, r.URL)
		return
	}
	h.Handler.ServeHTTP(w, r)
}

// isHTTPHeaderSizeTooLarge returns true if the provided
// header is larger than 8 KB or the user-defined metadata
// is larger than 2 KB.
func isHTTPHeaderSizeTooLarge(header http.Header) bool {
	var size, usersize int
	for key := range header {
		length := len(key) + len(header.Get(key))
		size += length
		for _, prefix := range userMetadataKeyPrefixes {
			if strings.HasPrefix(key, prefix) {
				usersize += length
				break
			}
		}
		if usersize > maxUserDataSize || size > maxHeaderSize {
			return true
		}
	}
	return false
}

// ReservedMetadataPrefix is the prefix of a metadata key which
// is reserved and for internal use only.
const ReservedMetadataPrefix = "X-Minio-Internal-"

type reservedMetadataHandler struct {
	http.Handler
}

func filterReservedMetadata(h http.Handler) http.Handler {
	return reservedMetadataHandler{h}
}

// ServeHTTP fails if the request contains at least one reserved header which
// would be treated as metadata.
func (h reservedMetadataHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if containsReservedMetadata(r.Header) {
		writeErrorResponse(w, ErrUnsupportedMetadata, r.URL)
		return
	}
	h.Handler.ServeHTTP(w, r)
}

// containsReservedMetadata returns true if the http.Header contains
// keys which are treated as metadata but are reserved for internal use
// and must not set by clients
func containsReservedMetadata(header http.Header) bool {
	for key := range header {
		if strings.HasPrefix(key, ReservedMetadataPrefix) {
			return true
		}
	}
	return false
}

// Reserved bucket.
const (
	minioReservedBucket     = "minio"
	minioReservedBucketPath = "/" + minioReservedBucket
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
	if urlPath == minioReservedBucketPath {
		rLocation = minioReservedBucketPath + "/"
	}
	if contains([]string{
		"/",
		"/webrpc",
		"/login",
		"/favicon.ico",
	}, urlPath) {
		rLocation = minioReservedBucketPath + urlPath
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

// guessIsRPCReq - returns true if the request is for an RPC endpoint.
func guessIsRPCReq(req *http.Request) bool {
	if req == nil {
		return false
	}
	return req.Method == http.MethodConnect && req.Proto == "HTTP/1.0"
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
	if r.Method == httpGET && guessIsBrowserReq(r) && globalIsBrowserEnabled {
		// For all browser requests set appropriate Cache-Control policies
		if hasPrefix(r.URL.Path, minioReservedBucketPath+"/") {
			if hasSuffix(r.URL.Path, ".js") || r.URL.Path == minioReservedBucketPath+"/favicon.ico" {
				// For assets set cache expiry of one year. For each release, the name
				// of the asset name will change and hence it can not be served from cache.
				w.Header().Set("Cache-Control", "max-age=31536000")
			} else {
				// For non asset requests we serve index.html which will never be cached.
				w.Header().Set("Cache-Control", "no-store")
			}
		}
	}

	h.handler.ServeHTTP(w, r)
}

// Adds verification for incoming paths.
type minioReservedBucketHandler struct {
	handler http.Handler
}

func setReservedBucketHandler(h http.Handler) http.Handler {
	return minioReservedBucketHandler{h}
}

func (h minioReservedBucketHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !guessIsRPCReq(r) && !guessIsBrowserReq(r) {
		// For all non browser, non RPC requests, reject access to 'minioReservedBucketPath'.
		bucketName, _ := urlPath2BucketObjectName(r.URL)
		if isMinioReservedBucket(bucketName) || isMinioMetaBucket(bucketName) {
			writeErrorResponse(w, ErrAllAccessDisabled, r.URL)
			return
		}
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
			writeErrorResponse(w, apiErr, r.URL)
			return
		}
		// Verify if the request date header is shifted by less than globalMaxSkewTime parameter in the past
		// or in the future, reject request otherwise.
		curTime := UTCNow()
		if curTime.Sub(amzDate) > globalMaxSkewTime || amzDate.Sub(curTime) > globalMaxSkewTime {
			writeErrorResponse(w, ErrRequestTimeTooSkewed, r.URL)
			return
		}
	}
	h.handler.ServeHTTP(w, r)
}

type resourceHandler struct {
	handler http.Handler
}

// List of http methods.
const (
	httpGET     = "GET"
	httpPUT     = "PUT"
	httpHEAD    = "HEAD"
	httpPOST    = "POST"
	httpDELETE  = "DELETE"
	httpOPTIONS = "OPTIONS"
)

// List of default allowable HTTP methods.
var defaultAllowableHTTPMethods = []string{
	httpGET,
	httpPUT,
	httpHEAD,
	httpPOST,
	httpDELETE,
	httpOPTIONS,
}

// setCorsHandler handler for CORS (Cross Origin Resource Sharing)
func setCorsHandler(h http.Handler) http.Handler {
	commonS3Headers := []string{"Content-Length", "Content-Type", "Connection",
		"Date", "ETag", "Server", "x-amz-delete-marker", "x-amz-id-2",
		"x-amz-request-id", "x-amz-version-id"}
	c := cors.New(cors.Options{
		AllowedOrigins:   []string{"*"},
		AllowedMethods:   defaultAllowableHTTPMethods,
		AllowedHeaders:   []string{"*"},
		ExposedHeaders:   commonS3Headers,
		AllowCredentials: true,
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
	bucketName, objectName := urlPath2BucketObjectName(r.URL)

	// If bucketName is present and not objectName check for bucket level resource queries.
	if bucketName != "" && objectName == "" {
		if ignoreNotImplementedBucketResources(r) {
			writeErrorResponse(w, ErrNotImplemented, r.URL)
			return
		}
	}
	// If bucketName and objectName are present check for its resource queries.
	if bucketName != "" && objectName != "" {
		if ignoreNotImplementedObjectResources(r) {
			writeErrorResponse(w, ErrNotImplemented, r.URL)
			return
		}
	}

	// Serve HTTP.
	h.handler.ServeHTTP(w, r)
}

// httpResponseRecorder wraps http.ResponseWriter
// to record some useful http response data.
type httpResponseRecorder struct {
	http.ResponseWriter
	respStatusCode int
}

// Wraps ResponseWriter's Write()
func (rww *httpResponseRecorder) Write(b []byte) (int, error) {
	return rww.ResponseWriter.Write(b)
}

// Wraps ResponseWriter's Flush()
func (rww *httpResponseRecorder) Flush() {
	rww.ResponseWriter.(http.Flusher).Flush()
}

// Wraps ResponseWriter's WriteHeader() and record
// the response status code
func (rww *httpResponseRecorder) WriteHeader(httpCode int) {
	rww.respStatusCode = httpCode
	rww.ResponseWriter.WriteHeader(httpCode)
}

func (rww *httpResponseRecorder) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	return rww.ResponseWriter.(http.Hijacker).Hijack()
}

// httpStatsHandler definition: gather HTTP statistics
type httpStatsHandler struct {
	handler http.Handler
}

// setHttpStatsHandler sets a http Stats Handler
func setHTTPStatsHandler(h http.Handler) http.Handler {
	return httpStatsHandler{handler: h}
}

func (h httpStatsHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Wraps w to record http response information
	ww := &httpResponseRecorder{ResponseWriter: w}

	// Time start before the call is about to start.
	tBefore := UTCNow()

	// Execute the request
	h.handler.ServeHTTP(ww, r)

	// Time after call has completed.
	tAfter := UTCNow()

	// Time duration in secs since the call started.
	//
	// We don't need to do nanosecond precision in this
	// simply for the fact that it is not human readable.
	durationSecs := tAfter.Sub(tBefore).Seconds()

	// Update http statistics
	globalHTTPStats.updateStats(r, ww, durationSecs)
}

// pathValidityHandler validates all the incoming paths for
// any bad components and rejects them.
type pathValidityHandler struct {
	handler http.Handler
}

func setPathValidityHandler(h http.Handler) http.Handler {
	return pathValidityHandler{handler: h}
}

// Bad path components to be rejected by the path validity handler.
const (
	dotdotComponent = ".."
	dotComponent    = "."
)

// Check if the incoming path has bad path components,
// such as ".." and "."
func hasBadPathComponent(path string) bool {
	path = strings.TrimSpace(path)
	for _, p := range strings.Split(path, slashSeparator) {
		switch strings.TrimSpace(p) {
		case dotdotComponent:
			return true
		case dotComponent:
			return true
		}
	}
	return false
}

func (h pathValidityHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Check for bad components in URL path.
	if hasBadPathComponent(r.URL.Path) {
		writeErrorResponse(w, ErrInvalidResourceName, r.URL)
		return
	}
	// Check for bad components in URL query values.
	for _, vv := range r.URL.Query() {
		for _, v := range vv {
			if hasBadPathComponent(v) {
				writeErrorResponse(w, ErrInvalidResourceName, r.URL)
				return
			}
		}
	}
	h.handler.ServeHTTP(w, r)
}

// setRateLimitHandler middleware limits the throughput to h using a
// rate.Limiter token bucket configured with maxOpenFileLimit and
// burst set to 1. The request will idle for up to 1*time.Second.
// If the limiter detects the deadline will be exceeded, the request is
// cancelled immediately.
func setRateLimitHandler(h http.Handler) http.Handler {
	_, maxLimit, err := sys.GetMaxOpenFileLimit()
	if err != nil {
		panic(err)
	}
	// Burst value is set to 1 to allow only maxOpenFileLimit
	// requests to happen at once.
	l := rate.NewLimiter(rate.Limit(maxLimit), 1)
	return rateLimit{l, h}
}

type rateLimit struct {
	*rate.Limiter
	handler http.Handler
}

func (l rateLimit) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// create a new context from the request with the wait timeout
	ctx, cancel := context.WithTimeout(r.Context(), 1*time.Second)
	defer cancel() // always cancel the context!

	// Wait errors out if the request cannot be processed within
	// the deadline. time/rate tries to reserve a slot if possible
	// with in the given duration if it's not possible then Wait(ctx)
	// returns an error and we cancel the request with ErrSlowDown
	// error message to the client. This context wait also ensures
	// requests doomed to fail are terminated early, preventing a
	// potential pileup on the server.
	if err := l.Wait(ctx); err != nil {
		// Send an S3 compatible error, SlowDown.
		writeErrorResponse(w, ErrSlowDown, r.URL)
		return
	}

	l.handler.ServeHTTP(w, r)
}

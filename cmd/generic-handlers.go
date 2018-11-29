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

	"github.com/minio/minio-go/pkg/set"

	humanize "github.com/dustin/go-humanize"
	"github.com/minio/minio/cmd/crypto"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/dns"
	"github.com/minio/minio/pkg/handlers"
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
		writeErrorResponse(w, ErrMetadataTooLarge, r.URL, guessIsBrowserReq(r))
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
		writeErrorResponse(w, ErrUnsupportedMetadata, r.URL, guessIsBrowserReq(r))
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

// guessIsHealthCheckReq - returns true if incoming request looks
// like healthcheck request
func guessIsHealthCheckReq(req *http.Request) bool {
	if req == nil {
		return false
	}
	aType := getRequestAuthType(req)
	return aType == authTypeAnonymous && (req.Method == http.MethodGet || req.Method == http.MethodHead) &&
		(req.URL.Path == healthCheckPathPrefix+healthCheckLivenessPath ||
			req.URL.Path == healthCheckPathPrefix+healthCheckReadinessPath)
}

// guessIsMetricsReq - returns true if incoming request looks
// like metrics request
func guessIsMetricsReq(req *http.Request) bool {
	if req == nil {
		return false
	}
	aType := getRequestAuthType(req)
	return aType == authTypeAnonymous &&
		req.URL.Path == minioReservedBucketPath+prometheusMetricsPath
}

// guessIsRPCReq - returns true if the request is for an RPC endpoint.
func guessIsRPCReq(req *http.Request) bool {
	if req == nil {
		return false
	}
	return req.Method == http.MethodPost &&
		strings.HasPrefix(req.URL.Path, minioReservedBucketPath+"/")
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
	if r.Method == http.MethodGet && guessIsBrowserReq(r) && globalIsBrowserEnabled {
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

// Check to allow access to the reserved "bucket" `/minio` for Admin
// API requests.
func isAdminReq(r *http.Request) bool {
	return strings.HasPrefix(r.URL.Path, adminAPIPathPrefix+"/")
}

// Adds verification for incoming paths.
type minioReservedBucketHandler struct {
	handler http.Handler
}

func setReservedBucketHandler(h http.Handler) http.Handler {
	return minioReservedBucketHandler{h}
}

func (h minioReservedBucketHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case guessIsRPCReq(r), guessIsBrowserReq(r), guessIsHealthCheckReq(r), guessIsMetricsReq(r), isAdminReq(r):
		// Allow access to reserved buckets
	default:
		// For all other requests reject access to reserved
		// buckets
		bucketName, _ := urlPath2BucketObjectName(r.URL.Path)
		if isMinioReservedBucket(bucketName) || isMinioMetaBucket(bucketName) {
			writeErrorResponse(w, ErrAllAccessDisabled, r.URL, guessIsBrowserReq(r))
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
			writeErrorResponse(w, apiErr, r.URL, guessIsBrowserReq(r))
			return
		}
		// Verify if the request date header is shifted by less than globalMaxSkewTime parameter in the past
		// or in the future, reject request otherwise.
		curTime := UTCNow()
		if curTime.Sub(amzDate) > globalMaxSkewTime || amzDate.Sub(curTime) > globalMaxSkewTime {
			writeErrorResponse(w, ErrRequestTimeTooSkewed, r.URL, guessIsBrowserReq(r))
			return
		}
	}
	h.handler.ServeHTTP(w, r)
}

type resourceHandler struct {
	handler http.Handler
}

// List of default allowable HTTP methods.
var defaultAllowableHTTPMethods = []string{
	http.MethodGet,
	http.MethodPut,
	http.MethodHead,
	http.MethodPost,
	http.MethodDelete,
	http.MethodOptions,
}

// setCorsHandler handler for CORS (Cross Origin Resource Sharing)
func setCorsHandler(h http.Handler) http.Handler {
	commonS3Headers := []string{
		"Date",
		"ETag",
		"Server",
		"Connection",
		"Accept-Ranges",
		"Content-Range",
		"Content-Encoding",
		"Content-Length",
		"Content-Type",
		"x-amz-request-id",
	}
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
		// Enable GetBucketACL dummy call specifically.
		if name == "acl" && req.Method == http.MethodGet {
			return false
		}

		if notimplementedBucketResourceNames[name] {
			return true
		}
	}
	return false
}

// Checks requests for not implemented Object resources
func ignoreNotImplementedObjectResources(req *http.Request) bool {
	for name := range req.URL.Query() {
		// Enable GetObjectACL dummy call specifically.
		if name == "acl" && req.Method == http.MethodGet {
			return false
		}
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
	"inventory":      true,
	"metrics":        true,
	"accelerate":     true,
}

// List of not implemented object queries
var notimplementedObjectResourceNames = map[string]bool{
	"torrent": true,
	"acl":     true,
	"policy":  true,
	"tagging": true,
	"restore": true,
}

// Resource handler ServeHTTP() wrapper
func (h resourceHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	bucketName, objectName := urlPath2BucketObjectName(r.URL.Path)

	// If bucketName is present and not objectName check for bucket level resource queries.
	if bucketName != "" && objectName == "" {
		if ignoreNotImplementedBucketResources(r) {
			writeErrorResponse(w, ErrNotImplemented, r.URL, guessIsBrowserReq(r))
			return
		}
	}
	// If bucketName and objectName are present check for its resource queries.
	if bucketName != "" && objectName != "" {
		if ignoreNotImplementedObjectResources(r) {
			writeErrorResponse(w, ErrNotImplemented, r.URL, guessIsBrowserReq(r))
			return
		}
	}
	// A put method on path "/" doesn't make sense, ignore it.
	if r.Method == http.MethodPut && r.URL.Path == "/" {
		writeErrorResponse(w, ErrNotImplemented, r.URL, guessIsBrowserReq(r))
		return
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
		writeErrorResponse(w, ErrInvalidResourceName, r.URL, guessIsBrowserReq(r))
		return
	}
	// Check for bad components in URL query values.
	for _, vv := range r.URL.Query() {
		for _, v := range vv {
			if hasBadPathComponent(v) {
				writeErrorResponse(w, ErrInvalidResourceName, r.URL, guessIsBrowserReq(r))
				return
			}
		}
	}
	h.handler.ServeHTTP(w, r)
}

// To forward the path style requests on a bucket to the right
// configured server, bucket to IP configuration is obtained
// from centralized etcd configuration service.
type bucketForwardingHandler struct {
	fwd     *handlers.Forwarder
	handler http.Handler
}

func (f bucketForwardingHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if globalDNSConfig == nil || globalDomainName == "" ||
		guessIsHealthCheckReq(r) || guessIsMetricsReq(r) ||
		guessIsRPCReq(r) || isAdminReq(r) {
		f.handler.ServeHTTP(w, r)
		return
	}

	// For browser requests, when federation is setup we need to
	// specifically handle download and upload for browser requests.
	if guessIsBrowserReq(r) && globalDNSConfig != nil && globalDomainName != "" {
		var bucket, _ string
		switch r.Method {
		case http.MethodPut:
			if getRequestAuthType(r) == authTypeJWT {
				bucket, _ = urlPath2BucketObjectName(strings.TrimPrefix(r.URL.Path, minioReservedBucketPath+"/upload"))
			}
		case http.MethodGet:
			if t := r.URL.Query().Get("token"); t != "" {
				bucket, _ = urlPath2BucketObjectName(strings.TrimPrefix(r.URL.Path, minioReservedBucketPath+"/download"))
			} else if getRequestAuthType(r) != authTypeJWT && !strings.HasPrefix(r.URL.Path, minioReservedBucketPath) {
				bucket, _ = urlPath2BucketObjectName(r.URL.Path)
			}
		}
		if bucket != "" {
			sr, err := globalDNSConfig.Get(bucket)
			if err != nil {
				if err == dns.ErrNoEntriesFound {
					writeErrorResponse(w, ErrNoSuchBucket, r.URL, guessIsBrowserReq(r))
				} else {
					writeErrorResponse(w, toAPIErrorCode(context.Background(), err), r.URL, guessIsBrowserReq(r))
				}
				return
			}
			if globalDomainIPs.Intersection(set.CreateStringSet(getHostsSlice(sr)...)).IsEmpty() {
				r.URL.Scheme = "http"
				if globalIsSSL {
					r.URL.Scheme = "https"
				}
				r.URL.Host = getHostFromSrv(sr)
				f.fwd.ServeHTTP(w, r)
				return
			}
		}
		f.handler.ServeHTTP(w, r)
		return
	}

	bucket, object := urlPath2BucketObjectName(r.URL.Path)
	// ListBucket requests should be handled at current endpoint as
	// all buckets data can be fetched from here.
	if r.Method == http.MethodGet && bucket == "" && object == "" {
		f.handler.ServeHTTP(w, r)
		return
	}

	// MakeBucket requests should be handled at current endpoint
	if r.Method == http.MethodPut && bucket != "" && object == "" {
		f.handler.ServeHTTP(w, r)
		return
	}

	// CopyObject requests should be handled at current endpoint as path style
	// requests have target bucket and object in URI and source details are in
	// header fields
	if r.Method == http.MethodPut && r.Header.Get("X-Amz-Copy-Source") != "" {
		f.handler.ServeHTTP(w, r)
		return
	}
	sr, err := globalDNSConfig.Get(bucket)
	if err != nil {
		if err == dns.ErrNoEntriesFound {
			writeErrorResponse(w, ErrNoSuchBucket, r.URL, guessIsBrowserReq(r))
		} else {
			writeErrorResponse(w, toAPIErrorCode(context.Background(), err), r.URL, guessIsBrowserReq(r))
		}
		return
	}
	if globalDomainIPs.Intersection(set.CreateStringSet(getHostsSlice(sr)...)).IsEmpty() {
		r.URL.Scheme = "http"
		if globalIsSSL {
			r.URL.Scheme = "https"
		}
		r.URL.Host = getHostFromSrv(sr)
		f.fwd.ServeHTTP(w, r)
		return
	}
	f.handler.ServeHTTP(w, r)
}

// setBucketForwardingHandler middleware forwards the path style requests
// on a bucket to the right bucket location, bucket to IP configuration
// is obtained from centralized etcd configuration service.
func setBucketForwardingHandler(h http.Handler) http.Handler {
	fwd := handlers.NewForwarder(&handlers.Forwarder{
		PassHost:     true,
		RoundTripper: NewCustomHTTPTransport(),
	})
	return bucketForwardingHandler{fwd, h}
}

// setRateLimitHandler middleware limits the throughput to h using a
// rate.Limiter token bucket configured with maxOpenFileLimit and
// burst set to 1. The request will idle for up to 1*time.Second.
// If the limiter detects the deadline will be exceeded, the request is
// canceled immediately.
func setRateLimitHandler(h http.Handler) http.Handler {
	_, maxLimit, err := sys.GetMaxOpenFileLimit()
	logger.FatalIf(err, "Unable to get maximum open file limit")
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
		writeErrorResponse(w, ErrSlowDown, r.URL, guessIsBrowserReq(r))
		return
	}

	l.handler.ServeHTTP(w, r)
}

// customHeaderHandler sets x-amz-request-id, x-minio-deployment-id header.
// Previously, this value was set right before a response was sent to
// the client. So, logger and Error response XML were not using this
// value. This is set here so that this header can be logged as
// part of the log entry, Error response XML and auditing.
type customHeaderHandler struct {
	handler http.Handler
}

func addCustomHeaders(h http.Handler) http.Handler {
	return customHeaderHandler{handler: h}
}

func (s customHeaderHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Set custom headers such as x-amz-request-id and x-minio-deployment-id
	// for each request.
	w.Header().Set(responseRequestIDKey, mustGetRequestID(UTCNow()))
	if globalDeploymentID != "" {
		w.Header().Set(responseDeploymentIDKey, globalDeploymentID)
	}
	s.handler.ServeHTTP(logger.NewResponseWriter(w), r)
}

type securityHeaderHandler struct {
	handler http.Handler
}

func addSecurityHeaders(h http.Handler) http.Handler {
	return securityHeaderHandler{handler: h}
}

func (s securityHeaderHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	header := w.Header()
	header.Set("X-XSS-Protection", "1; mode=block")                  // Prevents against XSS attacks
	header.Set("Content-Security-Policy", "block-all-mixed-content") // prevent mixed (HTTP / HTTPS content)
	s.handler.ServeHTTP(w, r)
}

// criticalErrorHandler handles critical server failures caused by
// `panic(logger.ErrCritical)` as done by `logger.CriticalIf`.
//
// It should be always the first / highest HTTP handler.
type criticalErrorHandler struct{ handler http.Handler }

func (h criticalErrorHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	defer func() {
		if err := recover(); err == logger.ErrCritical { // handle
			writeErrorResponse(w, ErrInternalError, r.URL, guessIsBrowserReq(r))
		} else if err != nil {
			panic(err) // forward other panic calls
		}
	}()
	h.handler.ServeHTTP(w, r)
}

func setSSETLSHandler(h http.Handler) http.Handler { return sseTLSHandler{h} }

// sseTLSHandler enforces certain rules for SSE requests which are made / must be made over TLS.
type sseTLSHandler struct{ handler http.Handler }

func (h sseTLSHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Deny SSE-C requests if not made over TLS
	if !globalIsSSL && (crypto.SSEC.IsRequested(r.Header) || crypto.SSECopy.IsRequested(r.Header)) {
		if r.Method == http.MethodHead {
			writeErrorResponseHeadersOnly(w, ErrInsecureSSECustomerRequest)
		} else {
			writeErrorResponse(w, ErrInsecureSSECustomerRequest, r.URL, guessIsBrowserReq(r))
		}
		return
	}
	h.handler.ServeHTTP(w, r)
}

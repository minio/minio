// Copyright (c) 2015-2021 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package cmd

import (
	"net"
	"net/http"
	"path"
	"runtime/debug"
	"strings"
	"sync/atomic"
	"time"

	"github.com/minio/minio-go/v7/pkg/set"
	xnet "github.com/minio/pkg/net"

	"github.com/dustin/go-humanize"
	"github.com/minio/minio/internal/config/dns"
	"github.com/minio/minio/internal/crypto"
	xhttp "github.com/minio/minio/internal/http"
	"github.com/minio/minio/internal/http/stats"
	"github.com/minio/minio/internal/logger"
)

const (
	// Maximum allowed form data field values. 64MiB is a guessed practical value
	// which is more than enough to accommodate any form data fields and headers.
	requestFormDataSize = 64 * humanize.MiByte

	// For any HTTP request, request body should be not more than 16GiB + requestFormDataSize
	// where, 16GiB is the maximum allowed object size for object upload.
	requestMaxBodySize = globalMaxObjectSize + requestFormDataSize

	// Maximum size for http headers - See: https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingMetadata.html
	maxHeaderSize = 8 * 1024

	// Maximum size for user-defined metadata - See: https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingMetadata.html
	maxUserDataSize = 2 * 1024
)

// ReservedMetadataPrefix is the prefix of a metadata key which
// is reserved and for internal use only.
const (
	ReservedMetadataPrefix      = "X-Minio-Internal-"
	ReservedMetadataPrefixLower = "x-minio-internal-"
)

// containsReservedMetadata returns true if the http.Header contains
// keys which are treated as metadata but are reserved for internal use
// and must not set by clients
func containsReservedMetadata(header http.Header) bool {
	for key := range header {
		if strings.HasPrefix(strings.ToLower(key), ReservedMetadataPrefixLower) {
			return true
		}
	}
	return false
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
			if strings.HasPrefix(strings.ToLower(key), prefix) {
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

// Limits body and header to specific allowed maximum limits as per S3/MinIO API requirements.
func setRequestLimitHandler(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Reject unsupported reserved metadata first before validation.
		if containsReservedMetadata(r.Header) {
			writeErrorResponse(r.Context(), w, errorCodes.ToAPIErr(ErrUnsupportedMetadata), r.URL)
			return
		}
		if isHTTPHeaderSizeTooLarge(r.Header) {
			writeErrorResponse(r.Context(), w, errorCodes.ToAPIErr(ErrMetadataTooLarge), r.URL)
			atomic.AddUint64(&globalHTTPStats.rejectedRequestsHeader, 1)
			return
		}
		// Restricting read data to a given maximum length
		r.Body = http.MaxBytesReader(w, r.Body, requestMaxBodySize)
		h.ServeHTTP(w, r)
	})
}

// Reserved bucket.
const (
	minioReservedBucket     = "minio"
	minioReservedBucketPath = SlashSeparator + minioReservedBucket
	loginPathPrefix         = SlashSeparator + "login"
)

func guessIsBrowserReq(r *http.Request) bool {
	aType := getRequestAuthType(r)
	return strings.Contains(r.Header.Get("User-Agent"), "Mozilla") &&
		globalBrowserEnabled && aType == authTypeAnonymous
}

func setBrowserRedirectHandler(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		read := r.Method == http.MethodGet || r.Method == http.MethodHead
		// Re-direction is handled specifically for browser requests.
		if guessIsBrowserReq(r) && read {
			// Fetch the redirect location if any.
			if u := getRedirectLocation(r); u != nil {
				// Employ a temporary re-direct.
				http.Redirect(w, r, u.String(), http.StatusTemporaryRedirect)
				return
			}
		}
		h.ServeHTTP(w, r)
	})
}

// Fetch redirect location if urlPath satisfies certain
// criteria. Some special names are considered to be
// redirectable, this is purely internal function and
// serves only limited purpose on redirect-handler for
// browser requests.
func getRedirectLocation(r *http.Request) *xnet.URL {
	resource, err := getResource(r.URL.Path, r.Host, globalDomainNames)
	if err != nil {
		return nil
	}
	for _, prefix := range []string{
		"favicon-16x16.png",
		"favicon-32x32.png",
		"favicon-96x96.png",
		"index.html",
		minioReservedBucket,
	} {
		bucket, _ := path2BucketObject(resource)
		if path.Clean(bucket) == prefix || resource == slashSeparator {
			if globalBrowserRedirectURL != nil {
				return globalBrowserRedirectURL
			}
			hostname, _, _ := net.SplitHostPort(r.Host)
			if hostname == "" {
				hostname = r.Host
			}
			return &xnet.URL{
				Host: net.JoinHostPort(hostname, globalMinioConsolePort),
				Scheme: func() string {
					scheme := "http"
					if r.TLS != nil {
						scheme = "https"
					}
					return scheme
				}(),
			}
		}
	}
	return nil
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
			req.URL.Path == healthCheckPathPrefix+healthCheckReadinessPath ||
			req.URL.Path == healthCheckPathPrefix+healthCheckClusterPath ||
			req.URL.Path == healthCheckPathPrefix+healthCheckClusterReadPath)
}

// guessIsMetricsReq - returns true if incoming request looks
// like metrics request
func guessIsMetricsReq(req *http.Request) bool {
	if req == nil {
		return false
	}
	aType := getRequestAuthType(req)
	return (aType == authTypeAnonymous || aType == authTypeJWT) &&
		req.URL.Path == minioReservedBucketPath+prometheusMetricsPathLegacy ||
		req.URL.Path == minioReservedBucketPath+prometheusMetricsV2ClusterPath ||
		req.URL.Path == minioReservedBucketPath+prometheusMetricsV2NodePath
}

// guessIsRPCReq - returns true if the request is for an RPC endpoint.
func guessIsRPCReq(req *http.Request) bool {
	if req == nil {
		return false
	}
	return req.Method == http.MethodPost &&
		strings.HasPrefix(req.URL.Path, minioReservedBucketPath+SlashSeparator)
}

// Check to allow access to the reserved "bucket" `/minio` for Admin
// API requests.
func isAdminReq(r *http.Request) bool {
	return strings.HasPrefix(r.URL.Path, adminPathPrefix)
}

// Supported amz date formats.
var amzDateFormats = []string{
	// Do not change this order, x-amz-date format is usually in
	// iso8601Format rest are meant for relaxed handling of other
	// odd SDKs that might be out there.
	iso8601Format,
	time.RFC1123,
	time.RFC1123Z,
	// Add new AMZ date formats here.
}

// Supported Amz date headers.
var amzDateHeaders = []string{
	// Do not chane this order, x-amz-date value should be
	// validated first.
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
		amzDateStr := req.Header.Get(amzDateHeader)
		if amzDateStr != "" {
			return parseAmzDate(amzDateStr)
		}
	}
	// Date header missing.
	return time.Time{}, ErrMissingDateHeader
}

// setHttpStatsHandler sets a http Stats handler to gather HTTP statistics
func setHTTPStatsHandler(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Meters s3 connection stats.
		meteredRequest := &stats.IncomingTrafficMeter{ReadCloser: r.Body}
		meteredResponse := &stats.OutgoingTrafficMeter{ResponseWriter: w}

		// Execute the request
		r.Body = meteredRequest
		h.ServeHTTP(meteredResponse, r)

		if strings.HasPrefix(r.URL.Path, minioReservedBucketPath) {
			globalConnStats.incInputBytes(meteredRequest.BytesRead())
			globalConnStats.incOutputBytes(meteredResponse.BytesWritten())
		} else {
			globalConnStats.incS3InputBytes(meteredRequest.BytesRead())
			globalConnStats.incS3OutputBytes(meteredResponse.BytesWritten())
		}
	})
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
	for _, p := range strings.Split(path, SlashSeparator) {
		switch strings.TrimSpace(p) {
		case dotdotComponent:
			return true
		case dotComponent:
			return true
		}
	}
	return false
}

// Check if client is sending a malicious request.
func hasMultipleAuth(r *http.Request) bool {
	authTypeCount := 0
	for _, hasValidAuth := range []func(*http.Request) bool{isRequestSignatureV2, isRequestPresignedSignatureV2, isRequestSignatureV4, isRequestPresignedSignatureV4, isRequestJWT, isRequestPostPolicySignatureV4} {
		if hasValidAuth(r) {
			authTypeCount++
		}
	}
	return authTypeCount > 1
}

// requestValidityHandler validates all the incoming paths for
// any malicious requests.
func setRequestValidityHandler(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Check for bad components in URL path.
		if hasBadPathComponent(r.URL.Path) {
			writeErrorResponse(r.Context(), w, errorCodes.ToAPIErr(ErrInvalidResourceName), r.URL)
			atomic.AddUint64(&globalHTTPStats.rejectedRequestsInvalid, 1)
			return
		}
		// Check for bad components in URL query values.
		for _, vv := range r.Form {
			for _, v := range vv {
				if hasBadPathComponent(v) {
					writeErrorResponse(r.Context(), w, errorCodes.ToAPIErr(ErrInvalidResourceName), r.URL)
					atomic.AddUint64(&globalHTTPStats.rejectedRequestsInvalid, 1)
					return
				}
			}
		}
		if hasMultipleAuth(r) {
			writeErrorResponse(r.Context(), w, errorCodes.ToAPIErr(ErrInvalidRequest), r.URL)
			atomic.AddUint64(&globalHTTPStats.rejectedRequestsInvalid, 1)
			return
		}
		// For all other requests reject access to reserved buckets
		bucketName, _ := request2BucketObjectName(r)
		if isMinioReservedBucket(bucketName) || isMinioMetaBucket(bucketName) {
			if !guessIsRPCReq(r) && !guessIsBrowserReq(r) && !guessIsHealthCheckReq(r) && !guessIsMetricsReq(r) && !isAdminReq(r) {
				writeErrorResponse(r.Context(), w, errorCodes.ToAPIErr(ErrAllAccessDisabled), r.URL)
				return
			}
		}
		// Deny SSE-C requests if not made over TLS
		if !globalIsTLS && (crypto.SSEC.IsRequested(r.Header) || crypto.SSECopy.IsRequested(r.Header)) {
			if r.Method == http.MethodHead {
				writeErrorResponseHeadersOnly(w, errorCodes.ToAPIErr(ErrInsecureSSECustomerRequest))
			} else {
				writeErrorResponse(r.Context(), w, errorCodes.ToAPIErr(ErrInsecureSSECustomerRequest), r.URL)
			}
			return
		}
		h.ServeHTTP(w, r)
	})
}

// setBucketForwardingHandler middleware forwards the path style requests
// on a bucket to the right bucket location, bucket to IP configuration
// is obtained from centralized etcd configuration service.
func setBucketForwardingHandler(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if guessIsHealthCheckReq(r) || guessIsMetricsReq(r) ||
			guessIsRPCReq(r) || guessIsLoginSTSReq(r) || isAdminReq(r) {
			h.ServeHTTP(w, r)
			return
		}

		bucket, object := request2BucketObjectName(r)

		// Requests in federated setups for STS type calls which are
		// performed at '/' resource should be routed by the muxer,
		// the assumption is simply such that requests without a bucket
		// in a federated setup cannot be proxied, so serve them at
		// current server.
		if bucket == "" {
			h.ServeHTTP(w, r)
			return
		}

		// MakeBucket requests should be handled at current endpoint
		if r.Method == http.MethodPut && bucket != "" && object == "" && r.URL.RawQuery == "" {
			h.ServeHTTP(w, r)
			return
		}

		// CopyObject requests should be handled at current endpoint as path style
		// requests have target bucket and object in URI and source details are in
		// header fields
		if r.Method == http.MethodPut && r.Header.Get(xhttp.AmzCopySource) != "" {
			bucket, object = path2BucketObject(r.Header.Get(xhttp.AmzCopySource))
			if bucket == "" || object == "" {
				h.ServeHTTP(w, r)
				return
			}
		}
		sr, err := globalDNSConfig.Get(bucket)
		if err != nil {
			if err == dns.ErrNoEntriesFound {
				writeErrorResponse(r.Context(), w, errorCodes.ToAPIErr(ErrNoSuchBucket), r.URL)
			} else {
				writeErrorResponse(r.Context(), w, toAPIError(r.Context(), err), r.URL)
			}
			return
		}
		if globalDomainIPs.Intersection(set.CreateStringSet(getHostsSlice(sr)...)).IsEmpty() {
			r.URL.Scheme = "http"
			if globalIsTLS {
				r.URL.Scheme = "https"
			}
			r.URL.Host = getHostFromSrv(sr)
			// Make sure we remove any existing headers before
			// proxying the request to another node.
			for k := range w.Header() {
				w.Header().Del(k)
			}
			globalForwarder.ServeHTTP(w, r)
			return
		}
		h.ServeHTTP(w, r)
	})
}

// addCustomHeaders adds various HTTP(S) response headers.
// Security Headers enable various security protections behaviors in the client's browser.
func addCustomHeaders(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		header := w.Header()
		header.Set("X-XSS-Protection", "1; mode=block")                                // Prevents against XSS attacks
		header.Set("Content-Security-Policy", "block-all-mixed-content")               // prevent mixed (HTTP / HTTPS content)
		header.Set("X-Content-Type-Options", "nosniff")                                // Prevent mime-sniff
		header.Set("Strict-Transport-Security", "max-age=31536000; includeSubDomains") // HSTS mitigates variants of MITM attacks

		// Previously, this value was set right before a response was sent to
		// the client. So, logger and Error response XML were not using this
		// value. This is set here so that this header can be logged as
		// part of the log entry, Error response XML and auditing.
		// Set custom headers such as x-amz-request-id for each request.
		w.Header().Set(xhttp.AmzRequestID, mustGetRequestID(UTCNow()))
		h.ServeHTTP(logger.NewResponseWriter(w), r)
	})
}

// criticalErrorHandler handles panics and fatal errors by
// `panic(logger.ErrCritical)` as done by `logger.CriticalIf`.
//
// It should be always the first / highest HTTP handler.
func setCriticalErrorHandler(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec == logger.ErrCritical { // handle
				stack := debug.Stack()
				logger.Error("critical: \"%s %s\": %v\n%s", r.Method, r.URL, rec, string(stack))
				writeErrorResponse(r.Context(), w, errorCodes.ToAPIErr(ErrInternalError), r.URL)
				return
			} else if rec != nil {
				stack := debug.Stack()
				logger.Error("panic: \"%s %s\": %v\n%s", r.Method, r.URL, rec, string(stack))
				// Try to write an error response, upstream may not have written header.
				writeErrorResponse(r.Context(), w, errorCodes.ToAPIErr(ErrInternalError), r.URL)
				return
			}
		}()
		h.ServeHTTP(w, r)
	})
}

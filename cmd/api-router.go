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

	consoleapi "github.com/minio/console/api"
	xhttp "github.com/minio/minio/internal/http"
	"github.com/minio/mux"
	"github.com/minio/pkg/v3/wildcard"
	"github.com/rs/cors"
)

func newHTTPServerFn() *xhttp.Server {
	globalObjLayerMutex.RLock()
	defer globalObjLayerMutex.RUnlock()
	return globalHTTPServer
}

func setHTTPServer(h *xhttp.Server) {
	globalObjLayerMutex.Lock()
	globalHTTPServer = h
	globalObjLayerMutex.Unlock()
}

func newConsoleServerFn() *consoleapi.Server {
	globalObjLayerMutex.RLock()
	defer globalObjLayerMutex.RUnlock()
	return globalConsoleSrv
}

func setConsoleSrv(srv *consoleapi.Server) {
	globalObjLayerMutex.Lock()
	globalConsoleSrv = srv
	globalObjLayerMutex.Unlock()
}

func newObjectLayerFn() ObjectLayer {
	globalObjLayerMutex.RLock()
	defer globalObjLayerMutex.RUnlock()
	return globalObjectAPI
}

func setObjectLayer(o ObjectLayer) {
	globalObjLayerMutex.Lock()
	globalObjectAPI = o
	globalObjLayerMutex.Unlock()
}

// objectAPIHandlers implements and provides http handlers for S3 API.
type objectAPIHandlers struct {
	ObjectAPI func() ObjectLayer
}

// getHost tries its best to return the request host.
// According to section 14.23 of RFC 2616 the Host header
// can include the port number if the default value of 80 is not used.
func getHost(r *http.Request) string {
	if r.URL.IsAbs() {
		return r.URL.Host
	}
	return r.Host
}

func notImplementedHandler(w http.ResponseWriter, r *http.Request) {
	writeErrorResponse(r.Context(), w, errorCodes.ToAPIErr(ErrNotImplemented), r.URL)
}

type rejectedAPI struct {
	api     string
	methods []string
	queries []string
	path    string
}

var rejectedObjAPIs = []rejectedAPI{
	{
		api:     "torrent",
		methods: []string{http.MethodPut, http.MethodDelete, http.MethodGet},
		queries: []string{"torrent", ""},
		path:    "/{object:.+}",
	},
	{
		api:     "acl",
		methods: []string{http.MethodDelete},
		queries: []string{"acl", ""},
		path:    "/{object:.+}",
	},
}

var rejectedBucketAPIs = []rejectedAPI{
	{
		api:     "inventory",
		methods: []string{http.MethodGet, http.MethodPut, http.MethodDelete},
		queries: []string{"inventory", ""},
	},
	{
		api:     "cors",
		methods: []string{http.MethodPut, http.MethodDelete},
		queries: []string{"cors", ""},
	},
	{
		api:     "metrics",
		methods: []string{http.MethodGet, http.MethodPut, http.MethodDelete},
		queries: []string{"metrics", ""},
	},
	{
		api:     "website",
		methods: []string{http.MethodPut},
		queries: []string{"website", ""},
	},
	{
		api:     "logging",
		methods: []string{http.MethodPut, http.MethodDelete},
		queries: []string{"logging", ""},
	},
	{
		api:     "accelerate",
		methods: []string{http.MethodPut, http.MethodDelete},
		queries: []string{"accelerate", ""},
	},
	{
		api:     "requestPayment",
		methods: []string{http.MethodPut, http.MethodDelete},
		queries: []string{"requestPayment", ""},
	},
	{
		api:     "acl",
		methods: []string{http.MethodDelete, http.MethodPut, http.MethodHead},
		queries: []string{"acl", ""},
	},
	{
		api:     "publicAccessBlock",
		methods: []string{http.MethodDelete, http.MethodPut, http.MethodGet},
		queries: []string{"publicAccessBlock", ""},
	},
	{
		api:     "ownershipControls",
		methods: []string{http.MethodDelete, http.MethodPut, http.MethodGet},
		queries: []string{"ownershipControls", ""},
	},
	{
		api:     "intelligent-tiering",
		methods: []string{http.MethodDelete, http.MethodPut, http.MethodGet},
		queries: []string{"intelligent-tiering", ""},
	},
	{
		api:     "analytics",
		methods: []string{http.MethodDelete, http.MethodPut, http.MethodGet},
		queries: []string{"analytics", ""},
	},
}

// Set of s3 handler options as bit flags.
type s3HFlag uint8

const (
	// when provided, disables Gzip compression.
	noGZS3HFlag = 1 << iota

	// when provided, enables only tracing of headers. Otherwise, both headers
	// and body are traced.
	traceHdrsS3HFlag

	// when provided, disables throttling via the `maxClients` middleware.
	noThrottleS3HFlag
)

func (h s3HFlag) has(flag s3HFlag) bool {
	// Use bitwise-AND and check if the result is non-zero.
	return h&flag != 0
}

// s3APIMiddleware - performs some common handler functionality for S3 API
// handlers.
//
// It is set per-"handler function registration" in the router to allow for
// behavior modification via flags.
//
// This middleware always calls `collectAPIStats` to collect API stats.
//
// The passed in handler function must be a method of `objectAPIHandlers` for
// the name displayed in logs and trace to be accurate. The name is extracted
// via reflection.
//
// When **no** flags are passed, the behavior is to trace both headers and body,
// gzip the response and throttle the handler via `maxClients`. Each of these
// can be disabled via the corresponding `s3HFlag`.
//
// CAUTION: for requests involving large req/resp bodies ensure to pass the
// `traceHdrsS3HFlag`, otherwise both headers and body will be traced, causing
// high memory usage!
func s3APIMiddleware(f http.HandlerFunc, flags ...s3HFlag) http.HandlerFunc {
	// Collect all flags with bitwise-OR and assign operator
	var handlerFlags s3HFlag
	for _, flag := range flags {
		handlerFlags |= flag
	}

	// Get name of the handler using reflection.
	handlerName := getHandlerName(f, "objectAPIHandlers")

	var handler http.HandlerFunc = func(w http.ResponseWriter, r *http.Request) {
		// Wrap the actual handler with the appropriate tracing middleware.
		var tracedHandler http.HandlerFunc
		if handlerFlags.has(traceHdrsS3HFlag) {
			tracedHandler = httpTraceHdrs(f)
		} else {
			tracedHandler = httpTraceAll(f)
		}

		// Skip wrapping with the gzip middleware if specified.
		gzippedHandler := tracedHandler
		if !handlerFlags.has(noGZS3HFlag) {
			gzippedHandler = gzipHandler(gzippedHandler)
		}

		// Skip wrapping with throttling middleware if specified.
		throttledHandler := gzippedHandler
		if !handlerFlags.has(noThrottleS3HFlag) {
			throttledHandler = maxClients(throttledHandler)
		}

		// Collect API stats using the API name got from reflection in
		// `getHandlerName`.
		statsCollectedHandler := collectAPIStats(handlerName, throttledHandler)

		// Call the final handler.
		statsCollectedHandler(w, r)
	}

	return handler
}

// registerAPIRouter - registers S3 compatible APIs.
func registerAPIRouter(router *mux.Router) {
	// Initialize API.
	api := objectAPIHandlers{
		ObjectAPI: newObjectLayerFn,
	}

	// API Router
	apiRouter := router.PathPrefix(SlashSeparator).Subrouter()

	var routers []*mux.Router
	for _, domainName := range globalDomainNames {
		if IsKubernetes() {
			routers = append(routers, apiRouter.MatcherFunc(func(r *http.Request, match *mux.RouteMatch) bool {
				host, _, err := net.SplitHostPort(getHost(r))
				if err != nil {
					host = r.Host
				}
				// Make sure to skip matching minio.<domain>` this is
				// specifically meant for operator/k8s deployment
				// The reason we need to skip this is for a special
				// usecase where we need to make sure that
				// minio.<namespace>.svc.<cluster_domain> is ignored
				// by the bucketDNS style to ensure that path style
				// is available and honored at this domain.
				//
				// All other `<bucket>.<namespace>.svc.<cluster_domain>`
				// makes sure that buckets are routed through this matcher
				// to match for `<bucket>`
				return host != minioReservedBucket+"."+domainName
			}).Host("{bucket:.+}."+domainName).Subrouter())
		} else {
			routers = append(routers, apiRouter.Host("{bucket:.+}."+domainName).Subrouter())
		}
	}
	routers = append(routers, apiRouter.PathPrefix("/{bucket}").Subrouter())

	for _, router := range routers {
		// Register all rejected object APIs
		for _, r := range rejectedObjAPIs {
			t := router.Methods(r.methods...).
				HandlerFunc(collectAPIStats(r.api, httpTraceAll(notImplementedHandler))).
				Queries(r.queries...)
			t.Path(r.path)
		}

		// Object operations
		// HeadObject
		router.Methods(http.MethodHead).Path("/{object:.+}").
			HandlerFunc(s3APIMiddleware(api.HeadObjectHandler))

		// GetObjectAttributes
		router.Methods(http.MethodGet).Path("/{object:.+}").
			HandlerFunc(s3APIMiddleware(api.GetObjectAttributesHandler, traceHdrsS3HFlag)).
			Queries("attributes", "")

		// CopyObjectPart
		router.Methods(http.MethodPut).Path("/{object:.+}").
			HeadersRegexp(xhttp.AmzCopySource, ".*?(\\/|%2F).*?").
			HandlerFunc(s3APIMiddleware(api.CopyObjectPartHandler)).
			Queries("partNumber", "{partNumber:.*}", "uploadId", "{uploadId:.*}")
		// PutObjectPart
		router.Methods(http.MethodPut).Path("/{object:.+}").
			HandlerFunc(s3APIMiddleware(api.PutObjectPartHandler, traceHdrsS3HFlag)).
			Queries("partNumber", "{partNumber:.*}", "uploadId", "{uploadId:.*}")
		// ListObjectParts
		router.Methods(http.MethodGet).Path("/{object:.+}").
			HandlerFunc(s3APIMiddleware(api.ListObjectPartsHandler)).
			Queries("uploadId", "{uploadId:.*}")
		// CompleteMultipartUpload
		router.Methods(http.MethodPost).Path("/{object:.+}").
			HandlerFunc(s3APIMiddleware(api.CompleteMultipartUploadHandler)).
			Queries("uploadId", "{uploadId:.*}")
		// NewMultipartUpload
		router.Methods(http.MethodPost).Path("/{object:.+}").
			HandlerFunc(s3APIMiddleware(api.NewMultipartUploadHandler)).
			Queries("uploads", "")
		// AbortMultipartUpload
		router.Methods(http.MethodDelete).Path("/{object:.+}").
			HandlerFunc(s3APIMiddleware(api.AbortMultipartUploadHandler)).
			Queries("uploadId", "{uploadId:.*}")
		// GetObjectACL - this is a dummy call.
		router.Methods(http.MethodGet).Path("/{object:.+}").
			HandlerFunc(s3APIMiddleware(api.GetObjectACLHandler, traceHdrsS3HFlag)).
			Queries("acl", "")
		// PutObjectACL - this is a dummy call.
		router.Methods(http.MethodPut).Path("/{object:.+}").
			HandlerFunc(s3APIMiddleware(api.PutObjectACLHandler, traceHdrsS3HFlag)).
			Queries("acl", "")
		// GetObjectTagging
		router.Methods(http.MethodGet).Path("/{object:.+}").
			HandlerFunc(s3APIMiddleware(api.GetObjectTaggingHandler, traceHdrsS3HFlag)).
			Queries("tagging", "")
		// PutObjectTagging
		router.Methods(http.MethodPut).Path("/{object:.+}").
			HandlerFunc(s3APIMiddleware(api.PutObjectTaggingHandler, traceHdrsS3HFlag)).
			Queries("tagging", "")
		// DeleteObjectTagging
		router.Methods(http.MethodDelete).Path("/{object:.+}").
			HandlerFunc(s3APIMiddleware(api.DeleteObjectTaggingHandler, traceHdrsS3HFlag)).
			Queries("tagging", "")
		// SelectObjectContent
		router.Methods(http.MethodPost).Path("/{object:.+}").
			HandlerFunc(s3APIMiddleware(api.SelectObjectContentHandler, traceHdrsS3HFlag)).
			Queries("select", "").Queries("select-type", "2")
		// GetObjectRetention
		router.Methods(http.MethodGet).Path("/{object:.+}").
			HandlerFunc(s3APIMiddleware(api.GetObjectRetentionHandler)).
			Queries("retention", "")
		// GetObjectLegalHold
		router.Methods(http.MethodGet).Path("/{object:.+}").
			HandlerFunc(s3APIMiddleware(api.GetObjectLegalHoldHandler)).
			Queries("legal-hold", "")
		// GetObject with lambda ARNs
		router.Methods(http.MethodGet).Path("/{object:.+}").
			HandlerFunc(s3APIMiddleware(api.GetObjectLambdaHandler, traceHdrsS3HFlag)).
			Queries("lambdaArn", "{lambdaArn:.*}")
		// GetObject
		router.Methods(http.MethodGet).Path("/{object:.+}").
			HandlerFunc(s3APIMiddleware(api.GetObjectHandler, traceHdrsS3HFlag))
		// CopyObject
		router.Methods(http.MethodPut).Path("/{object:.+}").
			HeadersRegexp(xhttp.AmzCopySource, ".*?(\\/|%2F).*?").
			HandlerFunc(s3APIMiddleware(api.CopyObjectHandler))
		// PutObjectRetention
		router.Methods(http.MethodPut).Path("/{object:.+}").
			HandlerFunc(s3APIMiddleware(api.PutObjectRetentionHandler)).
			Queries("retention", "")
		// PutObjectLegalHold
		router.Methods(http.MethodPut).Path("/{object:.+}").
			HandlerFunc(s3APIMiddleware(api.PutObjectLegalHoldHandler)).
			Queries("legal-hold", "")

		// PutObject with auto-extract support for zip
		router.Methods(http.MethodPut).Path("/{object:.+}").
			HeadersRegexp(xhttp.AmzSnowballExtract, "true").
			HandlerFunc(s3APIMiddleware(api.PutObjectExtractHandler, traceHdrsS3HFlag))

		// AppendObject to be rejected
		router.Methods(http.MethodPut).Path("/{object:.+}").
			HeadersRegexp(xhttp.AmzWriteOffsetBytes, "").
			HandlerFunc(s3APIMiddleware(errorResponseHandler))

		// PutObject
		router.Methods(http.MethodPut).Path("/{object:.+}").
			HandlerFunc(s3APIMiddleware(api.PutObjectHandler, traceHdrsS3HFlag))

		// DeleteObject
		router.Methods(http.MethodDelete).Path("/{object:.+}").
			HandlerFunc(s3APIMiddleware(api.DeleteObjectHandler))

		// PostRestoreObject
		router.Methods(http.MethodPost).Path("/{object:.+}").
			HandlerFunc(s3APIMiddleware(api.PostRestoreObjectHandler)).
			Queries("restore", "")

		// Bucket operations

		// GetBucketLocation
		router.Methods(http.MethodGet).
			HandlerFunc(s3APIMiddleware(api.GetBucketLocationHandler)).
			Queries("location", "")
		// GetBucketPolicy
		router.Methods(http.MethodGet).
			HandlerFunc(s3APIMiddleware(api.GetBucketPolicyHandler)).
			Queries("policy", "")
		// GetBucketLifecycle
		router.Methods(http.MethodGet).
			HandlerFunc(s3APIMiddleware(api.GetBucketLifecycleHandler)).
			Queries("lifecycle", "")
		// GetBucketEncryption
		router.Methods(http.MethodGet).
			HandlerFunc(s3APIMiddleware(api.GetBucketEncryptionHandler)).
			Queries("encryption", "")
		// GetBucketObjectLockConfig
		router.Methods(http.MethodGet).
			HandlerFunc(s3APIMiddleware(api.GetBucketObjectLockConfigHandler)).
			Queries("object-lock", "")
		// GetBucketReplicationConfig
		router.Methods(http.MethodGet).
			HandlerFunc(s3APIMiddleware(api.GetBucketReplicationConfigHandler)).
			Queries("replication", "")
		// GetBucketVersioning
		router.Methods(http.MethodGet).
			HandlerFunc(s3APIMiddleware(api.GetBucketVersioningHandler)).
			Queries("versioning", "")
		// GetBucketNotification
		router.Methods(http.MethodGet).
			HandlerFunc(s3APIMiddleware(api.GetBucketNotificationHandler)).
			Queries("notification", "")
		// ListenNotification
		router.Methods(http.MethodGet).
			HandlerFunc(s3APIMiddleware(api.ListenNotificationHandler, noThrottleS3HFlag, traceHdrsS3HFlag)).
			Queries("events", "{events:.*}")
		// ResetBucketReplicationStatus - MinIO extension API
		router.Methods(http.MethodGet).
			HandlerFunc(s3APIMiddleware(api.ResetBucketReplicationStatusHandler)).
			Queries("replication-reset-status", "")

		// Dummy Bucket Calls
		// GetBucketACL -- this is a dummy call.
		router.Methods(http.MethodGet).
			HandlerFunc(s3APIMiddleware(api.GetBucketACLHandler)).
			Queries("acl", "")
		// PutBucketACL -- this is a dummy call.
		router.Methods(http.MethodPut).
			HandlerFunc(s3APIMiddleware(api.PutBucketACLHandler)).
			Queries("acl", "")
		// GetBucketCors - this is a dummy call.
		router.Methods(http.MethodGet).
			HandlerFunc(s3APIMiddleware(api.GetBucketCorsHandler)).
			Queries("cors", "")
		// PutBucketCors - this is a dummy call.
		router.Methods(http.MethodPut).
			HandlerFunc(s3APIMiddleware(api.PutBucketCorsHandler)).
			Queries("cors", "")
		// DeleteBucketCors - this is a dummy call.
		router.Methods(http.MethodDelete).
			HandlerFunc(s3APIMiddleware(api.DeleteBucketCorsHandler)).
			Queries("cors", "")
		// GetBucketWebsiteHandler - this is a dummy call.
		router.Methods(http.MethodGet).
			HandlerFunc(s3APIMiddleware(api.GetBucketWebsiteHandler)).
			Queries("website", "")
		// GetBucketAccelerateHandler - this is a dummy call.
		router.Methods(http.MethodGet).
			HandlerFunc(s3APIMiddleware(api.GetBucketAccelerateHandler)).
			Queries("accelerate", "")
		// GetBucketRequestPaymentHandler - this is a dummy call.
		router.Methods(http.MethodGet).
			HandlerFunc(s3APIMiddleware(api.GetBucketRequestPaymentHandler)).
			Queries("requestPayment", "")
		// GetBucketLoggingHandler - this is a dummy call.
		router.Methods(http.MethodGet).
			HandlerFunc(s3APIMiddleware(api.GetBucketLoggingHandler)).
			Queries("logging", "")

		// GetBucketTaggingHandler
		router.Methods(http.MethodGet).
			HandlerFunc(s3APIMiddleware(api.GetBucketTaggingHandler)).
			Queries("tagging", "")
		// DeleteBucketWebsiteHandler
		router.Methods(http.MethodDelete).
			HandlerFunc(s3APIMiddleware(api.DeleteBucketWebsiteHandler)).
			Queries("website", "")
		// DeleteBucketTaggingHandler
		router.Methods(http.MethodDelete).
			HandlerFunc(s3APIMiddleware(api.DeleteBucketTaggingHandler)).
			Queries("tagging", "")

		// ListMultipartUploads
		router.Methods(http.MethodGet).
			HandlerFunc(s3APIMiddleware(api.ListMultipartUploadsHandler)).
			Queries("uploads", "")
		// ListObjectsV2M
		router.Methods(http.MethodGet).
			HandlerFunc(s3APIMiddleware(api.ListObjectsV2MHandler)).
			Queries("list-type", "2", "metadata", "true")
		// ListObjectsV2
		router.Methods(http.MethodGet).
			HandlerFunc(s3APIMiddleware(api.ListObjectsV2Handler)).
			Queries("list-type", "2")
		// ListObjectVersions
		router.Methods(http.MethodGet).
			HandlerFunc(s3APIMiddleware(api.ListObjectVersionsMHandler)).
			Queries("versions", "", "metadata", "true")
		// ListObjectVersions
		router.Methods(http.MethodGet).
			HandlerFunc(s3APIMiddleware(api.ListObjectVersionsHandler)).
			Queries("versions", "")
		// GetBucketPolicyStatus
		router.Methods(http.MethodGet).
			HandlerFunc(s3APIMiddleware(api.GetBucketPolicyStatusHandler)).
			Queries("policyStatus", "")
		// PutBucketLifecycle
		router.Methods(http.MethodPut).
			HandlerFunc(s3APIMiddleware(api.PutBucketLifecycleHandler)).
			Queries("lifecycle", "")
		// PutBucketReplicationConfig
		router.Methods(http.MethodPut).
			HandlerFunc(s3APIMiddleware(api.PutBucketReplicationConfigHandler)).
			Queries("replication", "")
		// PutBucketEncryption
		router.Methods(http.MethodPut).
			HandlerFunc(s3APIMiddleware(api.PutBucketEncryptionHandler)).
			Queries("encryption", "")

		// PutBucketPolicy
		router.Methods(http.MethodPut).
			HandlerFunc(s3APIMiddleware(api.PutBucketPolicyHandler)).
			Queries("policy", "")

		// PutBucketObjectLockConfig
		router.Methods(http.MethodPut).
			HandlerFunc(s3APIMiddleware(api.PutBucketObjectLockConfigHandler)).
			Queries("object-lock", "")
		// PutBucketTaggingHandler
		router.Methods(http.MethodPut).
			HandlerFunc(s3APIMiddleware(api.PutBucketTaggingHandler)).
			Queries("tagging", "")
		// PutBucketVersioning
		router.Methods(http.MethodPut).
			HandlerFunc(s3APIMiddleware(api.PutBucketVersioningHandler)).
			Queries("versioning", "")
		// PutBucketNotification
		router.Methods(http.MethodPut).
			HandlerFunc(s3APIMiddleware(api.PutBucketNotificationHandler)).
			Queries("notification", "")
		// ResetBucketReplicationStart - MinIO extension API
		router.Methods(http.MethodPut).
			HandlerFunc(s3APIMiddleware(api.ResetBucketReplicationStartHandler)).
			Queries("replication-reset", "")

		// PutBucket
		router.Methods(http.MethodPut).
			HandlerFunc(s3APIMiddleware(api.PutBucketHandler))
		// HeadBucket
		router.Methods(http.MethodHead).
			HandlerFunc(s3APIMiddleware(api.HeadBucketHandler))
		// PostPolicy
		router.Methods(http.MethodPost).
			MatcherFunc(func(r *http.Request, _ *mux.RouteMatch) bool {
				return isRequestPostPolicySignatureV4(r)
			}).
			HandlerFunc(s3APIMiddleware(api.PostPolicyBucketHandler, traceHdrsS3HFlag))
		// DeleteMultipleObjects
		router.Methods(http.MethodPost).
			HandlerFunc(s3APIMiddleware(api.DeleteMultipleObjectsHandler)).
			Queries("delete", "")
		// DeleteBucketPolicy
		router.Methods(http.MethodDelete).
			HandlerFunc(s3APIMiddleware(api.DeleteBucketPolicyHandler)).
			Queries("policy", "")
		// DeleteBucketReplication
		router.Methods(http.MethodDelete).
			HandlerFunc(s3APIMiddleware(api.DeleteBucketReplicationConfigHandler)).
			Queries("replication", "")
		// DeleteBucketLifecycle
		router.Methods(http.MethodDelete).
			HandlerFunc(s3APIMiddleware(api.DeleteBucketLifecycleHandler)).
			Queries("lifecycle", "")
		// DeleteBucketEncryption
		router.Methods(http.MethodDelete).
			HandlerFunc(s3APIMiddleware(api.DeleteBucketEncryptionHandler)).
			Queries("encryption", "")
		// DeleteBucket
		router.Methods(http.MethodDelete).
			HandlerFunc(s3APIMiddleware(api.DeleteBucketHandler))

		// MinIO extension API for replication.
		//
		router.Methods(http.MethodGet).
			HandlerFunc(s3APIMiddleware(api.GetBucketReplicationMetricsV2Handler)).
			Queries("replication-metrics", "2")
		// deprecated handler
		router.Methods(http.MethodGet).
			HandlerFunc(s3APIMiddleware(api.GetBucketReplicationMetricsHandler)).
			Queries("replication-metrics", "")

		// ValidateBucketReplicationCreds
		router.Methods(http.MethodGet).
			HandlerFunc(s3APIMiddleware(api.ValidateBucketReplicationCredsHandler)).
			Queries("replication-check", "")

		// Register rejected bucket APIs
		for _, r := range rejectedBucketAPIs {
			router.Methods(r.methods...).
				HandlerFunc(collectAPIStats(r.api, httpTraceAll(notImplementedHandler))).
				Queries(r.queries...)
		}

		// S3 ListObjectsV1 (Legacy)
		router.Methods(http.MethodGet).
			HandlerFunc(s3APIMiddleware(api.ListObjectsV1Handler))
	}

	// Root operation

	// ListenNotification
	apiRouter.Methods(http.MethodGet).Path(SlashSeparator).
		HandlerFunc(s3APIMiddleware(api.ListenNotificationHandler, noThrottleS3HFlag, traceHdrsS3HFlag)).
		Queries("events", "{events:.*}")

	// ListBuckets
	apiRouter.Methods(http.MethodGet).Path(SlashSeparator).
		HandlerFunc(s3APIMiddleware(api.ListBucketsHandler))

	// S3 browser with signature v4 adds '//' for ListBuckets request, so rather
	// than failing with UnknownAPIRequest we simply handle it for now.
	apiRouter.Methods(http.MethodGet).Path(SlashSeparator + SlashSeparator).
		HandlerFunc(s3APIMiddleware(api.ListBucketsHandler))

	// If none of the routes match add default error handler routes
	apiRouter.NotFoundHandler = collectAPIStats("notfound", httpTraceAll(errorResponseHandler))
	apiRouter.MethodNotAllowedHandler = collectAPIStats("methodnotallowed", httpTraceAll(methodNotAllowedHandler("S3")))
}

// corsHandler handler for CORS (Cross Origin Resource Sharing)
func corsHandler(handler http.Handler) http.Handler {
	commonS3Headers := []string{
		xhttp.Date,
		xhttp.ETag,
		xhttp.ServerInfo,
		xhttp.Connection,
		xhttp.AcceptRanges,
		xhttp.ContentRange,
		xhttp.ContentEncoding,
		xhttp.ContentLength,
		xhttp.ContentType,
		xhttp.ContentDisposition,
		xhttp.LastModified,
		xhttp.ContentLanguage,
		xhttp.CacheControl,
		xhttp.RetryAfter,
		xhttp.AmzBucketRegion,
		xhttp.Expires,
		"X-Amz*",
		"x-amz*",
		"*",
	}
	opts := cors.Options{
		AllowOriginFunc: func(origin string) bool {
			for _, allowedOrigin := range globalAPIConfig.getCorsAllowOrigins() {
				if wildcard.MatchSimple(allowedOrigin, origin) {
					return true
				}
			}
			return false
		},
		AllowedMethods: []string{
			http.MethodGet,
			http.MethodPut,
			http.MethodHead,
			http.MethodPost,
			http.MethodDelete,
			http.MethodOptions,
			http.MethodPatch,
		},
		AllowedHeaders:   commonS3Headers,
		ExposedHeaders:   commonS3Headers,
		AllowCredentials: true,
	}
	return cors.New(opts).Handler(handler)
}

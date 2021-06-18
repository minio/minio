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

	"github.com/gorilla/mux"
	xhttp "github.com/minio/minio/internal/http"
	"github.com/minio/pkg/wildcard"
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

func newObjectLayerFn() ObjectLayer {
	globalObjLayerMutex.RLock()
	defer globalObjLayerMutex.RUnlock()
	return globalObjectAPI
}

func newCachedObjectLayerFn() CacheObjectLayer {
	globalObjLayerMutex.RLock()
	defer globalObjLayerMutex.RUnlock()
	return globalCacheObjectAPI
}

func setCacheObjectLayer(c CacheObjectLayer) {
	globalObjLayerMutex.Lock()
	globalCacheObjectAPI = c
	globalObjLayerMutex.Unlock()
}

func setObjectLayer(o ObjectLayer) {
	globalObjLayerMutex.Lock()
	globalObjectAPI = o
	globalObjLayerMutex.Unlock()
}

// objectAPIHandler implements and provides http handlers for S3 API.
type objectAPIHandlers struct {
	ObjectAPI func() ObjectLayer
	CacheAPI  func() CacheObjectLayer
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

// registerAPIRouter - registers S3 compatible APIs.
func registerAPIRouter(router *mux.Router) {
	// Initialize API.
	api := objectAPIHandlers{
		ObjectAPI: newObjectLayerFn,
		CacheAPI:  newCachedObjectLayerFn,
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
		router.Methods(http.MethodHead).Path("/{object:.+}").HandlerFunc(
			collectAPIStats("headobject", maxClients(httpTraceAll(api.HeadObjectHandler))))
		// CopyObjectPart
		router.Methods(http.MethodPut).Path("/{object:.+}").
			HeadersRegexp(xhttp.AmzCopySource, ".*?(\\/|%2F).*?").
			HandlerFunc(collectAPIStats("copyobjectpart", maxClients(httpTraceAll(api.CopyObjectPartHandler)))).
			Queries("partNumber", "{partNumber:[0-9]+}", "uploadId", "{uploadId:.*}")
		// PutObjectPart
		router.Methods(http.MethodPut).Path("/{object:.+}").HandlerFunc(
			collectAPIStats("putobjectpart", maxClients(httpTraceHdrs(api.PutObjectPartHandler)))).Queries("partNumber", "{partNumber:[0-9]+}", "uploadId", "{uploadId:.*}")
		// ListObjectParts
		router.Methods(http.MethodGet).Path("/{object:.+}").HandlerFunc(
			collectAPIStats("listobjectparts", maxClients(httpTraceAll(api.ListObjectPartsHandler)))).Queries("uploadId", "{uploadId:.*}")
		// CompleteMultipartUpload
		router.Methods(http.MethodPost).Path("/{object:.+}").HandlerFunc(
			collectAPIStats("completemutipartupload", maxClients(httpTraceAll(api.CompleteMultipartUploadHandler)))).Queries("uploadId", "{uploadId:.*}")
		// NewMultipartUpload
		router.Methods(http.MethodPost).Path("/{object:.+}").HandlerFunc(
			collectAPIStats("newmultipartupload", maxClients(httpTraceAll(api.NewMultipartUploadHandler)))).Queries("uploads", "")
		// AbortMultipartUpload
		router.Methods(http.MethodDelete).Path("/{object:.+}").HandlerFunc(
			collectAPIStats("abortmultipartupload", maxClients(httpTraceAll(api.AbortMultipartUploadHandler)))).Queries("uploadId", "{uploadId:.*}")
		// GetObjectACL - this is a dummy call.
		router.Methods(http.MethodGet).Path("/{object:.+}").HandlerFunc(
			collectAPIStats("getobjectacl", maxClients(httpTraceHdrs(api.GetObjectACLHandler)))).Queries("acl", "")
		// PutObjectACL - this is a dummy call.
		router.Methods(http.MethodPut).Path("/{object:.+}").HandlerFunc(
			collectAPIStats("putobjectacl", maxClients(httpTraceHdrs(api.PutObjectACLHandler)))).Queries("acl", "")
		// GetObjectTagging
		router.Methods(http.MethodGet).Path("/{object:.+}").HandlerFunc(
			collectAPIStats("getobjecttagging", maxClients(httpTraceHdrs(api.GetObjectTaggingHandler)))).Queries("tagging", "")
		// PutObjectTagging
		router.Methods(http.MethodPut).Path("/{object:.+}").HandlerFunc(
			collectAPIStats("putobjecttagging", maxClients(httpTraceHdrs(api.PutObjectTaggingHandler)))).Queries("tagging", "")
		// DeleteObjectTagging
		router.Methods(http.MethodDelete).Path("/{object:.+}").HandlerFunc(
			collectAPIStats("deleteobjecttagging", maxClients(httpTraceHdrs(api.DeleteObjectTaggingHandler)))).Queries("tagging", "")
		// SelectObjectContent
		router.Methods(http.MethodPost).Path("/{object:.+}").HandlerFunc(
			collectAPIStats("selectobjectcontent", maxClients(httpTraceHdrs(api.SelectObjectContentHandler)))).Queries("select", "").Queries("select-type", "2")
		// GetObjectRetention
		router.Methods(http.MethodGet).Path("/{object:.+}").HandlerFunc(
			collectAPIStats("getobjectretention", maxClients(httpTraceAll(api.GetObjectRetentionHandler)))).Queries("retention", "")
		// GetObjectLegalHold
		router.Methods(http.MethodGet).Path("/{object:.+}").HandlerFunc(
			collectAPIStats("getobjectlegalhold", maxClients(httpTraceAll(api.GetObjectLegalHoldHandler)))).Queries("legal-hold", "")
		// GetObject
		router.Methods(http.MethodGet).Path("/{object:.+}").HandlerFunc(
			collectAPIStats("getobject", maxClients(httpTraceHdrs(api.GetObjectHandler))))
		// CopyObject
		router.Methods(http.MethodPut).Path("/{object:.+}").HeadersRegexp(xhttp.AmzCopySource, ".*?(\\/|%2F).*?").HandlerFunc(
			collectAPIStats("copyobject", maxClients(httpTraceAll(api.CopyObjectHandler))))
		// PutObjectRetention
		router.Methods(http.MethodPut).Path("/{object:.+}").HandlerFunc(
			collectAPIStats("putobjectretention", maxClients(httpTraceAll(api.PutObjectRetentionHandler)))).Queries("retention", "")
		// PutObjectLegalHold
		router.Methods(http.MethodPut).Path("/{object:.+}").HandlerFunc(
			collectAPIStats("putobjectlegalhold", maxClients(httpTraceAll(api.PutObjectLegalHoldHandler)))).Queries("legal-hold", "")

		// PutObject with auto-extract support for zip
		router.Methods(http.MethodPut).Path("/{object:.+}").HeadersRegexp(xhttp.AmzSnowballExtract, "true").HandlerFunc(
			collectAPIStats("putobject", maxClients(httpTraceHdrs(api.PutObjectExtractHandler))))

		// PutObject
		router.Methods(http.MethodPut).Path("/{object:.+}").HandlerFunc(
			collectAPIStats("putobject", maxClients(httpTraceHdrs(api.PutObjectHandler))))

		// DeleteObject
		router.Methods(http.MethodDelete).Path("/{object:.+}").HandlerFunc(
			collectAPIStats("deleteobject", maxClients(httpTraceAll(api.DeleteObjectHandler))))

		// PostRestoreObject
		router.Methods(http.MethodPost).Path("/{object:.+}").HandlerFunc(
			collectAPIStats("restoreobject", maxClients(httpTraceAll(api.PostRestoreObjectHandler)))).Queries("restore", "")

		/// Bucket operations
		// GetBucketLocation
		router.Methods(http.MethodGet).HandlerFunc(
			collectAPIStats("getbucketlocation", maxClients(httpTraceAll(api.GetBucketLocationHandler)))).Queries("location", "")
		// GetBucketPolicy
		router.Methods(http.MethodGet).HandlerFunc(
			collectAPIStats("getbucketpolicy", maxClients(httpTraceAll(api.GetBucketPolicyHandler)))).Queries("policy", "")
		// GetBucketLifecycle
		router.Methods(http.MethodGet).HandlerFunc(
			collectAPIStats("getbucketlifecycle", maxClients(httpTraceAll(api.GetBucketLifecycleHandler)))).Queries("lifecycle", "")
		// GetBucketEncryption
		router.Methods(http.MethodGet).HandlerFunc(
			collectAPIStats("getbucketencryption", maxClients(httpTraceAll(api.GetBucketEncryptionHandler)))).Queries("encryption", "")
		// GetBucketObjectLockConfig
		router.Methods(http.MethodGet).HandlerFunc(
			collectAPIStats("getbucketobjectlockconfiguration", maxClients(httpTraceAll(api.GetBucketObjectLockConfigHandler)))).Queries("object-lock", "")
		// GetBucketReplicationConfig
		router.Methods(http.MethodGet).HandlerFunc(
			collectAPIStats("getbucketreplicationconfiguration", maxClients(httpTraceAll(api.GetBucketReplicationConfigHandler)))).Queries("replication", "")
		// GetBucketVersioning
		router.Methods(http.MethodGet).HandlerFunc(
			collectAPIStats("getbucketversioning", maxClients(httpTraceAll(api.GetBucketVersioningHandler)))).Queries("versioning", "")
		// GetBucketNotification
		router.Methods(http.MethodGet).HandlerFunc(
			collectAPIStats("getbucketnotification", maxClients(httpTraceAll(api.GetBucketNotificationHandler)))).Queries("notification", "")
		// ListenNotification
		router.Methods(http.MethodGet).HandlerFunc(
			collectAPIStats("listennotification", maxClients(httpTraceAll(api.ListenNotificationHandler)))).Queries("events", "{events:.*}")

		// Dummy Bucket Calls
		// GetBucketACL -- this is a dummy call.
		router.Methods(http.MethodGet).HandlerFunc(
			collectAPIStats("getbucketacl", maxClients(httpTraceAll(api.GetBucketACLHandler)))).Queries("acl", "")
		// PutBucketACL -- this is a dummy call.
		router.Methods(http.MethodPut).HandlerFunc(
			collectAPIStats("putbucketacl", maxClients(httpTraceAll(api.PutBucketACLHandler)))).Queries("acl", "")
		// GetBucketCors - this is a dummy call.
		router.Methods(http.MethodGet).HandlerFunc(
			collectAPIStats("getbucketcors", maxClients(httpTraceAll(api.GetBucketCorsHandler)))).Queries("cors", "")
		// GetBucketWebsiteHandler - this is a dummy call.
		router.Methods(http.MethodGet).HandlerFunc(
			collectAPIStats("getbucketwebsite", maxClients(httpTraceAll(api.GetBucketWebsiteHandler)))).Queries("website", "")
		// GetBucketAccelerateHandler - this is a dummy call.
		router.Methods(http.MethodGet).HandlerFunc(
			collectAPIStats("getbucketaccelerate", maxClients(httpTraceAll(api.GetBucketAccelerateHandler)))).Queries("accelerate", "")
		// GetBucketRequestPaymentHandler - this is a dummy call.
		router.Methods(http.MethodGet).HandlerFunc(
			collectAPIStats("getbucketrequestpayment", maxClients(httpTraceAll(api.GetBucketRequestPaymentHandler)))).Queries("requestPayment", "")
		// GetBucketLoggingHandler - this is a dummy call.
		router.Methods(http.MethodGet).HandlerFunc(
			collectAPIStats("getbucketlogging", maxClients(httpTraceAll(api.GetBucketLoggingHandler)))).Queries("logging", "")
		// GetBucketTaggingHandler
		router.Methods(http.MethodGet).HandlerFunc(
			collectAPIStats("getbuckettagging", maxClients(httpTraceAll(api.GetBucketTaggingHandler)))).Queries("tagging", "")
		//DeleteBucketWebsiteHandler
		router.Methods(http.MethodDelete).HandlerFunc(
			collectAPIStats("deletebucketwebsite", maxClients(httpTraceAll(api.DeleteBucketWebsiteHandler)))).Queries("website", "")
		// DeleteBucketTaggingHandler
		router.Methods(http.MethodDelete).HandlerFunc(
			collectAPIStats("deletebuckettagging", maxClients(httpTraceAll(api.DeleteBucketTaggingHandler)))).Queries("tagging", "")

		// ListMultipartUploads
		router.Methods(http.MethodGet).HandlerFunc(
			collectAPIStats("listmultipartuploads", maxClients(httpTraceAll(api.ListMultipartUploadsHandler)))).Queries("uploads", "")
		// ListObjectsV2M
		router.Methods(http.MethodGet).HandlerFunc(
			collectAPIStats("listobjectsv2M", maxClients(httpTraceAll(api.ListObjectsV2MHandler)))).Queries("list-type", "2", "metadata", "true")
		// ListObjectsV2
		router.Methods(http.MethodGet).HandlerFunc(
			collectAPIStats("listobjectsv2", maxClients(httpTraceAll(api.ListObjectsV2Handler)))).Queries("list-type", "2")
		// ListObjectVersions
		router.Methods(http.MethodGet).HandlerFunc(
			collectAPIStats("listobjectversions", maxClients(httpTraceAll(api.ListObjectVersionsHandler)))).Queries("versions", "")
		// GetBucketPolicyStatus
		router.Methods(http.MethodGet).HandlerFunc(
			collectAPIStats("getpolicystatus", maxClients(httpTraceAll(api.GetBucketPolicyStatusHandler)))).Queries("policyStatus", "")
		// PutBucketLifecycle
		router.Methods(http.MethodPut).HandlerFunc(
			collectAPIStats("putbucketlifecycle", maxClients(httpTraceAll(api.PutBucketLifecycleHandler)))).Queries("lifecycle", "")
		// PutBucketReplicationConfig
		router.Methods(http.MethodPut).HandlerFunc(
			collectAPIStats("putbucketreplicationconfiguration", maxClients(httpTraceAll(api.PutBucketReplicationConfigHandler)))).Queries("replication", "")
		// PutBucketEncryption
		router.Methods(http.MethodPut).HandlerFunc(
			collectAPIStats("putbucketencryption", maxClients(httpTraceAll(api.PutBucketEncryptionHandler)))).Queries("encryption", "")

		// PutBucketPolicy
		router.Methods(http.MethodPut).HandlerFunc(
			collectAPIStats("putbucketpolicy", maxClients(httpTraceAll(api.PutBucketPolicyHandler)))).Queries("policy", "")

		// PutBucketObjectLockConfig
		router.Methods(http.MethodPut).HandlerFunc(
			collectAPIStats("putbucketobjectlockconfig", maxClients(httpTraceAll(api.PutBucketObjectLockConfigHandler)))).Queries("object-lock", "")
		// PutBucketTaggingHandler
		router.Methods(http.MethodPut).HandlerFunc(
			collectAPIStats("putbuckettagging", maxClients(httpTraceAll(api.PutBucketTaggingHandler)))).Queries("tagging", "")
		// PutBucketVersioning
		router.Methods(http.MethodPut).HandlerFunc(
			collectAPIStats("putbucketversioning", maxClients(httpTraceAll(api.PutBucketVersioningHandler)))).Queries("versioning", "")
		// PutBucketNotification
		router.Methods(http.MethodPut).HandlerFunc(
			collectAPIStats("putbucketnotification", maxClients(httpTraceAll(api.PutBucketNotificationHandler)))).Queries("notification", "")
		// ResetBucketReplicationState - MinIO extension API
		router.Methods(http.MethodPut).HandlerFunc(
			collectAPIStats("resetbucketreplicationstate", maxClients(httpTraceAll(api.ResetBucketReplicationStateHandler)))).Queries("replication-reset", "")
		// PutBucket
		router.Methods(http.MethodPut).HandlerFunc(
			collectAPIStats("putbucket", maxClients(httpTraceAll(api.PutBucketHandler))))
		// HeadBucket
		router.Methods(http.MethodHead).HandlerFunc(
			collectAPIStats("headbucket", maxClients(httpTraceAll(api.HeadBucketHandler))))
		// PostPolicy
		router.Methods(http.MethodPost).HeadersRegexp(xhttp.ContentType, "multipart/form-data*").HandlerFunc(
			collectAPIStats("postpolicybucket", maxClients(httpTraceHdrs(api.PostPolicyBucketHandler))))
		// DeleteMultipleObjects
		router.Methods(http.MethodPost).HandlerFunc(
			collectAPIStats("deletemultipleobjects", maxClients(httpTraceAll(api.DeleteMultipleObjectsHandler)))).Queries("delete", "")
		// DeleteBucketPolicy
		router.Methods(http.MethodDelete).HandlerFunc(
			collectAPIStats("deletebucketpolicy", maxClients(httpTraceAll(api.DeleteBucketPolicyHandler)))).Queries("policy", "")
		// DeleteBucketReplication
		router.Methods(http.MethodDelete).HandlerFunc(
			collectAPIStats("deletebucketreplicationconfiguration", maxClients(httpTraceAll(api.DeleteBucketReplicationConfigHandler)))).Queries("replication", "")
		// DeleteBucketLifecycle
		router.Methods(http.MethodDelete).HandlerFunc(
			collectAPIStats("deletebucketlifecycle", maxClients(httpTraceAll(api.DeleteBucketLifecycleHandler)))).Queries("lifecycle", "")
		// DeleteBucketEncryption
		router.Methods(http.MethodDelete).HandlerFunc(
			collectAPIStats("deletebucketencryption", maxClients(httpTraceAll(api.DeleteBucketEncryptionHandler)))).Queries("encryption", "")
		// DeleteBucket
		router.Methods(http.MethodDelete).HandlerFunc(
			collectAPIStats("deletebucket", maxClients(httpTraceAll(api.DeleteBucketHandler))))

		// MinIO extension API for replication.
		//
		// GetBucketReplicationMetrics
		router.Methods(http.MethodGet).HandlerFunc(
			collectAPIStats("getbucketreplicationmetrics", maxClients(httpTraceAll(api.GetBucketReplicationMetricsHandler)))).Queries("replication-metrics", "")

		// Register rejected bucket APIs
		for _, r := range rejectedBucketAPIs {
			router.Methods(r.methods...).
				HandlerFunc(collectAPIStats(r.api, httpTraceAll(notImplementedHandler))).
				Queries(r.queries...)
		}

		// S3 ListObjectsV1 (Legacy)
		router.Methods(http.MethodGet).HandlerFunc(
			collectAPIStats("listobjectsv1", maxClients(httpTraceAll(api.ListObjectsV1Handler))))
	}

	/// Root operation

	// ListenNotification
	apiRouter.Methods(http.MethodGet).Path(SlashSeparator).HandlerFunc(
		collectAPIStats("listennotification", maxClients(httpTraceAll(api.ListenNotificationHandler)))).Queries("events", "{events:.*}")

	// ListBuckets
	apiRouter.Methods(http.MethodGet).Path(SlashSeparator).HandlerFunc(
		collectAPIStats("listbuckets", maxClients(httpTraceAll(api.ListBucketsHandler))))

	// S3 browser with signature v4 adds '//' for ListBuckets request, so rather
	// than failing with UnknownAPIRequest we simply handle it for now.
	apiRouter.Methods(http.MethodGet).Path(SlashSeparator + SlashSeparator).HandlerFunc(
		collectAPIStats("listbuckets", maxClients(httpTraceAll(api.ListBucketsHandler))))

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

	return cors.New(cors.Options{
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
	}).Handler(handler)
}

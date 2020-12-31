/*
 * MinIO Cloud Storage, (C) 2016-2020 MinIO, Inc.
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
	"net"
	"net/http"

	"github.com/gorilla/mux"
	xhttp "github.com/minio/minio/cmd/http"
	"github.com/minio/minio/pkg/wildcard"
	"github.com/rs/cors"
)

func newHTTPServerFn() *xhttp.Server {
	globalObjLayerMutex.Lock()
	defer globalObjLayerMutex.Unlock()
	return globalHTTPServer
}

func newObjectLayerFn() ObjectLayer {
	globalObjLayerMutex.Lock()
	defer globalObjLayerMutex.Unlock()
	return globalObjectAPI
}

func newCachedObjectLayerFn() CacheObjectLayer {
	globalObjLayerMutex.Lock()
	defer globalObjLayerMutex.Unlock()
	return globalCacheObjectAPI
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

	for _, bucket := range routers {
		// Object operations
		// HeadObject
		bucket.Methods(http.MethodHead).Path("/{object:.+}").HandlerFunc(
			collectAPIStats("headobject", httpTraceAll(maxClients(api.HeadObjectHandler))))
		// CopyObjectPart
		bucket.Methods(http.MethodPut).Path("/{object:.+}").
			HeadersRegexp(xhttp.AmzCopySource, ".*?(\\/|%2F).*?").
			HandlerFunc(maxClients(collectAPIStats("copyobjectpart", httpTraceAll(api.CopyObjectPartHandler)))).
			Queries("partNumber", "{partNumber:[0-9]+}", "uploadId", "{uploadId:.*}")
		// PutObjectPart
		bucket.Methods(http.MethodPut).Path("/{object:.+}").HandlerFunc(
			collectAPIStats("putobjectpart", httpTraceHdrs(maxClients(api.PutObjectPartHandler)))).Queries("partNumber", "{partNumber:[0-9]+}", "uploadId", "{uploadId:.*}")
		// ListObjectParts
		bucket.Methods(http.MethodGet).Path("/{object:.+}").HandlerFunc(
			collectAPIStats("listobjectparts", httpTraceAll(maxClients(api.ListObjectPartsHandler)))).Queries("uploadId", "{uploadId:.*}")
		// CompleteMultipartUpload
		bucket.Methods(http.MethodPost).Path("/{object:.+}").HandlerFunc(
			collectAPIStats("completemutipartupload", httpTraceAll(maxClients(api.CompleteMultipartUploadHandler)))).Queries("uploadId", "{uploadId:.*}")
		// NewMultipartUpload
		bucket.Methods(http.MethodPost).Path("/{object:.+}").HandlerFunc(
			collectAPIStats("newmultipartupload", httpTraceAll(maxClients(api.NewMultipartUploadHandler)))).Queries("uploads", "")
		// AbortMultipartUpload
		bucket.Methods(http.MethodDelete).Path("/{object:.+}").HandlerFunc(
			collectAPIStats("abortmultipartupload", httpTraceAll(maxClients(api.AbortMultipartUploadHandler)))).Queries("uploadId", "{uploadId:.*}")
		// GetObjectACL - this is a dummy call.
		bucket.Methods(http.MethodGet).Path("/{object:.+}").HandlerFunc(
			collectAPIStats("getobjectacl", httpTraceHdrs(maxClients(api.GetObjectACLHandler)))).Queries("acl", "")
		// PutObjectACL - this is a dummy call.
		bucket.Methods(http.MethodPut).Path("/{object:.+}").HandlerFunc(
			collectAPIStats("putobjectacl", httpTraceHdrs(maxClients(api.PutObjectACLHandler)))).Queries("acl", "")
		// GetObjectTagging
		bucket.Methods(http.MethodGet).Path("/{object:.+}").HandlerFunc(
			collectAPIStats("getobjecttagging", httpTraceHdrs(maxClients(api.GetObjectTaggingHandler)))).Queries("tagging", "")
		// PutObjectTagging
		bucket.Methods(http.MethodPut).Path("/{object:.+}").HandlerFunc(
			collectAPIStats("putobjecttagging", httpTraceHdrs(maxClients(api.PutObjectTaggingHandler)))).Queries("tagging", "")
		// DeleteObjectTagging
		bucket.Methods(http.MethodDelete).Path("/{object:.+}").HandlerFunc(
			collectAPIStats("deleteobjecttagging", httpTraceHdrs(maxClients(api.DeleteObjectTaggingHandler)))).Queries("tagging", "")
		// SelectObjectContent
		bucket.Methods(http.MethodPost).Path("/{object:.+}").HandlerFunc(
			collectAPIStats("selectobjectcontent", httpTraceHdrs(maxClients(api.SelectObjectContentHandler)))).Queries("select", "").Queries("select-type", "2")
		// GetObjectRetention
		bucket.Methods(http.MethodGet).Path("/{object:.+}").HandlerFunc(
			collectAPIStats("getobjectretention", httpTraceAll(maxClients(api.GetObjectRetentionHandler)))).Queries("retention", "")
		// GetObjectLegalHold
		bucket.Methods(http.MethodGet).Path("/{object:.+}").HandlerFunc(
			collectAPIStats("getobjectlegalhold", httpTraceAll(maxClients(api.GetObjectLegalHoldHandler)))).Queries("legal-hold", "")
		// GetObject
		bucket.Methods(http.MethodGet).Path("/{object:.+}").HandlerFunc(
			collectAPIStats("getobject", httpTraceHdrs(maxClients(api.GetObjectHandler))))
		// CopyObject
		bucket.Methods(http.MethodPut).Path("/{object:.+}").HeadersRegexp(xhttp.AmzCopySource, ".*?(\\/|%2F).*?").HandlerFunc(
			collectAPIStats("copyobject", httpTraceAll(maxClients(api.CopyObjectHandler))))
		// PutObjectRetention
		bucket.Methods(http.MethodPut).Path("/{object:.+}").HandlerFunc(
			collectAPIStats("putobjectretention", httpTraceAll(maxClients(api.PutObjectRetentionHandler)))).Queries("retention", "")
		// PutObjectLegalHold
		bucket.Methods(http.MethodPut).Path("/{object:.+}").HandlerFunc(
			collectAPIStats("putobjectlegalhold", httpTraceAll(maxClients(api.PutObjectLegalHoldHandler)))).Queries("legal-hold", "")

		// PutObject
		bucket.Methods(http.MethodPut).Path("/{object:.+}").HandlerFunc(
			collectAPIStats("putobject", httpTraceHdrs(maxClients(api.PutObjectHandler))))
		// DeleteObject
		bucket.Methods(http.MethodDelete).Path("/{object:.+}").HandlerFunc(
			collectAPIStats("deleteobject", httpTraceAll(maxClients(api.DeleteObjectHandler))))

		/// Bucket operations
		// GetBucketLocation
		bucket.Methods(http.MethodGet).HandlerFunc(
			collectAPIStats("getbucketlocation", httpTraceAll(maxClients(api.GetBucketLocationHandler)))).Queries("location", "")
		// GetBucketPolicy
		bucket.Methods(http.MethodGet).HandlerFunc(
			collectAPIStats("getbucketpolicy", httpTraceAll(maxClients(api.GetBucketPolicyHandler)))).Queries("policy", "")
		// GetBucketLifecycle
		bucket.Methods(http.MethodGet).HandlerFunc(
			collectAPIStats("getbucketlifecycle", httpTraceAll(maxClients(api.GetBucketLifecycleHandler)))).Queries("lifecycle", "")
		// GetBucketEncryption
		bucket.Methods(http.MethodGet).HandlerFunc(
			collectAPIStats("getbucketencryption", httpTraceAll(maxClients(api.GetBucketEncryptionHandler)))).Queries("encryption", "")
		// GetBucketObjectLockConfig
		bucket.Methods(http.MethodGet).HandlerFunc(
			collectAPIStats("getbucketobjectlockconfiguration", httpTraceAll(maxClients(api.GetBucketObjectLockConfigHandler)))).Queries("object-lock", "")
		// GetBucketReplicationConfig
		bucket.Methods(http.MethodGet).HandlerFunc(
			collectAPIStats("getbucketreplicationconfiguration", httpTraceAll(maxClients(api.GetBucketReplicationConfigHandler)))).Queries("replication", "")

		// GetBucketVersioning
		bucket.Methods(http.MethodGet).HandlerFunc(
			collectAPIStats("getbucketversioning", httpTraceAll(maxClients(api.GetBucketVersioningHandler)))).Queries("versioning", "")
		// GetBucketNotification
		bucket.Methods(http.MethodGet).HandlerFunc(
			collectAPIStats("getbucketnotification", httpTraceAll(maxClients(api.GetBucketNotificationHandler)))).Queries("notification", "")
		// ListenNotification
		bucket.Methods(http.MethodGet).HandlerFunc(
			collectAPIStats("listennotification", httpTraceAll(maxClients(api.ListenNotificationHandler)))).Queries("events", "{events:.*}")

		// Dummy Bucket Calls
		// GetBucketACL -- this is a dummy call.
		bucket.Methods(http.MethodGet).HandlerFunc(
			collectAPIStats("getbucketacl", httpTraceAll(maxClients(api.GetBucketACLHandler)))).Queries("acl", "")
		// PutBucketACL -- this is a dummy call.
		bucket.Methods(http.MethodPut).HandlerFunc(
			collectAPIStats("putbucketacl", httpTraceAll(maxClients(api.PutBucketACLHandler)))).Queries("acl", "")
		// GetBucketCors - this is a dummy call.
		bucket.Methods(http.MethodGet).HandlerFunc(
			collectAPIStats("getbucketcors", httpTraceAll(maxClients(api.GetBucketCorsHandler)))).Queries("cors", "")
		// GetBucketWebsiteHandler - this is a dummy call.
		bucket.Methods(http.MethodGet).HandlerFunc(
			collectAPIStats("getbucketwebsite", httpTraceAll(maxClients(api.GetBucketWebsiteHandler)))).Queries("website", "")
		// GetBucketAccelerateHandler - this is a dummy call.
		bucket.Methods(http.MethodGet).HandlerFunc(
			collectAPIStats("getbucketaccelerate", httpTraceAll(maxClients(api.GetBucketAccelerateHandler)))).Queries("accelerate", "")
		// GetBucketRequestPaymentHandler - this is a dummy call.
		bucket.Methods(http.MethodGet).HandlerFunc(
			collectAPIStats("getbucketrequestpayment", httpTraceAll(maxClients(api.GetBucketRequestPaymentHandler)))).Queries("requestPayment", "")
		// GetBucketLoggingHandler - this is a dummy call.
		bucket.Methods(http.MethodGet).HandlerFunc(
			collectAPIStats("getbucketlogging", httpTraceAll(maxClients(api.GetBucketLoggingHandler)))).Queries("logging", "")
		// GetBucketLifecycleHandler - this is a dummy call.
		bucket.Methods(http.MethodGet).HandlerFunc(
			collectAPIStats("getbucketlifecycle", httpTraceAll(maxClients(api.GetBucketLifecycleHandler)))).Queries("lifecycle", "")
		// GetBucketTaggingHandler
		bucket.Methods(http.MethodGet).HandlerFunc(
			collectAPIStats("getbuckettagging", httpTraceAll(maxClients(api.GetBucketTaggingHandler)))).Queries("tagging", "")
		//DeleteBucketWebsiteHandler
		bucket.Methods(http.MethodDelete).HandlerFunc(
			collectAPIStats("deletebucketwebsite", httpTraceAll(maxClients(api.DeleteBucketWebsiteHandler)))).Queries("website", "")
		// DeleteBucketTaggingHandler
		bucket.Methods(http.MethodDelete).HandlerFunc(
			collectAPIStats("deletebuckettagging", httpTraceAll(maxClients(api.DeleteBucketTaggingHandler)))).Queries("tagging", "")

		// ListMultipartUploads
		bucket.Methods(http.MethodGet).HandlerFunc(
			collectAPIStats("listmultipartuploads", httpTraceAll(maxClients(api.ListMultipartUploadsHandler)))).Queries("uploads", "")
		// ListObjectsV2M
		bucket.Methods(http.MethodGet).HandlerFunc(
			collectAPIStats("listobjectsv2M", httpTraceAll(maxClients(api.ListObjectsV2MHandler)))).Queries("list-type", "2", "metadata", "true")
		// ListObjectsV2
		bucket.Methods(http.MethodGet).HandlerFunc(
			collectAPIStats("listobjectsv2", httpTraceAll(maxClients(api.ListObjectsV2Handler)))).Queries("list-type", "2")
		// ListObjectVersions
		bucket.Methods(http.MethodGet).HandlerFunc(
			collectAPIStats("listobjectversions", httpTraceAll(maxClients(api.ListObjectVersionsHandler)))).Queries("versions", "")
		// ListObjectsV1 (Legacy)
		bucket.Methods(http.MethodGet).HandlerFunc(
			collectAPIStats("listobjectsv1", httpTraceAll(maxClients(api.ListObjectsV1Handler))))
		// PutBucketLifecycle
		bucket.Methods(http.MethodPut).HandlerFunc(
			collectAPIStats("putbucketlifecycle", httpTraceAll(maxClients(api.PutBucketLifecycleHandler)))).Queries("lifecycle", "")
		// PutBucketReplicationConfig
		bucket.Methods(http.MethodPut).HandlerFunc(
			collectAPIStats("putbucketreplicationconfiguration", httpTraceAll(maxClients(api.PutBucketReplicationConfigHandler)))).Queries("replication", "")
		// GetObjectRetention

		// PutBucketEncryption
		bucket.Methods(http.MethodPut).HandlerFunc(
			collectAPIStats("putbucketencryption", httpTraceAll(maxClients(api.PutBucketEncryptionHandler)))).Queries("encryption", "")

		// PutBucketPolicy
		bucket.Methods(http.MethodPut).HandlerFunc(
			collectAPIStats("putbucketpolicy", httpTraceAll(maxClients(api.PutBucketPolicyHandler)))).Queries("policy", "")

		// PutBucketObjectLockConfig
		bucket.Methods(http.MethodPut).HandlerFunc(
			collectAPIStats("putbucketobjectlockconfig", httpTraceAll(maxClients(api.PutBucketObjectLockConfigHandler)))).Queries("object-lock", "")
		// PutBucketTaggingHandler
		bucket.Methods(http.MethodPut).HandlerFunc(
			collectAPIStats("putbuckettagging", httpTraceAll(maxClients(api.PutBucketTaggingHandler)))).Queries("tagging", "")
		// PutBucketVersioning
		bucket.Methods(http.MethodPut).HandlerFunc(
			collectAPIStats("putbucketversioning", httpTraceAll(maxClients(api.PutBucketVersioningHandler)))).Queries("versioning", "")
		// PutBucketNotification
		bucket.Methods(http.MethodPut).HandlerFunc(
			collectAPIStats("putbucketnotification", httpTraceAll(maxClients(api.PutBucketNotificationHandler)))).Queries("notification", "")
		// PutBucket
		bucket.Methods(http.MethodPut).HandlerFunc(
			collectAPIStats("putbucket", httpTraceAll(maxClients(api.PutBucketHandler))))
		// HeadBucket
		bucket.Methods(http.MethodHead).HandlerFunc(
			collectAPIStats("headbucket", httpTraceAll(maxClients(api.HeadBucketHandler))))
		// PostPolicy
		bucket.Methods(http.MethodPost).HeadersRegexp(xhttp.ContentType, "multipart/form-data*").HandlerFunc(
			collectAPIStats("postpolicybucket", httpTraceHdrs(maxClients(api.PostPolicyBucketHandler))))
		// DeleteMultipleObjects
		bucket.Methods(http.MethodPost).HandlerFunc(
			collectAPIStats("deletemultipleobjects", httpTraceAll(maxClients(api.DeleteMultipleObjectsHandler)))).Queries("delete", "")
		// DeleteBucketPolicy
		bucket.Methods(http.MethodDelete).HandlerFunc(
			collectAPIStats("deletebucketpolicy", httpTraceAll(maxClients(api.DeleteBucketPolicyHandler)))).Queries("policy", "")
		// DeleteBucketReplication
		bucket.Methods(http.MethodDelete).HandlerFunc(
			collectAPIStats("deletebucketreplicationconfiguration", httpTraceAll(maxClients(api.DeleteBucketReplicationConfigHandler)))).Queries("replication", "")
		// DeleteBucketLifecycle
		bucket.Methods(http.MethodDelete).HandlerFunc(
			collectAPIStats("deletebucketlifecycle", httpTraceAll(maxClients(api.DeleteBucketLifecycleHandler)))).Queries("lifecycle", "")
		// DeleteBucketEncryption
		bucket.Methods(http.MethodDelete).HandlerFunc(
			collectAPIStats("deletebucketencryption", httpTraceAll(maxClients(api.DeleteBucketEncryptionHandler)))).Queries("encryption", "")
		// DeleteBucket
		bucket.Methods(http.MethodDelete).HandlerFunc(
			collectAPIStats("deletebucket", httpTraceAll(maxClients(api.DeleteBucketHandler))))
		// PostRestoreObject
		bucket.Methods(http.MethodPost).Path("/{object:.+}").HandlerFunc(
			collectAPIStats("restoreobject", httpTraceAll(maxClients(api.PostRestoreObjectHandler)))).Queries("restore", "")
	}

	/// Root operation

	// ListenNotification
	apiRouter.Methods(http.MethodGet).Path(SlashSeparator).HandlerFunc(
		collectAPIStats("listennotification", httpTraceAll(maxClients(api.ListenNotificationHandler)))).Queries("events", "{events:.*}")

	// ListBuckets
	apiRouter.Methods(http.MethodGet).Path(SlashSeparator).HandlerFunc(
		collectAPIStats("listbuckets", httpTraceAll(maxClients(api.ListBucketsHandler))))

	// S3 browser with signature v4 adds '//' for ListBuckets request, so rather
	// than failing with UnknownAPIRequest we simply handle it for now.
	apiRouter.Methods(http.MethodGet).Path(SlashSeparator + SlashSeparator).HandlerFunc(
		collectAPIStats("listbuckets", httpTraceAll(maxClients(api.ListBucketsHandler))))

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

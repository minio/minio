/*
 * MinIO Cloud Storage, (C) 2016 MinIO, Inc.
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

	"github.com/gorilla/mux"
	xhttp "github.com/minio/minio/cmd/http"
)

func newObjectLayerWithoutSafeModeFn() ObjectLayer {
	globalObjLayerMutex.Lock()
	defer globalObjLayerMutex.Unlock()
	return globalObjectAPI
}

func newObjectLayerFn() ObjectLayer {
	globalObjLayerMutex.Lock()
	defer globalObjLayerMutex.Unlock()
	if globalSafeMode {
		return nil
	}
	return globalObjectAPI
}

func newCachedObjectLayerFn() CacheObjectLayer {
	globalObjLayerMutex.Lock()
	defer globalObjLayerMutex.Unlock()

	if globalSafeMode {
		return nil
	}
	return globalCacheObjectAPI
}

// objectAPIHandler implements and provides http handlers for S3 API.
type objectAPIHandlers struct {
	ObjectAPI func() ObjectLayer
	CacheAPI  func() CacheObjectLayer
	// Returns true of handlers should interpret encryption.
	EncryptionEnabled func() bool
	// Returns true if handlers allow SSE-KMS encryption headers.
	AllowSSEKMS func() bool
}

// registerAPIRouter - registers S3 compatible APIs.
func registerAPIRouter(router *mux.Router, encryptionEnabled, allowSSEKMS bool) {
	// Initialize API.
	api := objectAPIHandlers{
		ObjectAPI: newObjectLayerFn,
		CacheAPI:  newCachedObjectLayerFn,
		EncryptionEnabled: func() bool {
			return encryptionEnabled
		},
		AllowSSEKMS: func() bool {
			return allowSSEKMS
		},
	}

	// API Router
	apiRouter := router.PathPrefix(SlashSeparator).Subrouter()
	var routers []*mux.Router
	for _, domainName := range globalDomainNames {
		routers = append(routers, apiRouter.Host("{bucket:.+}."+domainName).Subrouter())
		routers = append(routers, apiRouter.Host("{bucket:.+}."+domainName+":{port:.*}").Subrouter())
	}
	routers = append(routers, apiRouter.PathPrefix("/{bucket}").Subrouter())

	for _, bucket := range routers {
		// Object operations
		// HeadObject
		bucket.Methods(http.MethodHead).Path("/{object:.+}").HandlerFunc(collectAPIStats("headobject", httpTraceAll(api.HeadObjectHandler)))
		// CopyObjectPart
		bucket.Methods(http.MethodPut).Path("/{object:.+}").HeadersRegexp(xhttp.AmzCopySource, ".*?(\\/|%2F).*?").HandlerFunc(collectAPIStats("copyobjectpart", httpTraceAll(api.CopyObjectPartHandler))).Queries("partNumber", "{partNumber:[0-9]+}", "uploadId", "{uploadId:.*}")
		// PutObjectPart
		bucket.Methods(http.MethodPut).Path("/{object:.+}").HandlerFunc(collectAPIStats("putobjectpart", httpTraceHdrs(api.PutObjectPartHandler))).Queries("partNumber", "{partNumber:[0-9]+}", "uploadId", "{uploadId:.*}")
		// ListObjectParts
		bucket.Methods(http.MethodGet).Path("/{object:.+}").HandlerFunc(collectAPIStats("listobjectparts", httpTraceAll(api.ListObjectPartsHandler))).Queries("uploadId", "{uploadId:.*}")
		// CompleteMultipartUpload
		bucket.Methods(http.MethodPost).Path("/{object:.+}").HandlerFunc(collectAPIStats("completemutipartupload", httpTraceAll(api.CompleteMultipartUploadHandler))).Queries("uploadId", "{uploadId:.*}")
		// NewMultipartUpload
		bucket.Methods(http.MethodPost).Path("/{object:.+}").HandlerFunc(collectAPIStats("newmultipartupload", httpTraceAll(api.NewMultipartUploadHandler))).Queries("uploads", "")
		// AbortMultipartUpload
		bucket.Methods(http.MethodDelete).Path("/{object:.+}").HandlerFunc(collectAPIStats("abortmultipartupload", httpTraceAll(api.AbortMultipartUploadHandler))).Queries("uploadId", "{uploadId:.*}")
		// GetObjectACL - this is a dummy call.
		bucket.Methods(http.MethodGet).Path("/{object:.+}").HandlerFunc(collectAPIStats("getobjectacl", httpTraceHdrs(api.GetObjectACLHandler))).Queries("acl", "")
		// GetObjectTagging - this is a dummy call.
		bucket.Methods(http.MethodGet).Path("/{object:.+}").HandlerFunc(collectAPIStats("getobjecttagging", httpTraceHdrs(api.GetObjectTaggingHandler))).Queries("tagging", "")
		// SelectObjectContent
		bucket.Methods(http.MethodPost).Path("/{object:.+}").HandlerFunc(collectAPIStats("selectobjectcontent", httpTraceHdrs(api.SelectObjectContentHandler))).Queries("select", "").Queries("select-type", "2")
		// GetObject
		bucket.Methods(http.MethodGet).Path("/{object:.+}").HandlerFunc(collectAPIStats("getobject", httpTraceHdrs(api.GetObjectHandler)))
		// CopyObject
		bucket.Methods(http.MethodPut).Path("/{object:.+}").HeadersRegexp(xhttp.AmzCopySource, ".*?(\\/|%2F).*?").HandlerFunc(collectAPIStats("copyobject", httpTraceAll(api.CopyObjectHandler)))
		// PutObject
		bucket.Methods(http.MethodPut).Path("/{object:.+}").HandlerFunc(collectAPIStats("putobject", httpTraceHdrs(api.PutObjectHandler)))
		// DeleteObject
		bucket.Methods(http.MethodDelete).Path("/{object:.+}").HandlerFunc(collectAPIStats("deleteobject", httpTraceAll(api.DeleteObjectHandler)))
		// PutObjectLegalHold
		bucket.Methods(http.MethodPut).Path("/{object:.+}").HandlerFunc(httpTraceHdrs(api.PutObjectLegalHoldHandler)).Queries("legal-hold", "").Queries("versionId", "")
		// GetObjectLegalHold
		bucket.Methods(http.MethodGet).Path("/{object:.+}").HandlerFunc(httpTraceHdrs(api.GetObjectLegalHoldHandler)).Queries("legal-hold", "").Queries("versionId", "")
		// PutObjectRetention
		bucket.Methods(http.MethodPut).Path("/{object:.+}").HandlerFunc(httpTraceHdrs(api.PutObjectRetentionHandler)).Queries("retention", "").Queries("versionId", "")
		// GetObjectRetention
		bucket.Methods(http.MethodGet).Path("/{object:.+}").HandlerFunc(httpTraceHdrs(api.GetObjectRetentionHandler)).Queries("retention", "").Queries("versionId", "")

		/// Bucket operations
		// GetBucketLocation
		bucket.Methods(http.MethodGet).HandlerFunc(collectAPIStats("getbucketlocation", httpTraceAll(api.GetBucketLocationHandler))).Queries("location", "")
		// GetBucketPolicy
		bucket.Methods("GET").HandlerFunc(collectAPIStats("getbucketpolicy", httpTraceAll(api.GetBucketPolicyHandler))).Queries("policy", "")
		// GetBucketLifecycle
		bucket.Methods("GET").HandlerFunc(collectAPIStats("getbucketlifecycle", httpTraceAll(api.GetBucketLifecycleHandler))).Queries("lifecycle", "")

		// Dummy Bucket Calls
		// GetBucketACL -- this is a dummy call.
		bucket.Methods(http.MethodGet).HandlerFunc(collectAPIStats("getbucketacl", httpTraceAll(api.GetBucketACLHandler))).Queries("acl", "")
		// GetBucketCors - this is a dummy call.
		bucket.Methods(http.MethodGet).HandlerFunc(collectAPIStats("getbucketcors", httpTraceAll(api.GetBucketCorsHandler))).Queries("cors", "")
		// GetBucketWebsiteHandler - this is a dummy call.
		bucket.Methods(http.MethodGet).HandlerFunc(collectAPIStats("getbucketwebsite", httpTraceAll(api.GetBucketWebsiteHandler))).Queries("website", "")
		// GetBucketAccelerateHandler - this is a dummy call.
		bucket.Methods(http.MethodGet).HandlerFunc(collectAPIStats("getbucketaccelerate", httpTraceAll(api.GetBucketAccelerateHandler))).Queries("accelerate", "")
		// GetBucketRequestPaymentHandler - this is a dummy call.
		bucket.Methods(http.MethodGet).HandlerFunc(collectAPIStats("getbucketrequestpayment", httpTraceAll(api.GetBucketRequestPaymentHandler))).Queries("requestPayment", "")
		// GetBucketLoggingHandler - this is a dummy call.
		bucket.Methods(http.MethodGet).HandlerFunc(collectAPIStats("getbucketlogging", httpTraceAll(api.GetBucketLoggingHandler))).Queries("logging", "")
		// GetBucketLifecycleHandler - this is a dummy call.
		bucket.Methods(http.MethodGet).HandlerFunc(collectAPIStats("getbucketlifecycle", httpTraceAll(api.GetBucketLifecycleHandler))).Queries("lifecycle", "")
		// GetBucketReplicationHandler - this is a dummy call.
		bucket.Methods(http.MethodGet).HandlerFunc(collectAPIStats("getbucketreplication", httpTraceAll(api.GetBucketReplicationHandler))).Queries("replication", "")
		// GetBucketTaggingHandler - this is a dummy call.
		bucket.Methods(http.MethodGet).HandlerFunc(collectAPIStats("getbuckettagging", httpTraceAll(api.GetBucketTaggingHandler))).Queries("tagging", "")
		//DeleteBucketWebsiteHandler
		bucket.Methods(http.MethodDelete).HandlerFunc(collectAPIStats("deletebucketwebsite", httpTraceAll(api.DeleteBucketWebsiteHandler))).Queries("website", "")
		// DeleteBucketTaggingHandler
		bucket.Methods(http.MethodDelete).HandlerFunc(collectAPIStats("deletebuckettagging", httpTraceAll(api.DeleteBucketTaggingHandler))).Queries("tagging", "")

		// GetBucketObjectLockConfig
		bucket.Methods(http.MethodGet).HandlerFunc(httpTraceAll(api.GetBucketObjectLockConfigHandler)).Queries("object-lock", "")
		// GetBucketVersioning
		bucket.Methods(http.MethodGet).HandlerFunc(httpTraceAll(api.GetBucketVersioningHandler)).Queries("versioning", "")
		// GetBucketNotification
		bucket.Methods(http.MethodGet).HandlerFunc(collectAPIStats("getbucketnotification", httpTraceAll(api.GetBucketNotificationHandler))).Queries("notification", "")
		// ListenBucketNotification
		bucket.Methods(http.MethodGet).HandlerFunc(collectAPIStats("listenbucketnotification", httpTraceAll(api.ListenBucketNotificationHandler))).Queries("events", "{events:.*}")
		// ListMultipartUploads
		bucket.Methods(http.MethodGet).HandlerFunc(collectAPIStats("listmultipartuploads", httpTraceAll(api.ListMultipartUploadsHandler))).Queries("uploads", "")
		// ListObjectsV2
		bucket.Methods(http.MethodGet).HandlerFunc(collectAPIStats("listobjectsv2", httpTraceAll(api.ListObjectsV2Handler))).Queries("list-type", "2")
		// ListBucketVersions
		bucket.Methods(http.MethodGet).HandlerFunc(collectAPIStats("listbucketversions", httpTraceAll(api.ListBucketObjectVersionsHandler))).Queries("versions", "")
		// ListObjectsV1 (Legacy)
		bucket.Methods("GET").HandlerFunc(collectAPIStats("listobjectsv1", httpTraceAll(api.ListObjectsV1Handler)))
		// PutBucketLifecycle
		bucket.Methods("PUT").HandlerFunc(collectAPIStats("putbucketlifecycle", httpTraceAll(api.PutBucketLifecycleHandler))).Queries("lifecycle", "")
		// PutBucketPolicy
		bucket.Methods("PUT").HandlerFunc(collectAPIStats("putbucketpolicy", httpTraceAll(api.PutBucketPolicyHandler))).Queries("policy", "")

		// PutBucketObjectLockConfig
		bucket.Methods(http.MethodPut).HandlerFunc(httpTraceAll(api.PutBucketObjectLockConfigHandler)).Queries("object-lock", "")
		// PutBucketVersioning
		bucket.Methods(http.MethodPut).HandlerFunc(httpTraceAll(api.PutBucketVersioningHandler)).Queries("versioning", "")
		// PutBucketNotification
		bucket.Methods(http.MethodPut).HandlerFunc(collectAPIStats("putbucketnotification", httpTraceAll(api.PutBucketNotificationHandler))).Queries("notification", "")
		// PutBucket
		bucket.Methods(http.MethodPut).HandlerFunc(collectAPIStats("putbucket", httpTraceAll(api.PutBucketHandler)))
		// HeadBucket
		bucket.Methods(http.MethodHead).HandlerFunc(collectAPIStats("headbucket", httpTraceAll(api.HeadBucketHandler)))
		// PostPolicy
		bucket.Methods(http.MethodPost).HeadersRegexp(xhttp.ContentType, "multipart/form-data*").HandlerFunc(collectAPIStats("postpolicybucket", httpTraceHdrs(api.PostPolicyBucketHandler)))
		// DeleteMultipleObjects
		bucket.Methods(http.MethodPost).HandlerFunc(collectAPIStats("deletemultipleobjects", httpTraceAll(api.DeleteMultipleObjectsHandler))).Queries("delete", "")
		// DeleteBucketPolicy
		bucket.Methods("DELETE").HandlerFunc(collectAPIStats("deletebucketpolicy", httpTraceAll(api.DeleteBucketPolicyHandler))).Queries("policy", "")
		// DeleteBucketLifecycle
		bucket.Methods("DELETE").HandlerFunc(collectAPIStats("deletebucketlifecycle", httpTraceAll(api.DeleteBucketLifecycleHandler))).Queries("lifecycle", "")
		// DeleteBucket
		bucket.Methods(http.MethodDelete).HandlerFunc(collectAPIStats("deletebucket", httpTraceAll(api.DeleteBucketHandler)))
	}

	/// Root operation

	// ListBuckets
	apiRouter.Methods(http.MethodGet).Path(SlashSeparator).HandlerFunc(collectAPIStats("listbuckets", httpTraceAll(api.ListBucketsHandler)))

	// If none of the routes match add default error handler routes
	apiRouter.NotFoundHandler = http.HandlerFunc(collectAPIStats("notfound", httpTraceAll(errorResponseHandler)))
	apiRouter.MethodNotAllowedHandler = http.HandlerFunc(collectAPIStats("methodnotallowed", httpTraceAll(errorResponseHandler)))

}

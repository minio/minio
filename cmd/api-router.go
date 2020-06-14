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
	"net/http"

	"github.com/gorilla/mux"
	xhttp "github.com/minio/minio/cmd/http"
)

func newHTTPServerFn() *xhttp.Server {
	globalObjLayerMutex.Lock()
	defer globalObjLayerMutex.Unlock()
	return globalHTTPServer
}

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
		bucket.Methods(http.MethodHead).Path("/{object:.+}").HandlerFunc(
			maxClients(collectAPIStats("headobject", httpTraceAll(api.HeadObjectHandler))))
		// CopyObjectPart
		bucket.Methods(http.MethodPut).Path("/{object:.+}").HeadersRegexp(xhttp.AmzCopySource, ".*?(\\/|%2F).*?").HandlerFunc(maxClients(collectAPIStats("copyobjectpart", httpTraceAll(api.CopyObjectPartHandler)))).Queries("partNumber", "{partNumber:[0-9]+}", "uploadId", "{uploadId:.*}")
		// PutObjectPart
		bucket.Methods(http.MethodPut).Path("/{object:.+}").HandlerFunc(
			maxClients(collectAPIStats("putobjectpart", httpTraceHdrs(api.PutObjectPartHandler)))).Queries("partNumber", "{partNumber:[0-9]+}", "uploadId", "{uploadId:.*}")
		// ListObjectParts
		bucket.Methods(http.MethodGet).Path("/{object:.+}").HandlerFunc(
			maxClients(collectAPIStats("listobjectparts", httpTraceAll(api.ListObjectPartsHandler)))).Queries("uploadId", "{uploadId:.*}")
		// CompleteMultipartUpload
		bucket.Methods(http.MethodPost).Path("/{object:.+}").HandlerFunc(
			maxClients(collectAPIStats("completemutipartupload", httpTraceAll(api.CompleteMultipartUploadHandler)))).Queries("uploadId", "{uploadId:.*}")
		// NewMultipartUpload
		bucket.Methods(http.MethodPost).Path("/{object:.+}").HandlerFunc(
			maxClients(collectAPIStats("newmultipartupload", httpTraceAll(api.NewMultipartUploadHandler)))).Queries("uploads", "")
		// AbortMultipartUpload
		bucket.Methods(http.MethodDelete).Path("/{object:.+}").HandlerFunc(
			maxClients(collectAPIStats("abortmultipartupload", httpTraceAll(api.AbortMultipartUploadHandler)))).Queries("uploadId", "{uploadId:.*}")
		// GetObjectACL - this is a dummy call.
		bucket.Methods(http.MethodGet).Path("/{object:.+}").HandlerFunc(
			maxClients(collectAPIStats("getobjectacl", httpTraceHdrs(api.GetObjectACLHandler)))).Queries("acl", "")
		// PutObjectACL - this is a dummy call.
		bucket.Methods(http.MethodPut).Path("/{object:.+}").HandlerFunc(
			maxClients(collectAPIStats("putobjectacl", httpTraceHdrs(api.PutObjectACLHandler)))).Queries("acl", "")
		// GetObjectTagging
		bucket.Methods(http.MethodGet).Path("/{object:.+}").HandlerFunc(
			maxClients(collectAPIStats("getobjecttagging", httpTraceHdrs(api.GetObjectTaggingHandler)))).Queries("tagging", "")
		// PutObjectTagging
		bucket.Methods(http.MethodPut).Path("/{object:.+}").HandlerFunc(
			maxClients(collectAPIStats("putobjecttagging", httpTraceHdrs(api.PutObjectTaggingHandler)))).Queries("tagging", "")
		// DeleteObjectTagging
		bucket.Methods(http.MethodDelete).Path("/{object:.+}").HandlerFunc(
			maxClients(collectAPIStats("deleteobjecttagging", httpTraceHdrs(api.DeleteObjectTaggingHandler)))).Queries("tagging", "")
		// SelectObjectContent
		bucket.Methods(http.MethodPost).Path("/{object:.+}").HandlerFunc(
			maxClients(collectAPIStats("selectobjectcontent", httpTraceHdrs(api.SelectObjectContentHandler)))).Queries("select", "").Queries("select-type", "2")
		// GetObjectRetention
		bucket.Methods(http.MethodGet).Path("/{object:.+}").HandlerFunc(
			maxClients(collectAPIStats("getobjectretention", httpTraceAll(api.GetObjectRetentionHandler)))).Queries("retention", "")
		// GetObjectLegalHold
		bucket.Methods(http.MethodGet).Path("/{object:.+}").HandlerFunc(
			maxClients(collectAPIStats("getobjectlegalhold", httpTraceAll(api.GetObjectLegalHoldHandler)))).Queries("legal-hold", "")
		// GetObject
		bucket.Methods(http.MethodGet).Path("/{object:.+}").HandlerFunc(
			maxClients(collectAPIStats("getobject", httpTraceHdrs(api.GetObjectHandler))))
		// CopyObject
		bucket.Methods(http.MethodPut).Path("/{object:.+}").HeadersRegexp(xhttp.AmzCopySource, ".*?(\\/|%2F).*?").HandlerFunc(maxClients(collectAPIStats("copyobject", httpTraceAll(api.CopyObjectHandler))))
		// PutObjectRetention
		bucket.Methods(http.MethodPut).Path("/{object:.+}").HandlerFunc(
			maxClients(collectAPIStats("putobjectretention", httpTraceAll(api.PutObjectRetentionHandler)))).Queries("retention", "")
		// PutObjectLegalHold
		bucket.Methods(http.MethodPut).Path("/{object:.+}").HandlerFunc(
			maxClients(collectAPIStats("putobjectlegalhold", httpTraceAll(api.PutObjectLegalHoldHandler)))).Queries("legal-hold", "")

		// PutObject
		bucket.Methods(http.MethodPut).Path("/{object:.+}").HandlerFunc(
			maxClients(collectAPIStats("putobject", httpTraceHdrs(api.PutObjectHandler))))
		// DeleteObject
		bucket.Methods(http.MethodDelete).Path("/{object:.+}").HandlerFunc(
			maxClients(collectAPIStats("deleteobject", httpTraceAll(api.DeleteObjectHandler))))

		/// Bucket operations
		// GetBucketLocation
		bucket.Methods(http.MethodGet).HandlerFunc(
			maxClients(collectAPIStats("getbucketlocation", httpTraceAll(api.GetBucketLocationHandler)))).Queries("location", "")
		// GetBucketPolicy
		bucket.Methods(http.MethodGet).HandlerFunc(
			maxClients(collectAPIStats("getbucketpolicy", httpTraceAll(api.GetBucketPolicyHandler)))).Queries("policy", "")
		// GetBucketLifecycle
		bucket.Methods(http.MethodGet).HandlerFunc(
			maxClients(collectAPIStats("getbucketlifecycle", httpTraceAll(api.GetBucketLifecycleHandler)))).Queries("lifecycle", "")
		// GetBucketEncryption
		bucket.Methods(http.MethodGet).HandlerFunc(
			maxClients(collectAPIStats("getbucketencryption", httpTraceAll(api.GetBucketEncryptionHandler)))).Queries("encryption", "")

		// Dummy Bucket Calls
		// GetBucketACL -- this is a dummy call.
		bucket.Methods(http.MethodGet).HandlerFunc(
			maxClients(collectAPIStats("getbucketacl", httpTraceAll(api.GetBucketACLHandler)))).Queries("acl", "")
		// PutBucketACL -- this is a dummy call.
		bucket.Methods(http.MethodPut).HandlerFunc(
			maxClients(collectAPIStats("putbucketacl", httpTraceAll(api.PutBucketACLHandler)))).Queries("acl", "")
		// GetBucketCors - this is a dummy call.
		bucket.Methods(http.MethodGet).HandlerFunc(
			maxClients(collectAPIStats("getbucketcors", httpTraceAll(api.GetBucketCorsHandler)))).Queries("cors", "")
		// GetBucketWebsiteHandler - this is a dummy call.
		bucket.Methods(http.MethodGet).HandlerFunc(
			maxClients(collectAPIStats("getbucketwebsite", httpTraceAll(api.GetBucketWebsiteHandler)))).Queries("website", "")
		// GetBucketAccelerateHandler - this is a dummy call.
		bucket.Methods(http.MethodGet).HandlerFunc(
			maxClients(collectAPIStats("getbucketaccelerate", httpTraceAll(api.GetBucketAccelerateHandler)))).Queries("accelerate", "")
		// GetBucketRequestPaymentHandler - this is a dummy call.
		bucket.Methods(http.MethodGet).HandlerFunc(
			maxClients(collectAPIStats("getbucketrequestpayment", httpTraceAll(api.GetBucketRequestPaymentHandler)))).Queries("requestPayment", "")
		// GetBucketLoggingHandler - this is a dummy call.
		bucket.Methods(http.MethodGet).HandlerFunc(
			maxClients(collectAPIStats("getbucketlogging", httpTraceAll(api.GetBucketLoggingHandler)))).Queries("logging", "")
		// GetBucketLifecycleHandler - this is a dummy call.
		bucket.Methods(http.MethodGet).HandlerFunc(
			maxClients(collectAPIStats("getbucketlifecycle", httpTraceAll(api.GetBucketLifecycleHandler)))).Queries("lifecycle", "")
		// GetBucketReplicationHandler - this is a dummy call.
		bucket.Methods(http.MethodGet).HandlerFunc(
			maxClients(collectAPIStats("getbucketreplication", httpTraceAll(api.GetBucketReplicationHandler)))).Queries("replication", "")
		// GetBucketTaggingHandler
		bucket.Methods(http.MethodGet).HandlerFunc(
			maxClients(collectAPIStats("getbuckettagging", httpTraceAll(api.GetBucketTaggingHandler)))).Queries("tagging", "")
		//DeleteBucketWebsiteHandler
		bucket.Methods(http.MethodDelete).HandlerFunc(
			maxClients(collectAPIStats("deletebucketwebsite", httpTraceAll(api.DeleteBucketWebsiteHandler)))).Queries("website", "")
		// DeleteBucketTaggingHandler
		bucket.Methods(http.MethodDelete).HandlerFunc(
			maxClients(collectAPIStats("deletebuckettagging", httpTraceAll(api.DeleteBucketTaggingHandler)))).Queries("tagging", "")

		// GetBucketObjectLockConfig
		bucket.Methods(http.MethodGet).HandlerFunc(
			maxClients(collectAPIStats("getbucketobjectlockconfiguration", httpTraceAll(api.GetBucketObjectLockConfigHandler)))).Queries("object-lock", "")
		// GetBucketVersioning
		bucket.Methods(http.MethodGet).HandlerFunc(
			maxClients(collectAPIStats("getbucketversioning", httpTraceAll(api.GetBucketVersioningHandler)))).Queries("versioning", "")
		// GetBucketNotification
		bucket.Methods(http.MethodGet).HandlerFunc(
			maxClients(collectAPIStats("getbucketnotification", httpTraceAll(api.GetBucketNotificationHandler)))).Queries("notification", "")
		// ListenBucketNotification
		bucket.Methods(http.MethodGet).HandlerFunc(collectAPIStats("listenbucketnotification", httpTraceAll(api.ListenBucketNotificationHandler))).Queries("events", "{events:.*}")
		// ListMultipartUploads
		bucket.Methods(http.MethodGet).HandlerFunc(
			maxClients(collectAPIStats("listmultipartuploads", httpTraceAll(api.ListMultipartUploadsHandler)))).Queries("uploads", "")
		// ListObjectsV2M
		bucket.Methods(http.MethodGet).HandlerFunc(
			maxClients(collectAPIStats("listobjectsv2M", httpTraceAll(api.ListObjectsV2MHandler)))).Queries("list-type", "2", "metadata", "true")
		// ListObjectsV2
		bucket.Methods(http.MethodGet).HandlerFunc(
			maxClients(collectAPIStats("listobjectsv2", httpTraceAll(api.ListObjectsV2Handler)))).Queries("list-type", "2")
		// ListObjectVersions
		bucket.Methods(http.MethodGet).HandlerFunc(
			maxClients(collectAPIStats("listobjectversions", httpTraceAll(api.ListObjectVersionsHandler)))).Queries("versions", "")
		// ListObjectsV1 (Legacy)
		bucket.Methods(http.MethodGet).HandlerFunc(
			maxClients(collectAPIStats("listobjectsv1", httpTraceAll(api.ListObjectsV1Handler))))
		// PutBucketLifecycle
		bucket.Methods(http.MethodPut).HandlerFunc(
			maxClients(collectAPIStats("putbucketlifecycle", httpTraceAll(api.PutBucketLifecycleHandler)))).Queries("lifecycle", "")
		// PutBucketEncryption
		bucket.Methods(http.MethodPut).HandlerFunc(
			maxClients(collectAPIStats("putbucketencryption", httpTraceAll(api.PutBucketEncryptionHandler)))).Queries("encryption", "")

		// PutBucketPolicy
		bucket.Methods(http.MethodPut).HandlerFunc(
			maxClients(collectAPIStats("putbucketpolicy", httpTraceAll(api.PutBucketPolicyHandler)))).Queries("policy", "")

		// PutBucketObjectLockConfig
		bucket.Methods(http.MethodPut).HandlerFunc(
			maxClients(collectAPIStats("putbucketobjectlockconfig", httpTraceAll(api.PutBucketObjectLockConfigHandler)))).Queries("object-lock", "")
		// PutBucketTaggingHandler
		bucket.Methods(http.MethodPut).HandlerFunc(
			maxClients(collectAPIStats("putbuckettagging", httpTraceAll(api.PutBucketTaggingHandler)))).Queries("tagging", "")
		// PutBucketVersioning
		bucket.Methods(http.MethodPut).HandlerFunc(
			maxClients(collectAPIStats("putbucketversioning", httpTraceAll(api.PutBucketVersioningHandler)))).Queries("versioning", "")
		// PutBucketNotification
		bucket.Methods(http.MethodPut).HandlerFunc(
			maxClients(collectAPIStats("putbucketnotification", httpTraceAll(api.PutBucketNotificationHandler)))).Queries("notification", "")
		// PutBucket
		bucket.Methods(http.MethodPut).HandlerFunc(
			maxClients(collectAPIStats("putbucket", httpTraceAll(api.PutBucketHandler))))
		// HeadBucket
		bucket.Methods(http.MethodHead).HandlerFunc(
			maxClients(collectAPIStats("headbucket", httpTraceAll(api.HeadBucketHandler))))
		// PostPolicy
		bucket.Methods(http.MethodPost).HeadersRegexp(xhttp.ContentType, "multipart/form-data*").HandlerFunc(
			maxClients(collectAPIStats("postpolicybucket", httpTraceHdrs(api.PostPolicyBucketHandler))))
		// DeleteMultipleObjects
		bucket.Methods(http.MethodPost).HandlerFunc(
			maxClients(collectAPIStats("deletemultipleobjects", httpTraceAll(api.DeleteMultipleObjectsHandler)))).Queries("delete", "")
		// DeleteBucketPolicy
		bucket.Methods(http.MethodDelete).HandlerFunc(
			maxClients(collectAPIStats("deletebucketpolicy", httpTraceAll(api.DeleteBucketPolicyHandler)))).Queries("policy", "")
		// DeleteBucketLifecycle
		bucket.Methods(http.MethodDelete).HandlerFunc(
			maxClients(collectAPIStats("deletebucketlifecycle", httpTraceAll(api.DeleteBucketLifecycleHandler)))).Queries("lifecycle", "")
		// DeleteBucketEncryption
		bucket.Methods(http.MethodDelete).HandlerFunc(
			maxClients(collectAPIStats("deletebucketencryption", httpTraceAll(api.DeleteBucketEncryptionHandler)))).Queries("encryption", "")
		// DeleteBucket
		bucket.Methods(http.MethodDelete).HandlerFunc(
			maxClients(collectAPIStats("deletebucket", httpTraceAll(api.DeleteBucketHandler))))
	}

	/// Root operation

	// ListBuckets
	apiRouter.Methods(http.MethodGet).Path(SlashSeparator).HandlerFunc(
		maxClients(collectAPIStats("listbuckets", httpTraceAll(api.ListBucketsHandler))))

	// S3 browser with signature v4 adds '//' for ListBuckets request, so rather
	// than failing with UnknownAPIRequest we simply handle it for now.
	apiRouter.Methods(http.MethodGet).Path(SlashSeparator + SlashSeparator).HandlerFunc(
		maxClients(collectAPIStats("listbuckets", httpTraceAll(api.ListBucketsHandler))))

	// If none of the routes match add default error handler routes
	apiRouter.NotFoundHandler = http.HandlerFunc(collectAPIStats("notfound", httpTraceAll(errorResponseHandler)))
	apiRouter.MethodNotAllowedHandler = http.HandlerFunc(collectAPIStats("methodnotallowed", httpTraceAll(errorResponseHandler)))

}

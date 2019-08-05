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
		CacheAPI:  newCacheObjectsFn,
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
		bucket.Methods(http.MethodHead).Path("/{object:.+}").HandlerFunc(httpTraceAll(api.HeadObjectHandler))
		// CopyObjectPart
		bucket.Methods(http.MethodPut).Path("/{object:.+}").HeadersRegexp(xhttp.AmzCopySource, ".*?(\\/|%2F).*?").HandlerFunc(httpTraceAll(api.CopyObjectPartHandler)).Queries("partNumber", "{partNumber:[0-9]+}", "uploadId", "{uploadId:.*}")
		// PutObjectPart
		bucket.Methods(http.MethodPut).Path("/{object:.+}").HandlerFunc(httpTraceHdrs(api.PutObjectPartHandler)).Queries("partNumber", "{partNumber:[0-9]+}", "uploadId", "{uploadId:.*}")
		// ListObjectPxarts
		bucket.Methods(http.MethodGet).Path("/{object:.+}").HandlerFunc(httpTraceAll(api.ListObjectPartsHandler)).Queries("uploadId", "{uploadId:.*}")
		// CompleteMultipartUpload
		bucket.Methods(http.MethodPost).Path("/{object:.+}").HandlerFunc(httpTraceAll(api.CompleteMultipartUploadHandler)).Queries("uploadId", "{uploadId:.*}")
		// NewMultipartUpload
		bucket.Methods(http.MethodPost).Path("/{object:.+}").HandlerFunc(httpTraceAll(api.NewMultipartUploadHandler)).Queries("uploads", "")
		// AbortMultipartUpload
		bucket.Methods(http.MethodDelete).Path("/{object:.+}").HandlerFunc(httpTraceAll(api.AbortMultipartUploadHandler)).Queries("uploadId", "{uploadId:.*}")
		// GetObjectACL - this is a dummy call.
		bucket.Methods(http.MethodGet).Path("/{object:.+}").HandlerFunc(httpTraceHdrs(api.GetObjectACLHandler)).Queries("acl", "")
		// GetObjectTagging - this is a dummy call.
		bucket.Methods(http.MethodGet).Path("/{object:.+}").HandlerFunc(httpTraceHdrs(api.GetObjectTaggingHandler)).Queries("tagging", "")
		// SelectObjectContent
		bucket.Methods(http.MethodPost).Path("/{object:.+}").HandlerFunc(httpTraceHdrs(api.SelectObjectContentHandler)).Queries("select", "").Queries("select-type", "2")
		// GetObject
		bucket.Methods(http.MethodGet).Path("/{object:.+}").HandlerFunc(httpTraceHdrs(api.GetObjectHandler))
		// CopyObject
		bucket.Methods(http.MethodPut).Path("/{object:.+}").HeadersRegexp(xhttp.AmzCopySource, ".*?(\\/|%2F).*?").HandlerFunc(httpTraceAll(api.CopyObjectHandler))
		// PutObject
		bucket.Methods(http.MethodPut).Path("/{object:.+}").HandlerFunc(httpTraceHdrs(api.PutObjectHandler))
		// DeleteObject
		bucket.Methods(http.MethodDelete).Path("/{object:.+}").HandlerFunc(httpTraceAll(api.DeleteObjectHandler))

		/// Bucket operations
		// GetBucketLocation
		bucket.Methods(http.MethodGet).HandlerFunc(httpTraceAll(api.GetBucketLocationHandler)).Queries("location", "")
		// GetBucketPolicy
		bucket.Methods("GET").HandlerFunc(httpTraceAll(api.GetBucketPolicyHandler)).Queries("policy", "")
		// GetBucketLifecycle
		bucket.Methods("GET").HandlerFunc(httpTraceAll(api.GetBucketLifecycleHandler)).Queries("lifecycle", "")

		// Dummy Bucket Calls
		// GetBucketACL -- this is a dummy call.
		bucket.Methods(http.MethodGet).HandlerFunc(httpTraceAll(api.GetBucketACLHandler)).Queries("acl", "")
		// GetBucketCors - this is a dummy call.
		bucket.Methods(http.MethodGet).HandlerFunc(httpTraceAll(api.GetBucketCorsHandler)).Queries("cors", "")
		// GetBucketWebsiteHandler - this is a dummy call.
		bucket.Methods(http.MethodGet).HandlerFunc(httpTraceAll(api.GetBucketWebsiteHandler)).Queries("website", "")
		// GetBucketVersioningHandler - this is a dummy call.
		bucket.Methods(http.MethodGet).HandlerFunc(httpTraceAll(api.GetBucketVersioningHandler)).Queries("versioning", "")
		// GetBucketAccelerateHandler - this is a dummy call.
		bucket.Methods(http.MethodGet).HandlerFunc(httpTraceAll(api.GetBucketAccelerateHandler)).Queries("accelerate", "")
		// GetBucketRequestPaymentHandler - this is a dummy call.
		bucket.Methods(http.MethodGet).HandlerFunc(httpTraceAll(api.GetBucketRequestPaymentHandler)).Queries("requestPayment", "")
		// GetBucketLoggingHandler - this is a dummy call.
		bucket.Methods(http.MethodGet).HandlerFunc(httpTraceAll(api.GetBucketLoggingHandler)).Queries("logging", "")
		// GetBucketLifecycleHandler - this is a dummy call.
		bucket.Methods(http.MethodGet).HandlerFunc(httpTraceAll(api.GetBucketLifecycleHandler)).Queries("lifecycle", "")
		// GetBucketReplicationHandler - this is a dummy call.
		bucket.Methods(http.MethodGet).HandlerFunc(httpTraceAll(api.GetBucketReplicationHandler)).Queries("replication", "")
		// GetBucketTaggingHandler - this is a dummy call.
		bucket.Methods(http.MethodGet).HandlerFunc(httpTraceAll(api.GetBucketTaggingHandler)).Queries("tagging", "")
		//DeleteBucketWebsiteHandler
		bucket.Methods(http.MethodDelete).HandlerFunc(httpTraceAll(api.DeleteBucketWebsiteHandler)).Queries("website", "")
		// DeleteBucketTaggingHandler
		bucket.Methods(http.MethodDelete).HandlerFunc(httpTraceAll(api.DeleteBucketTaggingHandler)).Queries("tagging", "")

		// GetBucketNotification
		bucket.Methods(http.MethodGet).HandlerFunc(httpTraceAll(api.GetBucketNotificationHandler)).Queries("notification", "")
		// ListenBucketNotification
		bucket.Methods(http.MethodGet).HandlerFunc(httpTraceAll(api.ListenBucketNotificationHandler)).Queries("events", "{events:.*}")
		// ListMultipartUploads
		bucket.Methods(http.MethodGet).HandlerFunc(httpTraceAll(api.ListMultipartUploadsHandler)).Queries("uploads", "")
		// ListObjectsV2
		bucket.Methods(http.MethodGet).HandlerFunc(httpTraceAll(api.ListObjectsV2Handler)).Queries("list-type", "2")
		// ListObjectsV1 (Legacy)
		bucket.Methods("GET").HandlerFunc(httpTraceAll(api.ListObjectsV1Handler))
		// PutBucketLifecycle
		bucket.Methods("PUT").HandlerFunc(httpTraceAll(api.PutBucketLifecycleHandler)).Queries("lifecycle", "")
		// PutBucketPolicy
		bucket.Methods("PUT").HandlerFunc(httpTraceAll(api.PutBucketPolicyHandler)).Queries("policy", "")

		// PutBucketNotification
		bucket.Methods(http.MethodPut).HandlerFunc(httpTraceAll(api.PutBucketNotificationHandler)).Queries("notification", "")
		// PutBucket
		bucket.Methods(http.MethodPut).HandlerFunc(httpTraceAll(api.PutBucketHandler))
		// HeadBucket
		bucket.Methods(http.MethodHead).HandlerFunc(httpTraceAll(api.HeadBucketHandler))
		// PostPolicy
		bucket.Methods(http.MethodPost).HeadersRegexp(xhttp.ContentType, "multipart/form-data*").HandlerFunc(httpTraceHdrs(api.PostPolicyBucketHandler))
		// DeleteMultipleObjects
		bucket.Methods(http.MethodPost).HandlerFunc(httpTraceAll(api.DeleteMultipleObjectsHandler)).Queries("delete", "")
		// DeleteBucketPolicy
		bucket.Methods("DELETE").HandlerFunc(httpTraceAll(api.DeleteBucketPolicyHandler)).Queries("policy", "")
		// DeleteBucketLifecycle
		bucket.Methods("DELETE").HandlerFunc(httpTraceAll(api.DeleteBucketLifecycleHandler)).Queries("lifecycle", "")
		// DeleteBucket
		bucket.Methods(http.MethodDelete).HandlerFunc(httpTraceAll(api.DeleteBucketHandler))
	}

	/// Root operation

	// ListBuckets
	apiRouter.Methods(http.MethodGet).Path(SlashSeparator).HandlerFunc(httpTraceAll(api.ListBucketsHandler))

	// If none of the routes match.
	apiRouter.NotFoundHandler = http.HandlerFunc(httpTraceAll(notFoundHandler))
}

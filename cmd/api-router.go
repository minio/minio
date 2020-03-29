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
	"strconv"
	"time"

	"github.com/gorilla/mux"
	"github.com/minio/minio/cmd/config"
	xhttp "github.com/minio/minio/cmd/http"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/env"
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

	requestsMax, _ := strconv.Atoi(env.Get(config.EnvAPIRequestsMax, "0"))
	if requestsMax < 0 {
		requestsMax = 0 // 0 means unlimited
	}

	var (
		enabled              bool
		requestsMaxPerServer int
		requestsDeadline     time.Duration
	)

	if len(globalEndpoints.Hosts()) == 0 {
		requestsMaxPerServer = requestsMax
	} else {
		requestsMaxPerServer = requestsMax / len(globalEndpoints.Hosts())
	}

	enabled = requestsMaxPerServer > 0
	if enabled {
		var err error
		requestsDeadline, err = time.ParseDuration(env.Get(config.EnvAPIRequestsDeadline, "10s"))
		if err != nil {
			logger.Info("(%s) parsing environment value MINIO_API_REQUESTS_DEADLINE, defaulting to 10 seconds", err)
			requestsDeadline = 10 * time.Second
		}
	}

	requestsMaxCh := make(chan struct{}, requestsMaxPerServer)

	for _, bucket := range routers {
		// Object operations
		// HeadObject
		bucket.Methods(http.MethodHead).Path("/{object:.+}").HandlerFunc(
			maxClients(collectAPIStats("headobject", httpTraceAll(api.HeadObjectHandler)), enabled, requestsMaxCh, requestsDeadline))
		// CopyObjectPart
		bucket.Methods(http.MethodPut).Path("/{object:.+}").HeadersRegexp(xhttp.AmzCopySource, ".*?(\\/|%2F).*?").HandlerFunc(maxClients(collectAPIStats("copyobjectpart", httpTraceAll(api.CopyObjectPartHandler)), enabled, requestsMaxCh, requestsDeadline)).Queries("partNumber", "{partNumber:[0-9]+}", "uploadId", "{uploadId:.*}")
		// PutObjectPart
		bucket.Methods(http.MethodPut).Path("/{object:.+}").HandlerFunc(
			maxClients(collectAPIStats("putobjectpart", httpTraceHdrs(api.PutObjectPartHandler)), enabled, requestsMaxCh, requestsDeadline)).Queries("partNumber", "{partNumber:[0-9]+}", "uploadId", "{uploadId:.*}")
		// ListObjectParts
		bucket.Methods(http.MethodGet).Path("/{object:.+}").HandlerFunc(
			maxClients(collectAPIStats("listobjectparts", httpTraceAll(api.ListObjectPartsHandler)), enabled, requestsMaxCh, requestsDeadline)).Queries("uploadId", "{uploadId:.*}")
		// CompleteMultipartUpload
		bucket.Methods(http.MethodPost).Path("/{object:.+}").HandlerFunc(
			maxClients(collectAPIStats("completemutipartupload", httpTraceAll(api.CompleteMultipartUploadHandler)), enabled, requestsMaxCh, requestsDeadline)).Queries("uploadId", "{uploadId:.*}")
		// NewMultipartUpload
		bucket.Methods(http.MethodPost).Path("/{object:.+}").HandlerFunc(
			maxClients(collectAPIStats("newmultipartupload", httpTraceAll(api.NewMultipartUploadHandler)), enabled, requestsMaxCh, requestsDeadline)).Queries("uploads", "")
		// AbortMultipartUpload
		bucket.Methods(http.MethodDelete).Path("/{object:.+}").HandlerFunc(
			maxClients(collectAPIStats("abortmultipartupload", httpTraceAll(api.AbortMultipartUploadHandler)), enabled, requestsMaxCh, requestsDeadline)).Queries("uploadId", "{uploadId:.*}")
		// GetObjectACL - this is a dummy call.
		bucket.Methods(http.MethodGet).Path("/{object:.+}").HandlerFunc(
			maxClients(collectAPIStats("getobjectacl", httpTraceHdrs(api.GetObjectACLHandler)), enabled, requestsMaxCh, requestsDeadline)).Queries("acl", "")
		// PutObjectACL - this is a dummy call.
		bucket.Methods(http.MethodPut).Path("/{object:.+}").HandlerFunc(
			maxClients(collectAPIStats("putobjectacl", httpTraceHdrs(api.PutObjectACLHandler)), enabled, requestsMaxCh, requestsDeadline)).Queries("acl", "")
		// GetObjectTagging
		bucket.Methods(http.MethodGet).Path("/{object:.+}").HandlerFunc(
			maxClients(collectAPIStats("getobjecttagging", httpTraceHdrs(api.GetObjectTaggingHandler)), enabled, requestsMaxCh, requestsDeadline)).Queries("tagging", "")
		// PutObjectTagging
		bucket.Methods(http.MethodPut).Path("/{object:.+}").HandlerFunc(
			maxClients(collectAPIStats("putobjecttagging", httpTraceHdrs(api.PutObjectTaggingHandler)), enabled, requestsMaxCh, requestsDeadline)).Queries("tagging", "")
		// DeleteObjectTagging
		bucket.Methods(http.MethodDelete).Path("/{object:.+}").HandlerFunc(
			maxClients(collectAPIStats("deleteobjecttagging", httpTraceHdrs(api.DeleteObjectTaggingHandler)), enabled, requestsMaxCh, requestsDeadline)).Queries("tagging", "")
		// SelectObjectContent
		bucket.Methods(http.MethodPost).Path("/{object:.+}").HandlerFunc(
			maxClients(collectAPIStats("selectobjectcontent", httpTraceHdrs(api.SelectObjectContentHandler)), enabled, requestsMaxCh, requestsDeadline)).Queries("select", "").Queries("select-type", "2")
		// GetObjectRetention
		bucket.Methods(http.MethodGet).Path("/{object:.+}").HandlerFunc(
			maxClients(collectAPIStats("getobjectretention", httpTraceAll(api.GetObjectRetentionHandler)), enabled, requestsMaxCh, requestsDeadline)).Queries("retention", "")
		// GetObjectLegalHold
		bucket.Methods(http.MethodGet).Path("/{object:.+}").HandlerFunc(
			maxClients(collectAPIStats("getobjectlegalhold", httpTraceAll(api.GetObjectLegalHoldHandler)), enabled, requestsMaxCh, requestsDeadline)).Queries("legal-hold", "")
		// GetObject
		bucket.Methods(http.MethodGet).Path("/{object:.+}").HandlerFunc(
			maxClients(collectAPIStats("getobject", httpTraceHdrs(api.GetObjectHandler)), enabled, requestsMaxCh, requestsDeadline))
		// CopyObject
		bucket.Methods(http.MethodPut).Path("/{object:.+}").HeadersRegexp(xhttp.AmzCopySource, ".*?(\\/|%2F).*?").HandlerFunc(maxClients(collectAPIStats("copyobject", httpTraceAll(api.CopyObjectHandler)), enabled, requestsMaxCh, requestsDeadline))
		// PutObjectRetention
		bucket.Methods(http.MethodPut).Path("/{object:.+}").HandlerFunc(
			maxClients(collectAPIStats("putobjectretention", httpTraceAll(api.PutObjectRetentionHandler)), enabled, requestsMaxCh, requestsDeadline)).Queries("retention", "")
		// PutObjectLegalHold
		bucket.Methods(http.MethodPut).Path("/{object:.+}").HandlerFunc(
			maxClients(collectAPIStats("putobjectlegalhold", httpTraceAll(api.PutObjectLegalHoldHandler)), enabled, requestsMaxCh, requestsDeadline)).Queries("legal-hold", "")

		// PutObject
		bucket.Methods(http.MethodPut).Path("/{object:.+}").HandlerFunc(
			maxClients(collectAPIStats("putobject", httpTraceHdrs(api.PutObjectHandler)), enabled, requestsMaxCh, requestsDeadline))
		// DeleteObject
		bucket.Methods(http.MethodDelete).Path("/{object:.+}").HandlerFunc(
			maxClients(collectAPIStats("deleteobject", httpTraceAll(api.DeleteObjectHandler)), enabled, requestsMaxCh, requestsDeadline))

		/// Bucket operations
		// GetBucketLocation
		bucket.Methods(http.MethodGet).HandlerFunc(
			maxClients(collectAPIStats("getbucketlocation", httpTraceAll(api.GetBucketLocationHandler)), enabled, requestsMaxCh, requestsDeadline)).Queries("location", "")
		// GetBucketPolicy
		bucket.Methods(http.MethodGet).HandlerFunc(
			maxClients(collectAPIStats("getbucketpolicy", httpTraceAll(api.GetBucketPolicyHandler)), enabled, requestsMaxCh, requestsDeadline)).Queries("policy", "")
		// GetBucketLifecycle
		bucket.Methods(http.MethodGet).HandlerFunc(
			maxClients(collectAPIStats("getbucketlifecycle", httpTraceAll(api.GetBucketLifecycleHandler)), enabled, requestsMaxCh, requestsDeadline)).Queries("lifecycle", "")
		// GetBucketEncryption
		bucket.Methods(http.MethodGet).HandlerFunc(
			maxClients(collectAPIStats("getbucketencryption", httpTraceAll(api.GetBucketEncryptionHandler)), enabled, requestsMaxCh, requestsDeadline)).Queries("encryption", "")

		// Dummy Bucket Calls
		// GetBucketACL -- this is a dummy call.
		bucket.Methods(http.MethodGet).HandlerFunc(
			maxClients(collectAPIStats("getbucketacl", httpTraceAll(api.GetBucketACLHandler)), enabled, requestsMaxCh, requestsDeadline)).Queries("acl", "")
		// PutBucketACL -- this is a dummy call.
		bucket.Methods(http.MethodPut).HandlerFunc(
			maxClients(collectAPIStats("putbucketacl", httpTraceAll(api.PutBucketACLHandler)), enabled, requestsMaxCh, requestsDeadline)).Queries("acl", "")
		// GetBucketCors - this is a dummy call.
		bucket.Methods(http.MethodGet).HandlerFunc(
			maxClients(collectAPIStats("getbucketcors", httpTraceAll(api.GetBucketCorsHandler)), enabled, requestsMaxCh, requestsDeadline)).Queries("cors", "")
		// GetBucketWebsiteHandler - this is a dummy call.
		bucket.Methods(http.MethodGet).HandlerFunc(
			maxClients(collectAPIStats("getbucketwebsite", httpTraceAll(api.GetBucketWebsiteHandler)), enabled, requestsMaxCh, requestsDeadline)).Queries("website", "")
		// GetBucketAccelerateHandler - this is a dummy call.
		bucket.Methods(http.MethodGet).HandlerFunc(
			maxClients(collectAPIStats("getbucketaccelerate", httpTraceAll(api.GetBucketAccelerateHandler)), enabled, requestsMaxCh, requestsDeadline)).Queries("accelerate", "")
		// GetBucketRequestPaymentHandler - this is a dummy call.
		bucket.Methods(http.MethodGet).HandlerFunc(
			maxClients(collectAPIStats("getbucketrequestpayment", httpTraceAll(api.GetBucketRequestPaymentHandler)), enabled, requestsMaxCh, requestsDeadline)).Queries("requestPayment", "")
		// GetBucketLoggingHandler - this is a dummy call.
		bucket.Methods(http.MethodGet).HandlerFunc(
			maxClients(collectAPIStats("getbucketlogging", httpTraceAll(api.GetBucketLoggingHandler)), enabled, requestsMaxCh, requestsDeadline)).Queries("logging", "")
		// GetBucketLifecycleHandler - this is a dummy call.
		bucket.Methods(http.MethodGet).HandlerFunc(
			maxClients(collectAPIStats("getbucketlifecycle", httpTraceAll(api.GetBucketLifecycleHandler)), enabled, requestsMaxCh, requestsDeadline)).Queries("lifecycle", "")
		// GetBucketReplicationHandler - this is a dummy call.
		bucket.Methods(http.MethodGet).HandlerFunc(
			maxClients(collectAPIStats("getbucketreplication", httpTraceAll(api.GetBucketReplicationHandler)), enabled, requestsMaxCh, requestsDeadline)).Queries("replication", "")
		// GetBucketTaggingHandler - this is a dummy call.
		bucket.Methods(http.MethodGet).HandlerFunc(
			maxClients(collectAPIStats("getbuckettagging", httpTraceAll(api.GetBucketTaggingHandler)), enabled, requestsMaxCh, requestsDeadline)).Queries("tagging", "")
		//DeleteBucketWebsiteHandler
		bucket.Methods(http.MethodDelete).HandlerFunc(
			maxClients(collectAPIStats("deletebucketwebsite", httpTraceAll(api.DeleteBucketWebsiteHandler)), enabled, requestsMaxCh, requestsDeadline)).Queries("website", "")
		// DeleteBucketTaggingHandler
		bucket.Methods(http.MethodDelete).HandlerFunc(
			maxClients(collectAPIStats("deletebuckettagging", httpTraceAll(api.DeleteBucketTaggingHandler)), enabled, requestsMaxCh, requestsDeadline)).Queries("tagging", "")

		// GetBucketObjectLockConfig
		bucket.Methods(http.MethodGet).HandlerFunc(
			maxClients(collectAPIStats("getbucketobjectlockconfiguration", httpTraceAll(api.GetBucketObjectLockConfigHandler)), enabled, requestsMaxCh, requestsDeadline)).Queries("object-lock", "")
		// GetBucketVersioning
		bucket.Methods(http.MethodGet).HandlerFunc(
			maxClients(collectAPIStats("getbucketversioning", httpTraceAll(api.GetBucketVersioningHandler)), enabled, requestsMaxCh, requestsDeadline)).Queries("versioning", "")
		// GetBucketNotification
		bucket.Methods(http.MethodGet).HandlerFunc(
			maxClients(collectAPIStats("getbucketnotification", httpTraceAll(api.GetBucketNotificationHandler)), enabled, requestsMaxCh, requestsDeadline)).Queries("notification", "")
		// ListenBucketNotification
		bucket.Methods(http.MethodGet).HandlerFunc(collectAPIStats("listenbucketnotification", httpTraceAll(api.ListenBucketNotificationHandler))).Queries("events", "{events:.*}")
		// ListMultipartUploads
		bucket.Methods(http.MethodGet).HandlerFunc(
			maxClients(collectAPIStats("listmultipartuploads", httpTraceAll(api.ListMultipartUploadsHandler)), enabled, requestsMaxCh, requestsDeadline)).Queries("uploads", "")
		// ListObjectsV2M
		bucket.Methods(http.MethodGet).HandlerFunc(
			maxClients(collectAPIStats("listobjectsv2M", httpTraceAll(api.ListObjectsV2MHandler)), enabled, requestsMaxCh, requestsDeadline)).Queries("list-type", "2", "metadata", "true")
		// ListObjectsV2
		bucket.Methods(http.MethodGet).HandlerFunc(
			maxClients(collectAPIStats("listobjectsv2", httpTraceAll(api.ListObjectsV2Handler)), enabled, requestsMaxCh, requestsDeadline)).Queries("list-type", "2")
		// ListBucketVersions
		bucket.Methods(http.MethodGet).HandlerFunc(
			maxClients(collectAPIStats("listbucketversions", httpTraceAll(api.ListBucketObjectVersionsHandler)), enabled, requestsMaxCh, requestsDeadline)).Queries("versions", "")
		// ListObjectsV1 (Legacy)
		bucket.Methods(http.MethodGet).HandlerFunc(
			maxClients(collectAPIStats("listobjectsv1", httpTraceAll(api.ListObjectsV1Handler)), enabled, requestsMaxCh, requestsDeadline))
		// PutBucketLifecycle
		bucket.Methods(http.MethodPut).HandlerFunc(
			maxClients(collectAPIStats("putbucketlifecycle", httpTraceAll(api.PutBucketLifecycleHandler)), enabled, requestsMaxCh, requestsDeadline)).Queries("lifecycle", "")
		// PutBucketEncryption
		bucket.Methods(http.MethodPut).HandlerFunc(
			maxClients(collectAPIStats("putbucketencryption", httpTraceAll(api.PutBucketEncryptionHandler)), enabled, requestsMaxCh, requestsDeadline)).Queries("encryption", "")

		// PutBucketPolicy
		bucket.Methods(http.MethodPut).HandlerFunc(
			maxClients(collectAPIStats("putbucketpolicy", httpTraceAll(api.PutBucketPolicyHandler)), enabled, requestsMaxCh, requestsDeadline)).Queries("policy", "")

		// PutBucketObjectLockConfig
		bucket.Methods(http.MethodPut).HandlerFunc(
			maxClients(collectAPIStats("putbucketobjectlockconfig", httpTraceAll(api.PutBucketObjectLockConfigHandler)), enabled, requestsMaxCh, requestsDeadline)).Queries("object-lock", "")
		// PutBucketVersioning
		bucket.Methods(http.MethodPut).HandlerFunc(
			maxClients(collectAPIStats("putbucketversioning", httpTraceAll(api.PutBucketVersioningHandler)), enabled, requestsMaxCh, requestsDeadline)).Queries("versioning", "")
		// PutBucketNotification
		bucket.Methods(http.MethodPut).HandlerFunc(
			maxClients(collectAPIStats("putbucketnotification", httpTraceAll(api.PutBucketNotificationHandler)), enabled, requestsMaxCh, requestsDeadline)).Queries("notification", "")
		// PutBucket
		bucket.Methods(http.MethodPut).HandlerFunc(
			maxClients(collectAPIStats("putbucket", httpTraceAll(api.PutBucketHandler)), enabled, requestsMaxCh, requestsDeadline))
		// HeadBucket
		bucket.Methods(http.MethodHead).HandlerFunc(
			maxClients(collectAPIStats("headbucket", httpTraceAll(api.HeadBucketHandler)), enabled, requestsMaxCh, requestsDeadline))
		// PostPolicy
		bucket.Methods(http.MethodPost).HeadersRegexp(xhttp.ContentType, "multipart/form-data*").HandlerFunc(
			maxClients(collectAPIStats("postpolicybucket", httpTraceHdrs(api.PostPolicyBucketHandler)), enabled, requestsMaxCh, requestsDeadline))
		// DeleteMultipleObjects
		bucket.Methods(http.MethodPost).HandlerFunc(
			maxClients(collectAPIStats("deletemultipleobjects", httpTraceAll(api.DeleteMultipleObjectsHandler)), enabled, requestsMaxCh, requestsDeadline)).Queries("delete", "")
		// DeleteBucketPolicy
		bucket.Methods(http.MethodDelete).HandlerFunc(
			maxClients(collectAPIStats("deletebucketpolicy", httpTraceAll(api.DeleteBucketPolicyHandler)), enabled, requestsMaxCh, requestsDeadline)).Queries("policy", "")
		// DeleteBucketLifecycle
		bucket.Methods(http.MethodDelete).HandlerFunc(
			maxClients(collectAPIStats("deletebucketlifecycle", httpTraceAll(api.DeleteBucketLifecycleHandler)), enabled, requestsMaxCh, requestsDeadline)).Queries("lifecycle", "")
		// DeleteBucketEncryption
		bucket.Methods(http.MethodDelete).HandlerFunc(
			maxClients(collectAPIStats("deletebucketencryption", httpTraceAll(api.DeleteBucketEncryptionHandler)), enabled, requestsMaxCh, requestsDeadline)).Queries("encryption", "")
		// DeleteBucket
		bucket.Methods(http.MethodDelete).HandlerFunc(
			maxClients(collectAPIStats("deletebucket", httpTraceAll(api.DeleteBucketHandler)), enabled, requestsMaxCh, requestsDeadline))
	}

	/// Root operation

	// ListBuckets
	apiRouter.Methods(http.MethodGet).Path(SlashSeparator).HandlerFunc(
		maxClients(collectAPIStats("listbuckets", httpTraceAll(api.ListBucketsHandler)), enabled, requestsMaxCh, requestsDeadline))

	// If none of the routes match add default error handler routes
	apiRouter.NotFoundHandler = http.HandlerFunc(collectAPIStats("notfound", httpTraceAll(errorResponseHandler)))
	apiRouter.MethodNotAllowedHandler = http.HandlerFunc(collectAPIStats("methodnotallowed", httpTraceAll(errorResponseHandler)))

}

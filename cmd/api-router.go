/*
 * Minio Cloud Storage, (C) 2016 Minio, Inc.
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

import router "github.com/gorilla/mux"

// objectAPIHandler implements and provides http handlers for S3 API.
type objectAPIHandlers struct {
	ObjectAPI func() ObjectLayer
}

// registerAPIRouter - registers S3 compatible APIs.
func registerAPIRouter(mux *router.Router) {
	// Initialize API.
	api := objectAPIHandlers{
		ObjectAPI: newObjectLayerFn,
	}

	// API Router
	apiRouter := mux.NewRoute().PathPrefix("/").Subrouter()

	// Bucket router
	bucket := apiRouter.PathPrefix("/{bucket}").Subrouter()

	/// Object operations

	// HeadObject
	bucket.Methods("HEAD").Path("/{object:.+}").HandlerFunc(httpTracelogAll(api.HeadObjectHandler))
	// CopyObjectPart
	bucket.Methods("PUT").Path("/{object:.+}").HeadersRegexp("X-Amz-Copy-Source", ".*?(\\/|%2F).*?").HandlerFunc(httpTracelogAll(api.CopyObjectPartHandler)).Queries("partNumber", "{partNumber:[0-9]+}", "uploadId", "{uploadId:.*}")
	// PutObjectPart
	bucket.Methods("PUT").Path("/{object:.+}").HandlerFunc(httpTracelogHeaders(api.PutObjectPartHandler)).Queries("partNumber", "{partNumber:[0-9]+}", "uploadId", "{uploadId:.*}")
	// ListObjectPxarts
	bucket.Methods("GET").Path("/{object:.+}").HandlerFunc(httpTracelogAll(api.ListObjectPartsHandler)).Queries("uploadId", "{uploadId:.*}")
	// CompleteMultipartUpload
	bucket.Methods("POST").Path("/{object:.+}").HandlerFunc(httpTracelogAll(api.CompleteMultipartUploadHandler)).Queries("uploadId", "{uploadId:.*}")
	// NewMultipartUpload
	bucket.Methods("POST").Path("/{object:.+}").HandlerFunc(httpTracelogAll(api.NewMultipartUploadHandler)).Queries("uploads", "")
	// AbortMultipartUpload
	bucket.Methods("DELETE").Path("/{object:.+}").HandlerFunc(httpTracelogAll(api.AbortMultipartUploadHandler)).Queries("uploadId", "{uploadId:.*}")
	// GetObject
	bucket.Methods("GET").Path("/{object:.+}").HandlerFunc(httpTracelogHeaders(api.GetObjectHandler))
	// CopyObject
	bucket.Methods("PUT").Path("/{object:.+}").HeadersRegexp("X-Amz-Copy-Source", ".*?(\\/|%2F).*?").HandlerFunc(httpTracelogAll(api.CopyObjectHandler))
	// PutObject
	bucket.Methods("PUT").Path("/{object:.+}").HandlerFunc(httpTracelogHeaders(api.PutObjectHandler))
	// DeleteObject
	bucket.Methods("DELETE").Path("/{object:.+}").HandlerFunc(httpTracelogAll(api.DeleteObjectHandler))

	/// Bucket operations

	// GetBucketLocation
	bucket.Methods("GET").HandlerFunc(httpTracelogAll(api.GetBucketLocationHandler)).Queries("location", "")
	// GetBucketPolicy
	bucket.Methods("GET").HandlerFunc(httpTracelogAll(api.GetBucketPolicyHandler)).Queries("policy", "")
	// GetBucketNotification
	bucket.Methods("GET").HandlerFunc(httpTracelogAll(api.GetBucketNotificationHandler)).Queries("notification", "")
	// ListenBucketNotification
	bucket.Methods("GET").HandlerFunc(httpTracelogAll(api.ListenBucketNotificationHandler)).Queries("events", "{events:.*}")
	// ListMultipartUploads
	bucket.Methods("GET").HandlerFunc(httpTracelogAll(api.ListMultipartUploadsHandler)).Queries("uploads", "")
	// ListObjectsV2
	bucket.Methods("GET").HandlerFunc(httpTracelogAll(api.ListObjectsV2Handler)).Queries("list-type", "2")
	// ListObjectsV1 (Legacy)
	bucket.Methods("GET").HandlerFunc(httpTracelogAll(api.ListObjectsV1Handler))
	// PutBucketPolicy
	bucket.Methods("PUT").HandlerFunc(httpTracelogAll(api.PutBucketPolicyHandler)).Queries("policy", "")
	// PutBucketNotification
	bucket.Methods("PUT").HandlerFunc(httpTracelogAll(api.PutBucketNotificationHandler)).Queries("notification", "")
	// PutBucket
	bucket.Methods("PUT").HandlerFunc(httpTracelogAll(api.PutBucketHandler))
	// HeadBucket
	bucket.Methods("HEAD").HandlerFunc(httpTracelogAll(api.HeadBucketHandler))
	// PostPolicy
	bucket.Methods("POST").HeadersRegexp("Content-Type", "multipart/form-data*").HandlerFunc(httpTracelogAll(api.PostPolicyBucketHandler))
	// DeleteMultipleObjects
	bucket.Methods("POST").HandlerFunc(httpTracelogAll(api.DeleteMultipleObjectsHandler))
	// DeleteBucketPolicy
	bucket.Methods("DELETE").HandlerFunc(httpTracelogAll(api.DeleteBucketPolicyHandler)).Queries("policy", "")
	// DeleteBucket
	bucket.Methods("DELETE").HandlerFunc(httpTracelogAll(api.DeleteBucketHandler))

	/// Root operation

	// ListBuckets
	apiRouter.Methods("GET").HandlerFunc(httpTracelogAll(api.ListBucketsHandler))
}

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
	bucket.Methods("HEAD").Path("/{object:.+}").HandlerFunc(api.HeadObjectHandler)
	// PutObjectPart
	bucket.Methods("PUT").Path("/{object:.+}").HandlerFunc(api.PutObjectPartHandler).Queries("partNumber", "{partNumber:[0-9]+}", "uploadId", "{uploadId:.*}")
	// ListObjectPxarts
	bucket.Methods("GET").Path("/{object:.+}").HandlerFunc(api.ListObjectPartsHandler).Queries("uploadId", "{uploadId:.*}")
	// CompleteMultipartUpload
	bucket.Methods("POST").Path("/{object:.+}").HandlerFunc(api.CompleteMultipartUploadHandler).Queries("uploadId", "{uploadId:.*}")
	// NewMultipartUpload
	bucket.Methods("POST").Path("/{object:.+}").HandlerFunc(api.NewMultipartUploadHandler).Queries("uploads", "")
	// AbortMultipartUpload
	bucket.Methods("DELETE").Path("/{object:.+}").HandlerFunc(api.AbortMultipartUploadHandler).Queries("uploadId", "{uploadId:.*}")
	// GetObject
	bucket.Methods("GET").Path("/{object:.+}").HandlerFunc(api.GetObjectHandler)
	// CopyObject
	bucket.Methods("PUT").Path("/{object:.+}").HeadersRegexp("X-Amz-Copy-Source", ".*?(\\/|%2F).*?").HandlerFunc(api.CopyObjectHandler)
	// PutObject
	bucket.Methods("PUT").Path("/{object:.+}").HandlerFunc(api.PutObjectHandler)
	// DeleteObject
	bucket.Methods("DELETE").Path("/{object:.+}").HandlerFunc(api.DeleteObjectHandler)

	/// Bucket operations

	// GetBucketLocation
	bucket.Methods("GET").HandlerFunc(api.GetBucketLocationHandler).Queries("location", "")
	// GetBucketPolicy
	bucket.Methods("GET").HandlerFunc(api.GetBucketPolicyHandler).Queries("policy", "")
	// GetBucketNotification
	bucket.Methods("GET").HandlerFunc(api.GetBucketNotificationHandler).Queries("notification", "")
	// ListenBucketNotification
	bucket.Methods("GET").HandlerFunc(api.ListenBucketNotificationHandler).Queries("events", "{events:.*}")
	// ListMultipartUploads
	bucket.Methods("GET").HandlerFunc(api.ListMultipartUploadsHandler).Queries("uploads", "")
	// ListObjectsV2
	bucket.Methods("GET").HandlerFunc(api.ListObjectsV2Handler).Queries("list-type", "2")
	// ListObjectsV1 (Legacy)
	bucket.Methods("GET").HandlerFunc(api.ListObjectsV1Handler)
	// PutBucketPolicy
	bucket.Methods("PUT").HandlerFunc(api.PutBucketPolicyHandler).Queries("policy", "")
	// PutBucketNotification
	bucket.Methods("PUT").HandlerFunc(api.PutBucketNotificationHandler).Queries("notification", "")
	// PutBucket
	bucket.Methods("PUT").HandlerFunc(api.PutBucketHandler)
	// HeadBucket
	bucket.Methods("HEAD").HandlerFunc(api.HeadBucketHandler)
	// PostPolicy
	bucket.Methods("POST").HeadersRegexp("Content-Type", "multipart/form-data*").HandlerFunc(api.PostPolicyBucketHandler)
	// DeleteMultipleObjects
	bucket.Methods("POST").HandlerFunc(api.DeleteMultipleObjectsHandler)
	// DeleteBucketPolicy
	bucket.Methods("DELETE").HandlerFunc(api.DeleteBucketPolicyHandler).Queries("policy", "")
	// DeleteBucket
	bucket.Methods("DELETE").HandlerFunc(api.DeleteBucketHandler)

	/// Root operation

	// ListBuckets
	apiRouter.Methods("GET").HandlerFunc(api.ListBucketsHandler)
}

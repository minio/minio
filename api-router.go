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

package main

import (
	router "github.com/gorilla/mux"
	"github.com/minio/minio/pkg/fs"
	"github.com/minio/minio/pkg/s3/signature4"
)

// storageAPI container for S3 compatible API.
type storageAPI struct {
	// Once true log all incoming requests.
	AccessLog bool
	// Filesystem instance.
	Filesystem fs.Filesystem
	// Signature instance.
	Signature *signature4.Sign
	// Region instance.
	Region string
}

// initAPI instantiate a new StorageAPI.
func initAPI(conf cloudServerConfig) storageAPI {
	fs, err := fs.New(conf.Path, conf.MinFreeDisk)
	fatalIf(err.Trace(), "Initializing filesystem failed.", nil)

	sign, err := signature4.New(conf.AccessKeyID, conf.SecretAccessKey, conf.Region)
	fatalIf(err.Trace(conf.AccessKeyID, conf.SecretAccessKey, conf.Region), "Initializing signature version '4' failed.", nil)

	return storageAPI{
		AccessLog:  conf.AccessLog,
		Filesystem: fs,
		Signature:  sign,
		Region:     conf.Region,
	}
}

// registerAPIRouter - registers S3 compatible APIs.
func registerAPIRouter(mux *router.Router, api storageAPI) {
	// API Router
	apiRouter := mux.NewRoute().PathPrefix("/").Subrouter()

	// Bucket router
	bucket := apiRouter.PathPrefix("/{bucket}").Subrouter()

	/// Object operations

	// HeadObject
	bucket.Methods("HEAD").Path("/{object:.+}").HandlerFunc(api.HeadObjectHandler)
	// PutObjecPart
	bucket.Methods("PUT").Path("/{object:.+}").HandlerFunc(api.PutObjectPartHandler).Queries("partNumber", "{partNumber:[0-9]+}", "uploadId", "{uploadId:.*}")
	// ListObjectParts
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
	bucket.Methods("PUT").Path("/{object:.+}").HeadersRegexp("X-Amz-Copy-Source", ".*?(\\/).*?").HandlerFunc(api.CopyObjectHandler)
	// PutObject
	bucket.Methods("PUT").Path("/{object:.+}").HandlerFunc(api.PutObjectHandler)
	// DeleteObject
	bucket.Methods("DELETE").Path("/{object:.+}").HandlerFunc(api.DeleteObjectHandler)

	/// Bucket operations

	// GetBucketLocation
	bucket.Methods("GET").HandlerFunc(api.GetBucketLocationHandler).Queries("location", "")
	// GetBucketACL
	bucket.Methods("GET").HandlerFunc(api.GetBucketACLHandler).Queries("acl", "")
	// ListMultipartUploads
	bucket.Methods("GET").HandlerFunc(api.ListMultipartUploadsHandler).Queries("uploads", "")
	// ListObjects
	bucket.Methods("GET").HandlerFunc(api.ListObjectsHandler)
	// PutBucketACL
	bucket.Methods("PUT").HandlerFunc(api.PutBucketACLHandler).Queries("acl", "")
	// PutBucket
	bucket.Methods("PUT").HandlerFunc(api.PutBucketHandler)
	// HeadBucket
	bucket.Methods("HEAD").HandlerFunc(api.HeadBucketHandler)
	// PostPolicyBucket
	bucket.Methods("POST").HeadersRegexp("Content-Type", "multipart/form-data").HandlerFunc(api.PostPolicyBucketHandler)
	// DeleteBucket
	bucket.Methods("DELETE").HandlerFunc(api.DeleteBucketHandler)

	/// Root operation

	// ListBuckets
	apiRouter.Methods("GET").HandlerFunc(api.ListBucketsHandler)
}

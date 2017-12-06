/*
 * Minio Cloud Storage, (C) 2017 Minio, Inc.
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
	"io"
	"net/http"

	router "github.com/gorilla/mux"
	"github.com/minio/minio-go/pkg/policy"
	"github.com/minio/minio/pkg/auth"
	"github.com/minio/minio/pkg/hash"
)

// GatewayMinioSysTmp prefix is used in Azure/GCS gateway for save metadata sent by Initialize Multipart Upload API.
const GatewayMinioSysTmp = "minio.sys.tmp/"

// Gateway represents a gateway backend.
type Gateway interface {
	// Name returns the unique name of the gateway.
	Name() string

	// NewGatewayLayer returns a new gateway layer.
	NewGatewayLayer(creds auth.Credentials) (GatewayLayer, error)

	// Returns true if gateway is ready for production.
	Production() bool
}

// GatewayLayer - interface to implement gateway mode.
type GatewayLayer interface {
	ObjectLayer

	AnonGetObject(bucket, object string, startOffset int64, length int64, writer io.Writer) (err error)
	AnonGetObjectInfo(bucket, object string) (objInfo ObjectInfo, err error)

	AnonPutObject(bucket string, object string, data *hash.Reader, metadata map[string]string) (ObjectInfo, error)

	SetBucketPolicies(string, policy.BucketAccessPolicy) error
	GetBucketPolicies(string) (policy.BucketAccessPolicy, error)
	DeleteBucketPolicies(string) error
	AnonListObjects(bucket, prefix, marker, delimiter string, maxKeys int) (result ListObjectsInfo, err error)
	AnonListObjectsV2(bucket, prefix, continuationToken, delimiter string, maxKeys int, fetchOwner bool, startAfter string) (result ListObjectsV2Info, err error)
	ListObjectsV2(bucket, prefix, continuationToken, delimiter string, maxKeys int, fetchOwner bool, startAfter string) (result ListObjectsV2Info, err error)
	AnonGetBucketInfo(bucket string) (bucketInfo BucketInfo, err error)
}

// Implements and provides http handlers for S3 API.
// Overrides GetObject HeadObject and Policy related handlers.
type gatewayAPIHandlers struct {
	objectAPIHandlers
	ObjectAPI func() GatewayLayer
}

// registerAPIRouter - registers S3 compatible APIs.
func registerGatewayAPIRouter(mux *router.Router, gw GatewayLayer) {
	// Initialize API.
	api := gatewayAPIHandlers{
		ObjectAPI: func() GatewayLayer { return gw },
		objectAPIHandlers: objectAPIHandlers{
			ObjectAPI: newObjectLayerFn,
		},
	}

	// API Router
	apiRouter := mux.NewRoute().PathPrefix("/").Subrouter()

	var routers []*router.Router
	if globalDomainName != "" {
		routers = append(routers, apiRouter.Host("{bucket:.+}."+globalDomainName).Subrouter())
	}
	routers = append(routers, apiRouter.PathPrefix("/{bucket}").Subrouter())

	// Object operations
	for _, bucket := range routers {
		/// Object operations

		// HeadObject
		bucket.Methods("HEAD").Path("/{object:.+}").HandlerFunc(httpTraceAll(api.HeadObjectHandler))
		// CopyObjectPart
		bucket.Methods("PUT").Path("/{object:.+}").HeadersRegexp("X-Amz-Copy-Source", ".*?(\\/|%2F).*?").HandlerFunc(httpTraceAll(api.CopyObjectPartHandler)).Queries("partNumber", "{partNumber:[0-9]+}", "uploadId", "{uploadId:.*}")
		// PutObjectPart
		bucket.Methods("PUT").Path("/{object:.+}").HandlerFunc(httpTraceHdrs(api.PutObjectPartHandler)).Queries("partNumber", "{partNumber:[0-9]+}", "uploadId", "{uploadId:.*}")
		// ListObjectPxarts
		bucket.Methods("GET").Path("/{object:.+}").HandlerFunc(httpTraceAll(api.ListObjectPartsHandler)).Queries("uploadId", "{uploadId:.*}")
		// CompleteMultipartUpload
		bucket.Methods("POST").Path("/{object:.+}").HandlerFunc(httpTraceAll(api.CompleteMultipartUploadHandler)).Queries("uploadId", "{uploadId:.*}")
		// NewMultipartUpload
		bucket.Methods("POST").Path("/{object:.+}").HandlerFunc(httpTraceAll(api.NewMultipartUploadHandler)).Queries("uploads", "")
		// AbortMultipartUpload
		bucket.Methods("DELETE").Path("/{object:.+}").HandlerFunc(httpTraceAll(api.AbortMultipartUploadHandler)).Queries("uploadId", "{uploadId:.*}")
		// GetObject
		bucket.Methods("GET").Path("/{object:.+}").HandlerFunc(httpTraceHdrs(api.GetObjectHandler))
		// CopyObject
		bucket.Methods("PUT").Path("/{object:.+}").HeadersRegexp("X-Amz-Copy-Source", ".*?(\\/|%2F).*?").HandlerFunc(httpTraceAll(api.CopyObjectHandler))
		// PutObject
		bucket.Methods("PUT").Path("/{object:.+}").HandlerFunc(httpTraceHdrs(api.PutObjectHandler))
		// DeleteObject
		bucket.Methods("DELETE").Path("/{object:.+}").HandlerFunc(httpTraceAll(api.DeleteObjectHandler))

		/// Bucket operations

		// GetBucketLocation
		bucket.Methods("GET").HandlerFunc(httpTraceAll(api.GetBucketLocationHandler)).Queries("location", "")
		// GetBucketPolicy
		bucket.Methods("GET").HandlerFunc(httpTraceAll(api.GetBucketPolicyHandler)).Queries("policy", "")
		// GetBucketNotification
		bucket.Methods("GET").HandlerFunc(httpTraceAll(api.GetBucketNotificationHandler)).Queries("notification", "")
		// ListenBucketNotification
		bucket.Methods("GET").HandlerFunc(httpTraceAll(api.ListenBucketNotificationHandler)).Queries("events", "{events:.*}")
		// ListMultipartUploads
		bucket.Methods("GET").HandlerFunc(httpTraceAll(api.ListMultipartUploadsHandler)).Queries("uploads", "")
		// ListObjectsV2
		bucket.Methods("GET").HandlerFunc(httpTraceAll(api.ListObjectsV2Handler)).Queries("list-type", "2")
		// ListObjectsV1 (Legacy)
		bucket.Methods("GET").HandlerFunc(httpTraceAll(api.ListObjectsV1Handler))
		// PutBucketPolicy
		bucket.Methods("PUT").HandlerFunc(httpTraceAll(api.PutBucketPolicyHandler)).Queries("policy", "")
		// PutBucketNotification
		bucket.Methods("PUT").HandlerFunc(httpTraceAll(api.PutBucketNotificationHandler)).Queries("notification", "")
		// PutBucket
		bucket.Methods("PUT").HandlerFunc(httpTraceAll(api.PutBucketHandler))
		// HeadBucket
		bucket.Methods("HEAD").HandlerFunc(httpTraceAll(api.HeadBucketHandler))
		// PostPolicy
		bucket.Methods("POST").HeadersRegexp("Content-Type", "multipart/form-data*").HandlerFunc(httpTraceAll(api.PostPolicyBucketHandler))
		// DeleteMultipleObjects
		bucket.Methods("POST").HandlerFunc(httpTraceAll(api.DeleteMultipleObjectsHandler)).Queries("delete", "")
		// DeleteBucketPolicy
		bucket.Methods("DELETE").HandlerFunc(httpTraceAll(api.DeleteBucketPolicyHandler)).Queries("policy", "")
		// DeleteBucket
		bucket.Methods("DELETE").HandlerFunc(httpTraceAll(api.DeleteBucketHandler))
	}

	/// Root operation

	// ListBuckets
	apiRouter.Methods("GET").Path("/").HandlerFunc(httpTraceAll(api.ListBucketsHandler))

	// If none of the routes match.
	apiRouter.NotFoundHandler = http.HandlerFunc(httpTraceAll(notFoundHandler))
}

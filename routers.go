/*
 * Minio Cloud Storage, (C) 2015 Minio, Inc.
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
	"net/http"

	router "github.com/gorilla/mux"
	"github.com/minio/minio/pkg/fs"
)

// CloudStorageAPI container for S3 compatible API.
type CloudStorageAPI struct {
	Filesystem fs.Filesystem
	Anonymous  bool // do not checking for incoming signatures, allow all requests
	AccessLog  bool // if true log all incoming request
}

// WebAPI container for Web API.
type WebAPI struct {
	Anonymous bool
	AccessLog bool
}

// registerWebAPI - register all the handlers to their respective paths
func registerWebAPI(mux *router.Router, w WebAPI) {
	// root Router
	root := mux.NewRoute().PathPrefix("/").Subrouter()
	root.Methods("POST").Path("/login").HandlerFunc(w.LoginHandler)
	root.Methods("GET").Path("/logout").HandlerFunc(w.LogoutHandler)
	root.Methods("GET").Path("/login-refresh-token").HandlerFunc(w.RefreshTokenHandler)
}

func getWebAPIHandler(web WebAPI) http.Handler {
	var mwHandlers = []MiddlewareHandler{
		CorsHandler, // CORS added only for testing purposes.
	}
	if !web.Anonymous {
		mwHandlers = append(mwHandlers, AuthHandler)
	}
	if web.AccessLog {
		mwHandlers = append(mwHandlers, AccessLogHandler)
	}
	mux := router.NewRouter()
	registerWebAPI(mux, web)
	return registerCustomMiddleware(mux, mwHandlers...)
}

// registerCloudStorageAPI - register all the handlers to their respective paths
func registerCloudStorageAPI(mux *router.Router, a CloudStorageAPI) {
	// root Router
	root := mux.NewRoute().PathPrefix("/").Subrouter()
	// Bucket router
	bucket := root.PathPrefix("/{bucket}").Subrouter()

	// Object operations
	bucket.Methods("HEAD").Path("/{object:.+}").HandlerFunc(a.HeadObjectHandler)
	bucket.Methods("PUT").Path("/{object:.+}").HandlerFunc(a.PutObjectPartHandler).Queries("partNumber", "{partNumber:[0-9]+}", "uploadId", "{uploadId:.*}")
	bucket.Methods("GET").Path("/{object:.+}").HandlerFunc(a.ListObjectPartsHandler).Queries("uploadId", "{uploadId:.*}")
	bucket.Methods("POST").Path("/{object:.+}").HandlerFunc(a.CompleteMultipartUploadHandler).Queries("uploadId", "{uploadId:.*}")
	bucket.Methods("POST").Path("/{object:.+}").HandlerFunc(a.NewMultipartUploadHandler).Queries("uploads", "")
	bucket.Methods("DELETE").Path("/{object:.+}").HandlerFunc(a.AbortMultipartUploadHandler).Queries("uploadId", "{uploadId:.*}")
	bucket.Methods("GET").Path("/{object:.+}").HandlerFunc(a.GetObjectHandler)
	bucket.Methods("PUT").Path("/{object:.+}").HandlerFunc(a.PutObjectHandler)
	bucket.Methods("DELETE").Path("/{object:.+}").HandlerFunc(a.DeleteObjectHandler)

	// Bucket operations
	bucket.Methods("GET").HandlerFunc(a.GetBucketLocationHandler).Queries("location", "")
	bucket.Methods("GET").HandlerFunc(a.GetBucketACLHandler).Queries("acl", "")
	bucket.Methods("GET").HandlerFunc(a.ListMultipartUploadsHandler).Queries("uploads", "")
	bucket.Methods("GET").HandlerFunc(a.ListObjectsHandler)
	bucket.Methods("PUT").HandlerFunc(a.PutBucketACLHandler).Queries("acl", "")
	bucket.Methods("PUT").HandlerFunc(a.PutBucketHandler)
	bucket.Methods("HEAD").HandlerFunc(a.HeadBucketHandler)
	bucket.Methods("POST").HandlerFunc(a.PostPolicyBucketHandler)
	bucket.Methods("DELETE").HandlerFunc(a.DeleteBucketHandler)

	// Root operation
	root.Methods("GET").HandlerFunc(a.ListBucketsHandler)
}

// getNewWebAPI instantiate a new WebAPI.
func getNewWebAPI(conf cloudServerConfig) WebAPI {
	return WebAPI{
		Anonymous: conf.Anonymous,
		AccessLog: conf.AccessLog,
	}
}

// getNewCloudStorageAPI instantiate a new CloudStorageAPI.
func getNewCloudStorageAPI(conf cloudServerConfig) CloudStorageAPI {
	fs, err := fs.New(conf.Path)
	fatalIf(err.Trace(), "Initializing filesystem failed.", nil)

	fs.SetMinFreeDisk(conf.MinFreeDisk)
	if conf.Expiry > 0 {
		go fs.AutoExpiryThread(conf.Expiry)
	}
	return CloudStorageAPI{
		Filesystem: fs,
		Anonymous:  conf.Anonymous,
		AccessLog:  conf.AccessLog,
	}
}

func getCloudStorageAPIHandler(api CloudStorageAPI) http.Handler {
	var mwHandlers = []MiddlewareHandler{
		CorsHandler,
		TimeValidityHandler,
		IgnoreResourcesHandler,
		IgnoreSignatureV2RequestHandler,
	}
	if !api.Anonymous {
		mwHandlers = append(mwHandlers, SignatureHandler)
	}
	if api.AccessLog {
		mwHandlers = append(mwHandlers, AccessLogHandler)
	}
	mux := router.NewRouter()
	registerCloudStorageAPI(mux, api)
	return registerCustomMiddleware(mux, mwHandlers...)
}

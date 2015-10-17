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

// registerAPI - register all the object API handlers to their respective paths
func registerAPI(mux *router.Router, a API) {
	mux.HandleFunc("/", a.ListBucketsHandler).Methods("GET")
	mux.HandleFunc("/{bucket}", a.GetBucketACLHandler).Queries("acl", "").Methods("GET")
	mux.HandleFunc("/{bucket}", a.ListMultipartUploadsHandler).Queries("uploads", "").Methods("GET")
	mux.HandleFunc("/{bucket}", a.ListObjectsHandler).Methods("GET")
	mux.HandleFunc("/{bucket}", a.PutBucketACLHandler).Queries("acl", "").Methods("PUT")
	mux.HandleFunc("/{bucket}", a.PutBucketHandler).Methods("PUT")
	mux.HandleFunc("/{bucket}", a.HeadBucketHandler).Methods("HEAD")
	mux.HandleFunc("/{bucket}", a.PostPolicyBucketHandler).Methods("POST")
	mux.HandleFunc("/{bucket}/{object:.*}", a.HeadObjectHandler).Methods("HEAD")
	mux.HandleFunc("/{bucket}/{object:.*}", a.PutObjectPartHandler).Queries("partNumber", "{partNumber:[0-9]+}", "uploadId", "{uploadId:.*}").Methods("PUT")
	mux.HandleFunc("/{bucket}/{object:.*}", a.ListObjectPartsHandler).Queries("uploadId", "{uploadId:.*}").Methods("GET")
	mux.HandleFunc("/{bucket}/{object:.*}", a.CompleteMultipartUploadHandler).Queries("uploadId", "{uploadId:.*}").Methods("POST")
	mux.HandleFunc("/{bucket}/{object:.*}", a.NewMultipartUploadHandler).Queries("uploads", "").Methods("POST")
	mux.HandleFunc("/{bucket}/{object:.*}", a.AbortMultipartUploadHandler).Queries("uploadId", "{uploadId:.*}").Methods("DELETE")
	mux.HandleFunc("/{bucket}/{object:.*}", a.GetObjectHandler).Methods("GET")
	mux.HandleFunc("/{bucket}/{object:.*}", a.PutObjectHandler).Methods("PUT")

	mux.HandleFunc("/{bucket}", a.DeleteBucketHandler).Methods("DELETE")
	mux.HandleFunc("/{bucket}/{object:.*}", a.DeleteObjectHandler).Methods("DELETE")
}

// API container for API and also carries OP (operation) channel
type API struct {
	Filesystem fs.CloudStorage
	Anonymous  bool // do not checking for incoming signatures, allow all requests
}

// getNewAPI instantiate a new minio API
func getNewAPI(path string, anonymous bool) API {
	// ignore errors for now
	fs, err := fs.New(path)
	fatalIf(err.Trace(), "Instantiating filesystem failed.", nil)

	return API{
		Filesystem: fs,
		Anonymous:  anonymous,
	}
}

func getAPIHandler(anonymous bool, api API) http.Handler {
	var mwHandlers = []MiddlewareHandler{
		TimeValidityHandler,
		IgnoreResourcesHandler,
		CorsHandler,
	}
	if !anonymous {
		mwHandlers = append(mwHandlers, SignatureHandler)
	}
	mux := router.NewRouter()
	registerAPI(mux, api)
	apiHandler := registerCustomMiddleware(mux, mwHandlers...)
	return apiHandler
}

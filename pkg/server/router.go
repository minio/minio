/*
 * Minimalist Object Storage, (C) 2014 Minio, Inc.
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

package server

import (
	"net/http"

	router "github.com/gorilla/mux"
	jsonRPC "github.com/gorilla/rpc/v2"
	"github.com/minio/minio/pkg/server/api"
	"github.com/minio/minio/pkg/server/rpc"
)

func registerAPI(mux *router.Router) http.Handler {
	api := api.MinioAPI{}

	mux.HandleFunc("/", api.ListBucketsHandler).Methods("GET")
	mux.HandleFunc("/{bucket}", api.ListObjectsHandler).Methods("GET")
	mux.HandleFunc("/{bucket}", api.PutBucketHandler).Methods("PUT")
	mux.HandleFunc("/{bucket}", api.HeadBucketHandler).Methods("HEAD")
	mux.HandleFunc("/{bucket}/{object:.*}", api.HeadObjectHandler).Methods("HEAD")
	mux.HandleFunc("/{bucket}/{object:.*}", api.PutObjectPartHandler).Queries("partNumber", "{partNumber:[0-9]+}", "uploadId", "{uploadId:.*}").Methods("PUT")
	mux.HandleFunc("/{bucket}/{object:.*}", api.ListObjectPartsHandler).Queries("uploadId", "{uploadId:.*}").Methods("GET")
	mux.HandleFunc("/{bucket}/{object:.*}", api.CompleteMultipartUploadHandler).Queries("uploadId", "{uploadId:.*}").Methods("POST")
	mux.HandleFunc("/{bucket}/{object:.*}", api.NewMultipartUploadHandler).Methods("POST")
	mux.HandleFunc("/{bucket}/{object:.*}", api.AbortMultipartUploadHandler).Queries("uploadId", "{uploadId:.*}").Methods("DELETE")
	mux.HandleFunc("/{bucket}/{object:.*}", api.GetObjectHandler).Methods("GET")
	mux.HandleFunc("/{bucket}/{object:.*}", api.PutObjectHandler).Methods("PUT")

	// not implemented yet
	mux.HandleFunc("/{bucket}", api.DeleteBucketHandler).Methods("DELETE")

	// unsupported API
	mux.HandleFunc("/{bucket}/{object:.*}", api.DeleteObjectHandler).Methods("DELETE")

	return mux
}

func registerOthers(mux http.Handler, conf api.Config) http.Handler {
	mux = api.ValidContentTypeHandler(mux)
	mux = api.TimeValidityHandler(mux)
	mux = api.IgnoreResourcesHandler(mux)
	mux = api.ValidateAuthHeaderHandler(mux)
	mux = api.RateLimitHandler(mux, conf.RateLimit)
	mux = api.LoggingHandler(mux)
	return mux
}

func registerRPC(mux *router.Router, r *jsonRPC.Server) http.Handler {
	mux.Handle("/rpc", r)
	return mux
}

// APIHandler api handler
func APIHandler(conf api.Config) http.Handler {
	mux := router.NewRouter()
	return registerOthers(registerAPI(mux), conf)
}

// RPCHandler rpc handler
func RPCHandler() http.Handler {
	return registerRPC(router.NewRouter(), rpc.HelloServiceHandler())
}

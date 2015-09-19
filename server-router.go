/*
 * Minio Cloud Storage, (C) 2014 Minio, Inc.
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
	jsonrpc "github.com/gorilla/rpc/v2"
	"github.com/gorilla/rpc/v2/json"
	"github.com/minio/minio/pkg/donut"
)

// registerAPI - register all the object API handlers to their respective paths
func registerAPI(mux *router.Router, a MinioAPI) {
	mux.HandleFunc("/", a.ListBucketsHandler).Methods("GET")
	mux.HandleFunc("/{bucket}", a.ListObjectsHandler).Methods("GET")
	mux.HandleFunc("/{bucket}", a.PutBucketHandler).Methods("PUT")
	mux.HandleFunc("/{bucket}", a.HeadBucketHandler).Methods("HEAD")
	mux.HandleFunc("/{bucket}/{object:.*}", a.HeadObjectHandler).Methods("HEAD")
	mux.HandleFunc("/{bucket}/{object:.*}", a.PutObjectPartHandler).Queries("partNumber", "{partNumber:[0-9]+}", "uploadId", "{uploadId:.*}").Methods("PUT")
	mux.HandleFunc("/{bucket}/{object:.*}", a.ListObjectPartsHandler).Queries("uploadId", "{uploadId:.*}").Methods("GET")
	mux.HandleFunc("/{bucket}/{object:.*}", a.CompleteMultipartUploadHandler).Queries("uploadId", "{uploadId:.*}").Methods("POST")
	mux.HandleFunc("/{bucket}/{object:.*}", a.NewMultipartUploadHandler).Methods("POST")
	mux.HandleFunc("/{bucket}/{object:.*}", a.AbortMultipartUploadHandler).Queries("uploadId", "{uploadId:.*}").Methods("DELETE")
	mux.HandleFunc("/{bucket}/{object:.*}", a.GetObjectHandler).Methods("GET")
	mux.HandleFunc("/{bucket}/{object:.*}", a.PutObjectHandler).Methods("PUT")

	// not implemented yet
	mux.HandleFunc("/{bucket}", a.DeleteBucketHandler).Methods("DELETE")

	// unsupported API
	mux.HandleFunc("/{bucket}/{object:.*}", a.DeleteObjectHandler).Methods("DELETE")
}

func registerCustomMiddleware(mux *router.Router, mwHandlers ...MiddlewareHandler) http.Handler {
	var f http.Handler
	f = mux
	for _, mw := range mwHandlers {
		f = mw(f)
	}
	return f
}

// APIOperation container for individual operations read by Ticket Master
type APIOperation struct {
	ProceedCh chan struct{}
}

// MinioAPI container for API and also carries OP (operation) channel
type MinioAPI struct {
	OP    chan APIOperation
	Donut donut.Interface
}

// getNewAPI instantiate a new minio API
func getNewAPI() MinioAPI {
	// ignore errors for now
	d, err := donut.New()
	fatalIf(err.Trace(), "Instantiating donut failed.", nil)

	return MinioAPI{
		OP:    make(chan APIOperation),
		Donut: d,
	}
}

// getAPIHandler api handler
func getAPIHandler() (http.Handler, MinioAPI) {
	var mwHandlers = []MiddlewareHandler{
		ValidContentTypeHandler,
		TimeValidityHandler,
		IgnoreResourcesHandler,
		ValidateAuthHeaderHandler,
		// api.LoggingHandler, // Disabled logging until we bring in external logging support
		CorsHandler,
	}

	mux := router.NewRouter()
	minioAPI := getNewAPI()
	registerAPI(mux, minioAPI)
	apiHandler := registerCustomMiddleware(mux, mwHandlers...)
	return apiHandler, minioAPI
}

func getRPCServerHandler() http.Handler {
	s := jsonrpc.NewServer()
	s.RegisterCodec(json.NewCodec(), "application/json")
	s.RegisterService(new(serverServerService), "Server")
	mux := router.NewRouter()
	mux.Handle("/rpc", s)
	return mux
}

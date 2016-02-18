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
	"net"
	"net/http"
	"path/filepath"

	router "github.com/gorilla/mux"
	jsonrpc "github.com/gorilla/rpc/v2"
	"github.com/gorilla/rpc/v2/json2"
	"github.com/minio/minio-go"
	"github.com/minio/minio/pkg/fs"
	"github.com/minio/minio/pkg/probe"
	signV4 "github.com/minio/minio/pkg/signature"
)

// storageAPI container for S3 compatible API.
type storageAPI struct {
	// Once true log all incoming requests.
	AccessLog bool
	// Filesystem instance.
	Filesystem fs.Filesystem
	// Signature instance.
	Signature *signV4.Signature
	// Region instance.
	Region string
}

// webAPI container for Web API.
type webAPI struct {
	// FSPath filesystem path.
	FSPath string
	// Once true log all incoming request.
	AccessLog bool
	// Minio client instance.
	Client minio.CloudStorageClient

	// private params.
	inSecure   bool   // Enabled if TLS is false.
	apiAddress string // api destination address.
	// accessKeys kept to be used internally.
	accessKeyID     string
	secretAccessKey string
}

// registerAPIHandlers - register all the handlers to their respective paths
func registerAPIHandlers(mux *router.Router, a storageAPI, w *webAPI) {
	// Minio rpc router
	minio := mux.NewRoute().PathPrefix(privateBucket).Subrouter()

	// Initialize json rpc handlers.
	rpc := jsonrpc.NewServer()
	codec := json2.NewCodec()
	rpc.RegisterCodec(codec, "application/json")
	rpc.RegisterCodec(codec, "application/json; charset=UTF-8")
	rpc.RegisterService(w, "Web")

	// RPC handler at URI - /minio/rpc
	minio.Path("/rpc").Handler(rpc)

	// Web handler assets at URI  - /minio/login
	minio.Path("/login").Handler(http.StripPrefix(filepath.Join(privateBucket, "login"), http.FileServer(assetFS())))
	minio.Path("/{file:.*}").Handler(http.StripPrefix(privateBucket, http.FileServer(assetFS())))

	// API Router
	api := mux.NewRoute().PathPrefix("/").Subrouter()

	// Bucket router
	bucket := api.PathPrefix("/{bucket}").Subrouter()

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
	api.Methods("GET").HandlerFunc(a.ListBucketsHandler)
}

// initWeb instantiate a new Web.
func initWeb(conf cloudServerConfig) *webAPI {
	// Split host port.
	host, port, e := net.SplitHostPort(conf.Address)
	fatalIf(probe.NewError(e), "Unable to parse web addess.", nil)

	// Default host is 'localhost', if no host present.
	if host == "" {
		host = "localhost"
	}

	// Initialize minio client for AWS Signature Version '4'
	inSecure := !conf.TLS // Insecure true when TLS is false.
	client, e := minio.NewV4(net.JoinHostPort(host, port), conf.AccessKeyID, conf.SecretAccessKey, inSecure)
	fatalIf(probe.NewError(e), "Unable to initialize minio client", nil)

	w := &webAPI{
		FSPath:          conf.Path,
		AccessLog:       conf.AccessLog,
		Client:          client,
		inSecure:        inSecure,
		apiAddress:      conf.Address,
		accessKeyID:     conf.AccessKeyID,
		secretAccessKey: conf.SecretAccessKey,
	}
	return w
}

// initAPI instantiate a new StorageAPI.
func initAPI(conf cloudServerConfig) storageAPI {
	fs, err := fs.New(conf.Path, conf.MinFreeDisk)
	fatalIf(err.Trace(), "Initializing filesystem failed.", nil)

	sign, err := signV4.New(conf.AccessKeyID, conf.SecretAccessKey, conf.Region)
	fatalIf(err.Trace(conf.AccessKeyID, conf.SecretAccessKey, conf.Region), "Initializing signature version '4' failed.", nil)

	return storageAPI{
		AccessLog:  conf.AccessLog,
		Filesystem: fs,
		Signature:  sign,
		Region:     conf.Region,
	}
}

// server handler returns final handler before initializing server.
func serverHandler(conf cloudServerConfig) http.Handler {
	// Initialize API.
	api := initAPI(conf)

	// Initialize Web.
	web := initWeb(conf)

	var handlerFns = []HandlerFunc{
		// Redirect some pre-defined browser request paths to a static
		// location prefix.
		setBrowserRedirectHandler,
		// Validates if incoming request is for restricted buckets.
		setPrivateBucketHandler,
		// Adds cache control for all browser requests.
		setBrowserCacheControlHandler,
		// Validates all incoming requests to have a valid date header.
		setTimeValidityHandler,
		// CORS setting for all browser API requests.
		setCorsHandler,
		// Validates all incoming URL resources, for invalid/unsupported
		// resources client receives a HTTP error.
		setIgnoreResourcesHandler,
		// Auth handler verifies incoming authorization headers and
		// routes them accordingly. Client receives a HTTP error for
		// invalid/unsupported signatures.
		setAuthHandler,
	}

	// Initialize router.
	mux := router.NewRouter()

	// Register all API handlers.
	registerAPIHandlers(mux, api, web)

	// Register rest of the handlers.
	return registerHandlers(mux, handlerFns...)
}

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
	"fmt"
	"net"
	"net/http"

	"github.com/elazarl/go-bindata-assetfs"
	"github.com/gorilla/handlers"
	router "github.com/gorilla/mux"
	jsonrpc "github.com/gorilla/rpc/v2"
	"github.com/gorilla/rpc/v2/json2"
	"github.com/minio/minio-go"
	"github.com/minio/minio/pkg/fs"
	"github.com/minio/minio/pkg/probe"
	"github.com/minio/minio/pkg/s3/signature4"
	"github.com/minio/miniobrowser"
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

// webAPI container for Web API.
type webAPI struct {
	// FSPath filesystem path.
	FSPath string
	// Once true log all incoming request.
	AccessLog bool
	// Minio client instance.
	Client *minio.Client

	// private params.
	apiAddress string // api destination address.
	// http or https
	insecure bool
	// accessKeys kept to be used internally.
	accessKeyID     string
	secretAccessKey string

	storageapi *storageAPI
}

// indexHandler - Handler to serve index.html
type indexHandler struct {
	handler http.Handler
}

func (h indexHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	r.URL.Path = privateBucket + "/"
	h.handler.ServeHTTP(w, r)
}

const assetPrefix = "production"

func assetFS() *assetfs.AssetFS {
	return &assetfs.AssetFS{
		Asset:     miniobrowser.Asset,
		AssetDir:  miniobrowser.AssetDir,
		AssetInfo: miniobrowser.AssetInfo,
		Prefix:    assetPrefix,
	}
}

// specialAssets are files which are unique files not embedded inside index_bundle.js.
const specialAssets = "loader.css|logo.svg|firefox.png|safari.png|chrome.png|favicon.ico"

// registerAPIHandlers - register all the handlers to their respective paths
func registerAPIHandlers(mux *router.Router, w *webAPI) {
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
	// Serve all assets.
	minio.Path(fmt.Sprintf("/{assets:[^/]+.js|%s}", specialAssets)).Handler(handlers.CompressHandler(http.StripPrefix(privateBucket, http.FileServer(assetFS()))))
	// Serve index.html for rest of the requests
	minio.Path("/{index:.*}").Handler(indexHandler{http.StripPrefix(privateBucket, http.FileServer(assetFS()))})

	// API Router
	api := mux.NewRoute().PathPrefix("/").Subrouter()

	// Bucket router
	bucket := api.PathPrefix("/{bucket}").Subrouter()

	/// Object operations

	// HeadObject
	bucket.Methods("HEAD").Path("/{object:.+}").HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		w.storageapi.HeadObjectHandler(rw, r)
	})
	// PutObjectPart
	bucket.Methods("PUT").Path("/{object:.+}").HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		w.storageapi.PutObjectPartHandler(rw, r)
	}).Queries("partNumber", "{partNumber:[0-9]+}", "uploadId", "{uploadId:.*}")
	// ListObjectPxarts
	bucket.Methods("GET").Path("/{object:.+}").HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		w.storageapi.ListObjectPartsHandler(rw, r)
	}).Queries("uploadId", "{uploadId:.*}")
	// CompleteMultipartUpload
	bucket.Methods("POST").Path("/{object:.+}").HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		w.storageapi.CompleteMultipartUploadHandler(rw, r)
	}).Queries("uploadId", "{uploadId:.*}")
	// NewMultipartUpload
	bucket.Methods("POST").Path("/{object:.+}").HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		w.storageapi.NewMultipartUploadHandler(rw, r)
	}).Queries("uploads", "")
	// AbortMultipartUpload
	bucket.Methods("DELETE").Path("/{object:.+}").HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		w.storageapi.AbortMultipartUploadHandler(rw, r)
	}).Queries("uploadId", "{uploadId:.*}")
	// GetObject
	bucket.Methods("GET").Path("/{object:.+}").HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		w.storageapi.GetObjectHandler(rw, r)
	})
	// CopyObject
	bucket.Methods("PUT").Path("/{object:.+}").HeadersRegexp("X-Amz-Copy-Source", ".*?(\\/).*?").HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		w.storageapi.CopyObjectHandler(rw, r)
	})
	// PutObject
	bucket.Methods("PUT").Path("/{object:.+}").HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		w.storageapi.PutObjectHandler(rw, r)
	})
	// DeleteObject
	bucket.Methods("DELETE").Path("/{object:.+}").HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		w.storageapi.DeleteObjectHandler(rw, r)
	})

	/// Bucket operations

	// GetBucketLocation
	bucket.Methods("GET").HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		w.storageapi.GetBucketLocationHandler(rw, r)
	}).Queries("location", "")
	// GetBucketPolicy
	bucket.Methods("GET").HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		w.storageapi.GetBucketPolicyHandler(rw, r)
	}).Queries("policy", "")
	// ListMultipartUploads
	bucket.Methods("GET").HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		w.storageapi.ListMultipartUploadsHandler(rw, r)
	}).Queries("uploads", "")
	// ListObjects
	bucket.Methods("GET").HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		w.storageapi.ListObjectsHandler(rw, r)
	})
	// PutBucketPolicy
	bucket.Methods("PUT").HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		w.storageapi.PutBucketPolicyHandler(rw, r)
	}).Queries("policy", "")
	// PutBucket
	bucket.Methods("PUT").HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		w.storageapi.PutBucketHandler(rw, r)
	})
	// HeadBucket
	bucket.Methods("HEAD").HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		w.storageapi.HeadBucketHandler(rw, r)
	})
	// DeleteMultipleObjects
	bucket.Methods("POST").HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		w.storageapi.DeleteMultipleObjectsHandler(rw, r)
	})
	// PostPolicy
	bucket.Methods("POST").Headers("Content-Type", "multipart/form-data").HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		w.storageapi.PostPolicyBucketHandler(rw, r)
	})
	// DeleteBucketPolicy
	bucket.Methods("DELETE").HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		w.storageapi.DeleteBucketPolicyHandler(rw, r)
	}).Queries("policy", "")
	// DeleteBucket
	bucket.Methods("DELETE").HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		w.storageapi.DeleteBucketHandler(rw, r)
	})

	/// Root operation

	// ListBuckets
	api.Methods("GET").HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		w.storageapi.ListBucketsHandler(rw, r)
	})
}

// initWeb instantiate a new Web.
func initWeb(conf cloudServerConfig, storageapi *storageAPI) *webAPI {
	// Split host port.
	host, port, e := net.SplitHostPort(conf.Address)
	fatalIf(probe.NewError(e), "Unable to parse web addess.", nil)

	// Default host is 'localhost', if no host present.
	if host == "" {
		host = "localhost"
	}

	// Initialize minio client for AWS Signature Version '4'
	insecure := !conf.TLS // Insecure true when TLS is false.
	client, e := minio.NewV4(net.JoinHostPort(host, port), conf.AccessKeyID, conf.SecretAccessKey, insecure)
	fatalIf(probe.NewError(e), "Unable to initialize minio client", nil)

	return &webAPI{
		FSPath:          conf.Path,
		AccessLog:       conf.AccessLog,
		Client:          client,
		apiAddress:      conf.Address,
		insecure:        insecure,
		accessKeyID:     conf.AccessKeyID,
		secretAccessKey: conf.SecretAccessKey,
		storageapi:      storageapi,
	}
}

// initAPI instantiate a new StorageAPI.
func initAPI(conf cloudServerConfig) *storageAPI {
	fs, err := fs.New(conf.Path, conf.MinFreeDisk)
	fatalIf(err.Trace(), "Initializing filesystem failed.", nil)

	sign, err := signature4.New(conf.AccessKeyID, conf.SecretAccessKey, conf.Region)
	fatalIf(err.Trace(conf.AccessKeyID, conf.SecretAccessKey, conf.Region), "Initializing signature version '4' failed.", nil)

	return &storageAPI{
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
	web := initWeb(conf, api)

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
	registerAPIHandlers(mux, web)

	// Register rest of the handlers.
	return registerHandlers(mux, handlerFns...)
}

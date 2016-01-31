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

	router "github.com/gorilla/mux"
	jsonrpc "github.com/gorilla/rpc/v2"
	"github.com/gorilla/rpc/v2/json"
	"github.com/minio/minio-go"
	"github.com/minio/minio-xl/pkg/probe"
	"github.com/minio/minio/pkg/fs"
)

// CloudStorageAPI container for S3 compatible API.
type CloudStorageAPI struct {
	// Do not check for incoming signatures, allow all requests.
	Anonymous bool
	// Once true log all incoming requests.
	AccessLog bool
	// Filesystem instance.
	Filesystem fs.Filesystem
}

// WebAPI container for Web API.
type WebAPI struct {
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

func getWebAPIHandler(web *WebAPI) http.Handler {
	var mwHandlers = []MiddlewareHandler{
		TimeValidityHandler, // Validate time.
		AuthHandler,         // Authentication handler for verifying tokens.
		CorsHandler,         // CORS added only for testing purposes.
	}
	if web.AccessLog {
		mwHandlers = append(mwHandlers, AccessLogHandler)
	}

	s := jsonrpc.NewServer()
	codec := json.NewCodec()
	s.RegisterCodec(codec, "application/json")
	s.RegisterCodec(codec, "application/json; charset=UTF-8")
	s.RegisterService(web, "Web")
	mux := router.NewRouter()
	// Root router.
	root := mux.NewRoute().PathPrefix("/").Subrouter()
	root.Handle("/rpc", s)

	// Enable this when we add assets.
	// root.PathPrefix("/login").Handler(http.StripPrefix("/login", http.FileServer(assetFS())))
	// root.Handle("/{file:.*}", http.FileServer(assetFS()))
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
func getNewWebAPI(conf cloudServerConfig) *WebAPI {
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

	web := &WebAPI{
		FSPath:          conf.Path,
		AccessLog:       conf.AccessLog,
		Client:          client,
		inSecure:        inSecure,
		apiAddress:      conf.Address,
		accessKeyID:     conf.AccessKeyID,
		secretAccessKey: conf.SecretAccessKey,
	}
	return web
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
	mwHandlers = append(mwHandlers, CorsHandler)
	mux := router.NewRouter()
	registerCloudStorageAPI(mux, api)
	return registerCustomMiddleware(mux, mwHandlers...)
}

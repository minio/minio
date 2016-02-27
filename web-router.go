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
	"fmt"
	"net"
	"net/http"
	"strings"

	"github.com/elazarl/go-bindata-assetfs"
	router "github.com/gorilla/mux"
	jsonrpc "github.com/gorilla/rpc/v2"
	"github.com/gorilla/rpc/v2/json2"
	"github.com/minio/minio-go"
	"github.com/minio/minio/pkg/probe"
	"github.com/minio/miniobrowser"
)

// Minio browser prefix
const webBrowserPrefix = restrictedBucket

// webAPI container for minio browser RPC API.
type webAPI struct {
	// FSPath filesystem path.
	FSPath string
	// Once true log all incoming request.
	AccessLog bool
	// Minio client instance.
	Client minio.CloudStorageClient

	// private params.
	apiAddress string // api destination address.
	// accessKeys kept to be used internally.
	accessKeyID     string
	secretAccessKey string
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
		apiAddress:      conf.Address,
		accessKeyID:     conf.AccessKeyID,
		secretAccessKey: conf.SecretAccessKey,
	}
	return w
}

// specialAssets are files which are unique files not embedded inside index_bundle.js.
const specialAssets = "loader.css|logo.svg|firefox.png|safari.png|chrome.png|favicon.ico"

// registerWebRouter - registers web router for serving minio browser.
func registerWebRouter(mux *router.Router, web *webAPI) {
	// Initialize a new json2 codec.
	codec := json2.NewCodec()

	// Web browser rpc router.
	webBrowserRouter := mux.NewRoute().PathPrefix(webBrowserPrefix).Subrouter()

	// Initialize web rpc handlers.
	webRPC := jsonrpc.NewServer()
	webRPC.RegisterCodec(codec, "application/json")
	webRPC.RegisterCodec(codec, "application/json; charset=UTF-8")
	webRPC.RegisterService(web, "Web")

	// RPC handler at URI - webBrowserPrefix/rpc.
	webBrowserRouter.Path("/rpc").Handler(webRPC)
	// Serve javascript files and favicon.ico from assets.
	webBrowserRouter.Path(fmt.Sprintf("/{assets:[^/]+.js|%s}", specialAssets)).Handler(http.StripPrefix(webBrowserPrefix, http.FileServer(assetFS())))
	// Serve index.html for all other requests.
	webBrowserRouter.Path("/{index:.*}").Handler(indexHandler{http.StripPrefix(webBrowserPrefix, http.FileServer(assetFS()))})
}

/// helpers

// indexHandler - Handler to serve index.html.
type indexHandler struct {
	handler http.Handler
}

// Index handler ServeHTTP wrapper.
func (h indexHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// For all incoming browser requests for index.html, serve them via
	// 'webBrowserPrefix/', also only for 'GET' method.
	if strings.Contains(r.Header.Get("User-Agent"), "Mozilla") && r.Method == "GET" {
		r.URL.Path = webBrowserPrefix + "/"
	}
	h.handler.ServeHTTP(w, r)
}

// assetPrefix path prefix for assets.
const assetPrefix = "production"

// assetFS returns back assets which implements http.Filesystem interface.
func assetFS() *assetfs.AssetFS {
	return &assetfs.AssetFS{
		Asset:     miniobrowser.Asset,
		AssetDir:  miniobrowser.AssetDir,
		AssetInfo: miniobrowser.AssetInfo,
		Prefix:    assetPrefix,
	}
}

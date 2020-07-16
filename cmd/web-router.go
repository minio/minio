/*
 * MinIO Cloud Storage, (C) 2016 MinIO, Inc.
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
	"fmt"
	"net/http"

	assetfs "github.com/elazarl/go-bindata-assetfs"
	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	jsonrpc "github.com/gorilla/rpc/v2"
	"github.com/gorilla/rpc/v2/json2"
	"github.com/minio/minio/browser"
)

// webAPI container for Web API.
type webAPIHandlers struct {
	ObjectAPI func() ObjectLayer
	CacheAPI  func() CacheObjectLayer
}

// indexHandler - Handler to serve index.html
type indexHandler struct {
	handler http.Handler
}

func (h indexHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	r.URL.Path = minioReservedBucketPath + SlashSeparator
	h.handler.ServeHTTP(w, r)
}

const assetPrefix = "production"

func assetFS() *assetfs.AssetFS {
	return &assetfs.AssetFS{
		Asset:     browser.Asset,
		AssetDir:  browser.AssetDir,
		AssetInfo: browser.AssetInfo,
		Prefix:    assetPrefix,
	}
}

// specialAssets are files which are unique files not embedded inside index_bundle.js.
const specialAssets = "index_bundle.*.js|loader.css|logo.svg|firefox.png|safari.png|chrome.png|favicon-16x16.png|favicon-32x32.png|favicon-96x96.png"

// registerWebRouter - registers web router for serving minio browser.
func registerWebRouter(router *mux.Router) error {
	// Initialize Web.
	web := &webAPIHandlers{
		ObjectAPI: newObjectLayerFn,
		CacheAPI:  newCachedObjectLayerFn,
	}

	// Initialize a new json2 codec.
	codec := json2.NewCodec()

	// MinIO browser router.
	webBrowserRouter := router.PathPrefix(minioReservedBucketPath).HeadersRegexp("User-Agent", ".*Mozilla.*").Subrouter()

	// Initialize json rpc handlers.
	webRPC := jsonrpc.NewServer()
	webRPC.RegisterCodec(codec, "application/json")
	webRPC.RegisterCodec(codec, "application/json; charset=UTF-8")

	// Register RPC handlers with server
	if err := webRPC.RegisterService(web, "Web"); err != nil {
		return err
	}

	// RPC handler at URI - /minio/webrpc
	webBrowserRouter.Methods(http.MethodPost).Path("/webrpc").Handler(webRPC)
	webBrowserRouter.Methods(http.MethodPut).Path("/upload/{bucket}/{object:.+}").HandlerFunc(httpTraceHdrs(web.Upload))

	// These methods use short-expiry tokens in the URLs. These tokens may unintentionally
	// be logged, so a new one must be generated for each request.
	webBrowserRouter.Methods(http.MethodGet).Path("/download/{bucket}/{object:.+}").Queries("token", "{token:.*}").HandlerFunc(httpTraceHdrs(web.Download))
	webBrowserRouter.Methods(http.MethodPost).Path("/zip").Queries("token", "{token:.*}").HandlerFunc(httpTraceHdrs(web.DownloadZip))

	// Create compressed assets handler
	compressAssets := handlers.CompressHandler(http.StripPrefix(minioReservedBucketPath, http.FileServer(assetFS())))

	// Serve javascript files and favicon from assets.
	webBrowserRouter.Path(fmt.Sprintf("/{assets:%s}", specialAssets)).Handler(compressAssets)

	// Serve index.html from assets for rest of the requests.
	webBrowserRouter.Path("/{index:.*}").Handler(indexHandler{compressAssets})

	return nil
}

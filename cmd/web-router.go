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

package cmd

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/elazarl/go-bindata-assetfs"
	"github.com/gorilla/handlers"
	router "github.com/gorilla/mux"
	jsonrpc "github.com/gorilla/rpc/v2"
	"github.com/gorilla/rpc/v2/json2"
	"github.com/minio/minio/browser"
)

// webAPI container for Web API.
type webAPIHandlers struct {
	ObjectAPI func() ObjectLayer
}

// indexHandler - Handler to serve index.html
type indexHandler struct {
	handler http.Handler
}

func (h indexHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	r.URL.Path = minioReservedBucketPath + "/"
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
const specialAssets = ".*index_bundle.*.js$|.*loader.css$|.*logo.svg$|.*firefox.png$|.*safari.png$|.*chrome.png$|.*favicon.ico$"

// registerWebRouter - registers web router for serving minio browser.
func registerWebRouter(mux *router.Router) error {
	// Initialize Web.
	web := &webAPIHandlers{
		ObjectAPI: newObjectLayerFn,
	}

	// Initialize a new json2 codec.
	codec := json2.NewCodec()

	// Minio browser router.
	webBrowserRouter := mux.NewRoute().PathPrefix(minioReservedBucketPath).Subrouter()

	// Initialize json rpc handlers.
	webRPC := jsonrpc.NewServer()
	webRPC.RegisterCodec(codec, "application/json")
	webRPC.RegisterCodec(codec, "application/json; charset=UTF-8")

	// Register RPC handlers with server
	if err := webRPC.RegisterService(web, "Web"); err != nil {
		return err
	}

	// RPC handler at URI - /minio/webrpc
	webBrowserRouter.Methods("POST").Path("/webrpc").Handler(webRPC)
	webBrowserRouter.Methods("PUT").Path("/upload/{bucket}/{object:.+}").HandlerFunc(web.Upload)

	// These methods use short-expiry tokens in the URLs. These tokens may unintentionally
	// be logged, so a new one must be generated for each request.
	webBrowserRouter.Methods("GET").Path("/download/{bucket}/{object:.+}").Queries("token", "{token:.*}").HandlerFunc(web.Download)
	webBrowserRouter.Methods("POST").Path("/zip").Queries("token", "{token:.*}").HandlerFunc(web.DownloadZip)

	// Add compression for assets.
	h := http.FileServer(assetFS())
	compressedAssets := handlers.CompressHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		idx := strings.LastIndex(r.URL.Path, slashSeparator)
		if idx != -1 {
			r.URL.Path = r.URL.Path[idx+1:]
		}
		h.ServeHTTP(w, r)
	}))

	// Serve javascript files and favicon from assets.
	webBrowserRouter.Path(fmt.Sprintf("/{assets:%s}", specialAssets)).Handler(compressedAssets)

	// Serve index.html for rest of the requests.
	webBrowserRouter.Path("/{index:.*}").Handler(indexHandler{http.StripPrefix(minioReservedBucketPath, h)})

	return nil
}

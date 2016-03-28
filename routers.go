/*
 * Minio Cloud Storage, (C) 2015, 2016 Minio, Inc.
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
	"os"

	router "github.com/gorilla/mux"
	"github.com/minio/minio/pkg/probe"
)

// configureServer handler returns final handler for the http server.
func configureServerHandler(srvCmdConfig serverCmdConfig) http.Handler {
	var storageHandlers StorageAPI
	var e error
	if len(srvCmdConfig.exportPaths) == 1 {
		// Verify if export path is a local file system path.
		var st os.FileInfo
		st, e = os.Stat(srvCmdConfig.exportPaths[0])
		if e == nil && st.Mode().IsDir() {
			// Initialize storage API.
			storageHandlers, e = newFS(srvCmdConfig.exportPaths[0])
			fatalIf(probe.NewError(e), "Initializing fs failed.", nil)
		} else {
			// Initialize network storage API.
			storageHandlers, e = newNetworkFS(srvCmdConfig.exportPaths[0])
			fatalIf(probe.NewError(e), "Initializing network fs failed.", nil)
		}
	} else {
		// Initialize XL storage API.
		storageHandlers, e = newXL(srvCmdConfig.exportPaths...)
		fatalIf(probe.NewError(e), "Initializing XL failed.", nil)
	}

	// Initialize object layer.
	objectAPI := newObjectLayer(storageHandlers)

	// Initialize API.
	apiHandlers := objectAPIHandlers{
		ObjectAPI: objectAPI,
	}

	// Initialize Web.
	webHandlers := &webAPIHandlers{
		ObjectAPI: objectAPI,
	}

	// Initialize router.
	mux := router.NewRouter()

	// Register all routers.
	registerStorageRPCRouter(mux, storageHandlers)
	registerWebRouter(mux, webHandlers)
	registerAPIRouter(mux, apiHandlers)
	// Add new routers here.

	// List of some generic handlers which are applied for all
	// incoming requests.
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
		// Add new handlers here.
	}

	// Register rest of the handlers.
	return registerHandlers(mux, handlerFns...)
}

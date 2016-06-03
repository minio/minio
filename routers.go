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
	"errors"
	"net/http"

	router "github.com/gorilla/mux"
)

// newObjectLayer - initialize any object layer depending on the
// number of export paths.
func newObjectLayer(exportPaths []string) (ObjectLayer, error) {
	if len(exportPaths) == 1 {
		exportPath := exportPaths[0]
		// Initialize FS object layer.
		return newFSObjects(exportPath)
	}
	// Initialize XL object layer.
	objAPI, err := newXLObjects(exportPaths)
	if err == errXLWriteQuorum {
		return objAPI, errors.New("Disks are different with last minio server run.")
	}
	return objAPI, err
}

// configureServer handler returns final handler for the http server.
func configureServerHandler(srvCmdConfig serverCmdConfig) http.Handler {
	objAPI, err := newObjectLayer(srvCmdConfig.exportPaths)
	fatalIf(err, "Unable to intialize object layer.")

	// Initialize storage rpc server.
	storageRPC, err := newRPCServer(srvCmdConfig.exportPaths[0]) // FIXME: should only have one path.
	fatalIf(err, "Unable to initialize storage RPC server.")

	// Initialize API.
	apiHandlers := objectAPIHandlers{
		ObjectAPI: objAPI,
	}

	// Initialize Web.
	webHandlers := &webAPIHandlers{
		ObjectAPI: objAPI,
	}

	// Initialize router.
	mux := router.NewRouter()

	// Register all routers.
	registerStorageRPCRouter(mux, storageRPC)
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

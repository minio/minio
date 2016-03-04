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
	"net/http"

	router "github.com/gorilla/mux"
)

// registerAllRouters - register all the routers on their respective paths.
func registerAllRouters(mux *router.Router, web *webAPI, node *nodeAPI, api storageAPI) {
	// Register node rpc router.
	registerNodeRPCRouter(mux, node)

	// Register web browser router.
	registerWebRouter(mux, web)

	// Register api router.
	registerAPIRouter(mux, api)
}

// server handler returns final handler before initializing server.
func serverHandler(conf cloudServerConfig) http.Handler {
	// Initialize Web.
	web := initWeb(conf)

	// Initialize Node.
	node := initNode(conf)

	// Initialize API.
	api := initAPI(conf)

	var handlerFns = []handlerFunc{
		// Redirect some pre-defined browser request paths to a static
		// location prefix.
		setBrowserRedirectHandler,
		// Validates if incoming request is for restricted buckets.
		setRestrictedBucketHandler,
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

	// Register all routers - web, node, api.
	registerAllRouters(mux, web, node, api)

	// Register rest of the handlers.
	return registerHandlers(mux, handlerFns...)
}

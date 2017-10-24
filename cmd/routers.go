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

package cmd

import (
	"net/http"

	router "github.com/gorilla/mux"
)

func newObjectLayerFn() (layer ObjectLayer) {
	globalObjLayerMutex.RLock()
	layer = globalObjectAPI
	globalObjLayerMutex.RUnlock()
	return
}

// Composed function registering routers for only distributed XL setup.
func registerDistXLRouters(mux *router.Router, endpoints EndpointList) error {
	// Register storage rpc router only if its a distributed setup.
	err := registerStorageRPCRouters(mux, endpoints)
	if err != nil {
		return err
	}

	// Register distributed namespace lock.
	err = registerDistNSLockRouter(mux, endpoints)
	if err != nil {
		return err
	}

	// Register S3 peer communication router.
	err = registerS3PeerRPCRouter(mux)
	if err != nil {
		return err
	}

	// Register RPC router for web related calls.
	return registerBrowserPeerRPCRouter(mux)
}

// configureServer handler returns final handler for the http server.
func configureServerHandler(endpoints EndpointList) (http.Handler, error) {
	// Initialize router. `SkipClean(true)` stops gorilla/mux from
	// normalizing URL path minio/minio#3256
	mux := router.NewRouter().SkipClean(true)

	// Initialize distributed NS lock.
	if globalIsDistXL {
		registerDistXLRouters(mux, endpoints)
	}

	// Add Admin RPC router
	err := registerAdminRPCRouter(mux)
	if err != nil {
		return nil, err
	}

	// Register web router when its enabled.
	if globalIsBrowserEnabled {
		if err := registerWebRouter(mux); err != nil {
			return nil, err
		}
	}

	// Add Admin router.
	registerAdminRouter(mux)

	// Add API router.
	registerAPIRouter(mux)

	// List of some generic handlers which are applied for all incoming requests.
	var handlerFns = []HandlerFunc{
		// Ratelimit the incoming requests using a token bucket algorithm
		setRateLimitHandler,
		// Validate all the incoming paths.
		setPathValidityHandler,
		// Network statistics
		setHTTPStatsHandler,
		// Limits all requests size to a maximum fixed limit
		setRequestSizeLimitHandler,
		// Limits all header sizes to a maximum fixed limit
		setRequestHeaderSizeLimitHandler,
		// Adds 'crossdomain.xml' policy handler to serve legacy flash clients.
		setCrossDomainPolicy,
		// Redirect some pre-defined browser request paths to a static location prefix.
		setBrowserRedirectHandler,
		// Validates if incoming request is for restricted buckets.
		setReservedBucketHandler,
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
		// filters HTTP headers which are treated as metadata and are reserved
		// for internal use only.
		filterReservedMetadata,
		// Add new handlers here.
	}

	// Register rest of the handlers.
	return registerHandlers(mux, handlerFns...), nil
}

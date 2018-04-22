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

	"github.com/gorilla/mux"
)

func newObjectLayerFn() (layer ObjectLayer) {
	globalObjLayerMutex.RLock()
	layer = globalObjectAPI
	globalObjLayerMutex.RUnlock()
	return
}

func newCacheObjectsFn() CacheObjectLayer {
	return globalCacheObjectAPI
}

// Composed function registering routers for only distributed XL setup.
func registerDistXLRouters(router *mux.Router, endpoints EndpointList) error {
	// Register storage rpc router only if its a distributed setup.
	err := registerStorageRPCRouters(router, endpoints)
	if err != nil {
		return err
	}

	// Register distributed namespace lock.
	err = registerDistNSLockRouter(router, endpoints)
	if err != nil {
		return err
	}

	// Register S3 peer communication router.
	err = registerS3PeerRPCRouter(router)
	if err != nil {
		return err
	}

	// Register RPC router for web related calls.
	return registerBrowserPeerRPCRouter(router)
}

// List of some generic handlers which are applied for all incoming requests.
var globalHandlers = []HandlerFunc{
	// set HTTP security headers such as Content-Security-Policy.
	addSecurityHeaders,
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

// configureServer handler returns final handler for the http server.
func configureServerHandler(endpoints EndpointList) (http.Handler, error) {
	// Initialize router. `SkipClean(true)` stops gorilla/mux from
	// normalizing URL path minio/minio#3256
	router := mux.NewRouter().SkipClean(true)

	// Initialize distributed NS lock.
	if globalIsDistXL {
		registerDistXLRouters(router, endpoints)
	}

	// Add Admin RPC router
	err := registerAdminRPCRouter(router)
	if err != nil {
		return nil, err
	}

	// Add Admin router.
	registerAdminRouter(router)

	// Add healthcheck router
	registerHealthCheckRouter(router)

	// Add server metrics router
	registerMetricsRouter(router)

	// Register web router when its enabled.
	if globalIsBrowserEnabled {
		if err := registerWebRouter(router); err != nil {
			return nil, err
		}
	}

	// Add API router.
	registerAPIRouter(router)

	// Register rest of the handlers.
	return registerHandlers(router, globalHandlers...), nil
}

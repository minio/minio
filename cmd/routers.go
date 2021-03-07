/*
 * MinIO Cloud Storage, (C) 2015, 2016 MinIO, Inc.
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
	"strings"

	"github.com/gorilla/mux"
)

// Composed function registering routers for only distributed Erasure setup.
func registerDistErasureRouters(router *mux.Router, endpointServerPools EndpointServerPools) {
	// Register storage REST router only if its a distributed setup.
	registerStorageRESTHandlers(router, endpointServerPools)

	// Register peer REST router only if its a distributed setup.
	registerPeerRESTHandlers(router)

	// Register bootstrap REST router for distributed setups.
	registerBootstrapRESTHandlers(router)

	// Register distributed namespace lock routers.
	registerLockRESTHandlers(router)
}

// List of some generic handlers which are applied for all incoming requests.
var globalHandlers = []mux.MiddlewareFunc{
	// filters HTTP headers which are treated as metadata and are reserved
	// for internal use only.
	filterReservedMetadata,
	// Enforce rules specific for TLS requests
	setSSETLSHandler,
	// Auth handler verifies incoming authorization headers and
	// routes them accordingly. Client receives a HTTP error for
	// invalid/unsupported signatures.
	setAuthHandler,
	// Validates all incoming URL resources, for invalid/unsupported
	// resources client receives a HTTP error.
	setIgnoreResourcesHandler,
	// Validates all incoming requests to have a valid date header.
	setTimeValidityHandler,
	// Adds cache control for all browser requests.
	setBrowserCacheControlHandler,
	// Validates if incoming request is for restricted buckets.
	setReservedBucketHandler,
	// Redirect some pre-defined browser request paths to a static location prefix.
	setBrowserRedirectHandler,
	// Adds 'crossdomain.xml' policy handler to serve legacy flash clients.
	setCrossDomainPolicy,
	// Limits all header sizes to a maximum fixed limit
	setRequestHeaderSizeLimitHandler,
	// Limits all requests size to a maximum fixed limit
	setRequestSizeLimitHandler,
	// Network statistics
	setHTTPStatsHandler,
	// Validate all the incoming requests.
	setRequestValidityHandler,
	// Forward path style requests to actual host in a bucket federated setup.
	setBucketForwardingHandler,
	// set HTTP security headers such as Content-Security-Policy.
	addSecurityHeaders,
	// set x-amz-request-id header.
	addCustomHeaders,
	// add redirect handler to redirect
	// requests when object layer is not
	// initialized.
	setRedirectHandler,
	// Add new handlers here.
}

// configureServer handler returns final handler for the http server.
func configureServerHandler(endpointServerPools EndpointServerPools) (http.Handler, error) {
	// Initialize router. `SkipClean(true)` stops gorilla/mux from
	// normalizing URL path minio/minio#3256
	router := mux.NewRouter().SkipClean(true).UseEncodedPath()

	s3router := router.MatcherFunc(func(r *http.Request, _ *mux.RouteMatch) bool {
		return !strings.HasPrefix(r.URL.Path, "/minio/")
	}).Subrouter()

	// Add API router
	registerAPIRouter(s3router)

	// Initialize distributed NS lock.
	if globalIsDistErasure {
		registerDistErasureRouters(router, endpointServerPools)
	}

	// Register web router when its enabled.
	if globalBrowserEnabled {
		if err := registerWebRouter(router); err != nil {
			return nil, err
		}
	}

	// Add Admin router, all APIs are enabled in server mode.
	registerAdminRouter(router, true, true)

	// Add healthcheck router
	registerHealthCheckRouter(router)

	// Add server metrics router
	registerMetricsRouter(router)

	// Add STS router always.
	registerSTSRouter(router)

	router.Use(globalHandlers...)

	return router, nil
}

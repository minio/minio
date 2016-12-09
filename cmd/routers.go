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

func newObjectLayerFn() ObjectLayer {
	globalObjLayerMutex.Lock()
	defer globalObjLayerMutex.Unlock()
	return globalObjectAPI
}

// newObjectLayer - initialize any object layer depending on the number of disks.
func newObjectLayer(storageDisks []StorageAPI) (ObjectLayer, error) {
	var objAPI ObjectLayer
	var err error
	if len(storageDisks) == 1 {
		// Initialize FS object layer.
		objAPI, err = newFSObjects(storageDisks[0])
	} else {
		// Initialize XL object layer.
		objAPI, err = newXLObjects(storageDisks)
	}
	if err != nil {
		return nil, err
	}

	// The following actions are performed here, so that any
	// requests coming in early in the bootup sequence don't fail
	// unexpectedly - e.g. if initEventNotifier was initialized
	// after this function completes, an event could be generated
	// before the notification system is ready, causing event
	// drops or crashes.

	// Migrate bucket policy from configDir to .minio.sys/buckets/
	err = migrateBucketPolicyConfig(objAPI)
	if err != nil {
		errorIf(err, "Unable to migrate bucket policy from config directory")
		return nil, err
	}

	err = cleanupOldBucketPolicyConfigs()
	if err != nil {
		errorIf(err, "Unable to clean up bucket policy from config directory.")
		return nil, err
	}

	// Initialize and load bucket policies.
	err = initBucketPolicies(objAPI)
	fatalIf(err, "Unable to load all bucket policies.")

	// Initialize a new event notifier.
	err = initEventNotifier(objAPI)
	fatalIf(err, "Unable to initialize event notification.")

	// Success.
	return objAPI, nil
}

// Composed function registering routers for only distributed XL setup.
func registerDistXLRouters(mux *router.Router, srvCmdConfig serverCmdConfig) error {
	// Register storage rpc router only if its a distributed setup.
	err := registerStorageRPCRouters(mux, srvCmdConfig)
	if err != nil {
		return err
	}

	// Register distributed namespace lock.
	err = registerDistNSLockRouter(mux, srvCmdConfig)
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
func configureServerHandler(srvCmdConfig serverCmdConfig) (http.Handler, error) {
	// Initialize router. `SkipClean(true)` stops gorilla/mux from
	// normalizing URL path minio/minio#3256
	mux := router.NewRouter().SkipClean(true)

	// Initialize distributed NS lock.
	if globalIsDistXL {
		registerDistXLRouters(mux, srvCmdConfig)
	}

	// Register web router when its enabled.
	if globalIsBrowserEnabled {
		if err := registerWebRouter(mux); err != nil {
			return nil, err
		}
	}

	// Add API router.
	registerAPIRouter(mux)

	// List of some generic handlers which are applied for all incoming requests.
	var handlerFns = []HandlerFunc{
		// Limits all requests size to a maximum fixed limit
		setRequestSizeLimitHandler,
		// Adds 'crossdomain.xml' policy handler to serve legacy flash clients.
		setCrossDomainPolicy,
		// Redirect some pre-defined browser request paths to a static location prefix.
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
	return registerHandlers(mux, handlerFns...), nil
}

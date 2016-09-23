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
	"os"
	"strings"

	router "github.com/gorilla/mux"
)

func newObjectLayerFn() ObjectLayer {
	objLayerMutex.Lock()
	defer objLayerMutex.Unlock()
	return globalObjectAPI
}

// newObjectLayer - initialize any object layer depending on the number of disks.
func newObjectLayer(disks, ignoredDisks []string) (ObjectLayer, error) {
	var objAPI ObjectLayer
	var err error
	if len(disks) == 1 {
		// Initialize FS object layer.
		objAPI, err = newFSObjects(disks[0])
	} else {
		// Initialize XL object layer.
		objAPI, err = newXLObjects(disks, ignoredDisks)
	}
	if err != nil {
		return nil, err
	}

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

	// Register the callback that should be called when the process shuts down.
	globalShutdownCBs.AddObjectLayerCB(func() errCode {
		if objAPI != nil {
			if sErr := objAPI.Shutdown(); sErr != nil {
				errorIf(err, "Unable to shutdown object API.")
				return exitFailure
			}
		}
		return exitSuccess
	})

	// Initialize a new event notifier.
	err = initEventNotifier(objAPI)
	fatalIf(err, "Unable to initialize event notification.")

	// Success.
	return objAPI, nil
}

// configureServer handler returns final handler for the http server.
func configureServerHandler(srvCmdConfig serverCmdConfig) http.Handler {
	// Initialize storage rpc servers for every disk that is hosted on this node.
	storageRPCs, err := newRPCServer(srvCmdConfig)
	fatalIf(err, "Unable to initialize storage RPC server.")

	// Initialize API.
	apiHandlers := objectAPIHandlers{
		ObjectAPI: newObjectLayerFn,
	}

	// Initialize Web.
	webHandlers := &webAPIHandlers{
		ObjectAPI: newObjectLayerFn,
	}

	// Initialize Controller.
	controllerHandlers := &controllerAPIHandlers{
		ObjectAPI: newObjectLayerFn,
	}

	// Initialize router.
	mux := router.NewRouter()

	// Register all routers.
	registerStorageRPCRouters(mux, storageRPCs)

	// Initialize distributed NS lock.
	initDistributedNSLock(mux, srvCmdConfig)

	// Register controller rpc router.
	registerControllerRPCRouter(mux, controllerHandlers)

	// set environmental variable MINIO_BROWSER=off to disable minio web browser.
	// By default minio web browser is enabled.
	if !strings.EqualFold(os.Getenv("MINIO_BROWSER"), "off") {
		registerWebRouter(mux, webHandlers)
	}

	// Add new routers here.
	registerAPIRouter(mux, apiHandlers)

	// List of some generic handlers which are applied for all incoming requests.
	var handlerFns = []HandlerFunc{
		// Limits the number of concurrent http requests.
		setRateLimitHandler,
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
	return registerHandlers(mux, handlerFns...)
}

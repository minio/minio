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
	"errors"
	"net/http"
	"os"
	"strings"

	router "github.com/gorilla/mux"
)

// newObjectLayer - initialize any object layer depending on the number of disks.
func newObjectLayer(disks, ignoredDisks []string) (ObjectLayer, error) {
	if len(disks) == 1 {
		exportPath := disks[0]
		// Initialize FS object layer.
		return newFSObjects(exportPath)
	}
	// Initialize XL object layer.
	objAPI, err := newXLObjects(disks, ignoredDisks)
	if err == errXLWriteQuorum {
		return objAPI, errors.New("Disks are different with last minio server run.")
	}
	return objAPI, err
}

// configureServer handler returns final handler for the http server.
func configureServerHandler(srvCmdConfig serverCmdConfig) http.Handler {
	// Initialize name space lock.
	initNSLock()

	objAPI, err := newObjectLayer(srvCmdConfig.disks, srvCmdConfig.ignoredDisks)
	fatalIf(err, "Unable to intialize object layer.")

	// Migrate bucket policy from configDir to .minio.sys/buckets/
	err = migrateBucketPolicyConfig(objAPI)
	fatalIf(err, "Unable to migrate bucket policy from config directory")

	err = cleanupOldBucketPolicyConfigs()
	fatalIf(err, "Unable to clean up bucket policy from config directory.")

	// Initialize storage rpc server.
	storageRPC, err := newRPCServer(srvCmdConfig.disks[0]) // FIXME: should only have one path.
	fatalIf(err, "Unable to initialize storage RPC server.")

	// Initialize API.
	apiHandlers := objectAPIHandlers{
		ObjectAPI: objAPI,
	}

	// Initialize Web.
	webHandlers := &webAPIHandlers{
		ObjectAPI: objAPI,
	}

	// Initialize Controller.
	ctrlHandlers := &controllerAPIHandlers{
		ObjectAPI: objAPI,
	}

	// Initialize and monitor shutdown signals.
	err = initGracefulShutdown(os.Exit)
	fatalIf(err, "Unable to initialize graceful shutdown operation")

	// Register the callback that should be called when the process shuts down.
	globalShutdownCBs.AddObjectLayerCB(func() errCode {
		if sErr := objAPI.Shutdown(); sErr != nil {
			return exitFailure
		}
		return exitSuccess
	})

	// Initialize a new event notifier.
	err = initEventNotifier(objAPI)
	fatalIf(err, "Unable to initialize event notification queue")

	// Initialize a new bucket policies.
	err = initBucketPolicies(objAPI)
	fatalIf(err, "Unable to load all bucket policies")

	// Initialize router.
	mux := router.NewRouter()

	// Register all routers.
	registerStorageRPCRouter(mux, storageRPC)

	// FIXME: till net/rpc auth is brought in "minio control" can be enabled only though
	// this env variable.
	if os.Getenv("MINIO_CONTROL") != "" {
		registerControlRPCRouter(mux, ctrlHandlers)
	}

	// set environmental variable MINIO_BROWSER=off to disable minio web browser.
	// By default minio web browser is enabled.
	if !strings.EqualFold(os.Getenv("MINIO_BROWSER"), "off") {
		registerWebRouter(mux, webHandlers)
	}

	registerAPIRouter(mux, apiHandlers)
	// Add new routers here.

	// List of some generic handlers which are applied for all
	// incoming requests.
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

/*
 * Minio Cloud Storage, (C) 2018 Minio, Inc.
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

const (
	healthCheckPath          = "/health"
	healthCheckLivenessPath  = "/live"
	healthCheckReadinessPath = "/ready"
	healthCheckPathPrefix    = minioReservedBucketPath + healthCheckPath
)

// registerHealthCheckRouter - add handler functions for liveness and readiness routes.
func registerHealthCheckRouter(router *mux.Router) {

	// Healthcheck router
	healthRouter := router.PathPrefix(healthCheckPathPrefix).Subrouter()

	// Liveness handler
	healthRouter.Methods(http.MethodGet).Path(healthCheckLivenessPath).HandlerFunc(httpTraceAll(LivenessCheckHandler))
	healthRouter.Methods(http.MethodHead).Path(healthCheckLivenessPath).HandlerFunc(httpTraceAll(LivenessCheckHandler))

	// Readiness handler
	healthRouter.Methods(http.MethodGet).Path(healthCheckReadinessPath).HandlerFunc(httpTraceAll(ReadinessCheckHandler))
	healthRouter.Methods(http.MethodHead).Path(healthCheckReadinessPath).HandlerFunc(httpTraceAll(ReadinessCheckHandler))
}

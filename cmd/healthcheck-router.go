// Copyright (c) 2015-2021 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package cmd

import (
	"net/http"

	"github.com/gorilla/mux"
)

const (
	healthCheckPath            = "/health"
	healthCheckLivenessPath    = "/live"
	healthCheckReadinessPath   = "/ready"
	healthCheckClusterPath     = "/cluster"
	healthCheckClusterReadPath = "/cluster/read"
	healthCheckPathPrefix      = minioReservedBucketPath + healthCheckPath
)

// registerHealthCheckRouter - add handler functions for liveness and readiness routes.
func registerHealthCheckRouter(router *mux.Router) {

	// Healthcheck router
	healthRouter := router.PathPrefix(healthCheckPathPrefix).Subrouter()

	// Cluster check handler to verify cluster is active
	healthRouter.Methods(http.MethodGet).Path(healthCheckClusterPath).HandlerFunc(httpTraceAll(ClusterCheckHandler))
	healthRouter.Methods(http.MethodHead).Path(healthCheckClusterPath).HandlerFunc(httpTraceAll(ClusterCheckHandler))
	healthRouter.Methods(http.MethodGet).Path(healthCheckClusterReadPath).HandlerFunc(httpTraceAll(ClusterReadCheckHandler))
	healthRouter.Methods(http.MethodHead).Path(healthCheckClusterReadPath).HandlerFunc(httpTraceAll(ClusterReadCheckHandler))

	// Liveness handler
	healthRouter.Methods(http.MethodGet).Path(healthCheckLivenessPath).HandlerFunc(httpTraceAll(LivenessCheckHandler))
	healthRouter.Methods(http.MethodHead).Path(healthCheckLivenessPath).HandlerFunc(httpTraceAll(LivenessCheckHandler))

	// Readiness handler
	healthRouter.Methods(http.MethodGet).Path(healthCheckReadinessPath).HandlerFunc(httpTraceAll(ReadinessCheckHandler))
	healthRouter.Methods(http.MethodHead).Path(healthCheckReadinessPath).HandlerFunc(httpTraceAll(ReadinessCheckHandler))
}

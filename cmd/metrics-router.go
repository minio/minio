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
	"os"
	"strings"

	"github.com/gorilla/mux"
)

const (
	prometheusMetricsPathLegacy    = "/prometheus/metrics"
	prometheusMetricsV2ClusterPath = "/v2/metrics/cluster"
	prometheusMetricsV2NodePath    = "/v2/metrics/node"
)

// Standard env prometheus auth type
const (
	EnvPrometheusAuthType = "MINIO_PROMETHEUS_AUTH_TYPE"
)

type prometheusAuthType string

const (
	prometheusJWT    prometheusAuthType = "jwt"
	prometheusPublic prometheusAuthType = "public"
)

// registerMetricsRouter - add handler functions for metrics.
func registerMetricsRouter(router *mux.Router) {
	// metrics router
	metricsRouter := router.NewRoute().PathPrefix(minioReservedBucketPath).Subrouter()
	authType := strings.ToLower(os.Getenv(EnvPrometheusAuthType))
	switch prometheusAuthType(authType) {
	case prometheusPublic:
		metricsRouter.Handle(prometheusMetricsPathLegacy, metricsHandler())
		metricsRouter.Handle(prometheusMetricsV2ClusterPath, metricsServerHandler())
		metricsRouter.Handle(prometheusMetricsV2NodePath, metricsNodeHandler())
	case prometheusJWT:
		fallthrough
	default:
		metricsRouter.Handle(prometheusMetricsPathLegacy, AuthMiddleware(metricsHandler()))
		metricsRouter.Handle(prometheusMetricsV2ClusterPath, AuthMiddleware(metricsServerHandler()))
		metricsRouter.Handle(prometheusMetricsV2NodePath, AuthMiddleware(metricsNodeHandler()))
	}
}

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
	"strings"

	"github.com/minio/mux"
	"github.com/minio/pkg/v2/env"
)

const (
	prometheusMetricsPathLegacy               = "/prometheus/metrics"
	prometheusMetricsV2ClusterPath            = "/v2/metrics/cluster"
	prometheusMetricsV2BucketPath             = "/v2/metrics/bucket"
	prometheusMetricsV2NodePath               = "/v2/metrics/node"
	prometheusMetricsV2ResourcePath           = "/v2/metrics/resource"
	prometheusMetricsV3APIPath                = "/v3/metrics/api"
	prometheusMetricsV3APIObjectPath          = "/v3/metrics/api/object"
	prometheusMetricsV3APIBucketPath          = "/v3/metrics/api/bucket"
	prometheusMetricsV3BucketReplicationPath  = "/v3/metrics/api/bucket/replication"
	prometheusMetricsV3NodePath               = "/v3/metrics/node"
	prometheusMetricsV3DebugPath              = "/v3/metrics/debug"
	prometheusMetricsV3ClusterReplicationPath = "/v3/metrics/cluster/replication"
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
	authType := strings.ToLower(env.Get(EnvPrometheusAuthType, string(prometheusJWT)))

	auth := AuthMiddleware
	if prometheusAuthType(authType) == prometheusPublic {
		auth = NoAuthMiddleware
	}
	metricsRouter.Handle(prometheusMetricsPathLegacy, auth(metricsHandler()))
	metricsRouter.Handle(prometheusMetricsV2ClusterPath, auth(metricsServerHandler()))
	metricsRouter.Handle(prometheusMetricsV2BucketPath, auth(metricsBucketHandler()))
	metricsRouter.Handle(prometheusMetricsV2NodePath, auth(metricsNodeHandler()))
	metricsRouter.Handle(prometheusMetricsV2ResourcePath, auth(metricsResourceHandler()))
	metricsRouter.Handle(prometheusMetricsV3APIPath, auth(metricsV3APIHandler()))
	metricsRouter.Handle(prometheusMetricsV3APIObjectPath, auth(metricsV3APIObjectHandler()))
	metricsRouter.Handle(prometheusMetricsV3APIBucketPath, auth(metricsV3APIBucketHandler()))
	metricsRouter.Handle(prometheusMetricsV3DebugPath, auth(metricsV3DebugHandler()))
	metricsRouter.Handle(prometheusMetricsV3BucketReplicationPath, auth(metricsV3BucketReplHandler()))
	metricsRouter.Handle(prometheusMetricsV3NodePath, auth(metricsNodeV3Handler()))
}

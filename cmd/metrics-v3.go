// Copyright (c) 2015-2024 MinIO, Inc.
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
	"slices"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

// Collector paths.
//
// These are paths under the top-level /minio/metrics/v3 metrics endpoint. Each
// of these paths returns a set of V3 metrics.
const (
	apiRequestsCollectorPath collectorPath = "/api/requests"
	apiBucketCollectorPath   collectorPath = "/api/bucket"

	systemNetworkInternodeCollectorPath collectorPath = "/system/network/internode"
	systemDriveCollectorPath            collectorPath = "/system/drive"
	systemMemoryCollectorPath           collectorPath = "/system/memory"
	systemCPUCollectorPath              collectorPath = "/system/cpu"
	systemProcessCollectorPath          collectorPath = "/system/process"
	systemGoCollectorPath               collectorPath = "/system/go"

	clusterHealthCollectorPath       collectorPath = "/cluster/health"
	clusterUsageObjectsCollectorPath collectorPath = "/cluster/usage/objects"
	clusterUsageBucketsCollectorPath collectorPath = "/cluster/usage/buckets"
	clusterErasureSetCollectorPath   collectorPath = "/cluster/erasure-set"
	clusterAuditCollectorPath        collectorPath = "/cluster/audit"
	clusterNotificationCollectorPath collectorPath = "/cluster/notification"
)

const (
	clusterBasePath = "/cluster"
)

type metricsV3Collection struct {
	mgMap       map[collectorPath]*MetricsGroup
	bucketMGMap map[collectorPath]*MetricsGroup

	// Gatherers for non-bucket MetricsGroup's
	mgGatherers map[collectorPath]prometheus.Gatherer

	collectorPaths []collectorPath
}

func newMetricGroups(r *prometheus.Registry) *metricsV3Collection {
	// Create all metric groups.
	apiRequestsMG := NewMetricsGroup(apiRequestsCollectorPath,
		[]MetricDescriptor{
			apiRejectedAuthTotalMD,
			apiRejectedHeaderTotalMD,
			apiRejectedTimestampTotalMD,
			apiRejectedInvalidTotalMD,

			apiRequestsWaitingTotalMD,
			apiRequestsIncomingTotalMD,
			apiRequestsInFlightTotalMD,
			apiRequestsTotalMD,
			apiRequestsErrorsTotalMD,
			apiRequests5xxErrorsTotalMD,
			apiRequests4xxErrorsTotalMD,
			apiRequestsCanceledTotalMD,

			apiRequestsTTFBSecondsDistributionMD,

			apiTrafficSentBytesMD,
			apiTrafficRecvBytesMD,
		},
		JoinLoaders(loadAPIRequestsHTTPMetrics, loadAPIRequestsTTFBMetrics,
			loadAPIRequestsNetworkMetrics),
	)

	apiBucketMG := NewBucketMetricsGroup(apiBucketCollectorPath,
		[]MetricDescriptor{
			apiBucketTrafficRecvBytesMD,
			apiBucketTrafficSentBytesMD,

			apiBucketRequestsInFlightMD,
			apiBucketRequestsTotalMD,
			apiBucketRequestsCanceledMD,
			apiBucketRequests4xxErrorsMD,
			apiBucketRequests5xxErrorsMD,

			apiBucketRequestsTTFBSecondsDistributionMD,
		},
		JoinBucketLoaders(loadAPIBucketHTTPMetrics, loadAPIBucketTTFBMetrics),
	)

	systemNetworkInternodeMG := NewMetricsGroup(systemNetworkInternodeCollectorPath,
		[]MetricDescriptor{
			internodeErrorsTotalMD,
			internodeDialedErrorsTotalMD,
			internodeDialAvgTimeNanosMD,
			internodeSentBytesTotalMD,
			internodeRecvBytesTotalMD,
		},
		loadNetworkInternodeMetrics,
	)

	systemMemoryMG := NewMetricsGroup(systemMemoryCollectorPath,
		[]MetricDescriptor{
			memTotalMD,
			memUsedMD,
			memFreeMD,
			memAvailableMD,
			memBuffersMD,
			memCacheMD,
			memSharedMD,
			memUsedPercMD,
		},
		loadMemoryMetrics,
	)

	systemCPUMG := NewMetricsGroup(systemCPUCollectorPath,
		[]MetricDescriptor{
			sysCPUAvgIdleMD,
			sysCPUAvgIOWaitMD,
			sysCPULoadMD,
			sysCPULoadPercMD,
			sysCPUNiceMD,
			sysCPUStealMD,
			sysCPUSystemMD,
			sysCPUUserMD,
		},
		loadCPUMetrics,
	)

	systemDriveMG := NewMetricsGroup(systemDriveCollectorPath,
		[]MetricDescriptor{
			driveUsedBytesMD,
			driveFreeBytesMD,
			driveTotalBytesMD,
			driveUsedInodesMD,
			driveFreeInodesMD,
			driveTotalInodesMD,
			driveTimeoutErrorsMD,
			driveIOErrorsMD,
			driveAvailabilityErrorsMD,
			driveWaitingIOMD,
			driveAPILatencyMD,
			driveHealingMD,
			driveOnlineMD,

			driveOfflineCountMD,
			driveOnlineCountMD,
			driveCountMD,

			// iostat related
			driveReadsPerSecMD,
			driveReadsKBPerSecMD,
			driveReadsAwaitMD,
			driveWritesPerSecMD,
			driveWritesKBPerSecMD,
			driveWritesAwaitMD,
			drivePercUtilMD,
		},
		loadDriveMetrics,
	)

	clusterHealthMG := NewMetricsGroup(clusterHealthCollectorPath,
		[]MetricDescriptor{
			healthDrivesOfflineCountMD,
			healthDrivesOnlineCountMD,
			healthDrivesCountMD,

			healthNodesOfflineCountMD,
			healthNodesOnlineCountMD,

			healthCapacityRawTotalBytesMD,
			healthCapacityRawFreeBytesMD,
			healthCapacityUsableTotalBytesMD,
			healthCapacityUsableFreeBytesMD,
		},
		JoinLoaders(loadClusterHealthDriveMetrics,
			loadClusterHealthNodeMetrics,
			loadClusterHealthCapacityMetrics),
	)

	clusterUsageObjectsMG := NewMetricsGroup(clusterUsageObjectsCollectorPath,
		[]MetricDescriptor{
			usageSinceLastUpdateSecondsMD,
			usageTotalBytesMD,
			usageObjectsCountMD,
			usageVersionsCountMD,
			usageDeleteMarkersCountMD,
			usageBucketsCountMD,
			usageObjectsDistributionMD,
			usageVersionsDistributionMD,
		},
		loadClusterUsageObjectMetrics,
	)

	clusterUsageBucketsMG := NewBucketMetricsGroup(clusterUsageBucketsCollectorPath,
		[]MetricDescriptor{
			usageSinceLastUpdateSecondsMD,
			usageBucketTotalBytesMD,
			usageBucketObjectsTotalMD,
			usageBucketVersionsCountMD,
			usageBucketDeleteMarkersCountMD,
			usageBucketQuotaTotalBytesMD,
			usageBucketObjectSizeDistributionMD,
			usageBucketObjectVersionCountDistributionMD,
		},
		loadClusterUsageBucketMetrics,
	)

	clusterErasureSetMG := NewMetricsGroup(clusterErasureSetCollectorPath,
		[]MetricDescriptor{
			erasureSetOverallWriteQuorumMD,
			erasureSetOverallHealthMD,
			erasureSetReadQuorumMD,
			erasureSetWriteQuorumMD,
			erasureSetOnlineDrivesCountMD,
			erasureSetHealingDrivesCountMD,
			erasureSetHealthMD,
		},
		loadClusterErasureSetMetrics,
	)

	clusterAuditMG := NewMetricsGroup(clusterAuditCollectorPath,
		[]MetricDescriptor{
			auditFailedMessagesMD,
			auditTargetQueueLengthMD,
			auditTotalMessagesMD,
		},
		loadClusterAuditMetrics,
	)

	clusterNotificationMG := NewMetricsGroup(clusterNotificationCollectorPath,
		[]MetricDescriptor{
			notificationCurrentSendInProgressMD,
			notificationEventsErrorsTotalMD,
			notificationEventsSentTotalMD,
			notificationEventsSkippedTotalMD,
		},
		loadClusterNotificationMetrics,
	)

	allMetricGroups := []*MetricsGroup{
		apiRequestsMG,
		apiBucketMG,

		systemNetworkInternodeMG,
		systemDriveMG,
		systemMemoryMG,
		systemCPUMG,

		clusterHealthMG,
		clusterUsageObjectsMG,
		clusterUsageBucketsMG,
		clusterErasureSetMG,
		clusterAuditMG,
		clusterNotificationMG,
	}

	// Bucket metrics are special, they always include the bucket label. These
	// metrics required a list of buckets to be passed to the loader, and the list
	// of buckets is not known until the request is made. So we keep a separate
	// map for bucket metrics and handle them specially.

	// Add the serverName and poolIndex labels to all non-cluster metrics.
	//
	// Also create metric group maps and set the cache.
	metricsCache := newMetricsCache()
	mgMap := make(map[collectorPath]*MetricsGroup)
	bucketMGMap := make(map[collectorPath]*MetricsGroup)
	for _, mg := range allMetricGroups {
		if !strings.HasPrefix(string(mg.CollectorPath), clusterBasePath) {
			mg.AddExtraLabels(
				serverName, globalLocalNodeName,
				// poolIndex, strconv.Itoa(globalLocalPoolIdx),
			)
		}
		mg.SetCache(metricsCache)
		if mg.IsBucketMetricsGroup() {
			bucketMGMap[mg.CollectorPath] = mg
		} else {
			mgMap[mg.CollectorPath] = mg
		}
	}

	// Prepare to register the collectors. Other than `MetricGroup` collectors,
	// we also have standard collectors like `ProcessCollector` and `GoCollector`.

	// Create all Non-`MetricGroup` collectors here.
	collectors := map[collectorPath]prometheus.Collector{
		systemProcessCollectorPath: collectors.NewProcessCollector(collectors.ProcessCollectorOpts{
			ReportErrors: true,
		}),
		systemGoCollectorPath: collectors.NewGoCollector(),
	}

	// Add all `MetricGroup` collectors to the map.
	for _, mg := range allMetricGroups {
		collectors[mg.CollectorPath] = mg
	}

	// Helper function to register a collector and return a gatherer for it.
	mustRegister := func(c ...prometheus.Collector) prometheus.Gatherer {
		subRegistry := prometheus.NewRegistry()
		for _, col := range c {
			subRegistry.MustRegister(col)
		}
		r.MustRegister(subRegistry)
		return subRegistry
	}

	// Register all collectors and create gatherers for them.
	gatherers := make(map[collectorPath]prometheus.Gatherer, len(collectors))
	collectorPaths := make([]collectorPath, 0, len(collectors))
	for path, collector := range collectors {
		gatherers[path] = mustRegister(collector)
		collectorPaths = append(collectorPaths, path)
	}
	slices.Sort(collectorPaths)
	return &metricsV3Collection{
		mgMap:          mgMap,
		bucketMGMap:    bucketMGMap,
		mgGatherers:    gatherers,
		collectorPaths: collectorPaths,
	}
}

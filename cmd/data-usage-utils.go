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
	"sort"
	"time"

	"github.com/minio/madmin-go/v3"
)

// BucketTargetUsageInfo - bucket target usage info provides
// - replicated size for all objects sent to this target
// - replica size for all objects received from this target
// - replication pending size for all objects pending replication to this target
// - replication failed size for all objects failed replication to this target
// - replica pending count
// - replica failed count
type BucketTargetUsageInfo struct {
	ReplicationPendingSize  uint64 `json:"objectsPendingReplicationTotalSize"`
	ReplicationFailedSize   uint64 `json:"objectsFailedReplicationTotalSize"`
	ReplicatedSize          uint64 `json:"objectsReplicatedTotalSize"`
	ReplicaSize             uint64 `json:"objectReplicaTotalSize"`
	ReplicationPendingCount uint64 `json:"objectsPendingReplicationCount"`
	ReplicationFailedCount  uint64 `json:"objectsFailedReplicationCount"`
	ReplicatedCount         uint64 `json:"objectsReplicatedCount"`
}

// BucketUsageInfo - bucket usage info provides
// - total size of the bucket
// - total objects in a bucket
// - object size histogram per bucket
type BucketUsageInfo struct {
	Size uint64 `json:"size"`
	// Following five fields suffixed with V1 are here for backward compatibility
	// Total Size for objects that have not yet been replicated
	ReplicationPendingSizeV1 uint64 `json:"objectsPendingReplicationTotalSize"`
	// Total size for objects that have witness one or more failures and will be retried
	ReplicationFailedSizeV1 uint64 `json:"objectsFailedReplicationTotalSize"`
	// Total size for objects that have been replicated to destination
	ReplicatedSizeV1 uint64 `json:"objectsReplicatedTotalSize"`
	// Total number of objects pending replication
	ReplicationPendingCountV1 uint64 `json:"objectsPendingReplicationCount"`
	// Total number of objects that failed replication
	ReplicationFailedCountV1 uint64 `json:"objectsFailedReplicationCount"`

	ObjectsCount            uint64                           `json:"objectsCount"`
	ObjectSizesHistogram    map[string]uint64                `json:"objectsSizesHistogram"`
	ObjectVersionsHistogram map[string]uint64                `json:"objectsVersionsHistogram"`
	VersionsCount           uint64                           `json:"versionsCount"`
	DeleteMarkersCount      uint64                           `json:"deleteMarkersCount"`
	ReplicaSize             uint64                           `json:"objectReplicaTotalSize"`
	ReplicaCount            uint64                           `json:"objectReplicaCount"`
	ReplicationInfo         map[string]BucketTargetUsageInfo `json:"objectsReplicationInfo"`
}

// DataUsageInfo represents data usage stats of the underlying Object API
type DataUsageInfo struct {
	TotalCapacity     uint64 `json:"capacity,omitempty"`
	TotalUsedCapacity uint64 `json:"usedCapacity,omitempty"`
	TotalFreeCapacity uint64 `json:"freeCapacity,omitempty"`

	// LastUpdate is the timestamp of when the data usage info was last updated.
	// This does not indicate a full scan.
	LastUpdate time.Time `json:"lastUpdate"`

	// Objects total count across all buckets
	ObjectsTotalCount uint64 `json:"objectsCount"`

	// Versions total count across all buckets
	VersionsTotalCount uint64 `json:"versionsCount"`

	// Delete markers total count across all buckets
	DeleteMarkersTotalCount uint64 `json:"deleteMarkersCount"`

	// Objects total size across all buckets
	ObjectsTotalSize uint64                           `json:"objectsTotalSize"`
	ReplicationInfo  map[string]BucketTargetUsageInfo `json:"objectsReplicationInfo"`

	// Total number of buckets in this cluster
	BucketsCount uint64 `json:"bucketsCount"`

	// Buckets usage info provides following information across all buckets
	// - total size of the bucket
	// - total objects in a bucket
	// - object size histogram per bucket
	BucketsUsage map[string]BucketUsageInfo `json:"bucketsUsageInfo"`
	// Deprecated kept here for backward compatibility reasons.
	BucketSizes map[string]uint64 `json:"bucketsSizes"`

	// TierStats contains per-tier stats of all configured remote tiers
	TierStats *allTierStats `json:"tierStats,omitempty"`
}

func (dui DataUsageInfo) tierStats() []madmin.TierInfo {
	if dui.TierStats == nil {
		return nil
	}

	if globalTierConfigMgr.Empty() {
		return nil
	}

	ts := make(map[string]madmin.TierStats)
	dui.TierStats.populateStats(ts)

	infos := make([]madmin.TierInfo, 0, len(ts))
	for tier, stats := range ts {
		infos = append(infos, madmin.TierInfo{
			Name:  tier,
			Type:  globalTierConfigMgr.TierType(tier),
			Stats: stats,
		})
	}

	sort.Slice(infos, func(i, j int) bool {
		if infos[i].Type == "internal" {
			return true
		}
		if infos[j].Type == "internal" {
			return false
		}
		return infos[i].Name < infos[j].Name
	})
	return infos
}

func (dui DataUsageInfo) tierMetrics() (metrics []MetricV2) {
	if dui.TierStats == nil {
		return nil
	}
	// e.g minio_cluster_ilm_transitioned_bytes{tier="S3TIER-1"}=136314880
	//     minio_cluster_ilm_transitioned_objects{tier="S3TIER-1"}=1
	//     minio_cluster_ilm_transitioned_versions{tier="S3TIER-1"}=3
	for tier, st := range dui.TierStats.Tiers {
		metrics = append(metrics, MetricV2{
			Description:    getClusterTransitionedBytesMD(),
			Value:          float64(st.TotalSize),
			VariableLabels: map[string]string{"tier": tier},
		})
		metrics = append(metrics, MetricV2{
			Description:    getClusterTransitionedObjectsMD(),
			Value:          float64(st.NumObjects),
			VariableLabels: map[string]string{"tier": tier},
		})
		metrics = append(metrics, MetricV2{
			Description:    getClusterTransitionedVersionsMD(),
			Value:          float64(st.NumVersions),
			VariableLabels: map[string]string{"tier": tier},
		})
	}

	return metrics
}

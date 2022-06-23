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
	"time"
)

//go:generate msgp -file $GOFILE

// ReplicationLatency holds information of bucket operations latency, such us uploads
type ReplicationLatency struct {
	// Single & Multipart PUTs latency
	UploadHistogram LastMinuteHistogram
}

// Merge two replication latency into a new one
func (rl ReplicationLatency) merge(other ReplicationLatency) (newReplLatency ReplicationLatency) {
	newReplLatency.UploadHistogram = rl.UploadHistogram.Merge(other.UploadHistogram)
	return
}

// Get upload latency of each object size range
func (rl ReplicationLatency) getUploadLatency() (ret map[string]uint64) {
	ret = make(map[string]uint64)
	avg := rl.UploadHistogram.GetAvgData()
	for k, v := range avg {
		// Convert nanoseconds to milliseconds
		ret[sizeTagToString(k)] = uint64(v.avg() / time.Millisecond)
	}
	return
}

// Update replication upload latency with a new value
func (rl *ReplicationLatency) update(size int64, duration time.Duration) {
	rl.UploadHistogram.Add(size, duration)
}

// BucketStatsMap captures bucket statistics for all buckets
type BucketStatsMap map[string]BucketStats

// BucketStats bucket statistics
type BucketStats struct {
	ReplicationStats BucketReplicationStats
}

// BucketReplicationStats represents inline replication statistics
// such as pending, failed and completed bytes in total for a bucket
type BucketReplicationStats struct {
	Stats map[string]*BucketReplicationStat
	// Pending size in bytes
	PendingSize int64 `json:"pendingReplicationSize"`
	// Completed size in bytes
	ReplicatedSize int64 `json:"completedReplicationSize"`
	// Total Replica size in bytes
	ReplicaSize int64 `json:"replicaSize"`
	// Failed size in bytes
	FailedSize int64 `json:"failedReplicationSize"`
	// Total number of pending operations including metadata updates
	PendingCount int64 `json:"pendingReplicationCount"`
	// Total number of failed operations including metadata updates
	FailedCount int64 `json:"failedReplicationCount"`
}

// Empty returns true if there are no target stats
func (brs *BucketReplicationStats) Empty() bool {
	return len(brs.Stats) == 0 && brs.ReplicaSize == 0
}

// Clone creates a new BucketReplicationStats copy
func (brs BucketReplicationStats) Clone() (c BucketReplicationStats) {
	// This is called only by replicationStats cache and already holds a
	// read lock before calling Clone()

	c = brs
	// We need to copy the map, so we do not reference the one in `brs`.
	c.Stats = make(map[string]*BucketReplicationStat, len(brs.Stats))
	for arn, st := range brs.Stats {
		// make a copy of `*st`
		s := *st
		c.Stats[arn] = &s
	}
	return c
}

// BucketReplicationStat represents inline replication statistics
// such as pending, failed and completed bytes in total for a bucket
// remote target
type BucketReplicationStat struct {
	// Pending size in bytes
	PendingSize int64 `json:"pendingReplicationSize"`
	// Completed size in bytes
	ReplicatedSize int64 `json:"completedReplicationSize"`
	// Total Replica size in bytes
	ReplicaSize int64 `json:"replicaSize"`
	// Failed size in bytes
	FailedSize int64 `json:"failedReplicationSize"`
	// Total number of pending operations including metadata updates
	PendingCount int64 `json:"pendingReplicationCount"`
	// Total number of failed operations including metadata updates
	FailedCount int64 `json:"failedReplicationCount"`
	// Replication latency information
	Latency ReplicationLatency `json:"replicationLatency"`
}

func (bs *BucketReplicationStat) hasReplicationUsage() bool {
	return bs.FailedSize > 0 ||
		bs.ReplicatedSize > 0 ||
		bs.ReplicaSize > 0 ||
		bs.FailedCount > 0 ||
		bs.PendingCount > 0 ||
		bs.PendingSize > 0
}

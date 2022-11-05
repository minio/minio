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
	"context"
	"encoding/binary"
	"math"
	"sync"
	"time"

	"github.com/minio/minio/internal/bucket/replication"
)

func (b *BucketReplicationStats) hasReplicationUsage() bool {
	for _, s := range b.Stats {
		if s.hasReplicationUsage() {
			return true
		}
	}
	return false
}

// ReplicationStats holds the global in-memory replication stats
type ReplicationStats struct {
	Cache             map[string]*BucketReplicationStats
	UsageCache        map[string]*BucketReplicationStats
	mostRecentStats   BucketStatsMap
	sync.RWMutex                   // mutex for Cache
	ulock             sync.RWMutex // mutex for UsageCache
	mostRecentStatsMu sync.Mutex   // mutex for mostRecentStats
}

// Delete deletes in-memory replication statistics for a bucket.
func (r *ReplicationStats) Delete(bucket string) {
	if r == nil {
		return
	}

	r.Lock()
	defer r.Unlock()
	delete(r.Cache, bucket)

	r.ulock.Lock()
	defer r.ulock.Unlock()
	delete(r.UsageCache, bucket)
}

// UpdateReplicaStat updates in-memory replica statistics with new values.
func (r *ReplicationStats) UpdateReplicaStat(bucket string, n int64) {
	if r == nil {
		return
	}

	r.Lock()
	defer r.Unlock()
	bs, ok := r.Cache[bucket]
	if !ok {
		bs = &BucketReplicationStats{Stats: make(map[string]*BucketReplicationStat)}
	}
	bs.ReplicaSize += n
	r.Cache[bucket] = bs
}

// Update updates in-memory replication statistics with new values.
func (r *ReplicationStats) Update(bucket string, arn string, n int64, duration time.Duration, status, prevStatus replication.StatusType, opType replication.Type) {
	if r == nil {
		return
	}
	r.Lock()
	defer r.Unlock()

	bs, ok := r.Cache[bucket]
	if !ok {
		bs = &BucketReplicationStats{Stats: make(map[string]*BucketReplicationStat)}
		r.Cache[bucket] = bs
	}
	b, ok := bs.Stats[arn]
	if !ok {
		b = &BucketReplicationStat{}
		bs.Stats[arn] = b
	}
	switch status {
	case replication.Pending:
		if opType.IsDataReplication() && prevStatus != status {
			b.PendingSize += n
			b.PendingCount++
		}
	case replication.Completed:
		switch prevStatus { // adjust counters based on previous state
		case replication.Pending:
			b.PendingCount--
		case replication.Failed:
			b.FailedCount--
		}
		if opType.IsDataReplication() {
			b.ReplicatedSize += n
			switch prevStatus {
			case replication.Pending:
				b.PendingSize -= n
			case replication.Failed:
				b.FailedSize -= n
			}
			if duration > 0 {
				b.Latency.update(n, duration)
			}
		}
	case replication.Failed:
		if opType.IsDataReplication() {
			if prevStatus == replication.Pending {
				b.FailedSize += n
				b.FailedCount++
				b.PendingSize -= n
				b.PendingCount--
			}
		}
	case replication.Replica:
		if opType == replication.ObjectReplicationType {
			b.ReplicaSize += n
		}
	}
}

// GetInitialUsage get replication metrics available at the time of cluster initialization
func (r *ReplicationStats) GetInitialUsage(bucket string) BucketReplicationStats {
	if r == nil {
		return BucketReplicationStats{}
	}
	r.ulock.RLock()
	defer r.ulock.RUnlock()
	st, ok := r.UsageCache[bucket]
	if !ok {
		return BucketReplicationStats{}
	}
	return st.Clone()
}

// GetAll returns replication metrics for all buckets at once.
func (r *ReplicationStats) GetAll() map[string]BucketReplicationStats {
	if r == nil {
		return map[string]BucketReplicationStats{}
	}

	r.RLock()
	defer r.RUnlock()

	bucketReplicationStats := make(map[string]BucketReplicationStats, len(r.Cache))
	for k, v := range r.Cache {
		bucketReplicationStats[k] = v.Clone()
	}

	return bucketReplicationStats
}

// Get replication metrics for a bucket from this node since this node came up.
func (r *ReplicationStats) Get(bucket string) BucketReplicationStats {
	if r == nil {
		return BucketReplicationStats{Stats: make(map[string]*BucketReplicationStat)}
	}

	r.RLock()
	defer r.RUnlock()

	st, ok := r.Cache[bucket]
	if !ok {
		return BucketReplicationStats{}
	}
	return st.Clone()
}

// NewReplicationStats initialize in-memory replication statistics
func NewReplicationStats(ctx context.Context, objectAPI ObjectLayer) *ReplicationStats {
	return &ReplicationStats{
		Cache:      make(map[string]*BucketReplicationStats),
		UsageCache: make(map[string]*BucketReplicationStats),
	}
}

// load replication metrics at cluster start from latest replication stats saved in .minio.sys/buckets/replication/node-name.stats
// fallback to replication stats in data usage to be backward compatible
func (r *ReplicationStats) loadInitialReplicationMetrics(ctx context.Context) {
	m := make(map[string]*BucketReplicationStats)
	if stats, err := globalReplicationPool.loadStatsFromDisk(); err == nil {
		for b, st := range stats {
			m[b] = &st
		}
		r.ulock.Lock()
		r.UsageCache = m
		r.ulock.Unlock()
		return
	}
	rTimer := time.NewTimer(time.Second * 5)
	defer rTimer.Stop()
	var (
		dui DataUsageInfo
		err error
	)
outer:
	for {
		select {
		case <-ctx.Done():
			return
		case <-rTimer.C:
			dui, err = loadDataUsageFromBackend(GlobalContext, newObjectLayerFn())
			// If LastUpdate is set, data usage is available.
			if err == nil {
				break outer
			}
			rTimer.Reset(time.Second * 5)
		}
	}
	for bucket, usage := range dui.BucketsUsage {
		b := &BucketReplicationStats{
			Stats: make(map[string]*BucketReplicationStat, len(usage.ReplicationInfo)),
		}
		for arn, uinfo := range usage.ReplicationInfo {
			b.Stats[arn] = &BucketReplicationStat{
				FailedSize:     int64(uinfo.ReplicationFailedSize),
				ReplicatedSize: int64(uinfo.ReplicatedSize),
				ReplicaSize:    int64(uinfo.ReplicaSize),
				FailedCount:    int64(uinfo.ReplicationFailedCount),
			}
		}
		b.ReplicaSize += int64(usage.ReplicaSize)
		if b.hasReplicationUsage() {
			m[bucket] = b
		}
	}
	r.ulock.Lock()
	r.UsageCache = m
	r.ulock.Unlock()
}

// serializeStats will serialize the current stats.
// Will return (nil, nil) if no data.
func (r *ReplicationStats) serializeStats() ([]byte, error) {
	if r == nil {
		return nil, nil
	}
	r.mostRecentStatsMu.Lock()
	defer r.mostRecentStatsMu.Unlock()
	bsm := r.mostRecentStats
	if len(bsm.Stats) == 0 {
		return nil, nil
	}
	data := make([]byte, 4, 4+bsm.Msgsize())
	// Add the replication stats meta header.
	binary.LittleEndian.PutUint16(data[0:2], replStatsMetaFormat)
	binary.LittleEndian.PutUint16(data[2:4], replStatsVersion)
	// Add data
	return r.mostRecentStats.MarshalMsg(data)
}

func (r *ReplicationStats) getAllLatest(bucketsUsage map[string]BucketUsageInfo) (bucketsReplicationStats map[string]BucketReplicationStats) {
	peerBucketStatsList := globalNotificationSys.GetClusterAllBucketStats(GlobalContext)
	bucketsReplicationStats = make(map[string]BucketReplicationStats, len(bucketsUsage))

	for bucket, u := range bucketsUsage {
		bucketStats := make([]BucketStats, len(peerBucketStatsList))
		for i, peerBucketStats := range peerBucketStatsList {
			bucketStat, ok := peerBucketStats.Stats[bucket]
			if !ok {
				continue
			}
			bucketStats[i] = bucketStat
		}
		bucketsReplicationStats[bucket] = r.calculateBucketReplicationStats(bucket, u, bucketStats)
	}
	return bucketsReplicationStats
}

func (r *ReplicationStats) calculateBucketReplicationStats(bucket string, u BucketUsageInfo, bucketStats []BucketStats) (s BucketReplicationStats) {
	if r == nil {
		s = BucketReplicationStats{
			Stats: make(map[string]*BucketReplicationStat),
		}
		return s
	}

	// accumulate cluster bucket stats
	stats := make(map[string]*BucketReplicationStat)
	var totReplicaSize int64
	for _, bucketStat := range bucketStats {
		totReplicaSize += bucketStat.ReplicationStats.ReplicaSize
		for arn, stat := range bucketStat.ReplicationStats.Stats {
			oldst := stats[arn]
			if oldst == nil {
				oldst = &BucketReplicationStat{}
			}
			stats[arn] = &BucketReplicationStat{
				FailedCount:    stat.FailedCount + oldst.FailedCount,
				FailedSize:     stat.FailedSize + oldst.FailedSize,
				ReplicatedSize: stat.ReplicatedSize + oldst.ReplicatedSize,
				Latency:        stat.Latency.merge(oldst.Latency),
				PendingCount:   stat.PendingCount + oldst.PendingCount,
				PendingSize:    stat.PendingSize + oldst.PendingSize,
			}
		}
	}

	// add initial usage stat to cluster stats
	usageStat := globalReplicationStats.GetInitialUsage(bucket)

	totReplicaSize += usageStat.ReplicaSize
	for arn, stat := range usageStat.Stats {
		st, ok := stats[arn]
		if !ok {
			st = &BucketReplicationStat{}
			stats[arn] = st
		}
		st.ReplicatedSize += stat.ReplicatedSize
		st.FailedSize += stat.FailedSize
		st.FailedCount += stat.FailedCount
		st.PendingSize += stat.PendingSize
		st.PendingCount += stat.PendingCount
	}

	s = BucketReplicationStats{
		Stats: make(map[string]*BucketReplicationStat, len(stats)),
	}
	var latestTotReplicatedSize int64
	for _, st := range u.ReplicationInfo {
		latestTotReplicatedSize += int64(st.ReplicatedSize)
	}

	// normalize computed real time stats with latest usage stat
	for arn, tgtstat := range stats {
		st := BucketReplicationStat{}
		bu, ok := u.ReplicationInfo[arn]
		if !ok {
			bu = BucketTargetUsageInfo{}
		}
		// use in memory replication stats if it is ahead of usage info.
		st.ReplicatedSize = int64(bu.ReplicatedSize)
		if tgtstat.ReplicatedSize >= int64(bu.ReplicatedSize) {
			st.ReplicatedSize = tgtstat.ReplicatedSize
		}
		s.ReplicatedSize += st.ReplicatedSize
		// Reset FailedSize and FailedCount to 0 for negative overflows which can
		// happen since data usage picture can lag behind actual usage state at the time of cluster start
		st.FailedSize = int64(math.Max(float64(tgtstat.FailedSize), 0))
		st.FailedCount = int64(math.Max(float64(tgtstat.FailedCount), 0))
		st.PendingSize = int64(math.Max(float64(tgtstat.PendingSize), 0))
		st.PendingCount = int64(math.Max(float64(tgtstat.PendingCount), 0))
		st.Latency = tgtstat.Latency

		s.Stats[arn] = &st
		s.FailedSize += st.FailedSize
		s.FailedCount += st.FailedCount
		s.PendingCount += st.PendingCount
		s.PendingSize += st.PendingSize
	}
	// normalize overall stats
	s.ReplicaSize = int64(math.Max(float64(totReplicaSize), float64(u.ReplicaSize)))
	s.ReplicatedSize = int64(math.Max(float64(s.ReplicatedSize), float64(latestTotReplicatedSize)))
	r.mostRecentStatsMu.Lock()
	if len(r.mostRecentStats.Stats) == 0 {
		r.mostRecentStats = BucketStatsMap{Stats: make(map[string]BucketStats, 1), Timestamp: UTCNow()}
	}
	r.mostRecentStats.Stats[bucket] = BucketStats{ReplicationStats: s}
	r.mostRecentStats.Timestamp = UTCNow()
	r.mostRecentStatsMu.Unlock()
	return s
}

// get the most current of in-memory replication stats  and data usage info from crawler.
func (r *ReplicationStats) getLatestReplicationStats(bucket string, u BucketUsageInfo) (s BucketReplicationStats) {
	bucketStats := globalNotificationSys.GetClusterBucketStats(GlobalContext, bucket)
	return r.calculateBucketReplicationStats(bucket, u, bucketStats)
}

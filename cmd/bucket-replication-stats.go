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
	"sync"
	"sync/atomic"
	"time"

	"github.com/minio/minio/internal/bucket/replication"
	"github.com/rcrowley/go-metrics"
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
	// map of site deployment ID to site replication status
	// for site replication - maintain stats at global level
	srStats *SRStats
	// active worker stats
	workers *ActiveWorkerStat
	// queue stats cache
	qCache queueCache

	pCache proxyStatsCache
	// mrf backlog stats
	mrfStats ReplicationMRFStats
	// for bucket replication, continue to use existing cache
	Cache             map[string]*BucketReplicationStats
	mostRecentStats   BucketStatsMap
	registry          metrics.Registry
	sync.RWMutex                 // mutex for Cache
	mostRecentStatsMu sync.Mutex // mutex for mostRecentStats

	wlock sync.RWMutex // mutex for active workers

	movingAvgTicker *time.Ticker // Ticker for calculating moving averages
	wTimer          *time.Ticker // ticker for calculating active workers
	qTimer          *time.Ticker // ticker for calculating queue stats
}

func (r *ReplicationStats) trackEWMA() {
	for {
		select {
		case <-r.movingAvgTicker.C:
			r.updateMovingAvg()
		case <-GlobalContext.Done():
			return
		}
	}
}

func (r *ReplicationStats) updateMovingAvg() {
	r.RLock()
	for _, s := range r.Cache {
		for _, st := range s.Stats {
			st.XferRateLrg.measure.updateExponentialMovingAverage(time.Now())
			st.XferRateSml.measure.updateExponentialMovingAverage(time.Now())
		}
	}
	r.RUnlock()
}

// ActiveWorkers returns worker stats
func (r *ReplicationStats) ActiveWorkers() ActiveWorkerStat {
	if r == nil {
		return ActiveWorkerStat{}
	}
	r.wlock.RLock()
	defer r.wlock.RUnlock()
	w := r.workers.get()
	return ActiveWorkerStat{
		Curr: w.Curr,
		Max:  w.Max,
		Avg:  w.Avg,
	}
}

func (r *ReplicationStats) collectWorkerMetrics(ctx context.Context) {
	if r == nil {
		return
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-r.wTimer.C:
			r.wlock.Lock()
			r.workers.update()
			r.wlock.Unlock()
		}
	}
}

func (r *ReplicationStats) collectQueueMetrics(ctx context.Context) {
	if r == nil {
		return
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-r.qTimer.C:
			r.qCache.update()
		}
	}
}

// Delete deletes in-memory replication statistics for a bucket.
func (r *ReplicationStats) Delete(bucket string) {
	if r == nil {
		return
	}

	r.Lock()
	defer r.Unlock()
	delete(r.Cache, bucket)
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
		bs = newBucketReplicationStats()
	}
	bs.ReplicaSize += n
	bs.ReplicaCount++
	r.Cache[bucket] = bs
	r.srUpdateReplicaStat(n)
}

func (r *ReplicationStats) srUpdateReplicaStat(sz int64) {
	if r == nil {
		return
	}
	atomic.AddInt64(&r.srStats.ReplicaSize, sz)
	atomic.AddInt64(&r.srStats.ReplicaCount, 1)
}

func (r *ReplicationStats) srUpdate(sr replStat) {
	dID, err := globalSiteReplicationSys.getDeplIDForEndpoint(sr.endpoint())
	if err == nil {
		r.srStats.update(sr, dID)
	}
}

// Update updates in-memory replication statistics with new values.
func (r *ReplicationStats) Update(bucket string, ri replicatedTargetInfo, status, prevStatus replication.StatusType) {
	if r == nil {
		return
	}
	var rs replStat
	switch status {
	case replication.Pending:
		if ri.OpType.IsDataReplication() && prevStatus != status {
			rs.set(ri.Arn, ri.Size, 0, status, ri.OpType, ri.endpoint, ri.secure, ri.Err)
		}
	case replication.Completed:
		if ri.OpType.IsDataReplication() {
			rs.set(ri.Arn, ri.Size, ri.Duration, status, ri.OpType, ri.endpoint, ri.secure, ri.Err)
		}
	case replication.Failed:
		if ri.OpType.IsDataReplication() && prevStatus == replication.Pending {
			rs.set(ri.Arn, ri.Size, ri.Duration, status, ri.OpType, ri.endpoint, ri.secure, ri.Err)
		}
	case replication.Replica:
		if ri.OpType == replication.ObjectReplicationType {
			rs.set(ri.Arn, ri.Size, 0, status, ri.OpType, "", false, ri.Err)
		}
	}

	// update site-replication in-memory stats
	if rs.Completed || rs.Failed {
		r.srUpdate(rs)
	}

	r.Lock()
	defer r.Unlock()

	// update bucket replication in-memory stats
	bs, ok := r.Cache[bucket]
	if !ok {
		bs = newBucketReplicationStats()
		r.Cache[bucket] = bs
	}
	b, ok := bs.Stats[ri.Arn]
	if !ok {
		b = &BucketReplicationStat{
			XferRateLrg: newXferStats(),
			XferRateSml: newXferStats(),
		}
		bs.Stats[ri.Arn] = b
	}

	switch {
	case rs.Completed:
		b.ReplicatedSize += rs.TransferSize
		b.ReplicatedCount++
		if rs.TransferDuration > 0 {
			b.Latency.update(rs.TransferSize, rs.TransferDuration)
			b.updateXferRate(rs.TransferSize, rs.TransferDuration)
		}
	case rs.Failed:
		b.FailStats.addsize(rs.TransferSize, rs.Err)
	case rs.Pending:
	}
}

type replStat struct {
	Arn       string
	Completed bool
	Pending   bool
	Failed    bool
	opType    replication.Type
	// transfer size
	TransferSize int64
	// transfer duration
	TransferDuration time.Duration
	Endpoint         string
	Secure           bool
	Err              error
}

func (rs *replStat) endpoint() string {
	scheme := "http"
	if rs.Secure {
		scheme = "https"
	}
	return scheme + "://" + rs.Endpoint
}

func (rs *replStat) set(arn string, n int64, duration time.Duration, status replication.StatusType, opType replication.Type, endpoint string, secure bool, err error) {
	rs.Endpoint = endpoint
	rs.Secure = secure
	rs.TransferSize = n
	rs.Arn = arn
	rs.TransferDuration = duration
	rs.opType = opType
	switch status {
	case replication.Completed:
		rs.Completed = true
	case replication.Pending:
		rs.Pending = true
	case replication.Failed:
		rs.Failed = true
		rs.Err = err
	}
}

// GetAll returns replication metrics for all buckets at once.
func (r *ReplicationStats) GetAll() map[string]BucketReplicationStats {
	if r == nil {
		return map[string]BucketReplicationStats{}
	}

	r.RLock()

	bucketReplicationStats := make(map[string]BucketReplicationStats, len(r.Cache))
	for k, v := range r.Cache {
		bucketReplicationStats[k] = v.Clone()
	}
	r.RUnlock()
	for k, v := range bucketReplicationStats {
		v.QStat = r.qCache.getBucketStats(k)
		bucketReplicationStats[k] = v
	}

	return bucketReplicationStats
}

func (r *ReplicationStats) getSRMetricsForNode() SRMetricsSummary {
	if r == nil {
		return SRMetricsSummary{}
	}

	m := SRMetricsSummary{
		Uptime:        UTCNow().Unix() - globalBootTime.Unix(),
		Queued:        r.qCache.getSiteStats(),
		ActiveWorkers: r.ActiveWorkers(),
		Metrics:       r.srStats.get(),
		Proxied:       r.pCache.getSiteStats(),
		ReplicaSize:   atomic.LoadInt64(&r.srStats.ReplicaSize),
		ReplicaCount:  atomic.LoadInt64(&r.srStats.ReplicaCount),
	}
	return m
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
		return BucketReplicationStats{Stats: make(map[string]*BucketReplicationStat)}
	}
	return st.Clone()
}

// NewReplicationStats initialize in-memory replication statistics
func NewReplicationStats(ctx context.Context, objectAPI ObjectLayer) *ReplicationStats {
	r := metrics.NewRegistry()
	rs := ReplicationStats{
		Cache:           make(map[string]*BucketReplicationStats),
		qCache:          newQueueCache(r),
		pCache:          newProxyStatsCache(),
		srStats:         newSRStats(),
		movingAvgTicker: time.NewTicker(2 * time.Second),
		wTimer:          time.NewTicker(2 * time.Second),
		qTimer:          time.NewTicker(2 * time.Second),

		workers:  newActiveWorkerStat(r),
		registry: r,
	}
	go rs.collectWorkerMetrics(ctx)
	go rs.collectQueueMetrics(ctx)
	return &rs
}

func (r *ReplicationStats) getAllLatest(bucketsUsage map[string]BucketUsageInfo) (bucketsReplicationStats map[string]BucketStats) {
	if r == nil {
		return nil
	}
	peerBucketStatsList := globalNotificationSys.GetClusterAllBucketStats(GlobalContext)
	bucketsReplicationStats = make(map[string]BucketStats, len(bucketsUsage))

	for bucket := range bucketsUsage {
		bucketStats := make([]BucketStats, len(peerBucketStatsList))
		for i, peerBucketStats := range peerBucketStatsList {
			bucketStat, ok := peerBucketStats.Stats[bucket]
			if !ok {
				continue
			}
			bucketStats[i] = bucketStat
		}
		bucketsReplicationStats[bucket] = r.calculateBucketReplicationStats(bucket, bucketStats)
	}
	return bucketsReplicationStats
}

func (r *ReplicationStats) calculateBucketReplicationStats(bucket string, bucketStats []BucketStats) (bs BucketStats) {
	if r == nil {
		bs = BucketStats{
			ReplicationStats: BucketReplicationStats{
				Stats: make(map[string]*BucketReplicationStat),
			},
			QueueStats: ReplicationQueueStats{},
			ProxyStats: ProxyMetric{},
		}
		return bs
	}
	var s BucketReplicationStats
	// accumulate cluster bucket stats
	stats := make(map[string]*BucketReplicationStat)
	var (
		totReplicaSize, totReplicatedSize   int64
		totReplicaCount, totReplicatedCount int64
		totFailed                           RTimedMetrics
		tq                                  InQueueMetric
	)
	for _, bucketStat := range bucketStats {
		totReplicaSize += bucketStat.ReplicationStats.ReplicaSize
		totReplicaCount += bucketStat.ReplicationStats.ReplicaCount
		for _, q := range bucketStat.QueueStats.Nodes {
			tq = tq.merge(q.QStats)
		}

		for arn, stat := range bucketStat.ReplicationStats.Stats {
			oldst := stats[arn]
			if oldst == nil {
				oldst = &BucketReplicationStat{
					XferRateLrg: newXferStats(),
					XferRateSml: newXferStats(),
				}
			}
			fstats := stat.FailStats.merge(oldst.FailStats)
			lrg := oldst.XferRateLrg.merge(*stat.XferRateLrg)
			sml := oldst.XferRateSml.merge(*stat.XferRateSml)
			stats[arn] = &BucketReplicationStat{
				Failed:          fstats.toMetric(),
				FailStats:       fstats,
				ReplicatedSize:  stat.ReplicatedSize + oldst.ReplicatedSize,
				ReplicatedCount: stat.ReplicatedCount + oldst.ReplicatedCount,
				Latency:         stat.Latency.merge(oldst.Latency),
				XferRateLrg:     &lrg,
				XferRateSml:     &sml,
			}
			totReplicatedSize += stat.ReplicatedSize
			totReplicatedCount += stat.ReplicatedCount
			totFailed = totFailed.merge(stat.FailStats)
		}
	}

	s = BucketReplicationStats{
		Stats:           stats,
		QStat:           tq,
		ReplicaSize:     totReplicaSize,
		ReplicaCount:    totReplicaCount,
		ReplicatedSize:  totReplicatedSize,
		ReplicatedCount: totReplicatedCount,
		Failed:          totFailed.toMetric(),
	}

	var qs ReplicationQueueStats
	for _, bs := range bucketStats {
		qs.Nodes = append(qs.Nodes, bs.QueueStats.Nodes...)
	}
	qs.Uptime = UTCNow().Unix() - globalBootTime.Unix()

	var ps ProxyMetric
	for _, bs := range bucketStats {
		ps.add(bs.ProxyStats)
	}
	bs = BucketStats{
		ReplicationStats: s,
		QueueStats:       qs,
		ProxyStats:       ps,
	}
	r.mostRecentStatsMu.Lock()
	if len(r.mostRecentStats.Stats) == 0 {
		r.mostRecentStats = BucketStatsMap{Stats: make(map[string]BucketStats, 1), Timestamp: UTCNow()}
	}
	if len(bs.ReplicationStats.Stats) > 0 {
		r.mostRecentStats.Stats[bucket] = bs
	}
	r.mostRecentStats.Timestamp = UTCNow()
	r.mostRecentStatsMu.Unlock()
	return bs
}

// get the most current of in-memory replication stats  and data usage info from crawler.
func (r *ReplicationStats) getLatestReplicationStats(bucket string) (s BucketStats) {
	if r == nil {
		return s
	}
	bucketStats := globalNotificationSys.GetClusterBucketStats(GlobalContext, bucket)
	return r.calculateBucketReplicationStats(bucket, bucketStats)
}

func (r *ReplicationStats) incQ(bucket string, sz int64, isDeleteRepl bool, opType replication.Type) {
	r.qCache.Lock()
	defer r.qCache.Unlock()
	v, ok := r.qCache.bucketStats[bucket]
	if !ok {
		v = newInQueueStats(r.registry, bucket)
	}
	atomic.AddInt64(&v.nowBytes, sz)
	atomic.AddInt64(&v.nowCount, 1)
	r.qCache.bucketStats[bucket] = v
	atomic.AddInt64(&r.qCache.srQueueStats.nowBytes, sz)
	atomic.AddInt64(&r.qCache.srQueueStats.nowCount, 1)
}

func (r *ReplicationStats) decQ(bucket string, sz int64, isDelMarker bool, opType replication.Type) {
	r.qCache.Lock()
	defer r.qCache.Unlock()
	v, ok := r.qCache.bucketStats[bucket]
	if !ok {
		v = newInQueueStats(r.registry, bucket)
	}
	atomic.AddInt64(&v.nowBytes, -1*sz)
	atomic.AddInt64(&v.nowCount, -1)
	r.qCache.bucketStats[bucket] = v

	atomic.AddInt64(&r.qCache.srQueueStats.nowBytes, -1*sz)
	atomic.AddInt64(&r.qCache.srQueueStats.nowCount, -1)
}

// incProxy increments proxy metrics for proxied calls
func (r *ReplicationStats) incProxy(bucket string, api replProxyAPI, isErr bool) {
	if r != nil {
		r.pCache.inc(bucket, api, isErr)
	}
}

func (r *ReplicationStats) getProxyStats(bucket string) ProxyMetric {
	if r == nil {
		return ProxyMetric{}
	}
	return r.pCache.getBucketStats(bucket)
}

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
	"fmt"
	"maps"
	"math"
	"sync/atomic"
	"time"

	"github.com/minio/madmin-go/v3"
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
	return newReplLatency
}

// Get upload latency of each object size range
func (rl ReplicationLatency) getUploadLatency() (ret map[string]uint64) {
	ret = make(map[string]uint64)
	avg := rl.UploadHistogram.GetAvgData()
	for k, v := range avg {
		// Convert nanoseconds to milliseconds
		ret[sizeTagToString(k)] = uint64(v.avg() / time.Millisecond)
	}
	return ret
}

// Update replication upload latency with a new value
func (rl *ReplicationLatency) update(size int64, duration time.Duration) {
	rl.UploadHistogram.Add(size, duration)
}

// ReplicationLastMinute has last minute replication counters
type ReplicationLastMinute struct {
	LastMinute lastMinuteLatency
}

func (rl ReplicationLastMinute) merge(other ReplicationLastMinute) (nl ReplicationLastMinute) {
	nl = ReplicationLastMinute{rl.LastMinute.merge(other.LastMinute)}
	return nl
}

func (rl *ReplicationLastMinute) addsize(n int64) {
	t := time.Now().Unix()
	rl.LastMinute.addAll(t-1, AccElem{Total: t - 1, Size: n, N: 1})
}

func (rl *ReplicationLastMinute) String() string {
	t := rl.LastMinute.getTotal()
	return fmt.Sprintf("ReplicationLastMinute sz= %d, n=%d , dur=%d", t.Size, t.N, t.Total)
}

func (rl *ReplicationLastMinute) getTotal() AccElem {
	return rl.LastMinute.getTotal()
}

// ReplicationLastHour keeps track of replication counts over the last hour
type ReplicationLastHour struct {
	Totals  [60]AccElem
	LastMin int64
}

// Merge data of two ReplicationLastHour structure
func (l ReplicationLastHour) merge(o ReplicationLastHour) (merged ReplicationLastHour) {
	if l.LastMin > o.LastMin {
		o.forwardTo(l.LastMin)
		merged.LastMin = l.LastMin
	} else {
		l.forwardTo(o.LastMin)
		merged.LastMin = o.LastMin
	}

	for i := range merged.Totals {
		merged.Totals[i] = AccElem{
			Total: l.Totals[i].Total + o.Totals[i].Total,
			N:     l.Totals[i].N + o.Totals[i].N,
			Size:  l.Totals[i].Size + o.Totals[i].Size,
		}
	}
	return merged
}

// Add  a new duration data
func (l *ReplicationLastHour) addsize(sz int64) {
	minutes := time.Now().Unix() / 60
	l.forwardTo(minutes)
	winIdx := minutes % 60
	l.Totals[winIdx].merge(AccElem{Total: minutes, Size: sz, N: 1})
	l.LastMin = minutes
}

// Merge all recorded counts of last hour into one
func (l *ReplicationLastHour) getTotal() AccElem {
	var res AccElem
	minutes := time.Now().Unix() / 60
	l.forwardTo(minutes)
	for _, elem := range l.Totals[:] {
		res.merge(elem)
	}
	return res
}

// forwardTo time t, clearing any entries in between.
func (l *ReplicationLastHour) forwardTo(t int64) {
	if l.LastMin >= t {
		return
	}
	if t-l.LastMin >= 60 {
		l.Totals = [60]AccElem{}
		return
	}
	for l.LastMin != t {
		// Clear next element.
		idx := (l.LastMin + 1) % 60
		l.Totals[idx] = AccElem{}
		l.LastMin++
	}
}

// BucketStatsMap captures bucket statistics for all buckets
type BucketStatsMap struct {
	Stats     map[string]BucketStats
	Timestamp time.Time
}

// BucketStats bucket statistics
type BucketStats struct {
	Uptime           int64                  `json:"uptime"`
	ReplicationStats BucketReplicationStats `json:"currStats"`  // current replication stats since cluster startup
	QueueStats       ReplicationQueueStats  `json:"queueStats"` // replication queue stats
	ProxyStats       ProxyMetric            `json:"proxyStats"`
}

// BucketReplicationStats represents inline replication statistics
// such as pending, failed and completed bytes in total for a bucket
type BucketReplicationStats struct {
	Stats map[string]*BucketReplicationStat `json:",omitempty"`
	// Completed size in bytes
	ReplicatedSize int64 `json:"completedReplicationSize"`
	// Total Replica size in bytes
	ReplicaSize int64 `json:"replicaSize"`
	// Total failed operations including metadata updates for various time frames
	Failed madmin.TimedErrStats `json:"failed"`

	// Total number of completed operations
	ReplicatedCount int64 `json:"replicationCount"`
	// Total number of replica received
	ReplicaCount int64 `json:"replicaCount"`

	// in Queue stats for bucket - from qCache
	QStat InQueueMetric `json:"queued"`
	// Deprecated fields
	// Pending size in bytes
	PendingSize int64 `json:"pendingReplicationSize"`
	// Failed size in bytes
	FailedSize int64 `json:"failedReplicationSize"`
	// Total number of pending operations including metadata updates
	PendingCount int64 `json:"pendingReplicationCount"`
	// Total number of failed operations including metadata updates
	FailedCount int64 `json:"failedReplicationCount"`
}

func newBucketReplicationStats() *BucketReplicationStats {
	return &BucketReplicationStats{
		Stats: make(map[string]*BucketReplicationStat),
	}
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
		s := BucketReplicationStat{
			ReplicatedSize:                   st.ReplicatedSize,
			ReplicaSize:                      st.ReplicaSize,
			Latency:                          st.Latency,
			BandWidthLimitInBytesPerSecond:   st.BandWidthLimitInBytesPerSecond,
			CurrentBandwidthInBytesPerSecond: st.CurrentBandwidthInBytesPerSecond,
			XferRateLrg:                      st.XferRateLrg.Clone(),
			XferRateSml:                      st.XferRateSml.Clone(),
			ReplicatedCount:                  st.ReplicatedCount,
			Failed:                           st.Failed,
			FailStats:                        st.FailStats,
		}
		if s.Failed.ErrCounts == nil {
			s.Failed.ErrCounts = make(map[string]int)
			maps.Copy(s.Failed.ErrCounts, st.Failed.ErrCounts)
		}
		c.Stats[arn] = &s
	}
	return c
}

// BucketReplicationStat represents inline replication statistics
// such as pending, failed and completed bytes in total for a bucket
// remote target
type BucketReplicationStat struct {
	// Pending size in bytes
	//	PendingSize int64 `json:"pendingReplicationSize"`
	// Completed size in bytes
	ReplicatedSize int64 `json:"completedReplicationSize"`
	// Total Replica size in bytes
	ReplicaSize int64 `json:"replicaSize"`
	// Collect stats for failures
	FailStats RTimedMetrics `json:"-"`

	// Total number of failed operations including metadata updates in the last minute
	Failed madmin.TimedErrStats `json:"failed"`
	// Total number of completed operations
	ReplicatedCount int64 `json:"replicationCount"`
	// Replication latency information
	Latency ReplicationLatency `json:"replicationLatency"`
	// bandwidth limit for target
	BandWidthLimitInBytesPerSecond int64 `json:"limitInBits"`
	// current bandwidth reported
	CurrentBandwidthInBytesPerSecond float64 `json:"currentBandwidth"`
	// transfer rate for large uploads
	XferRateLrg *XferStats `json:"-" msg:"lt"`
	// transfer rate for small uploads
	XferRateSml *XferStats `json:"-" msg:"st"`

	// Deprecated fields
	// Pending size in bytes
	PendingSize int64 `json:"pendingReplicationSize"`
	// Failed size in bytes
	FailedSize int64 `json:"failedReplicationSize"`
	// Total number of pending operations including metadata updates
	PendingCount int64 `json:"pendingReplicationCount"`
	// Total number of failed operations including metadata updates
	FailedCount int64 `json:"failedReplicationCount"`
}

func (bs *BucketReplicationStat) hasReplicationUsage() bool {
	return bs.FailStats.SinceUptime.Count > 0 ||
		bs.ReplicatedSize > 0 ||
		bs.ReplicaSize > 0
}

func (bs *BucketReplicationStat) updateXferRate(sz int64, duration time.Duration) {
	if sz > minLargeObjSize {
		bs.XferRateLrg.addSize(sz, duration)
	} else {
		bs.XferRateSml.addSize(sz, duration)
	}
}

// RMetricName - name of replication metric
type RMetricName string

const (
	// Large - objects larger than 128MiB
	Large RMetricName = "Large"
	// Small - objects smaller than 128MiB
	Small RMetricName = "Small"
	// Total - metric pertaining to totals
	Total RMetricName = "Total"
)

// ReplQNodeStats holds queue stats for replication per node
type ReplQNodeStats struct {
	NodeName      string                               `json:"nodeName"`
	Uptime        int64                                `json:"uptime"`
	ActiveWorkers ActiveWorkerStat                     `json:"activeWorkers"`
	XferStats     map[RMetricName]XferStats            `json:"transferSummary"`
	TgtXferStats  map[string]map[RMetricName]XferStats `json:"tgtTransferStats"`
	QStats        InQueueMetric                        `json:"queueStats"`
	MRFStats      ReplicationMRFStats                  `json:"mrfStats"`
}

// getNodeQueueStats returns replication operational stats at the node level
func (r *ReplicationStats) getNodeQueueStats(bucket string) (qs ReplQNodeStats) {
	qs.NodeName = globalLocalNodeName
	qs.Uptime = UTCNow().Unix() - globalBootTime.Unix()
	grs := globalReplicationStats.Load()
	if grs != nil {
		qs.ActiveWorkers = grs.ActiveWorkers()
	} else {
		qs.ActiveWorkers = ActiveWorkerStat{}
	}
	qs.XferStats = make(map[RMetricName]XferStats)
	qs.QStats = r.qCache.getBucketStats(bucket)
	qs.TgtXferStats = make(map[string]map[RMetricName]XferStats)
	qs.MRFStats = ReplicationMRFStats{
		LastFailedCount: atomic.LoadUint64(&r.mrfStats.LastFailedCount),
	}

	r.RLock()
	defer r.RUnlock()

	brs, ok := r.Cache[bucket]
	if !ok {
		return qs
	}
	for arn := range brs.Stats {
		qs.TgtXferStats[arn] = make(map[RMetricName]XferStats)
	}
	count := 0
	var totPeak float64
	// calculate large, small transfers and total transfer rates per replication target at bucket level
	for arn, v := range brs.Stats {
		lcurrTgt := v.XferRateLrg.curr()
		scurrTgt := v.XferRateSml.curr()
		totPeak = math.Max(math.Max(v.XferRateLrg.Peak, v.XferRateSml.Peak), totPeak)
		totPeak = math.Max(math.Max(lcurrTgt, scurrTgt), totPeak)
		tcount := 0
		if v.XferRateLrg.Peak > 0 {
			tcount++
		}
		if v.XferRateSml.Peak > 0 {
			tcount++
		}
		qs.TgtXferStats[arn][Large] = XferStats{
			Avg:  v.XferRateLrg.Avg,
			Curr: lcurrTgt,
			Peak: math.Max(v.XferRateLrg.Peak, lcurrTgt),
		}
		qs.TgtXferStats[arn][Small] = XferStats{
			Avg:  v.XferRateSml.Avg,
			Curr: scurrTgt,
			Peak: math.Max(v.XferRateSml.Peak, scurrTgt),
		}
		if tcount > 0 {
			qs.TgtXferStats[arn][Total] = XferStats{
				Avg:  (v.XferRateLrg.Avg + v.XferRateSml.Avg) / float64(tcount),
				Curr: (scurrTgt + lcurrTgt) / float64(tcount),
				Peak: totPeak,
			}
		}
	}
	// calculate large, small and total transfer rates for a minio node
	var lavg, lcurr, lpeak, savg, scurr, speak, totpeak float64
	for _, v := range qs.TgtXferStats {
		tot := v[Total]
		lavg += v[Large].Avg
		lcurr += v[Large].Curr
		savg += v[Small].Avg
		scurr += v[Small].Curr
		totpeak = math.Max(math.Max(tot.Peak, totpeak), tot.Curr)
		lpeak = math.Max(math.Max(v[Large].Peak, lpeak), v[Large].Curr)
		speak = math.Max(math.Max(v[Small].Peak, speak), v[Small].Curr)
		if lpeak > 0 || speak > 0 {
			count++
		}
	}
	if count > 0 {
		lrg := XferStats{
			Avg:  lavg / float64(count),
			Curr: lcurr / float64(count),
			Peak: lpeak,
		}
		sml := XferStats{
			Avg:  savg / float64(count),
			Curr: scurr / float64(count),
			Peak: speak,
		}
		qs.XferStats[Large] = lrg
		qs.XferStats[Small] = sml
		qs.XferStats[Total] = XferStats{
			Avg:  (savg + lavg) / float64(count),
			Curr: (lcurr + scurr) / float64(count),
			Peak: totpeak,
		}
	}
	return qs
}

// populate queue totals for node and active workers in use for metrics
func (r *ReplicationStats) getNodeQueueStatsSummary() (qs ReplQNodeStats) {
	qs.NodeName = globalLocalNodeName
	qs.Uptime = UTCNow().Unix() - globalBootTime.Unix()
	qs.ActiveWorkers = globalReplicationStats.Load().ActiveWorkers()
	qs.XferStats = make(map[RMetricName]XferStats)
	qs.QStats = r.qCache.getSiteStats()
	qs.MRFStats = ReplicationMRFStats{
		LastFailedCount: atomic.LoadUint64(&r.mrfStats.LastFailedCount),
	}
	r.RLock()
	defer r.RUnlock()
	tx := newXferStats()
	for _, brs := range r.Cache {
		for _, v := range brs.Stats {
			tx := tx.merge(*v.XferRateLrg)
			tx = tx.merge(*v.XferRateSml)
		}
	}
	qs.XferStats[Total] = *tx
	return qs
}

// ReplicationQueueStats holds overall queue stats for replication
type ReplicationQueueStats struct {
	Nodes  []ReplQNodeStats `json:"nodes"`
	Uptime int64            `json:"uptime"`
}

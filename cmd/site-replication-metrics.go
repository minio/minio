// Copyright (c) 2015-2023 MinIO, Inc.
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
	"sync"
	"sync/atomic"
	"time"

	"github.com/minio/madmin-go/v3"
	"github.com/minio/minio-go/v7"
)

//go:generate msgp -file $GOFILE

// RStat has replication error stats
type RStat struct {
	Count int64 `json:"count"`
	Bytes int64 `json:"bytes"`
}

// RTimedMetrics has replication error stats for various time windows
type RTimedMetrics struct {
	LastHour    ReplicationLastHour `json:"lastHour"`
	SinceUptime RStat               `json:"sinceUptime"`
	LastMinute  ReplicationLastMinute
	// Error counts
	ErrCounts map[string]int `json:"errCounts"` // Count of credential errors
}

func (rt *RTimedMetrics) String() string {
	s := rt.toMetric()
	return fmt.Sprintf("Errors in LastMinute: %v, LastHour: %v, SinceUptime: %v", s.LastMinute.Count, s.LastHour.Count, s.Totals.Count)
}

func (rt *RTimedMetrics) toMetric() madmin.TimedErrStats {
	if rt == nil {
		return madmin.TimedErrStats{}
	}
	errCounts := make(map[string]int)
	maps.Copy(errCounts, rt.ErrCounts)
	minuteTotals := rt.LastMinute.getTotal()
	hourTotals := rt.LastHour.getTotal()
	return madmin.TimedErrStats{
		LastMinute: madmin.RStat{
			Count: float64(minuteTotals.N),
			Bytes: minuteTotals.Size,
		},
		LastHour: madmin.RStat{
			Count: float64(hourTotals.N),
			Bytes: hourTotals.Size,
		},
		Totals: madmin.RStat{
			Count: float64(rt.SinceUptime.Count),
			Bytes: rt.SinceUptime.Bytes,
		},
		ErrCounts: errCounts,
	}
}

func (rt *RTimedMetrics) addsize(size int64, err error) {
	// failures seen since uptime
	atomic.AddInt64(&rt.SinceUptime.Bytes, size)
	atomic.AddInt64(&rt.SinceUptime.Count, 1)
	rt.LastMinute.addsize(size)
	rt.LastHour.addsize(size)
	if err != nil && minio.ToErrorResponse(err).Code == "AccessDenied" {
		if rt.ErrCounts == nil {
			rt.ErrCounts = make(map[string]int)
		}
		rt.ErrCounts["AccessDenied"]++
	}
}

func (rt *RTimedMetrics) merge(o RTimedMetrics) (n RTimedMetrics) {
	n.SinceUptime.Bytes = atomic.LoadInt64(&rt.SinceUptime.Bytes) + atomic.LoadInt64(&o.SinceUptime.Bytes)
	n.SinceUptime.Count = atomic.LoadInt64(&rt.SinceUptime.Count) + atomic.LoadInt64(&o.SinceUptime.Count)

	n.LastMinute = n.LastMinute.merge(rt.LastMinute)
	n.LastMinute = n.LastMinute.merge(o.LastMinute)
	n.LastHour = n.LastHour.merge(rt.LastHour)
	n.LastHour = n.LastHour.merge(o.LastHour)
	n.ErrCounts = make(map[string]int)
	maps.Copy(n.ErrCounts, rt.ErrCounts)
	for k, v := range o.ErrCounts {
		n.ErrCounts[k] += v
	}
	return n
}

// SRStats has replication stats at site level
type SRStats struct {
	// Total Replica size in bytes
	ReplicaSize int64 `json:"replicaSize"`
	// Total Replica received
	ReplicaCount int64                `json:"replicaCount"`
	M            map[string]*SRStatus `json:"srStatusMap"`

	movingAvgTicker *time.Ticker // Ticker for calculating moving averages
	lock            sync.RWMutex // mutex for srStats
}

// SRStatus has replication stats at deployment level
type SRStatus struct {
	ReplicatedSize int64 `json:"completedReplicationSize"`
	// Total number of failed operations including metadata updates in the last minute
	Failed RTimedMetrics `json:"failedReplication"`
	// Total number of completed operations
	ReplicatedCount int64 `json:"replicationCount"`
	// Replication latency information
	Latency ReplicationLatency `json:"replicationLatency"`
	// transfer rate for large uploads
	XferRateLrg *XferStats `json:"largeTransferRate" msg:"lt"`
	// transfer rate for small uploads
	XferRateSml *XferStats `json:"smallTransferRate" msg:"st"`
	// Endpoint is the replication target endpoint
	Endpoint string `json:"-"`
	// Secure is true if the replication target endpoint is secure
	Secure bool `json:"-"`
}

func (sr *SRStats) update(st replStat, dID string) {
	sr.lock.Lock()
	defer sr.lock.Unlock()
	srs, ok := sr.M[dID]
	if !ok {
		srs = &SRStatus{
			XferRateLrg: newXferStats(),
			XferRateSml: newXferStats(),
		}
	}
	srs.Endpoint = st.Endpoint
	srs.Secure = st.Secure
	switch {
	case st.Completed:
		srs.ReplicatedSize += st.TransferSize
		srs.ReplicatedCount++
		if st.TransferDuration > 0 {
			srs.Latency.update(st.TransferSize, st.TransferDuration)
			srs.updateXferRate(st.TransferSize, st.TransferDuration)
		}
	case st.Failed:
		srs.Failed.addsize(st.TransferSize, st.Err)
	case st.Pending:
	}
	sr.M[dID] = srs
}

func (sr *SRStats) get() map[string]SRMetric {
	epMap := globalBucketTargetSys.healthStats()

	sr.lock.RLock()
	defer sr.lock.RUnlock()
	m := make(map[string]SRMetric, len(sr.M))
	for dID, v := range sr.M {
		t := newXferStats()
		mx := make(map[RMetricName]XferStats)

		if v.XferRateLrg != nil {
			mx[Large] = *v.XferRateLrg.Clone()
			m := t.merge(*v.XferRateLrg)
			t = &m
		}
		if v.XferRateSml != nil {
			mx[Small] = *v.XferRateSml.Clone()
			m := t.merge(*v.XferRateSml)
			t = &m
		}

		mx[Total] = *t
		metric := SRMetric{
			ReplicatedSize:  v.ReplicatedSize,
			ReplicatedCount: v.ReplicatedCount,
			DeploymentID:    dID,
			Failed:          v.Failed.toMetric(),
			XferStats:       mx,
		}
		epHealth, ok := epMap[v.Endpoint]
		if ok {
			metric.Endpoint = epHealth.Endpoint
			metric.TotalDowntime = epHealth.offlineDuration
			metric.LastOnline = epHealth.lastOnline
			metric.Online = epHealth.Online
			metric.Latency = madmin.LatencyStat{
				Curr: epHealth.latency.curr,
				Avg:  epHealth.latency.avg,
				Max:  epHealth.latency.peak,
			}
		}
		m[dID] = metric
	}
	return m
}

func (srs *SRStatus) updateXferRate(sz int64, duration time.Duration) {
	if sz > minLargeObjSize {
		srs.XferRateLrg.addSize(sz, duration)
	} else {
		srs.XferRateSml.addSize(sz, duration)
	}
}

func newSRStats() *SRStats {
	s := SRStats{
		M:               make(map[string]*SRStatus),
		movingAvgTicker: time.NewTicker(time.Second * 2),
	}
	go s.trackEWMA()
	return &s
}

func (sr *SRStats) trackEWMA() {
	for {
		select {
		case <-sr.movingAvgTicker.C:
			sr.updateMovingAvg()
		case <-GlobalContext.Done():
			return
		}
	}
}

func (sr *SRStats) updateMovingAvg() {
	sr.lock.Lock()
	defer sr.lock.Unlock()
	for _, s := range sr.M {
		s.XferRateLrg.measure.updateExponentialMovingAverage(time.Now())
		s.XferRateSml.measure.updateExponentialMovingAverage(time.Now())
	}
}

// SRMetric captures replication metrics for a deployment
type SRMetric struct {
	DeploymentID  string             `json:"deploymentID"`
	Endpoint      string             `json:"endpoint"`
	TotalDowntime time.Duration      `json:"totalDowntime"`
	LastOnline    time.Time          `json:"lastOnline"`
	Online        bool               `json:"isOnline"`
	Latency       madmin.LatencyStat `json:"latency"`

	// replication metrics across buckets roll up
	ReplicatedSize int64 `json:"replicatedSize"`
	// Total number of completed operations
	ReplicatedCount int64 `json:"replicatedCount"`
	// Failed captures replication errors in various time windows

	Failed madmin.TimedErrStats `json:"failed"`

	XferStats map[RMetricName]XferStats `json:"transferSummary"`
}

// SRMetricsSummary captures summary of replication counts across buckets on site
// along with op metrics rollup.
type SRMetricsSummary struct {
	// op metrics roll up
	ActiveWorkers ActiveWorkerStat `json:"activeWorkers"`

	// Total Replica size in bytes
	ReplicaSize int64 `json:"replicaSize"`

	// Total number of replica received
	ReplicaCount int64 `json:"replicaCount"`
	// Queued operations
	Queued InQueueMetric `json:"queued"`
	// Proxy stats
	Proxied ProxyMetric `json:"proxied"`
	// replication metrics summary for each site replication peer
	Metrics map[string]SRMetric `json:"replMetrics"`
	// uptime of node being queried for site replication metrics
	Uptime int64 `json:"uptime"`
}

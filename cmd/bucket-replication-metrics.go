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
	"sync"
	"sync/atomic"
	"time"

	"github.com/rcrowley/go-metrics"
)

//go:generate msgp -file $GOFILE

const (
	// beta is the weight used to calculate exponential moving average
	beta = 0.1 // Number of averages considered = 1/(1-beta)
)

// rateMeasurement captures the transfer details for one bucket/target
//msgp:ignore rateMeasurement

type rateMeasurement struct {
	lock                 sync.Mutex
	bytesSinceLastWindow uint64    // Total bytes since last window was processed
	startTime            time.Time // Start time for window
	expMovingAvg         float64   // Previously calculated exponential moving average
}

// newRateMeasurement creates a new instance of the measurement with the initial start time.
func newRateMeasurement(initTime time.Time) *rateMeasurement {
	return &rateMeasurement{
		startTime: initTime,
	}
}

// incrementBytes add bytes reported for a bucket/target.
func (m *rateMeasurement) incrementBytes(bytes uint64) {
	atomic.AddUint64(&m.bytesSinceLastWindow, bytes)
}

// updateExponentialMovingAverage processes the measurements captured so far.
func (m *rateMeasurement) updateExponentialMovingAverage(endTime time.Time) {
	// Calculate aggregate avg bandwidth and exp window avg
	m.lock.Lock()
	defer func() {
		m.startTime = endTime
		m.lock.Unlock()
	}()

	if m.startTime.IsZero() {
		return
	}

	if endTime.Before(m.startTime) {
		return
	}

	duration := endTime.Sub(m.startTime)

	bytesSinceLastWindow := atomic.SwapUint64(&m.bytesSinceLastWindow, 0)

	if m.expMovingAvg == 0 {
		// Should address initial calculation and should be fine for resuming from 0
		m.expMovingAvg = float64(bytesSinceLastWindow) / duration.Seconds()
		return
	}

	increment := float64(bytesSinceLastWindow) / duration.Seconds()
	m.expMovingAvg = exponentialMovingAverage(beta, m.expMovingAvg, increment)
}

// exponentialMovingAverage calculates the exponential moving average
func exponentialMovingAverage(beta, previousAvg, incrementAvg float64) float64 {
	return (1-beta)*incrementAvg + beta*previousAvg
}

// getExpMovingAvgBytesPerSecond returns the exponential moving average for the bucket/target in bytes
func (m *rateMeasurement) getExpMovingAvgBytesPerSecond() float64 {
	m.lock.Lock()
	defer m.lock.Unlock()
	return m.expMovingAvg
}

// ActiveWorkerStat is stat for active replication workers
type ActiveWorkerStat struct {
	Curr int     `json:"curr"`
	Avg  float32 `json:"avg"`
	Max  int     `json:"max"`
	hist metrics.Histogram
}

func newActiveWorkerStat(r metrics.Registry) *ActiveWorkerStat {
	h := metrics.NewHistogram(metrics.NewUniformSample(100))
	r.Register("replication.active_workers", h)
	return &ActiveWorkerStat{
		hist: h,
	}
}

// update curr and max workers;
func (a *ActiveWorkerStat) update() {
	if a == nil {
		return
	}
	a.Curr = globalReplicationPool.ActiveWorkers()
	a.hist.Update(int64(a.Curr))
	a.Avg = float32(a.hist.Mean())
	a.Max = int(a.hist.Max())
}

func (a *ActiveWorkerStat) get() ActiveWorkerStat {
	w := ActiveWorkerStat{
		Curr: a.Curr,
		Avg:  a.Avg,
		Max:  a.Max,
	}
	return w
}

// QStat holds queue stats for replication
type QStat struct {
	Count float64 `json:"count"`
	Bytes float64 `json:"bytes"`
}

func (q *QStat) add(o QStat) QStat {
	return QStat{Bytes: q.Bytes + o.Bytes, Count: q.Count + o.Count}
}

// InQueueMetric holds queue stats for replication
type InQueueMetric struct {
	Curr QStat `json:"curr" msg:"cq"`
	Avg  QStat `json:"avg" msg:"aq"`
	Max  QStat `json:"max" msg:"pq"`
}

func (qm InQueueMetric) merge(o InQueueMetric) InQueueMetric {
	return InQueueMetric{
		Curr: qm.Curr.add(o.Curr),
		Avg:  qm.Avg.add(o.Avg),
		Max:  qm.Max.add(o.Max),
	}
}

type queueCache struct {
	srQueueStats InQueueStats
	bucketStats  map[string]InQueueStats
	sync.RWMutex // mutex for queue stats
}

func newQueueCache(r metrics.Registry) queueCache {
	return queueCache{
		bucketStats:  make(map[string]InQueueStats),
		srQueueStats: newInQueueStats(r, "site"),
	}
}

func (q *queueCache) update() {
	q.Lock()
	defer q.Unlock()
	q.srQueueStats.update()
	for _, s := range q.bucketStats {
		s.update()
	}
}

func (q *queueCache) getBucketStats(bucket string) InQueueMetric {
	q.RLock()
	defer q.RUnlock()
	v, ok := q.bucketStats[bucket]
	if !ok {
		return InQueueMetric{}
	}
	return InQueueMetric{
		Curr: QStat{Bytes: float64(v.nowBytes), Count: float64(v.nowCount)},
		Max:  QStat{Bytes: float64(v.histBytes.Max()), Count: float64(v.histCounts.Max())},
		Avg:  QStat{Bytes: v.histBytes.Mean(), Count: v.histCounts.Mean()},
	}
}

func (q *queueCache) getSiteStats() InQueueMetric {
	q.RLock()
	defer q.RUnlock()
	v := q.srQueueStats
	return InQueueMetric{
		Curr: QStat{Bytes: float64(v.nowBytes), Count: float64(v.nowCount)},
		Max:  QStat{Bytes: float64(v.histBytes.Max()), Count: float64(v.histCounts.Max())},
		Avg:  QStat{Bytes: v.histBytes.Mean(), Count: v.histCounts.Mean()},
	}
}

// InQueueStats holds queue stats for replication
type InQueueStats struct {
	nowBytes   int64 `json:"-"`
	nowCount   int64 `json:"-"`
	histCounts metrics.Histogram
	histBytes  metrics.Histogram
}

func newInQueueStats(r metrics.Registry, lbl string) InQueueStats {
	histCounts := metrics.NewHistogram(metrics.NewUniformSample(100))
	histBytes := metrics.NewHistogram(metrics.NewUniformSample(100))

	r.Register("replication.queue.counts."+lbl, histCounts)
	r.Register("replication.queue.bytes."+lbl, histBytes)

	return InQueueStats{
		histCounts: histCounts,
		histBytes:  histBytes,
	}
}

func (q *InQueueStats) update() {
	q.histBytes.Update(atomic.LoadInt64(&q.nowBytes))
	q.histCounts.Update(atomic.LoadInt64(&q.nowCount))
}

// XferStats has transfer stats for replication
type XferStats struct {
	Curr    float64          `json:"currRate" msg:"cr"`
	Avg     float64          `json:"avgRate" msg:"av"`
	Peak    float64          `json:"peakRate" msg:"p"`
	N       int64            `json:"n" msg:"n"`
	measure *rateMeasurement `json:"-"`
	sma     *SMA             `json:"-"`
}

// Clone returns a copy of XferStats
func (rx *XferStats) Clone() *XferStats {
	curr := rx.curr()
	peak := rx.Peak
	if curr > peak {
		peak = curr
	}
	return &XferStats{
		Curr:    curr,
		Avg:     rx.Avg,
		Peak:    peak,
		N:       rx.N,
		measure: rx.measure,
	}
}

func newXferStats() *XferStats {
	return &XferStats{
		measure: newRateMeasurement(time.Now()),
		sma:     newSMA(50),
	}
}

func (rx *XferStats) String() string {
	return fmt.Sprintf("curr=%f avg=%f, peak=%f", rx.curr(), rx.Avg, rx.Peak)
}

func (rx *XferStats) curr() float64 {
	if rx.measure == nil {
		return 0.0
	}
	return rx.measure.getExpMovingAvgBytesPerSecond()
}

func (rx *XferStats) merge(o XferStats) XferStats {
	curr := calcAvg(rx.curr(), o.curr(), rx.N, o.N)
	peak := rx.Peak
	if o.Peak > peak {
		peak = o.Peak
	}
	if curr > peak {
		peak = curr
	}
	return XferStats{
		Avg:     calcAvg(rx.Avg, o.Avg, rx.N, o.N),
		Peak:    peak,
		Curr:    curr,
		measure: rx.measure,
		N:       rx.N + o.N,
	}
}

func calcAvg(x, y float64, n1, n2 int64) float64 {
	if n1+n2 == 0 {
		return 0
	}
	avg := (x*float64(n1) + y*float64(n2)) / float64(n1+n2)
	return avg
}

// Add a new transfer
func (rx *XferStats) addSize(sz int64, t time.Duration) {
	if rx.measure == nil {
		rx.measure = newRateMeasurement(time.Now())
	}
	rx.measure.incrementBytes(uint64(sz))
	rx.Curr = rx.measure.getExpMovingAvgBytesPerSecond()
	rx.sma.addSample(rx.Curr)
	rx.Avg = rx.sma.simpleMovingAvg()
	if rx.Curr > rx.Peak {
		rx.Peak = rx.Curr
	}
	rx.N++
}

// ReplicationMRFStats holds stats of MRF backlog saved to disk in the last 5 minutes
// and number of entries that failed replication after 3 retries
type ReplicationMRFStats struct {
	LastFailedCount uint64 `json:"failedCount_last5min"`
	// Count of unreplicated entries that were dropped after MRF retry limit reached since cluster start.
	TotalDroppedCount uint64 `json:"droppedCount_since_uptime"`
	// Bytes of unreplicated entries that were dropped after MRF retry limit reached since cluster start.
	TotalDroppedBytes uint64 `json:"droppedBytes_since_uptime"`
}

// SMA struct for calculating simple moving average
type SMA struct {
	buf       []float64
	window    int     // len of buf
	idx       int     // current index in buf
	CAvg      float64 // cumulative average
	prevSMA   float64
	filledBuf bool
}

func newSMA(len int) *SMA {
	if len <= 0 {
		len = defaultWindowSize
	}
	return &SMA{
		buf:    make([]float64, len),
		window: len,
		idx:    0,
	}
}

func (s *SMA) addSample(next float64) {
	prev := s.buf[s.idx]
	s.buf[s.idx] = next

	if s.filledBuf {
		s.prevSMA += (next - prev) / float64(s.window)
		s.CAvg += (next - s.CAvg) / float64(s.window)
	} else {
		s.CAvg = s.simpleMovingAvg()
		s.prevSMA = s.CAvg
	}
	if s.idx == s.window-1 {
		s.filledBuf = true
	}
	s.idx = (s.idx + 1) % s.window
}

func (s *SMA) simpleMovingAvg() float64 {
	if s.filledBuf {
		return s.prevSMA
	}
	var tot float64
	for _, r := range s.buf {
		tot += r
	}
	return tot / float64(s.idx+1)
}

const (
	defaultWindowSize = 10
)

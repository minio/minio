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

package bandwidth

import (
	"context"
	"sync"
	"time"

	"github.com/minio/madmin-go"
	"golang.org/x/time/rate"
)

type throttle struct {
	*rate.Limiter
	NodeBandwidthPerSec int64
}

// Monitor holds the state of the global bucket monitor
type Monitor struct {
	tlock                 sync.RWMutex // mutex for bucketThrottle
	bucketThrottle        map[string]*throttle
	mlock                 sync.RWMutex                  // mutex for activeBuckets map
	activeBuckets         map[string]*bucketMeasurement // Buckets with objects in flight
	bucketMovingAvgTicker *time.Ticker                  // Ticker for calculating moving averages
	ctx                   context.Context               // Context for generate
	NodeCount             uint64
}

//NewMonitor returns a monitor with defaults.
func NewMonitor(ctx context.Context, numNodes uint64) *Monitor {
	m := &Monitor{
		activeBuckets:         make(map[string]*bucketMeasurement),
		bucketThrottle:        make(map[string]*throttle),
		bucketMovingAvgTicker: time.NewTicker(2 * time.Second),
		ctx:                   ctx,
		NodeCount:             numNodes,
	}
	go m.trackEWMA()
	return m
}

func (m *Monitor) updateMeasurement(bucket string, bytes uint64) {
	m.mlock.Lock()
	defer m.mlock.Unlock()
	if m, ok := m.activeBuckets[bucket]; ok {
		m.incrementBytes(bytes)
	}
}

//SelectionFunction for buckets
type SelectionFunction func(bucket string) bool

// SelectBuckets will select all the buckets passed in.
func SelectBuckets(buckets ...string) SelectionFunction {
	if len(buckets) == 0 {
		return func(bucket string) bool {
			return true
		}
	}
	return func(bucket string) bool {
		for _, b := range buckets {
			if b == "" || b == bucket {
				return true
			}
		}
		return false
	}
}

// GetReport gets the report for all bucket bandwidth details.
func (m *Monitor) GetReport(selectBucket SelectionFunction) *madmin.BucketBandwidthReport {
	m.mlock.RLock()
	defer m.mlock.RUnlock()
	return m.getReport(selectBucket)
}

func (m *Monitor) getReport(selectBucket SelectionFunction) *madmin.BucketBandwidthReport {
	report := &madmin.BucketBandwidthReport{
		BucketStats: make(map[string]madmin.BandwidthDetails),
	}
	for bucket, bucketMeasurement := range m.activeBuckets {
		if !selectBucket(bucket) {
			continue
		}
		m.tlock.RLock()
		bucketThrottle, ok := m.bucketThrottle[bucket]
		if ok {
			report.BucketStats[bucket] = madmin.BandwidthDetails{
				LimitInBytesPerSecond:            bucketThrottle.NodeBandwidthPerSec * int64(m.NodeCount),
				CurrentBandwidthInBytesPerSecond: bucketMeasurement.getExpMovingAvgBytesPerSecond(),
			}
		}
		m.tlock.RUnlock()

	}
	return report
}
func (m *Monitor) trackEWMA() {
	for {
		select {
		case <-m.bucketMovingAvgTicker.C:
			m.updateMovingAvg()
		case <-m.ctx.Done():
			return
		}
	}
}

func (m *Monitor) updateMovingAvg() {
	m.mlock.Lock()
	defer m.mlock.Unlock()
	for _, bucketMeasurement := range m.activeBuckets {
		bucketMeasurement.updateExponentialMovingAverage(time.Now())
	}
}

func (m *Monitor) getBucketMeasurement(bucket string, initTime time.Time) *bucketMeasurement {
	bucketTracker, ok := m.activeBuckets[bucket]
	if !ok {
		bucketTracker = newBucketMeasurement(initTime)
		m.activeBuckets[bucket] = bucketTracker
	}
	return bucketTracker
}

// track returns the measurement object for bucket
func (m *Monitor) track(bucket string) {
	m.mlock.Lock()
	defer m.mlock.Unlock()
	m.getBucketMeasurement(bucket, time.Now())
}

// DeleteBucket deletes monitoring the 'bucket'
func (m *Monitor) DeleteBucket(bucket string) {
	m.tlock.Lock()
	delete(m.bucketThrottle, bucket)
	m.tlock.Unlock()
	m.mlock.Lock()
	delete(m.activeBuckets, bucket)
	m.mlock.Unlock()
}

// throttle returns currently configured throttle for this bucket
func (m *Monitor) throttle(bucket string) *throttle {
	m.tlock.RLock()
	defer m.tlock.RUnlock()
	return m.bucketThrottle[bucket]
}

// SetBandwidthLimit sets the bandwidth limit for a bucket
func (m *Monitor) SetBandwidthLimit(bucket string, limit int64) {
	m.tlock.Lock()
	defer m.tlock.Unlock()
	bw := limit / int64(m.NodeCount)
	t, ok := m.bucketThrottle[bucket]
	if !ok {
		t = &throttle{
			NodeBandwidthPerSec: bw,
		}
	}
	t.NodeBandwidthPerSec = bw
	newlimit := rate.Every(time.Second / time.Duration(t.NodeBandwidthPerSec))
	t.Limiter = rate.NewLimiter(newlimit, int(t.NodeBandwidthPerSec))
	m.bucketThrottle[bucket] = t
}

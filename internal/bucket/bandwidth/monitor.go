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

//go:generate msgp -file=$GOFILE -unexported

import (
	"context"
	"slices"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

//msgp:ignore bucketThrottle Monitor

type bucketThrottle struct {
	*rate.Limiter
	NodeBandwidthPerSec int64
}

// Monitor holds the state of the global bucket monitor
type Monitor struct {
	tlock sync.RWMutex // mutex for bucket throttling
	mlock sync.RWMutex // mutex for bucket measurement

	bucketsThrottle    map[BucketOptions]*bucketThrottle
	bucketsMeasurement map[BucketOptions]*bucketMeasurement // Buckets with objects in flight

	bucketMovingAvgTicker *time.Ticker    // Ticker for calculating moving averages
	ctx                   context.Context // Context for generate
	NodeCount             uint64
}

// NewMonitor returns a monitor with defaults.
func NewMonitor(ctx context.Context, numNodes uint64) *Monitor {
	m := &Monitor{
		bucketsMeasurement:    make(map[BucketOptions]*bucketMeasurement),
		bucketsThrottle:       make(map[BucketOptions]*bucketThrottle),
		bucketMovingAvgTicker: time.NewTicker(2 * time.Second),
		ctx:                   ctx,
		NodeCount:             numNodes,
	}
	go m.trackEWMA()
	return m
}

func (m *Monitor) updateMeasurement(opts BucketOptions, bytes uint64) {
	m.mlock.Lock()
	defer m.mlock.Unlock()

	tm, ok := m.bucketsMeasurement[opts]
	if !ok {
		tm = &bucketMeasurement{}
	}
	tm.incrementBytes(bytes)
	m.bucketsMeasurement[opts] = tm
}

// SelectionFunction for buckets
type SelectionFunction func(bucket string) bool

// SelectBuckets will select all the buckets passed in.
func SelectBuckets(buckets ...string) SelectionFunction {
	if len(buckets) == 0 {
		return func(bucket string) bool {
			return true
		}
	}
	return func(bucket string) bool {
		return slices.Contains(buckets, bucket)
	}
}

// Details for the measured bandwidth
type Details struct {
	LimitInBytesPerSecond            int64   `json:"limitInBits"`
	CurrentBandwidthInBytesPerSecond float64 `json:"currentBandwidth"`
}

// BucketBandwidthReport captures the details for all buckets.
type BucketBandwidthReport struct {
	BucketStats map[BucketOptions]Details `json:"bucketStats,omitempty"`
}

// GetReport gets the report for all bucket bandwidth details.
func (m *Monitor) GetReport(selectBucket SelectionFunction) *BucketBandwidthReport {
	m.mlock.RLock()
	defer m.mlock.RUnlock()
	return m.getReport(selectBucket)
}

func (m *Monitor) getReport(selectBucket SelectionFunction) *BucketBandwidthReport {
	report := &BucketBandwidthReport{
		BucketStats: make(map[BucketOptions]Details),
	}
	for bucketOpts, bucketMeasurement := range m.bucketsMeasurement {
		if !selectBucket(bucketOpts.Name) {
			continue
		}
		m.tlock.RLock()
		if tgtThrottle, ok := m.bucketsThrottle[bucketOpts]; ok {
			currBw := bucketMeasurement.getExpMovingAvgBytesPerSecond()
			report.BucketStats[bucketOpts] = Details{
				LimitInBytesPerSecond:            tgtThrottle.NodeBandwidthPerSec * int64(m.NodeCount),
				CurrentBandwidthInBytesPerSecond: currBw,
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
	for _, bucketMeasurement := range m.bucketsMeasurement {
		bucketMeasurement.updateExponentialMovingAverage(time.Now())
	}
}

func (m *Monitor) init(opts BucketOptions) {
	m.mlock.Lock()
	defer m.mlock.Unlock()

	_, ok := m.bucketsMeasurement[opts]
	if !ok {
		m.bucketsMeasurement[opts] = newBucketMeasurement(time.Now())
	}
}

// DeleteBucket deletes monitoring the 'bucket'
func (m *Monitor) DeleteBucket(bucket string) {
	m.tlock.Lock()
	for opts := range m.bucketsThrottle {
		if opts.Name == bucket {
			delete(m.bucketsThrottle, opts)
		}
	}
	m.tlock.Unlock()

	m.mlock.Lock()
	for opts := range m.bucketsMeasurement {
		if opts.Name == bucket {
			delete(m.bucketsMeasurement, opts)
		}
	}
	m.mlock.Unlock()
}

// DeleteBucketThrottle deletes monitoring for a bucket's target
func (m *Monitor) DeleteBucketThrottle(bucket, arn string) {
	m.tlock.Lock()
	delete(m.bucketsThrottle, BucketOptions{Name: bucket, ReplicationARN: arn})
	m.tlock.Unlock()
	m.mlock.Lock()
	delete(m.bucketsMeasurement, BucketOptions{Name: bucket, ReplicationARN: arn})
	m.mlock.Unlock()
}

// throttle returns currently configured throttle for this bucket
func (m *Monitor) throttle(opts BucketOptions) *bucketThrottle {
	m.tlock.RLock()
	defer m.tlock.RUnlock()
	return m.bucketsThrottle[opts]
}

// SetBandwidthLimit sets the bandwidth limit for a bucket
func (m *Monitor) SetBandwidthLimit(bucket, arn string, limit int64) {
	m.tlock.Lock()
	defer m.tlock.Unlock()
	limitBytes := limit / int64(m.NodeCount)
	throttle, ok := m.bucketsThrottle[BucketOptions{Name: bucket, ReplicationARN: arn}]
	if !ok {
		throttle = &bucketThrottle{}
	}
	throttle.NodeBandwidthPerSec = limitBytes
	throttle.Limiter = rate.NewLimiter(rate.Limit(float64(limitBytes)), int(limitBytes))
	m.bucketsThrottle[BucketOptions{Name: bucket, ReplicationARN: arn}] = throttle
}

// IsThrottled returns true if a bucket has bandwidth throttling enabled.
func (m *Monitor) IsThrottled(bucket, arn string) bool {
	m.tlock.RLock()
	defer m.tlock.RUnlock()
	_, ok := m.bucketsThrottle[BucketOptions{Name: bucket, ReplicationARN: arn}]
	return ok
}

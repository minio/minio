/*
 * MinIO Cloud Storage, (C) 2020 MinIO, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package bandwidth

import (
	"context"
	"sync"
	"time"

	"github.com/minio/minio/pkg/bandwidth"
)

// throttleBandwidth gets the throttle for bucket with the configured value
func (m *Monitor) throttleBandwidth(ctx context.Context, bucket string, bandwidthBytesPerSecond int64, clusterBandwidth int64) *throttle {
	m.lock.Lock()
	defer m.lock.Unlock()
	throttle, ok := m.bucketThrottle[bucket]
	if !ok {
		throttle = newThrottle(ctx, bandwidthBytesPerSecond, clusterBandwidth)
		m.bucketThrottle[bucket] = throttle
		return throttle
	}
	throttle.SetBandwidth(bandwidthBytesPerSecond, clusterBandwidth)
	return throttle
}

// Monitor implements the monitoring for bandwidth measurements.
type Monitor struct {
	lock sync.Mutex // lock for all updates

	activeBuckets map[string]*bucketMeasurement // Buckets with objects in flight

	bucketMovingAvgTicker *time.Ticker // Ticker for calculating moving averages

	bucketThrottle map[string]*throttle

	doneCh <-chan struct{}
}

// NewMonitor returns a monitor with defaults.
func NewMonitor(doneCh <-chan struct{}) *Monitor {
	m := &Monitor{
		activeBuckets:         make(map[string]*bucketMeasurement),
		bucketMovingAvgTicker: time.NewTicker(2 * time.Second),
		bucketThrottle:        make(map[string]*throttle),
		doneCh:                doneCh,
	}
	go m.trackEWMA()
	return m
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
		for _, b := range buckets {
			if b == "" || b == bucket {
				return true
			}
		}
		return false
	}
}

// GetReport gets the report for all bucket bandwidth details.
func (m *Monitor) GetReport(selectBucket SelectionFunction) *bandwidth.Report {
	m.lock.Lock()
	defer m.lock.Unlock()
	return m.getReport(selectBucket)
}

func (m *Monitor) getReport(selectBucket SelectionFunction) *bandwidth.Report {
	report := &bandwidth.Report{
		BucketStats: make(map[string]bandwidth.Details),
	}
	for bucket, bucketMeasurement := range m.activeBuckets {
		if !selectBucket(bucket) {
			continue
		}
		bucketThrottle, ok := m.bucketThrottle[bucket]
		if !ok {
			continue
		}
		report.BucketStats[bucket] = bandwidth.Details{
			LimitInBytesPerSecond:            bucketThrottle.clusterBandwidth,
			CurrentBandwidthInBytesPerSecond: bucketMeasurement.getExpMovingAvgBytesPerSecond(),
		}
	}
	return report
}

func (m *Monitor) trackEWMA() {
	for {
		select {
		case <-m.bucketMovingAvgTicker.C:
			m.updateMovingAvg()
		case <-m.doneCh:
			return
		}
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

func (m *Monitor) updateMovingAvg() {
	m.lock.Lock()
	defer m.lock.Unlock()
	for _, bucketMeasurement := range m.activeBuckets {
		bucketMeasurement.updateExponentialMovingAverage(time.Now())
	}
}

// track returns the measurement object for bucket and object
func (m *Monitor) track(bucket string, object string) *bucketMeasurement {
	m.lock.Lock()
	defer m.lock.Unlock()
	return m.getBucketMeasurement(bucket, time.Now())
}

// DeleteBucket deletes monitoring the 'bucket'
func (m *Monitor) DeleteBucket(bucket string) {
	m.lock.Lock()
	defer m.lock.Unlock()
	delete(m.activeBuckets, bucket)
	delete(m.bucketThrottle, bucket)
}

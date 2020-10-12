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
	"github.com/minio/minio/pkg/pubsub"
)

// throttleBandwidth gets the throttle for bucket with the configured value
func (m *Monitor) throttleBandwidth(ctx context.Context, bucket string, bandwidthBytesPerSecond int64) *throttle {
	m.lock.Lock()
	defer m.lock.Unlock()
	throttle, ok := m.bucketThrottle[bucket]
	if !ok {
		throttle = newThrottle(ctx, bandwidthBytesPerSecond)
		m.bucketThrottle[bucket] = throttle
		return throttle
	}
	throttle.SetBandwidth(bandwidthBytesPerSecond)
	return throttle
}

// SubscribeToBuckets subscribes to buckets. Empty array for monitoring all buckets.
func (m *Monitor) SubscribeToBuckets(subCh chan interface{}, doneCh <-chan struct{}, buckets []string) {
	m.pubsub.Subscribe(subCh, doneCh, func(f interface{}) bool {
		if buckets != nil || len(buckets) == 0 {
			return true
		}
		report, ok := f.(*bandwidth.Report)
		if !ok {
			return false
		}
		for _, b := range buckets {
			_, ok := report.BucketStats[b]
			if ok {
				return true
			}
		}
		return false
	})
}

// Monitor implements the monitoring for bandwidth measurements.
type Monitor struct {
	lock sync.Mutex // lock for all updates

	activeBuckets map[string]*bucketMeasurement // Buckets with objects in flight

	bucketMovingAvgTicker *time.Ticker // Ticker for calculating moving averages

	pubsub *pubsub.PubSub // PubSub for reporting bandwidths.

	bucketThrottle map[string]*throttle

	startProcessing sync.Once

	doneCh <-chan struct{}
}

// NewMonitor returns a monitor with defaults.
func NewMonitor(doneCh <-chan struct{}) *Monitor {
	m := &Monitor{
		activeBuckets:         make(map[string]*bucketMeasurement),
		bucketMovingAvgTicker: time.NewTicker(1 * time.Second),
		pubsub:                pubsub.New(),
		bucketThrottle:        make(map[string]*throttle),
		doneCh:                doneCh,
	}
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
		report.BucketStats[bucket] = bandwidth.Details{
			LimitInBytesPerSecond:            m.bucketThrottle[bucket].bytesPerSecond,
			CurrentBandwidthInBytesPerSecond: bucketMeasurement.getExpMovingAvgBytesPerSecond(),
		}
	}
	return report
}

func (m *Monitor) process(doneCh <-chan struct{}) {
	for {
		select {
		case <-m.bucketMovingAvgTicker.C:
			m.processAvg()
		case <-doneCh:
			return
		default:
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

func (m *Monitor) processAvg() {
	m.lock.Lock()
	defer m.lock.Unlock()
	for _, bucketMeasurement := range m.activeBuckets {
		bucketMeasurement.updateExponentialMovingAverage(time.Now())
	}
	m.pubsub.Publish(m.getReport(SelectBuckets()))
}

// track returns the measurement object for bucket and object
func (m *Monitor) track(bucket string, object string, timeNow time.Time) *bucketMeasurement {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.startProcessing.Do(func() {
		go m.process(m.doneCh)
	})
	b := m.getBucketMeasurement(bucket, timeNow)
	return b
}

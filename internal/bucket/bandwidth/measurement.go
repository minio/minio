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
	"sync"
	"sync/atomic"
	"time"
)

const (
	// betaBucket is the weight used to calculate exponential moving average
	betaBucket = 0.1 // Number of averages considered = 1/(1-betaObject)
)

// bucketMeasurement captures the bandwidth details for one bucket
type bucketMeasurement struct {
	lock                 sync.Mutex
	bytesSinceLastWindow uint64    // Total bytes since last window was processed
	startTime            time.Time // Start time for window
	expMovingAvg         float64   // Previously calculate sliding window
}

// newBucketMeasurement creates a new instance of the measurement with the initial start time.
func newBucketMeasurement(initTime time.Time) *bucketMeasurement {
	return &bucketMeasurement{
		startTime: initTime,
	}
}

// incrementBytes add bytes reported for a bucket.
func (m *bucketMeasurement) incrementBytes(bytes uint64) {
	atomic.AddUint64(&m.bytesSinceLastWindow, bytes)
}

// updateExponentialMovingAverage processes the measurements captured so far.
func (m *bucketMeasurement) updateExponentialMovingAverage(endTime time.Time) {
	// Calculate aggregate avg bandwidth and exp window avg
	m.lock.Lock()
	defer func() {
		m.startTime = endTime
		m.lock.Unlock()
	}()

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
	m.expMovingAvg = exponentialMovingAverage(betaBucket, m.expMovingAvg, increment)
}

// exponentialMovingAverage calculates the exponential moving average
func exponentialMovingAverage(beta, previousAvg, incrementAvg float64) float64 {
	return (1-beta)*incrementAvg + beta*previousAvg
}

// getExpMovingAvgBytesPerSecond returns the exponential moving average for the bucket in bytes
func (m *bucketMeasurement) getExpMovingAvgBytesPerSecond() float64 {
	m.lock.Lock()
	defer m.lock.Unlock()
	return m.expMovingAvg
}

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
	"math"
	"sync"
	"sync/atomic"
	"time"
)

const (
	dynamicTimeoutIncreaseThresholdPct = 0.33 // Upper threshold for failures in order to increase timeout
	dynamicTimeoutDecreaseThresholdPct = 0.10 // Lower threshold for failures in order to decrease timeout
	dynamicTimeoutLogSize              = 16
	maxDuration                        = math.MaxInt64
	maxDynamicTimeout                  = 24 * time.Hour // Never set timeout bigger than this.
)

// timeouts that are dynamically adapted based on actual usage results
type dynamicTimeout struct {
	timeout int64
	minimum int64
	entries int64
	log     [dynamicTimeoutLogSize]time.Duration
	mutex   sync.Mutex
}

// newDynamicTimeout returns a new dynamic timeout initialized with timeout value
func newDynamicTimeout(timeout, minimum time.Duration) *dynamicTimeout {
	if timeout <= 0 || minimum <= 0 {
		panic("newDynamicTimeout: negative or zero timeout")
	}
	if minimum > timeout {
		minimum = timeout
	}
	return &dynamicTimeout{timeout: int64(timeout), minimum: int64(minimum)}
}

// Timeout returns the current timeout value
func (dt *dynamicTimeout) Timeout() time.Duration {
	return time.Duration(atomic.LoadInt64(&dt.timeout))
}

// LogSuccess logs the duration of a successful action that
// did not hit the timeout
func (dt *dynamicTimeout) LogSuccess(duration time.Duration) {
	dt.logEntry(duration)
}

// LogFailure logs an action that hit the timeout
func (dt *dynamicTimeout) LogFailure() {
	dt.logEntry(maxDuration)
}

// logEntry stores a log entry
func (dt *dynamicTimeout) logEntry(duration time.Duration) {
	if duration < 0 {
		return
	}
	entries := int(atomic.AddInt64(&dt.entries, 1))
	index := entries - 1
	if index < dynamicTimeoutLogSize {
		dt.mutex.Lock()
		dt.log[index] = duration

		// We leak entries while we copy
		if entries == dynamicTimeoutLogSize {

			// Make copy on stack in order to call adjust()
			logCopy := [dynamicTimeoutLogSize]time.Duration{}
			copy(logCopy[:], dt.log[:])

			// reset log entries
			atomic.StoreInt64(&dt.entries, 0)
			dt.mutex.Unlock()

			dt.adjust(logCopy)
			return
		}
		dt.mutex.Unlock()
	}
}

// adjust changes the value of the dynamic timeout based on the
// previous results
func (dt *dynamicTimeout) adjust(entries [dynamicTimeoutLogSize]time.Duration) {
	failures, max := 0, time.Duration(0)
	for _, dur := range entries[:] {
		if dur == maxDuration {
			failures++
		} else if dur > max {
			max = dur
		}
	}

	failPct := float64(failures) / float64(len(entries))

	if failPct > dynamicTimeoutIncreaseThresholdPct {
		// We are hitting the timeout too often, so increase the timeout by 25%
		timeout := atomic.LoadInt64(&dt.timeout) * 125 / 100

		// Set upper cap.
		if timeout > int64(maxDynamicTimeout) {
			timeout = int64(maxDynamicTimeout)
		}
		// Safety, shouldn't happen
		if timeout < dt.minimum {
			timeout = dt.minimum
		}
		atomic.StoreInt64(&dt.timeout, timeout)
	} else if failPct < dynamicTimeoutDecreaseThresholdPct {
		// We are hitting the timeout relatively few times,
		// so decrease the timeout towards 25 % of maximum time spent.
		max = max * 125 / 100

		timeout := atomic.LoadInt64(&dt.timeout)
		if max < time.Duration(timeout) {
			// Move 50% toward the max.
			timeout = (int64(max) + timeout) / 2
		}
		if timeout < dt.minimum {
			timeout = dt.minimum
		}
		atomic.StoreInt64(&dt.timeout, timeout)
	}
}

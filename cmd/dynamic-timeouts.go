/*
 * Minio Cloud Storage, (C) 2016 Minio, Inc.
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

package cmd

import (
	"sync/atomic"
	"time"
)

const (
	dynamicTimeoutIncreaseThresholdPct = 0.33 // Upper threshold for failures in order to increase timeout
	dynamicTimeoutDecreaseThresholdPct = 0.10 // Lower threshold for failures in order to decrease timeout
	dynamicTimeoutLogSize              = 16
	maxDuration                        = time.Duration(1<<63 - 1)
)

// timeouts that are dynamically adapted based on actual usage results
type dynamicTimout struct {
	timeout int64
	minimum int64
	entries int64
	log     [dynamicTimeoutLogSize]time.Duration
}

// newDynamicTimeout returns a new dynamic timeout initialized with timeout value
func newDynamicTimeout(timeout, minimum time.Duration) *dynamicTimout {
	return &dynamicTimout{timeout: int64(timeout), minimum: int64(minimum)}
}

// Timeout returns the current timeout value
func (dt *dynamicTimout) Timeout() time.Duration {
	return time.Duration(atomic.LoadInt64(&dt.timeout))
}

// LogSuccess logs the duration of a successful action that
// did not hit the timeout
func (dt *dynamicTimout) LogSuccess(duration time.Duration) {
	dt.logEntry(duration)
}

// LogFailure logs an action that hit the timeout
func (dt *dynamicTimout) LogFailure() {
	dt.logEntry(maxDuration)
}

// logEntry stores a log entry
func (dt *dynamicTimout) logEntry(duration time.Duration) {
	entries := int(atomic.AddInt64(&dt.entries, 1))
	index := entries - 1
	if index < dynamicTimeoutLogSize {
		dt.log[index] = duration
	}
	if entries == dynamicTimeoutLogSize {
		dt.adjust(entries)
	}
}

// adjust changes the value of the dynamic timeout based on the
// previous results
func (dt *dynamicTimout) adjust(entries int) {

	failures, average := 0, 0
	for i := 0; i < entries; i++ {
		if dt.log[i] == maxDuration {
			failures++
		} else {
			average += int(dt.log[i])
		}
	}
	if failures < entries {
		average /= entries - failures
	}

	timeOutHitPct := float64(failures) / float64(entries)

	if timeOutHitPct > dynamicTimeoutIncreaseThresholdPct {
		// We are hitting the timeout too often, so increase the timeout by 25%
		timeout := atomic.LoadInt64(&dt.timeout) * 125 / 100
		atomic.StoreInt64(&dt.timeout, timeout)
	} else if timeOutHitPct < dynamicTimeoutDecreaseThresholdPct {
		// We are hitting the timeout relatively few times, so decrease the timeout
		average = average * 125 / 100 // Add buffer of 25% on top of average

		timeout := (atomic.LoadInt64(&dt.timeout) + int64(average)) / 2 // Middle between current timeout and average success
		if timeout < dt.minimum {
			timeout = dt.minimum
		}
		atomic.StoreInt64(&dt.timeout, timeout)
	}

	// reset log entries
	atomic.StoreInt64(&dt.entries, 0)
}

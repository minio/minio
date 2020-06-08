/*
 * Minio Cloud Storage, (C) 2020 MinIO, Inc.
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

package retry

import (
	"context"
	"math"
	"math/rand"
	"time"
)

// MaxJitter will randomize over the full exponential backoff time
const MaxJitter = 1.0

// NoJitter disables the use of jitter for randomizing the
// exponential backoff time
const NoJitter = 0.0

// defaultTimer implements Timer interface using time.Timer
type defaultTimer struct {
	timer *time.Timer
}

// C returns the timers channel which receives the current time when the timer fires.
func (t *defaultTimer) C() <-chan time.Time {
	return t.timer.C
}

// Start starts the timer to fire after the given duration
// don't use this code concurrently.
func (t *defaultTimer) Start(duration time.Duration) {
	if t.timer == nil {
		t.timer = time.NewTimer(duration)
	} else {
		t.timer.Reset(duration)
	}
}

// Stop is called when the timer is not used anymore and resources may be freed.
func (t *defaultTimer) Stop() {
	if t.timer != nil {
		t.timer.Stop()
	}
}

// NewTimerWithJitter creates a timer with exponentially increasing delays
// until the maximum retry attempts are reached. - this function is a fully
// configurable version, meant for only advanced use cases. For the most part
// one should use newRetryTimerSimple and newRetryTimer.
func NewTimerWithJitter(ctx context.Context, unit time.Duration, cap time.Duration, jitter float64) <-chan int {
	attemptCh := make(chan int)

	// normalize jitter to the range [0, 1.0]
	jitter = math.Max(NoJitter, math.Min(MaxJitter, jitter))

	// computes the exponential backoff duration according to
	// https://www.awsarchitectureblog.com/2015/03/backoff.html
	exponentialBackoffWait := func(attempt int) time.Duration {
		// 1<<uint(attempt) below could overflow, so limit the value of attempt
		const maxAttempt = 30
		if attempt > maxAttempt {
			attempt = maxAttempt
		}
		//sleep = random_between(0, min(cap, base * 2 ** attempt))
		sleep := unit * time.Duration(1<<uint(attempt))
		if sleep > cap {
			sleep = cap
		}
		if jitter != NoJitter {
			sleep -= time.Duration(rand.Float64() * float64(sleep) * jitter)
		}
		return sleep
	}

	go func() {
		nextBackoff := 0
		t := &defaultTimer{}

		defer func() {
			t.Stop()
		}()

		defer close(attemptCh)

		// Channel used to signal after the expiry of backoff wait seconds.
		for {
			select {
			case attemptCh <- nextBackoff:
				nextBackoff++
			case <-ctx.Done():
				return
			}

			t.Start(exponentialBackoffWait(nextBackoff))

			select {
			case <-ctx.Done():
				return
			case <-t.C():
			}
		}
	}()

	// Start reading..
	return attemptCh
}

// Default retry constants.
const (
	defaultRetryUnit = 50 * time.Millisecond  // 50 millisecond.
	defaultRetryCap  = 500 * time.Millisecond // 500 millisecond.
)

// NewTimer creates a timer with exponentially increasing delays
// until the maximum retry attempts are reached. - this function is a
// simpler version with all default values.
func NewTimer(ctx context.Context) <-chan int {
	return NewTimerWithJitter(ctx, defaultRetryUnit, defaultRetryCap, MaxJitter)
}

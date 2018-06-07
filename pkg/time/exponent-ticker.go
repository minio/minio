/*
 * Minio Cloud Storage, (C) 2018 Minio, Inc.
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

package time

import (
	"math"
	"time"
)

// ExponentTicker - holds a channel that delivers `ticks' of a clock at exponential
// interval of minimum duration but not greater than maximum duration.
type ExponentTicker struct {
	timer  *time.Timer
	doneCh chan struct{}
	C      <-chan time.Time
}

// Stop - turns off a ticker. After Stop, no more ticks will be sent. Stop closes the
// channel hence a read from the channel needs to check whether the channel is closed
// or not.
func (ticker *ExponentTicker) Stop() {
	if ticker.timer != nil {
		ticker.timer.Stop()
	}

	if ticker.doneCh != nil {
		close(ticker.doneCh)
	}
}

// NewExponentTicker - returns a new ExponentTicker containing a channel that
// will send `ticks' of a clock at exponential interval of minimum duration
// (min * 2 ** x) but not greater than maximum duration.  min and max
// durations must be greater than zero and max duration must not be less than
// min duration; if not, NewExponentTicker will panic.
// Stop the ticker to release associated resources.
func NewExponentTicker(min, max time.Duration) *ExponentTicker {
	if min <= time.Duration(0) {
		panic("non-positive min duration for NewExponentTicker")
	}

	if max <= time.Duration(0) {
		panic("non-positive max duration for NewExponentTicker")
	}

	if min > max {
		panic("min duration must be greater than max duration for NewExponentTicker")
	}

	var exponent float64
	getExponentDuration := func() time.Duration {
		d := min * time.Duration(math.Exp2(exponent))
		if d > max {
			d = min
			exponent = 0.0
		}
		exponent += 1.0

		return d
	}

	timer := time.NewTimer(getExponentDuration())
	timeCh := make(chan time.Time)
	doneCh := make(chan struct{})

	go func() {
		// Read from timer and return time/status. If doneCh is closed when
		// reading, it returns read failure.
		read := func() (t time.Time, ok bool) {
			select {
			case t, ok = <-timer.C:
				return t, ok
			case <-doneCh:
				return t, false
			}
		}

		// Write given time to timeCh and return status. If doneCh is closed when
		// writing, it returns write failure.
		send := func(t time.Time) bool {
			select {
			case timeCh <- t:
				return true
			case <-doneCh:
				return false
			}
		}

		for {
			t, ok := read()
			if !ok {
				break
			}

			if !send(t) {
				break
			}

			timer.Reset(getExponentDuration())
		}

		close(timeCh)
	}()

	return &ExponentTicker{
		timer:  timer,
		doneCh: doneCh,
		C:      timeCh,
	}
}

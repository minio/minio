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
	"crypto/rand"
	"math/big"
	"time"
)

func getRandomDuration(min, max time.Duration) time.Duration {
	n, err := rand.Int(rand.Reader, big.NewInt(int64(max)))
	if err != nil {
		panic(err)
	}

	d := time.Duration(n.Int64())
	if d < min {
		if d = max - d; d < min {
			d = min
		}
	}

	return d
}

// RandTicker - holds a channel that delivers `ticks' of a clock at random intervals
// between minimum and maximum duration.
type RandTicker struct {
	timer  *time.Timer
	doneCh chan struct{}
	C      <-chan time.Time
}

// Stop - turns off a ticker. After Stop, no more ticks will be sent. Stop closes the
// channel hence a read from the channel needs to check whether the channel is closed
// or not.
func (ticker *RandTicker) Stop() {
	if ticker.timer != nil {
		ticker.timer.Stop()
	}

	if ticker.doneCh != nil {
		close(ticker.doneCh)
	}
}

// NewRandTicker - returns a new RandTicker containing a channel that
// will send `ticks' of a clock at random intervals between minimum and
// maximum duration.  min and max durations must be greater than zero and
// max duration must not be less than min duration; if not, NewRandTicker
// will panic.  Stop the ticker to release associated resources.
func NewRandTicker(min, max time.Duration) *RandTicker {
	if min <= time.Duration(0) {
		panic("non-positive min duration for NewRandTicker")
	}

	if max <= time.Duration(0) {
		panic("non-positive max duration for NewRandTicker")
	}

	if min > max {
		panic("min duration must be greater than max duration for NewRandTicker")
	}

	timer := time.NewTimer(getRandomDuration(min, max))
	timeCh := make(chan time.Time)
	doneCh := make(chan struct{})

	go func() {
		for t := range timer.C {
			select {
			case timeCh <- t:
			case <-doneCh:
				break
			}

			timer.Reset(getRandomDuration(min, max))
		}

		close(timeCh)
	}()

	return &RandTicker{
		timer:  timer,
		doneCh: doneCh,
		C:      timeCh,
	}
}

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
	"sync/atomic"
	"time"
)

const (
	throttleInternal = 250 * time.Millisecond
)

// throttle implements the throttling for bandwidth
type throttle struct {
	generateTicker   *time.Ticker // Ticker to generate available bandwidth
	freeBytes        int64        // unused bytes in the interval
	bytesPerSecond   int64        // max limit for bandwidth
	bytesPerInterval int64        // bytes allocated for the interval
	cond             *sync.Cond   // Used to notify waiting threads for bandwidth availability
}

// newThrottle returns a new bandwidth throttle. Set bytesPerSecond to 0 for no limit
func newThrottle(ctx context.Context, bytesPerSecond int64) *throttle {
	if bytesPerSecond == 0 {
		return &throttle{}
	}
	t := &throttle{
		bytesPerSecond: bytesPerSecond,
		generateTicker: time.NewTicker(throttleInternal),
	}

	t.cond = sync.NewCond(&sync.Mutex{})
	t.SetBandwidth(bytesPerSecond)
	t.freeBytes = t.bytesPerInterval
	go t.generateBandwidth(ctx)
	return t
}

// GetLimitForBytes gets the bytes that are possible to send within the limit
// if want is <= 0 or no bandwidth limit set, returns want.
// Otherwise a value > 0 will always be returned.
func (t *throttle) GetLimitForBytes(want int64) int64 {
	if want <= 0 || atomic.LoadInt64(&t.bytesPerInterval) == 0 {
		return want
	}
	t.cond.L.Lock()
	defer t.cond.L.Unlock()
	for {
		var send int64
		freeBytes := atomic.LoadInt64(&t.freeBytes)
		send = want
		if freeBytes < want {
			send = freeBytes
			if send <= 0 {
				t.cond.Wait()
				continue
			}
		}
		atomic.AddInt64(&t.freeBytes, -send)
		return send
	}
}

// SetBandwidth sets a new bandwidth limit in bytes per second.
func (t *throttle) SetBandwidth(bandwidthBiPS int64) {
	bpi := int64(throttleInternal) * bandwidthBiPS / int64(time.Second)
	atomic.StoreInt64(&t.bytesPerInterval, bpi)
}

// ReleaseUnusedBandwidth releases bandwidth that was allocated for a user
func (t *throttle) ReleaseUnusedBandwidth(bytes int64) {
	atomic.AddInt64(&t.freeBytes, bytes)
}

// generateBandwidth periodically allocates new bandwidth to use
func (t *throttle) generateBandwidth(ctx context.Context) {
	for {
		select {
		case <-t.generateTicker.C:
			// A new window is available
			t.cond.L.Lock()
			atomic.StoreInt64(&t.freeBytes, atomic.LoadInt64(&t.bytesPerInterval))
			t.cond.Broadcast()
			t.cond.L.Unlock()
		case <-ctx.Done():
			return
		default:
		}
	}
}

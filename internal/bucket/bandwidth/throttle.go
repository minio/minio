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
	generateTicker   *time.Ticker    // Ticker to generate available bandwidth
	freeBytes        int64           // unused bytes in the interval
	bytesPerSecond   int64           // max limit for bandwidth
	bytesPerInterval int64           // bytes allocated for the interval
	clusterBandwidth int64           // Cluster wide bandwidth needed for reporting
	cond             *sync.Cond      // Used to notify waiting threads for bandwidth availability
	goGenerate       int64           // Flag to track if generate routine should be running. 0 == stopped
	ctx              context.Context // Context for generate
}

// newThrottle returns a new bandwidth throttle. Set bytesPerSecond to 0 for no limit
func newThrottle(ctx context.Context, bytesPerSecond int64, clusterBandwidth int64) *throttle {
	if bytesPerSecond == 0 {
		return &throttle{}
	}
	t := &throttle{
		bytesPerSecond:   bytesPerSecond,
		generateTicker:   time.NewTicker(throttleInternal),
		clusterBandwidth: clusterBandwidth,
		ctx:              ctx,
	}

	t.cond = sync.NewCond(&sync.Mutex{})
	t.SetBandwidth(bytesPerSecond, clusterBandwidth)
	t.freeBytes = t.bytesPerInterval
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

		// Bandwidth was consumed, start generate routine to allocate bandwidth
		if atomic.CompareAndSwapInt64(&t.goGenerate, 0, 1) {
			go t.generateBandwidth(t.ctx)
		}
		return send
	}
}

// SetBandwidth sets a new bandwidth limit in bytes per second.
func (t *throttle) SetBandwidth(bandwidthBiPS int64, clusterBandwidth int64) {
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
			if atomic.LoadInt64(&t.freeBytes) == atomic.LoadInt64(&t.bytesPerInterval) {
				// No bandwidth consumption stop the routine.
				atomic.StoreInt64(&t.goGenerate, 0)
				return
			}
			// A new window is available
			t.cond.L.Lock()
			atomic.StoreInt64(&t.freeBytes, atomic.LoadInt64(&t.bytesPerInterval))
			t.cond.Broadcast()
			t.cond.L.Unlock()
		case <-ctx.Done():
			return
		}
	}
}

/*
 * Minio Cloud Storage, (C) 2019 Minio, Inc.
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

package net

import (
	"sync/atomic"
	"time"
)

// window of 15 minutes of past recorded net stats
const windowSize = 16

var (
	GlobalNetStats   = &Stats{}
	OneMinWindow     = &Stats{}
	FiveMinWindow    = &Stats{}
	FifteenMinWindow = &Stats{}

	circularIndex = 0
)

func init() {
	slidingWindow := [windowSize]*Stats{}
	for i := range slidingWindow {
		slidingWindow[i] = &Stats{}
	}
	// use a circular queue to hold 15 minutes of past recorded net stats
	// at given time
	slidingCounter := func() {
		for {
			select {
			case <-time.After(1 * time.Minute):
				curr := slidingWindow[circularIndex]
				curr.TotalReceived = GlobalNetStats.TotalReceived
				curr.TotalTransmitted = GlobalNetStats.TotalTransmitted

				OneMinWindow = slidingWindow[minuteSelector(windowSize, 1)]
				FiveMinWindow = slidingWindow[minuteSelector(windowSize, 5)]
				FifteenMinWindow = slidingWindow[minuteSelector(windowSize, 15)]

				circularIndex = circularIndex + 1
				circularIndex = circularIndex % windowSize
			}
		}
	}

	go slidingCounter()
}

// Stats holds information about the amount of bytes
// received and sent by a minio node, both historically and currently.
type Stats struct {
	TotalReceived    uint64 `json:"totalReceived"`
	TotalTransmitted uint64 `json:"totalTransmitted"`
}

// Transmit adds transmitted bytes to the current transmission and total
// transmission data structure in a thread safe manner
func (n *Stats) Transmit(size uint64) {
	go func() {
		atomic.AddUint64(&n.TotalTransmitted, size)
	}()
}

// Receive adds received bytes to the current received and total
// received data structure in a thread safe manner
func (n *Stats) Receive(size uint64) {
	go func() {
		atomic.AddUint64(&n.TotalReceived, size)
	}()
}

func minuteSelector(windowSize, min int) int {
	if min > windowSize {
		panic("invalid minute window selected")
	}
	return (circularIndex + (windowSize - min)) % windowSize
}

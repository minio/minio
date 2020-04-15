/*
 * MinIO Cloud Storage, (C) 2019 MinIO, Inc.
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
	"reflect"
	"sync"
	"time"
)

const (
	globalMergeLookupTimeout = time.Minute * 1 // 1 minutes.
)

// mergeWalk - represents the go routine that does the merge walk.
type mergeWalk struct {
	entryChs   []FileInfoCh
	endWalkCh  chan struct{}   // To signal when mergeWalk go-routine should end.
	endTimerCh chan<- struct{} // To signal when timer go-routine should end.
}

// MergeWalkPool - pool of mergeWalk go routines.
// A mergeWalk is added to the pool by Set() and removed either by
// doing a Release() or if the concerned timer goes off.
// mergeWalkPool's purpose is to maintain active mergeWalk go-routines in a map so that
// it can be looked up across related list calls.
type MergeWalkPool struct {
	pool    map[listParams][]mergeWalk
	timeOut time.Duration
	lock    *sync.Mutex
}

// NewMergeWalkPool - initialize new tree walk pool.
func NewMergeWalkPool(timeout time.Duration) *MergeWalkPool {
	tPool := &MergeWalkPool{
		pool:    make(map[listParams][]mergeWalk),
		timeOut: timeout,
		lock:    &sync.Mutex{},
	}
	return tPool
}

// Release - selects a mergeWalk from the pool based on the input
// listParams, removes it from the pool, and returns the MergeWalkResult
// channel.
// Returns nil if listParams does not have an asccociated mergeWalk.
func (t MergeWalkPool) Release(params listParams) ([]FileInfoCh, chan struct{}) {
	t.lock.Lock()
	defer t.lock.Unlock()
	walks, ok := t.pool[params] // Pick the valid walks.
	if ok {
		if len(walks) > 0 {
			// Pop out the first valid walk entry.
			walk := walks[0]
			walks = walks[1:]
			if len(walks) > 0 {
				t.pool[params] = walks
			} else {
				delete(t.pool, params)
			}
			walk.endTimerCh <- struct{}{}
			return walk.entryChs, walk.endWalkCh
		}
	}
	// Release return nil if params not found.
	return nil, nil
}

// Set - adds a mergeWalk to the mergeWalkPool.
// Also starts a timer go-routine that ends when:
// 1) time.After() expires after t.timeOut seconds.
//    The expiration is needed so that the mergeWalk go-routine resources are freed after a timeout
//    if the S3 client does only partial listing of objects.
// 2) Relase() signals the timer go-routine to end on endTimerCh.
//    During listing the timer should not timeout and end the mergeWalk go-routine, hence the
//    timer go-routine should be ended.
func (t MergeWalkPool) Set(params listParams, resultChs []FileInfoCh, endWalkCh chan struct{}) {
	t.lock.Lock()
	defer t.lock.Unlock()

	// Should be a buffered channel so that Release() never blocks.
	endTimerCh := make(chan struct{}, 1)

	walkInfo := mergeWalk{
		entryChs:   resultChs,
		endWalkCh:  endWalkCh,
		endTimerCh: endTimerCh,
	}

	// Append new walk info.
	t.pool[params] = append(t.pool[params], walkInfo)

	// Timer go-routine which times out after t.timeOut seconds.
	go func(endTimerCh <-chan struct{}, walkInfo mergeWalk) {
		select {
		// Wait until timeOut
		case <-time.After(t.timeOut):
			// Timeout has expired. Remove the mergeWalk from mergeWalkPool and
			// end the mergeWalk go-routine.
			t.lock.Lock()
			walks, ok := t.pool[params]
			if ok {
				// Trick of filtering without allocating
				// https://github.com/golang/go/wiki/SliceTricks#filtering-without-allocating
				nwalks := walks[:0]
				// Look for walkInfo, remove it from the walks list.
				for _, walk := range walks {
					if !reflect.DeepEqual(walk, walkInfo) {
						nwalks = append(nwalks, walk)
					}
				}
				if len(nwalks) == 0 {
					// No more mergeWalk go-routines associated with listParams
					// hence remove map entry.
					delete(t.pool, params)
				} else {
					// There are more mergeWalk go-routines associated with listParams
					// hence save the list in the map.
					t.pool[params] = nwalks
				}
			}
			// Signal the mergeWalk go-routine to die.
			close(endWalkCh)
			t.lock.Unlock()
		case <-endTimerCh:
			return
		}
	}(endTimerCh, walkInfo)
}

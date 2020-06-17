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

// mergeWalkVersions - represents the go routine that does the merge walk versions.
type mergeWalkVersions struct {
	entryChs   []FileInfoVersionsCh
	endWalkCh  chan struct{}   // To signal when mergeWalk go-routine should end.
	endTimerCh chan<- struct{} // To signal when timer go-routine should end.
}

// mergeWalk - represents the go routine that does the merge walk.
type mergeWalk struct {
	added      time.Time
	entryChs   []FileInfoCh
	endWalkCh  chan struct{}   // To signal when mergeWalk go-routine should end.
	endTimerCh chan<- struct{} // To signal when timer go-routine should end.
}

// MergeWalkVersionsPool - pool of mergeWalk go routines.
// A mergeWalk is added to the pool by Set() and removed either by
// doing a Release() or if the concerned timer goes off.
// mergeWalkPool's purpose is to maintain active mergeWalk go-routines in a map so that
// it can be looked up across related list calls.
type MergeWalkVersionsPool struct {
	sync.Mutex
	pool    map[listParams][]mergeWalkVersions
	timeOut time.Duration
}

// NewMergeWalkVersionsPool - initialize new tree walk pool for versions.
func NewMergeWalkVersionsPool(timeout time.Duration) *MergeWalkVersionsPool {
	tPool := &MergeWalkVersionsPool{
		pool:    make(map[listParams][]mergeWalkVersions),
		timeOut: timeout,
	}
	return tPool
}

// Release - similar to mergeWalkPool.Release but for versions.
func (t *MergeWalkVersionsPool) Release(params listParams) ([]FileInfoVersionsCh, chan struct{}) {
	t.Lock()
	defer t.Unlock()
	walks, ok := t.pool[params] // Pick the valid walks.
	if !ok || len(walks) == 0 {
		// Release return nil if params not found.
		return nil, nil
	}

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

// Set - similar to mergeWalkPool.Set but for file versions
func (t *MergeWalkVersionsPool) Set(params listParams, resultChs []FileInfoVersionsCh, endWalkCh chan struct{}) {
	t.Lock()
	defer t.Unlock()

	// Should be a buffered channel so that Release() never blocks.
	endTimerCh := make(chan struct{}, 1)

	walkInfo := mergeWalkVersions{
		entryChs:   resultChs,
		endWalkCh:  endWalkCh,
		endTimerCh: endTimerCh,
	}

	// Append new walk info.
	t.pool[params] = append(t.pool[params], walkInfo)

	// Timer go-routine which times out after t.timeOut seconds.
	go func(endTimerCh <-chan struct{}, walkInfo mergeWalkVersions) {
		select {
		// Wait until timeOut
		case <-time.After(t.timeOut):
			// Timeout has expired. Remove the mergeWalk from mergeWalkPool and
			// end the mergeWalk go-routine.
			t.Lock()
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
			t.Unlock()
		case <-endTimerCh:
			return
		}
	}(endTimerCh, walkInfo)
}

// MergeWalkPool - pool of mergeWalk go routines.
// A mergeWalk is added to the pool by Set() and removed either by
// doing a Release() or if the concerned timer goes off.
// mergeWalkPool's purpose is to maintain active mergeWalk go-routines in a map so that
// it can be looked up across related list calls.
type MergeWalkPool struct {
	sync.Mutex
	pool    map[listParams][]mergeWalk
	timeOut time.Duration
}

// NewMergeWalkPool - initialize new tree walk pool.
func NewMergeWalkPool(timeout time.Duration) *MergeWalkPool {
	tPool := &MergeWalkPool{
		pool:    make(map[listParams][]mergeWalk),
		timeOut: timeout,
	}
	return tPool
}

// Release - selects a mergeWalk from the pool based on the input
// listParams, removes it from the pool, and returns the MergeWalkResult
// channel.
// Returns nil if listParams does not have an associated mergeWalk.
func (t *MergeWalkPool) Release(params listParams) ([]FileInfoCh, chan struct{}) {
	t.Lock()
	defer t.Unlock()
	walks, ok := t.pool[params] // Pick the valid walks.
	if ok {
		if len(walks) > 0 {
			// Pop out the first valid walk entry.
			walk := walks[0]
			walks[0] = mergeWalk{} // clear references.
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
// 2) Release() signals the timer go-routine to end on endTimerCh.
//    During listing the timer should not timeout and end the mergeWalk go-routine, hence the
//    timer go-routine should be ended.
func (t *MergeWalkPool) Set(params listParams, resultChs []FileInfoCh, endWalkCh chan struct{}) {
	t.Lock()
	defer t.Unlock()

	// If we are above the limit delete at least one entry from the pool.
	if len(t.pool) > treeWalkEntryLimit {
		age := time.Now()
		var oldest listParams
		for k, v := range t.pool {
			if len(v) == 0 {
				delete(t.pool, k)
				continue
			}
			// The first element is the oldest, so we only check that.
			if v[0].added.Before(age) {
				oldest = k
			}
		}
		// Invalidate and delete oldest.
		if walks, ok := t.pool[oldest]; ok {
			walk := walks[0]
			walks[0] = mergeWalk{} // clear references.
			walks = walks[1:]
			if len(walks) > 0 {
				t.pool[params] = walks
			} else {
				delete(t.pool, params)
			}
			walk.endTimerCh <- struct{}{}
		}
	}

	// Should be a buffered channel so that Release() never blocks.
	endTimerCh := make(chan struct{}, 1)

	walkInfo := mergeWalk{
		added:      UTCNow(),
		entryChs:   resultChs,
		endWalkCh:  endWalkCh,
		endTimerCh: endTimerCh,
	}

	// Append new walk info.
	walks := t.pool[params]
	if len(walks) < treeWalkSameEntryLimit {
		t.pool[params] = append(walks, walkInfo)
	} else {
		// We are at limit, invalidate oldest, move list down and add new as last.
		walks[0].endTimerCh <- struct{}{}
		copy(walks, walks[1:])
		walks[len(walks)-1] = walkInfo
	}

	// Timer go-routine which times out after t.timeOut seconds.
	go func(endTimerCh <-chan struct{}, walkInfo mergeWalk) {
		select {
		// Wait until timeOut
		case <-time.After(t.timeOut):
			// Timeout has expired. Remove the mergeWalk from mergeWalkPool and
			// end the mergeWalk go-routine.
			t.Lock()
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
			t.Unlock()
		case <-endTimerCh:
			return
		}
	}(endTimerCh, walkInfo)
}

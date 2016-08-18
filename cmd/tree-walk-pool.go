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
	"errors"
	"sync"
	"time"
)

// Global lookup timeout.
const (
	globalLookupTimeout = time.Minute * 30 // 30minutes.
)

// listParams - list object params used for list object map
type listParams struct {
	bucket    string
	recursive bool
	marker    string
	prefix    string
	heal      bool
}

// errWalkAbort - returned by doTreeWalk() if it returns prematurely.
// doTreeWalk() can return prematurely if
// 1) treeWalk is timed out by the timer go-routine.
// 2) there is an error during tree walk.
var errWalkAbort = errors.New("treeWalk abort")

// treeWalk - represents the go routine that does the file tree walk.
type treeWalk struct {
	resultCh   chan treeWalkResult
	endWalkCh  chan struct{}   // To signal when treeWalk go-routine should end.
	endTimerCh chan<- struct{} // To signal when timer go-routine should end.
}

// treeWalkPool - pool of treeWalk go routines.
// A treeWalk is added to the pool by Set() and removed either by
// doing a Release() or if the concerned timer goes off.
// treeWalkPool's purpose is to maintain active treeWalk go-routines in a map so that
// it can be looked up across related list calls.
type treeWalkPool struct {
	pool    map[listParams][]treeWalk
	timeOut time.Duration
	lock    *sync.Mutex
}

// newTreeWalkPool - initialize new tree walk pool.
func newTreeWalkPool(timeout time.Duration) *treeWalkPool {
	tPool := &treeWalkPool{
		pool:    make(map[listParams][]treeWalk),
		timeOut: timeout,
		lock:    &sync.Mutex{},
	}
	return tPool
}

// Release - selects a treeWalk from the pool based on the input
// listParams, removes it from the pool, and returns the treeWalkResult
// channel.
// Returns nil if listParams does not have an asccociated treeWalk.
func (t treeWalkPool) Release(params listParams) (resultCh chan treeWalkResult, endWalkCh chan struct{}) {
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
			return walk.resultCh, walk.endWalkCh
		}
	}
	// Release return nil if params not found.
	return nil, nil
}

// Set - adds a treeWalk to the treeWalkPool.
// Also starts a timer go-routine that ends when:
// 1) time.After() expires after t.timeOut seconds.
//    The expiration is needed so that the treeWalk go-routine resources are freed after a timeout
//    if the S3 client does only partial listing of objects.
// 2) Relase() signals the timer go-routine to end on endTimerCh.
//    During listing the timer should not timeout and end the treeWalk go-routine, hence the
//    timer go-routine should be ended.
func (t treeWalkPool) Set(params listParams, resultCh chan treeWalkResult, endWalkCh chan struct{}) {
	t.lock.Lock()
	defer t.lock.Unlock()

	// Should be a buffered channel so that Release() never blocks.
	endTimerCh := make(chan struct{}, 1)
	walkInfo := treeWalk{
		resultCh:   resultCh,
		endWalkCh:  endWalkCh,
		endTimerCh: endTimerCh,
	}
	// Append new walk info.
	t.pool[params] = append(t.pool[params], walkInfo)

	// Timer go-routine which times out after t.timeOut seconds.
	go func(endTimerCh <-chan struct{}) {
		select {
		// Wait until timeOut
		case <-time.After(t.timeOut):
			// Timeout has expired. Remove the treeWalk from treeWalkPool and
			// end the treeWalk go-routine.
			t.lock.Lock()
			walks, ok := t.pool[params]
			if ok {
				// Look for walkInfo, remove it from the walks list.
				for i, walk := range walks {
					if walk == walkInfo {
						walks = append(walks[:i], walks[i+1:]...)
					}
				}
				if len(walks) == 0 {
					// No more treeWalk go-routines associated with listParams
					// hence remove map entry.
					delete(t.pool, params)
				} else {
					// There are more treeWalk go-routines associated with listParams
					// hence save the list in the map.
					t.pool[params] = walks
				}
			}
			// Signal the treeWalk go-routine to die.
			close(endWalkCh)
			t.lock.Unlock()
		case <-endTimerCh:
			return
		}
	}(endTimerCh)
}

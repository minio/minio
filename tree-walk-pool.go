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

package main

import (
	"errors"
	"sync"
	"time"
)

// Global lookup timeout.
const (
	globalLookupTimeout = time.Minute * 30 // 30minutes.
)

// errWalkAbort - returned by the treeWalker routine, it signals the end of treeWalk.
var errWalkAbort = errors.New("treeWalk abort")

// treeWalkerPoolInfo - tree walker pool info carries temporary walker
// channel stored until timeout is called.
type treeWalkerPoolInfo struct {
	treeWalkerCh     chan treeWalker
	treeWalkerDoneCh chan struct{}
	doneCh           chan<- struct{}
}

// treeWalkerPool - tree walker pool is a set of temporary tree walker
// objects. Any item stored in the pool will be removed automatically at
// a given timeOut value. This pool is safe for use by multiple
// goroutines simultaneously. pool's purpose is to cache tree walker
// channels for later reuse.
type treeWalkerPool struct {
	pool    map[listParams][]treeWalkerPoolInfo
	timeOut time.Duration
	lock    *sync.Mutex
}

// newTreeWalkerPool - initialize new tree walker pool.
func newTreeWalkerPool(timeout time.Duration) *treeWalkerPool {
	tPool := &treeWalkerPool{
		pool:    make(map[listParams][]treeWalkerPoolInfo),
		timeOut: timeout,
		lock:    &sync.Mutex{},
	}
	return tPool
}

// Release - selects an item from the pool based on the input
// listParams, removes it from the pool, and returns treeWalker
// channels. Release will return nil, if listParams is not
// recognized.
func (t treeWalkerPool) Release(params listParams) (treeWalkerCh chan treeWalker, treeWalkerDoneCh chan struct{}) {
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
			walk.doneCh <- struct{}{}
			return walk.treeWalkerCh, walk.treeWalkerDoneCh
		}
	}
	// Release return nil if params not found.
	return nil, nil
}

// Set - adds new list params along with treeWalker channel to the
// pool for future. Additionally this also starts a go routine which
// waits at the configured timeout. Additionally this go-routine is
// also closed pro-actively by 'Release' call when the treeWalker
// item is obtained from the pool.
func (t treeWalkerPool) Set(params listParams, treeWalkerCh chan treeWalker, treeWalkerDoneCh chan struct{}) {
	t.lock.Lock()
	defer t.lock.Unlock()

	// Should be a buffered channel so that Release() never blocks.
	var doneCh = make(chan struct{}, 1)
	walkInfo := treeWalkerPoolInfo{
		treeWalkerCh:     treeWalkerCh,
		treeWalkerDoneCh: treeWalkerDoneCh,
		doneCh:           doneCh,
	}
	// Append new walk info.
	t.pool[params] = append(t.pool[params], walkInfo)

	// Safe expiry of treeWalkerCh after timeout.
	go func(doneCh <-chan struct{}) {
		select {
		// Wait until timeOut
		case <-time.After(t.timeOut):
			t.lock.Lock()
			walks, ok := t.pool[params] // Look for valid walks.
			if ok {
				// Look for walkInfo, remove it from the walks list.
				for i, walk := range walks {
					if walk == walkInfo {
						walks = append(walks[:i], walks[i+1:]...)
					}
				}
				// Walks is empty we have no more pending requests.
				// Remove map entry.
				if len(walks) == 0 {
					delete(t.pool, params)
				} else { // Save the updated walks.
					t.pool[params] = walks
				}
			}
			// Close tree walker for the backing go-routine to die.
			close(treeWalkerDoneCh)
			t.lock.Unlock()
		case <-doneCh:
			return
		}
	}(doneCh)
}

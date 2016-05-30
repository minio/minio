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
	"sync"
	"time"
)

// Global lookup timeout.
const (
	globalLookupTimeout = time.Minute * 30 // 30minutes.
)

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
	treeWalk, ok := t.pool[params]
	if ok {
		if len(treeWalk) > 0 {
			treeWalker := treeWalk[0]
			if len(treeWalk[1:]) > 0 {
				t.pool[params] = treeWalk[1:]
			} else {
				delete(t.pool, params)
			}
			treeWalker.doneCh <- struct{}{}
			return treeWalker.treeWalkerCh, treeWalker.treeWalkerDoneCh
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

	var treeWalkerIdx = len(t.pool[params])
	var doneCh = make(chan struct{})
	t.pool[params] = append(t.pool[params], treeWalkerPoolInfo{
		treeWalkerCh:     treeWalkerCh,
		treeWalkerDoneCh: treeWalkerDoneCh,
		doneCh:           doneCh,
	})

	// Safe expiry of treeWalkerCh after timeout.
	go func(doneCh <-chan struct{}) {
		select {
		// Wait until timeOut
		case <-time.After(t.timeOut):
			t.lock.Lock()
			treeWalk := t.pool[params]
			treeWalk = append(treeWalk[:treeWalkerIdx], treeWalk[treeWalkerIdx+1:]...)
			if len(treeWalk) == 0 {
				delete(t.pool, params)
			} else {
				t.pool[params] = treeWalk
			}
			close(treeWalkerDoneCh)
			t.lock.Unlock()
		case <-doneCh:
			return
		}
	}(doneCh)
}

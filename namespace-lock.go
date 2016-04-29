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

import "sync"

// nsParam - carries name space resource.
type nsParam struct {
	volume string
	path   string
}

// nsLock - provides primitives for locking critical namespace regions.
type nsLock struct {
	*sync.RWMutex
	ref uint
}

// nsLockMap - namespace lock map, provides primitives to Lock,
// Unlock, RLock and RUnlock.
type nsLockMap struct {
	lockMap map[nsParam]*nsLock
	mutex   *sync.Mutex
}

// Global name space lock.
var nsMutex *nsLockMap

// initNSLock - initialize name space lock map.
func initNSLock() {
	nsMutex = &nsLockMap{
		lockMap: make(map[nsParam]*nsLock),
		mutex:   &sync.Mutex{},
	}
}

// Lock - locks the given resource for writes, using a previously
// allocated name space lock or initializing a new one.
func (n *nsLockMap) Lock(volume, path string) {
	n.mutex.Lock()
	defer n.mutex.Unlock()

	param := nsParam{volume, path}
	nsLk, found := n.lockMap[param]
	if !found {
		nsLk = &nsLock{
			RWMutex: &sync.RWMutex{},
			ref:     0,
		}
	}

	// Acquire a write lock and update reference counter.
	nsLk.Lock()
	nsLk.ref++

	n.lockMap[param] = nsLk
}

// Unlock - unlocks any previously acquired write locks.
func (n *nsLockMap) Unlock(volume, path string) {
	n.mutex.Lock()
	defer n.mutex.Unlock()

	param := nsParam{volume, path}
	if nsLk, found := n.lockMap[param]; found {
		// Unlock a write lock and update reference counter.
		nsLk.Unlock()
		if nsLk.ref != 0 {
			nsLk.ref--
		}
		if nsLk.ref != 0 {
			n.lockMap[param] = nsLk
		}
	}
}

// RLock - locks any previously acquired read locks.
func (n *nsLockMap) RLock(volume, path string) {
	n.mutex.Lock()
	defer n.mutex.Unlock()

	param := nsParam{volume, path}
	nsLk, found := n.lockMap[param]
	if !found {
		nsLk = &nsLock{
			RWMutex: &sync.RWMutex{},
			ref:     0,
		}
	}

	// Acquire a read lock and update reference counter.
	nsLk.RLock()
	nsLk.ref++

	n.lockMap[param] = nsLk
}

// RUnlock - unlocks any previously acquired read locks.
func (n *nsLockMap) RUnlock(volume, path string) {
	n.mutex.Lock()
	defer n.mutex.Unlock()

	param := nsParam{volume, path}
	if nsLk, found := n.lockMap[param]; found {
		// Unlock a read lock and update reference counter.
		nsLk.RUnlock()
		if nsLk.ref != 0 {
			nsLk.ref--
		}
		if nsLk.ref != 0 {
			n.lockMap[param] = nsLk
		}
	}
}

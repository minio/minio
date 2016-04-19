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

// nameSpaceParam - carries name space resource.
type nameSpaceParam struct {
	volume string
	path   string
}

// nameSpaceLock - provides primitives for locking critical namespace regions.
type nameSpaceLock struct {
	rwMutex *sync.RWMutex
	rcount  uint
	wcount  uint
}

func (nsLock *nameSpaceLock) InUse() bool {
	return nsLock.rcount != 0 || nsLock.wcount != 0
}

// Lock acquires write lock and increments the namespace counter.
func (nsLock *nameSpaceLock) Lock() {
	nsLock.Lock()
	nsLock.wcount++
}

// Unlock releases write lock and decrements the namespace counter.
func (nsLock *nameSpaceLock) Unlock() {
	nsLock.Unlock()
	if nsLock.wcount != 0 {
		nsLock.wcount--
	}
}

// RLock acquires read lock and increments the namespace counter.
func (nsLock *nameSpaceLock) RLock() {
	nsLock.RLock()
	nsLock.rcount++
}

// RUnlock release read lock and decrements the namespace counter.
func (nsLock *nameSpaceLock) RUnlock() {
	nsLock.RUnlock()
	if nsLock.rcount != 0 {
		nsLock.rcount--
	}
}

// newNSLock - provides a new instance of namespace locking primitives.
func newNSLock() *nameSpaceLock {
	return &nameSpaceLock{
		rwMutex: &sync.RWMutex{},
		rcount:  0,
		wcount:  0,
	}
}

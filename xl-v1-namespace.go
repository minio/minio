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
	count   uint
}

func (nsLock *nameSpaceLock) InUse() bool {
	return nsLock.count != 0
}

// Lock acquires write lock and increments the namespace counter.
func (nsLock *nameSpaceLock) Lock() {
	nsLock.rwMutex.Lock()
	nsLock.count++
}

// Unlock releases write lock and decrements the namespace counter.
func (nsLock *nameSpaceLock) Unlock() {
	nsLock.rwMutex.Unlock()
	if nsLock.count != 0 {
		nsLock.count--
	}
}

// RLock acquires read lock and increments the namespace counter.
func (nsLock *nameSpaceLock) RLock() {
	nsLock.rwMutex.RLock()
	nsLock.count++
}

// RUnlock release read lock and decrements the namespace counter.
func (nsLock *nameSpaceLock) RUnlock() {
	nsLock.rwMutex.RUnlock()
	if nsLock.count != 0 {
		nsLock.count--
	}
}

// newNSLock - provides a new instance of namespace locking primitives.
func newNSLock() *nameSpaceLock {
	return &nameSpaceLock{
		rwMutex: &sync.RWMutex{},
		count:   0,
	}
}

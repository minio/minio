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
	"fmt"
	slashpath "path"
	"strings"
	"sync"
)

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

func (n *nsLockMap) getParentLock(volume, path string) (nsLck *nsLock) {
	path2 := slashpath.Clean(path)
	tokens2 := strings.Split(path2, "/")
	currDepth := len(tokens2)
	for nsParam, lock := range n.lockMap {
		if nsParam.volume != volume {
			// As volume names are different, look for next entry in the map.
			continue
		}

		path1 := slashpath.Clean(nsParam.path)
		if path1 == path2 {
			// As both paths are same, return existing lock
			nsLck = lock
			return
		}

		tokens1 := strings.Split(path1, "/")
		fmt.Println("tokens1 =", tokens1, "tokens2 =", tokens2)
		fmt.Println("nsLck =", nsLck)
		if tokens1[0] != tokens2[0] {
			continue
		}

		if len(tokens1) == len(tokens2) {
			continue
		}

		maxDepth := len(tokens1)
		if len(tokens2) < maxDepth {
			maxDepth = len(tokens2)
		}

		if len(tokens1) < len(tokens2) {
			depth := 1
			for i := 1; i < maxDepth; i++ {
				if tokens1[i] == tokens2[i] {
					depth++
				} else {
					break
				}
			}

			if depth < currDepth {
				currDepth = depth
				nsLck = lock
			}
		}
	}

	fmt.Println("nsLck =", nsLck)
	return
}

// Lock the namespace resource.
func (n *nsLockMap) lock(volume, path string, readLock bool) {
	n.mutex.Lock()

	param := nsParam{volume, path}
	nsLk, found := n.lockMap[param]
	if !found {
		nsLk = n.getParentLock(volume, path)
		if nsLk == nil {
			nsLk = &nsLock{
				RWMutex: &sync.RWMutex{},
				ref:     0,
			}
		}
		n.lockMap[param] = nsLk
	}
	nsLk.ref++
	// Unlock map before Locking NS which might block.
	n.mutex.Unlock()

	// Locking here can block.
	if readLock {
		nsLk.RLock()
	} else {
		nsLk.Lock()
	}
}

// Unlock the namespace resource.
func (n *nsLockMap) unlock(volume, path string, readLock bool) {
	// nsLk.Unlock() will not block, hence locking the map for the entire function is fine.
	n.mutex.Lock()
	defer n.mutex.Unlock()

	param := nsParam{volume, path}
	if nsLk, found := n.lockMap[param]; found {
		if readLock {
			nsLk.RUnlock()
		} else {
			nsLk.Unlock()
		}
		if nsLk.ref == 0 {
			errorIf(errors.New("Namespace reference count cannot be 0."), "Invalid reference count detected.")
		}
		if nsLk.ref != 0 {
			nsLk.ref--
		}
		if nsLk.ref == 0 {
			// Remove from the map if there are no more references.
			delete(n.lockMap, param)
		}
	}
}

// Lock - locks the given resource for writes, using a previously
// allocated name space lock or initializing a new one.
func (n *nsLockMap) Lock(volume, path string) {
	readLock := false
	n.lock(volume, path, readLock)
}

// Unlock - unlocks any previously acquired write locks.
func (n *nsLockMap) Unlock(volume, path string) {
	readLock := false
	n.unlock(volume, path, readLock)
}

// RLock - locks any previously acquired read locks.
func (n *nsLockMap) RLock(volume, path string) {
	readLock := true
	n.lock(volume, path, readLock)
}

// RUnlock - unlocks any previously acquired read locks.
func (n *nsLockMap) RUnlock(volume, path string) {
	readLock := true
	n.unlock(volume, path, readLock)
}

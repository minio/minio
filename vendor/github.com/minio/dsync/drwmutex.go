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

package dsync

import (
	"fmt"
	"sync"
)

const maxReaders = 8

type DRWMutex struct {
	rArray       []*DMutex
	rLockedArray []bool
	w            DMutex     // held if there are pending writers
	m            sync.Mutex // Mutex to prevent multiple simultaneous locks from this node
	m2           sync.Mutex // Mutex to prevent multiple simultaneous locks from this node
}

func NewDRWMutex(name string) (drw *DRWMutex) {

	rArray := make([]*DMutex, maxReaders)
	rLockedArray := make([]bool, maxReaders)

	for r := 0; r < maxReaders; r++ {
		rArray[r] = &DMutex{Name: fmt.Sprintf("%s-r%d", name, r)}
	}

	return &DRWMutex{
		rArray:       rArray,
		rLockedArray: rLockedArray,
		w:            DMutex{Name: name + "-w"}}
}

// RLock locks drw for reading.
func (drw *DRWMutex) RLock() {

	drw.m.Lock()
	defer drw.m.Unlock()

	// Check if no write is active, block otherwise
	// Can skip this?
	drw.w.Lock()
	drw.w.Unlock()

	// Lock either one of the reader locks
	for i := 0; ; i++ {
		drw.rLockedArray[i%maxReaders] = drw.rArray[i%maxReaders].tryLockTimeout()
		if drw.rLockedArray[i%maxReaders] {
			return
		}
	}
}

// RUnlock undoes a single RLock call;
// it does not affect other simultaneous readers.
// It is a run-time error if rw is not locked for reading
// on entry to RUnlock.
func (drw *DRWMutex) RUnlock() {

	drw.m.Lock()
	defer drw.m.Unlock()

	// Unlock whichever readlock that was acquired)
	for r := 0; r < maxReaders; r++ {
		if drw.rLockedArray[r] {
			drw.rArray[r].Unlock()
			drw.rLockedArray[r] = false
			// we only want to release a single read lock at a time
			break
		}
	}
}

// Lock locks rw for writing.
// If the lock is already locked for reading or writing,
// Lock blocks until the lock is available.
// To ensure that the lock eventually becomes available,
// a blocked Lock call excludes new readers from acquiring
// the lock.
func (drw *DRWMutex) Lock() {

	drw.m.Lock()
	defer drw.m.Unlock()

	// First, resolve competition with other writers.
	drw.w.Lock()

	// Acquire all read locks.
	var wg sync.WaitGroup
	wg.Add(maxReaders)

	for r := 0; r < maxReaders; r++ {
		go func(r int) {
			defer wg.Done()
			drw.rArray[r].Lock()
			drw.rLockedArray[r] = true
		}(r)
	}

	wg.Wait()
}

// Unlock unlocks rw for writing.  It is a run-time error if rw is
// not locked for writing on entry to Unlock.
//
// As with Mutexes, a locked RWMutex is not associated with a particular
// goroutine.  One goroutine may RLock (Lock) an RWMutex and then
// arrange for another goroutine to RUnlock (Unlock) it.
func (drw *DRWMutex) Unlock() {

	drw.m.Lock()
	defer drw.m.Unlock()

	for r := 0; r < maxReaders; r++ {
		if !drw.rLockedArray[r] {
			panic("dsync: unlock of unlocked distributed rwmutex")
		}
	}

	// Unlock all read locks
	for r := 0; r < maxReaders; r++ {
		drw.rArray[r].Unlock()
		drw.rLockedArray[r] = false
	}

	// Allow other writers to proceed.
	drw.w.Unlock()
}

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

package dsync_test

import (
	"fmt"
	"sync"

	. "github.com/minio/minio/pkg/dsync"
)

const WriteLock = -1

type lockServer struct {
	mutex sync.Mutex
	// Map of locks, with negative value indicating (exclusive) write lock
	// and positive values indicating number of read locks
	lockMap map[string]int64
}

func (l *lockServer) Lock(args *LockArgs, reply *bool) error {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	if _, *reply = l.lockMap[args.Resources[0]]; !*reply {
		l.lockMap[args.Resources[0]] = WriteLock // No locks held on the given name, so claim write lock
	}
	*reply = !*reply // Negate *reply to return true when lock is granted or false otherwise
	return nil
}

func (l *lockServer) Unlock(args *LockArgs, reply *bool) error {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	var locksHeld int64
	if locksHeld, *reply = l.lockMap[args.Resources[0]]; !*reply { // No lock is held on the given name
		return fmt.Errorf("Unlock attempted on an unlocked entity: %s", args.Resources[0])
	}
	if *reply = locksHeld == WriteLock; !*reply { // Unless it is a write lock
		return fmt.Errorf("Unlock attempted on a read locked entity: %s (%d read locks active)", args.Resources[0], locksHeld)
	}
	delete(l.lockMap, args.Resources[0]) // Remove the write lock
	return nil
}

const ReadLock = 1

func (l *lockServer) RLock(args *LockArgs, reply *bool) error {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	var locksHeld int64
	if locksHeld, *reply = l.lockMap[args.Resources[0]]; !*reply {
		l.lockMap[args.Resources[0]] = ReadLock // No locks held on the given name, so claim (first) read lock
		*reply = true
	} else {
		if *reply = locksHeld != WriteLock; *reply { // Unless there is a write lock
			l.lockMap[args.Resources[0]] = locksHeld + ReadLock // Grant another read lock
		}
	}
	return nil
}

func (l *lockServer) RUnlock(args *LockArgs, reply *bool) error {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	var locksHeld int64
	if locksHeld, *reply = l.lockMap[args.Resources[0]]; !*reply { // No lock is held on the given name
		return fmt.Errorf("RUnlock attempted on an unlocked entity: %s", args.Resources[0])
	}
	if *reply = locksHeld != WriteLock; !*reply { // A write-lock is held, cannot release a read lock
		return fmt.Errorf("RUnlock attempted on a write locked entity: %s", args.Resources[0])
	}
	if locksHeld > ReadLock {
		l.lockMap[args.Resources[0]] = locksHeld - ReadLock // Remove one of the read locks held
	} else {
		delete(l.lockMap, args.Resources[0]) // Remove the (last) read lock
	}
	return nil
}

func (l *lockServer) ForceUnlock(args *LockArgs, reply *bool) error {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	if len(args.UID) != 0 {
		return fmt.Errorf("ForceUnlock called with non-empty UID: %s", args.UID)
	}
	delete(l.lockMap, args.Resources[0]) // Remove the lock (irrespective of write or read lock)
	*reply = true
	return nil
}

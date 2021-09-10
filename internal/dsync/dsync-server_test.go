// Copyright (c) 2015-2021 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package dsync

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

const WriteLock = -1

type lockServer struct {
	mutex sync.Mutex
	// Map of locks, with negative value indicating (exclusive) write lock
	// and positive values indicating number of read locks
	lockMap map[string]int64

	// Refresh returns lock not found if set to true
	lockNotFound bool

	// Set to true if you want peers servers to do not respond
	responseDelay int64
}

func (l *lockServer) setRefreshReply(refreshed bool) {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	l.lockNotFound = !refreshed
}

func (l *lockServer) setResponseDelay(responseDelay time.Duration) {
	atomic.StoreInt64(&l.responseDelay, int64(responseDelay))
}

func (l *lockServer) Lock(args *LockArgs, reply *bool) error {
	if d := atomic.LoadInt64(&l.responseDelay); d != 0 {
		time.Sleep(time.Duration(d))
	}

	l.mutex.Lock()
	defer l.mutex.Unlock()
	if _, *reply = l.lockMap[args.Resources[0]]; !*reply {
		l.lockMap[args.Resources[0]] = WriteLock // No locks held on the given name, so claim write lock
	}
	*reply = !*reply // Negate *reply to return true when lock is granted or false otherwise
	return nil
}

func (l *lockServer) Unlock(args *LockArgs, reply *bool) error {
	if d := atomic.LoadInt64(&l.responseDelay); d != 0 {
		time.Sleep(time.Duration(d))
	}

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
	if d := atomic.LoadInt64(&l.responseDelay); d != 0 {
		time.Sleep(time.Duration(d))
	}

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
	if d := atomic.LoadInt64(&l.responseDelay); d != 0 {
		time.Sleep(time.Duration(d))
	}

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

func (l *lockServer) Refresh(args *LockArgs, reply *bool) error {
	if d := atomic.LoadInt64(&l.responseDelay); d != 0 {
		time.Sleep(time.Duration(d))
	}

	l.mutex.Lock()
	defer l.mutex.Unlock()
	*reply = !l.lockNotFound
	return nil
}

func (l *lockServer) ForceUnlock(args *LockArgs, reply *bool) error {
	if d := atomic.LoadInt64(&l.responseDelay); d != 0 {
		time.Sleep(time.Duration(d))
	}

	l.mutex.Lock()
	defer l.mutex.Unlock()
	if len(args.UID) != 0 {
		return fmt.Errorf("ForceUnlock called with non-empty UID: %s", args.UID)
	}
	delete(l.lockMap, args.Resources[0]) // Remove the lock (irrespective of write or read lock)
	*reply = true
	return nil
}

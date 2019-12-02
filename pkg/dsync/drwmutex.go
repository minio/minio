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
	"context"
	"fmt"
	golog "log"
	"math"
	"math/rand"
	"os"
	"path"
	"runtime"
	"sync"
	"time"
)

// Indicator if logging is enabled.
var dsyncLog bool

func init() {
	// Check for MINIO_DSYNC_TRACE env variable, if set logging will be enabled for failed REST operations.
	dsyncLog = os.Getenv("MINIO_DSYNC_TRACE") == "1"
	rand.Seed(time.Now().UnixNano())
}

func log(msg ...interface{}) {
	if dsyncLog {
		golog.Println(msg...)
	}
}

// DRWMutexAcquireTimeout - tolerance limit to wait for lock acquisition before.
const DRWMutexAcquireTimeout = 1 * time.Second // 1 second.
const drwMutexInfinite = time.Duration(1<<63 - 1)

// A DRWMutex is a distributed mutual exclusion lock.
type DRWMutex struct {
	Name         string
	writeLocks   []string   // Array of nodes that granted a write lock
	readersLocks [][]string // Array of array of nodes that granted reader locks
	m            sync.Mutex // Mutex to prevent multiple simultaneous locks from this node
	clnt         *Dsync
	ctx          context.Context
}

// Granted - represents a structure of a granted lock.
type Granted struct {
	index   int
	lockUID string // Locked if set with UID string, unlocked if empty
}

func (g *Granted) isLocked() bool {
	return isLocked(g.lockUID)
}

func isLocked(uid string) bool {
	return len(uid) > 0
}

// NewDRWMutex - initializes a new dsync RW mutex.
func NewDRWMutex(ctx context.Context, name string, clnt *Dsync) *DRWMutex {
	return &DRWMutex{
		Name:       name,
		writeLocks: make([]string, len(clnt.GetLockersFn())),
		clnt:       clnt,
		ctx:        ctx,
	}
}

// Lock holds a write lock on dm.
//
// If the lock is already in use, the calling go routine
// blocks until the mutex is available.
func (dm *DRWMutex) Lock(id, source string) {

	isReadLock := false
	dm.lockBlocking(drwMutexInfinite, id, source, isReadLock)
}

// GetLock tries to get a write lock on dm before the timeout elapses.
//
// If the lock is already in use, the calling go routine
// blocks until either the mutex becomes available and return success or
// more time has passed than the timeout value and return false.
func (dm *DRWMutex) GetLock(id, source string, timeout time.Duration) (locked bool) {

	isReadLock := false
	return dm.lockBlocking(timeout, id, source, isReadLock)
}

// RLock holds a read lock on dm.
//
// If one or more read locks are already in use, it will grant another lock.
// Otherwise the calling go routine blocks until the mutex is available.
func (dm *DRWMutex) RLock(id, source string) {

	isReadLock := true
	dm.lockBlocking(drwMutexInfinite, id, source, isReadLock)
}

// GetRLock tries to get a read lock on dm before the timeout elapses.
//
// If one or more read locks are already in use, it will grant another lock.
// Otherwise the calling go routine blocks until either the mutex becomes
// available and return success or more time has passed than the timeout
// value and return false.
func (dm *DRWMutex) GetRLock(id, source string, timeout time.Duration) (locked bool) {

	isReadLock := true
	return dm.lockBlocking(timeout, id, source, isReadLock)
}

// lockBlocking will try to acquire either a read or a write lock
//
// The function will loop using a built-in timing randomized back-off
// algorithm until either the lock is acquired successfully or more
// time has elapsed than the timeout value.
func (dm *DRWMutex) lockBlocking(timeout time.Duration, id, source string, isReadLock bool) (locked bool) {
	doneCh, start := make(chan struct{}), time.Now().UTC()
	defer close(doneCh)

	restClnts := dm.clnt.GetLockersFn()

	// Use incremental back-off algorithm for repeated attempts to acquire the lock
	for range newRetryTimerSimple(doneCh) {
		select {
		case <-dm.ctx.Done():
			return
		default:
		}

		// Create temp array on stack.
		locks := make([]string, len(restClnts))

		// Try to acquire the lock.
		success := lock(dm.clnt, &locks, dm.Name, id, source, isReadLock)
		if success {
			dm.m.Lock()

			// If success, copy array to object
			if isReadLock {
				// Append new array of strings at the end
				dm.readersLocks = append(dm.readersLocks, make([]string, len(restClnts)))
				// and copy stack array into last spot
				copy(dm.readersLocks[len(dm.readersLocks)-1], locks[:])
			} else {
				copy(dm.writeLocks, locks[:])
			}

			dm.m.Unlock()
			return true
		}
		if time.Now().UTC().Sub(start) >= timeout { // Are we past the timeout?
			break
		}
		// Failed to acquire the lock on this attempt, incrementally wait
		// for a longer back-off time and try again afterwards.
	}
	return false
}

// lock tries to acquire the distributed lock, returning true or false.
func lock(ds *Dsync, locks *[]string, lockName, id, source string, isReadLock bool) bool {

	restClnts := ds.GetLockersFn()

	// Create buffered channel of size equal to total number of nodes.
	ch := make(chan Granted, len(restClnts))
	defer close(ch)

	var wg sync.WaitGroup
	for index, c := range restClnts {

		wg.Add(1)
		// broadcast lock request to all nodes
		go func(index int, isReadLock bool, c NetLocker) {
			defer wg.Done()

			args := LockArgs{
				UID:      id,
				Resource: lockName,
				Source:   source,
			}

			var locked bool
			var err error
			if isReadLock {
				if locked, err = c.RLock(args); err != nil {
					log("Unable to call RLock", err)
				}
			} else {
				if locked, err = c.Lock(args); err != nil {
					log("Unable to call Lock", err)
				}
			}

			g := Granted{index: index}
			if locked {
				g.lockUID = args.UID
			}

			ch <- g

		}(index, isReadLock, c)
	}

	quorum := false

	wg.Add(1)
	go func(isReadLock bool) {

		// Wait until we have either
		//
		// a) received all lock responses
		// b) received too many 'non-'locks for quorum to be still possible
		// c) time out
		//
		i, locksFailed := 0, 0
		done := false
		timeout := time.After(DRWMutexAcquireTimeout)

		dquorum := int(len(restClnts)/2) + 1
		dquorumReads := int(math.Ceil(float64(len(restClnts)) / 2.0))

		for ; i < len(restClnts); i++ { // Loop until we acquired all locks

			select {
			case grant := <-ch:
				if grant.isLocked() {
					// Mark that this node has acquired the lock
					(*locks)[grant.index] = grant.lockUID
				} else {
					locksFailed++
					if !isReadLock && locksFailed > len(restClnts)-dquorum ||
						isReadLock && locksFailed > len(restClnts)-dquorumReads {
						// We know that we are not going to get the lock anymore,
						// so exit out and release any locks that did get acquired
						done = true
						// Increment the number of grants received from the buffered channel.
						i++
						releaseAll(ds, locks, lockName, isReadLock, restClnts)
					}
				}
			case <-timeout:
				done = true
				// timeout happened, maybe one of the nodes is slow, count
				// number of locks to check whether we have quorum or not
				if !quorumMet(locks, isReadLock, dquorum, dquorumReads) {
					releaseAll(ds, locks, lockName, isReadLock, restClnts)
				}
			}

			if done {
				break
			}
		}

		// Count locks in order to determine whether we have quorum or not
		quorum = quorumMet(locks, isReadLock, dquorum, dquorumReads)

		// Signal that we have the quorum
		wg.Done()

		// Wait for the other responses and immediately release the locks
		// (do not add them to the locks array because the DRWMutex could
		//  already has been unlocked again by the original calling thread)
		for ; i < len(restClnts); i++ {
			grantToBeReleased := <-ch
			if grantToBeReleased.isLocked() {
				// release lock
				sendRelease(ds, restClnts[grantToBeReleased.index], lockName,
					grantToBeReleased.lockUID, isReadLock)
			}
		}
	}(isReadLock)

	wg.Wait()

	return quorum
}

// quorumMet determines whether we have acquired the required quorum of underlying locks or not
func quorumMet(locks *[]string, isReadLock bool, quorum, quorumReads int) bool {

	count := 0
	for _, uid := range *locks {
		if isLocked(uid) {
			count++
		}
	}

	var metQuorum bool
	if isReadLock {
		metQuorum = count >= quorumReads
	} else {
		metQuorum = count >= quorum
	}

	return metQuorum
}

// releaseAll releases all locks that are marked as locked
func releaseAll(ds *Dsync, locks *[]string, lockName string, isReadLock bool, restClnts []NetLocker) {
	for lock := 0; lock < len(restClnts); lock++ {
		if isLocked((*locks)[lock]) {
			sendRelease(ds, restClnts[lock], lockName, (*locks)[lock], isReadLock)
			(*locks)[lock] = ""
		}
	}
}

// Unlock unlocks the write lock.
//
// It is a run-time error if dm is not locked on entry to Unlock.
func (dm *DRWMutex) Unlock() {

	restClnts := dm.clnt.GetLockersFn()
	// create temp array on stack
	locks := make([]string, len(restClnts))

	{
		dm.m.Lock()
		defer dm.m.Unlock()

		// Check if minimally a single bool is set in the writeLocks array
		lockFound := false
		for _, uid := range dm.writeLocks {
			if isLocked(uid) {
				lockFound = true
				break
			}
		}
		if !lockFound {
			panic("Trying to Unlock() while no Lock() is active")
		}

		// Copy write locks to stack array
		copy(locks, dm.writeLocks[:])
		// Clear write locks array
		dm.writeLocks = make([]string, len(restClnts))
	}

	isReadLock := false
	unlock(dm.clnt, locks, dm.Name, isReadLock, restClnts)
}

// RUnlock releases a read lock held on dm.
//
// It is a run-time error if dm is not locked on entry to RUnlock.
func (dm *DRWMutex) RUnlock() {

	// create temp array on stack
	restClnts := dm.clnt.GetLockersFn()

	locks := make([]string, len(restClnts))
	{
		dm.m.Lock()
		defer dm.m.Unlock()
		if len(dm.readersLocks) == 0 {
			panic("Trying to RUnlock() while no RLock() is active")
		}
		// Copy out first element to release it first (FIFO)
		copy(locks, dm.readersLocks[0][:])
		// Drop first element from array
		dm.readersLocks = dm.readersLocks[1:]
	}

	isReadLock := true
	unlock(dm.clnt, locks, dm.Name, isReadLock, restClnts)
}

func unlock(ds *Dsync, locks []string, name string, isReadLock bool, restClnts []NetLocker) {

	// We don't need to synchronously wait until we have released all the locks (or the quorum)
	// (a subsequent lock will retry automatically in case it would fail to get quorum)

	for index, c := range restClnts {

		if isLocked(locks[index]) {
			// broadcast lock release to all nodes that granted the lock
			sendRelease(ds, c, name, locks[index], isReadLock)
		}
	}
}

// sendRelease sends a release message to a node that previously granted a lock
func sendRelease(ds *Dsync, c NetLocker, name, uid string, isReadLock bool) {
	args := LockArgs{
		UID:      uid,
		Resource: name,
	}
	if isReadLock {
		if _, err := c.RUnlock(args); err != nil {
			log("Unable to call RUnlock", err)
		}
	} else {
		if _, err := c.Unlock(args); err != nil {
			log("Unable to call Unlock", err)
		}
	}
}

// DRLocker returns a sync.Locker interface that implements
// the Lock and Unlock methods by calling drw.RLock and drw.RUnlock.
func (dm *DRWMutex) DRLocker() sync.Locker {
	return (*drlocker)(dm)
}

type drlocker DRWMutex

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func randString(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

func getSource() string {
	var funcName string
	pc, filename, lineNum, ok := runtime.Caller(2)
	if ok {
		filename = path.Base(filename)
		funcName = runtime.FuncForPC(pc).Name()
	} else {
		filename = "<unknown>"
		lineNum = 0
	}

	return fmt.Sprintf("[%s:%d:%s()]", filename, lineNum, funcName)
}

func (dr *drlocker) Lock()   { (*DRWMutex)(dr).RLock(randString(16), getSource()) }
func (dr *drlocker) Unlock() { (*DRWMutex)(dr).RUnlock() }

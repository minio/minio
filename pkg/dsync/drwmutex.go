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
	"errors"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/minio/minio/pkg/console"
)

// Indicator if logging is enabled.
var dsyncLog bool

func init() {
	// Check for MINIO_DSYNC_TRACE env variable, if set logging will be enabled for failed REST operations.
	dsyncLog = os.Getenv("MINIO_DSYNC_TRACE") == "1"
	rand.Seed(time.Now().UnixNano())
}

func log(format string, data ...interface{}) {
	if dsyncLog {
		console.Printf(format, data...)
	}
}

// DRWMutexAcquireTimeout - tolerance limit to wait for lock acquisition before.
const DRWMutexAcquireTimeout = 1 * time.Second // 1 second.
const drwMutexInfinite = 1<<63 - 1

// A DRWMutex is a distributed mutual exclusion lock.
type DRWMutex struct {
	Names        []string
	writeLocks   []string   // Array of nodes that granted a write lock
	readersLocks [][]string // Array of array of nodes that granted reader locks
	m            sync.Mutex // Mutex to prevent multiple simultaneous locks from this node
	clnt         *Dsync
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
func NewDRWMutex(clnt *Dsync, names ...string) *DRWMutex {
	restClnts, _ := clnt.GetLockers()
	return &DRWMutex{
		writeLocks: make([]string, len(restClnts)),
		Names:      names,
		clnt:       clnt,
	}
}

// Lock holds a write lock on dm.
//
// If the lock is already in use, the calling go routine
// blocks until the mutex is available.
func (dm *DRWMutex) Lock(id, source string) {

	isReadLock := false
	dm.lockBlocking(context.Background(), id, source, isReadLock, Options{
		Timeout: drwMutexInfinite,
	})
}

// Options lock options.
type Options struct {
	Timeout   time.Duration
	Tolerance int
}

// GetLock tries to get a write lock on dm before the timeout elapses.
//
// If the lock is already in use, the calling go routine
// blocks until either the mutex becomes available and return success or
// more time has passed than the timeout value and return false.
func (dm *DRWMutex) GetLock(ctx context.Context, id, source string, opts Options) (locked bool) {

	isReadLock := false
	return dm.lockBlocking(ctx, id, source, isReadLock, opts)
}

// RLock holds a read lock on dm.
//
// If one or more read locks are already in use, it will grant another lock.
// Otherwise the calling go routine blocks until the mutex is available.
func (dm *DRWMutex) RLock(id, source string) {

	isReadLock := true
	dm.lockBlocking(context.Background(), id, source, isReadLock, Options{
		Timeout: drwMutexInfinite,
	})
}

// GetRLock tries to get a read lock on dm before the timeout elapses.
//
// If one or more read locks are already in use, it will grant another lock.
// Otherwise the calling go routine blocks until either the mutex becomes
// available and return success or more time has passed than the timeout
// value and return false.
func (dm *DRWMutex) GetRLock(ctx context.Context, id, source string, opts Options) (locked bool) {

	isReadLock := true
	return dm.lockBlocking(ctx, id, source, isReadLock, opts)
}

const (
	lockRetryInterval = 1 * time.Second
)

// lockBlocking will try to acquire either a read or a write lock
//
// The function will loop using a built-in timing randomized back-off
// algorithm until either the lock is acquired successfully or more
// time has elapsed than the timeout value.
func (dm *DRWMutex) lockBlocking(ctx context.Context, id, source string, isReadLock bool, opts Options) (locked bool) {
	restClnts, owner := dm.clnt.GetLockers()

	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	// Create lock array to capture the successful lockers
	locks := make([]string, len(restClnts))

	log("lockBlocking %s/%s for %#v: lockType readLock(%t), additional opts: %#v\n", id, source, dm.Names, isReadLock, opts)
	retryCtx, cancel := context.WithTimeout(ctx, opts.Timeout)

	defer cancel()

	// Tolerance is not set, defaults to half of the locker clients.
	tolerance := opts.Tolerance
	if tolerance == 0 {
		tolerance = len(restClnts) / 2
	}

	// Quorum is effectively = total clients subtracted with tolerance limit
	quorum := len(restClnts) - tolerance
	if !isReadLock {
		// In situations for write locks, as a special case
		// to avoid split brains we make sure to acquire
		// quorum + 1 when tolerance is exactly half of the
		// total locker clients.
		if quorum == tolerance {
			quorum++
		}
	}

	tolerance = len(restClnts) - quorum

	for {
		select {
		case <-retryCtx.Done():
			log("lockBlocking canceled %s/%s for %#v: lockType readLock(%t), additional opts: %#v\n", id, source, dm.Names, isReadLock, opts)

			// Caller context canceled or we timedout,
			// return false anyways for both situations.

			// make sure to unlock any successful locks, since caller has timedout or canceled the request.
			releaseAll(dm.clnt, tolerance, owner, &locks, isReadLock, restClnts, dm.Names...)

			return false
		default:
			// Try to acquire the lock.
			if locked = lock(retryCtx, dm.clnt, &locks, id, source, isReadLock, tolerance, quorum, dm.Names...); locked {
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
				return locked
			}

			time.Sleep(time.Duration(r.Float64() * float64(lockRetryInterval)))
		}
	}
}

// lock tries to acquire the distributed lock, returning true or false.
func lock(ctx context.Context, ds *Dsync, locks *[]string, id, source string, isReadLock bool, tolerance, quorum int, lockNames ...string) bool {
	for i := range *locks {
		(*locks)[i] = ""
	}

	restClnts, owner := ds.GetLockers()

	// Create buffered channel of size equal to total number of nodes.
	ch := make(chan Granted, len(restClnts))
	defer close(ch)

	var wg sync.WaitGroup
	for index, c := range restClnts {

		wg.Add(1)
		// broadcast lock request to all nodes
		go func(index int, isReadLock bool, c NetLocker) {
			defer wg.Done()

			g := Granted{index: index}
			if c == nil {
				log("dsync: nil locker")
				ch <- g
				return
			}

			args := LockArgs{
				Owner:     owner,
				UID:       id,
				Resources: lockNames,
				Source:    source,
			}

			var locked bool
			var err error
			if isReadLock {
				if locked, err = c.RLock(ctx, args); err != nil {
					log("dsync: Unable to call RLock failed with %s for %#v at %s\n", err, args, c)
				}
			} else {
				if locked, err = c.Lock(ctx, args); err != nil {
					log("dsync: Unable to call Lock failed with %s for %#v at %s\n", err, args, c)
				}
			}

			if locked {
				g.lockUID = args.UID
			}

			ch <- g

		}(index, isReadLock, c)
	}

	quorumLocked := false

	wg.Add(1)
	go func(isReadLock bool) {
		defer wg.Done()

		// Wait until we have either
		//
		// a) received all lock responses
		// b) received too many 'non-'locks for quorum to be still possible
		// c) timedout
		//
		i, locksFailed := 0, 0
		done := false
		timeout := time.After(DRWMutexAcquireTimeout)

		for ; i < len(restClnts); i++ { // Loop until we acquired all locks

			select {
			case grant := <-ch:
				if grant.isLocked() {
					// Mark that this node has acquired the lock
					(*locks)[grant.index] = grant.lockUID
				} else {
					locksFailed++
					if locksFailed > tolerance {
						// We know that we are not going to get the lock anymore,
						// so exit out and release any locks that did get acquired
						done = true
						// Increment the number of grants received from the buffered channel.
						i++
						releaseAll(ds, tolerance, owner, locks, isReadLock, restClnts, lockNames...)
					}
				}
			case <-timeout:
				done = true
				// timeout happened, maybe one of the nodes is slow, count
				// number of locks to check whether we have quorum or not
				if !checkQuorumLocked(locks, quorum) {
					log("Quorum not met after timeout\n")
					releaseAll(ds, tolerance, owner, locks, isReadLock, restClnts, lockNames...)
				} else {
					log("Quorum met after timeout\n")
				}
			}

			if done {
				break
			}
		}

		// Count locks in order to determine whether we have quorum or not
		quorumLocked = checkQuorumLocked(locks, quorum)

		// Wait for the other responses and immediately release the locks
		// (do not add them to the locks array because the DRWMutex could
		//  already has been unlocked again by the original calling thread)
		for ; i < len(restClnts); i++ {
			grantToBeReleased := <-ch
			if grantToBeReleased.isLocked() {
				// release lock
				sendRelease(ds, restClnts[grantToBeReleased.index],
					owner,
					grantToBeReleased.lockUID, isReadLock, lockNames...)
			}
		}
	}(isReadLock)

	wg.Wait()

	return quorumLocked
}

// checkFailedUnlocks determines whether we have sufficiently unlocked all
// resources to ensure no deadlocks for future callers
func checkFailedUnlocks(locks []string, tolerance int) bool {
	unlocksFailed := 0
	for lockID := range locks {
		if isLocked(locks[lockID]) {
			unlocksFailed++
		}
	}

	// Unlock failures are higher than tolerance limit
	// for this instance of unlocker, we should let the
	// caller know that lock is not successfully released
	// yet.
	if len(locks)-tolerance == tolerance {
		// Incase of split brain scenarios where
		// tolerance is exactly half of the len(*locks)
		// then we need to make sure we have unlocked
		// upto tolerance+1 - especially for RUnlock
		// to ensure that we don't end up with active
		// read locks on the resource after unlocking
		// only half of the lockers.
		return unlocksFailed >= tolerance
	}
	return unlocksFailed > tolerance
}

// checkQuorumLocked determines whether we have locked the required quorum of underlying locks or not
func checkQuorumLocked(locks *[]string, quorum int) bool {
	count := 0
	for _, uid := range *locks {
		if isLocked(uid) {
			count++
		}
	}

	return count >= quorum
}

// releaseAll releases all locks that are marked as locked
func releaseAll(ds *Dsync, tolerance int, owner string, locks *[]string, isReadLock bool, restClnts []NetLocker, lockNames ...string) bool {
	var wg sync.WaitGroup
	for lockID := range restClnts {
		wg.Add(1)
		go func(lockID int) {
			defer wg.Done()
			if isLocked((*locks)[lockID]) {
				if sendRelease(ds, restClnts[lockID], owner, (*locks)[lockID], isReadLock, lockNames...) {
					(*locks)[lockID] = ""
				}
			}
		}(lockID)
	}
	wg.Wait()

	// Return true if releaseAll was successful, otherwise we return 'false'
	// to indicate we haven't sufficiently unlocked lockers to avoid deadlocks.
	//
	// Caller may use this as an indication to call again.
	return !checkFailedUnlocks(*locks, tolerance)
}

// Unlock unlocks the write lock.
//
// It is a run-time error if dm is not locked on entry to Unlock.
func (dm *DRWMutex) Unlock() {

	restClnts, owner := dm.clnt.GetLockers()
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
	}

	// Tolerance is not set, defaults to half of the locker clients.
	tolerance := len(restClnts) / 2

	isReadLock := false
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for !releaseAll(dm.clnt, tolerance, owner, &locks, isReadLock, restClnts, dm.Names...) {
		time.Sleep(time.Duration(r.Float64() * float64(lockRetryInterval)))
	}
}

// RUnlock releases a read lock held on dm.
//
// It is a run-time error if dm is not locked on entry to RUnlock.
func (dm *DRWMutex) RUnlock() {

	// create temp array on stack
	restClnts, owner := dm.clnt.GetLockers()

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

	// Tolerance is not set, defaults to half of the locker clients.
	tolerance := len(restClnts) / 2

	isReadLock := true
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for !releaseAll(dm.clnt, tolerance, owner, &locks, isReadLock, restClnts, dm.Names...) {
		time.Sleep(time.Duration(r.Float64() * float64(lockRetryInterval)))
	}
}

// sendRelease sends a release message to a node that previously granted a lock
func sendRelease(ds *Dsync, c NetLocker, owner string, uid string, isReadLock bool, names ...string) bool {
	if c == nil {
		log("Unable to call RUnlock failed with %s\n", errors.New("netLocker is offline"))
		return false
	}

	args := LockArgs{
		Owner:     owner,
		UID:       uid,
		Resources: names,
	}

	if isReadLock {
		if _, err := c.RUnlock(args); err != nil {
			log("dsync: Unable to call RUnlock failed with %s for %#v at %s\n", err, args, c)
			return false
		}
	} else {
		if _, err := c.Unlock(args); err != nil {
			log("dsync: Unable to call Unlock failed with %s for %#v at %s\n", err, args, c)
			return false
		}
	}

	return true
}

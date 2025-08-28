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
	"context"
	"errors"
	"math/rand"
	"slices"
	"sort"
	"strconv"
	"sync"
	"time"

	xioutil "github.com/minio/minio/internal/ioutil"
	"github.com/minio/minio/internal/mcontext"
	"github.com/minio/pkg/v3/console"
	"github.com/minio/pkg/v3/env"
)

// Indicator if logging is enabled.
var dsyncLog bool

// Retry unit interval
var lockRetryMinInterval time.Duration

var lockRetryBackOff func(*rand.Rand, uint) time.Duration

func init() {
	// Check for MINIO_DSYNC_TRACE env variable, if set logging will be enabled for failed REST operations.
	dsyncLog = env.Get("_MINIO_DSYNC_TRACE", "0") == "1"

	lockRetryMinInterval = 250 * time.Millisecond
	if lri := env.Get("_MINIO_LOCK_RETRY_INTERVAL", ""); lri != "" {
		v, err := strconv.Atoi(lri)
		if err != nil {
			panic(err)
		}
		lockRetryMinInterval = time.Duration(v) * time.Millisecond
	}

	lockRetryBackOff = backoffWait(
		lockRetryMinInterval,
		100*time.Millisecond,
		5*time.Second,
	)
}

func log(format string, data ...any) {
	if dsyncLog {
		console.Printf(format, data...)
	}
}

const (
	// dRWMutexAcquireTimeout - default tolerance limit to wait for lock acquisition before.
	drwMutexAcquireTimeout = 1 * time.Second // 1 second.

	// dRWMutexRefreshTimeout - default timeout for the refresh call
	drwMutexRefreshCallTimeout = 5 * time.Second

	// dRWMutexUnlockTimeout - default timeout for the unlock call
	drwMutexUnlockCallTimeout = 30 * time.Second

	// dRWMutexForceUnlockTimeout - default timeout for the unlock call
	drwMutexForceUnlockCallTimeout = 30 * time.Second

	// dRWMutexRefreshInterval - default the interval between two refresh calls
	drwMutexRefreshInterval = 10 * time.Second

	drwMutexInfinite = 1<<63 - 1
)

// Timeouts are timeouts for specific operations.
type Timeouts struct {
	// Acquire - tolerance limit to wait for lock acquisition before.
	Acquire time.Duration

	// RefreshCall - timeout for the refresh call
	RefreshCall time.Duration

	// UnlockCall - timeout for the unlock call
	UnlockCall time.Duration

	// ForceUnlockCall - timeout for the force unlock call
	ForceUnlockCall time.Duration
}

// DefaultTimeouts contains default timeouts.
var DefaultTimeouts = Timeouts{
	Acquire:         drwMutexAcquireTimeout,
	RefreshCall:     drwMutexRefreshCallTimeout,
	UnlockCall:      drwMutexUnlockCallTimeout,
	ForceUnlockCall: drwMutexForceUnlockCallTimeout,
}

// A DRWMutex is a distributed mutual exclusion lock.
type DRWMutex struct {
	Names                []string
	writeLocks           []string // Array of nodes that granted a write lock
	readLocks            []string // Array of array of nodes that granted reader locks
	rng                  *rand.Rand
	m                    sync.Mutex // Mutex to prevent multiple simultaneous locks from this node
	clnt                 *Dsync
	cancelRefresh        context.CancelFunc
	refreshInterval      time.Duration
	lockRetryMinInterval time.Duration
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
	sort.Strings(names)
	return &DRWMutex{
		writeLocks:           make([]string, len(restClnts)),
		readLocks:            make([]string, len(restClnts)),
		Names:                names,
		clnt:                 clnt,
		rng:                  rand.New(&lockedRandSource{src: rand.NewSource(time.Now().UTC().UnixNano())}),
		refreshInterval:      drwMutexRefreshInterval,
		lockRetryMinInterval: lockRetryMinInterval,
	}
}

// Lock holds a write lock on dm.
//
// If the lock is already in use, the calling go routine
// blocks until the mutex is available.
func (dm *DRWMutex) Lock(id, source string) {
	isReadLock := false
	dm.lockBlocking(context.Background(), nil, id, source, isReadLock, Options{
		Timeout: drwMutexInfinite,
	})
}

// Options lock options.
type Options struct {
	Timeout       time.Duration
	RetryInterval time.Duration
}

// GetLock tries to get a write lock on dm before the timeout elapses.
//
// If the lock is already in use, the calling go routine
// blocks until either the mutex becomes available and return success or
// more time has passed than the timeout value and return false.
func (dm *DRWMutex) GetLock(ctx context.Context, cancel context.CancelFunc, id, source string, opts Options) (locked bool) {
	isReadLock := false
	return dm.lockBlocking(ctx, cancel, id, source, isReadLock, opts)
}

// RLock holds a read lock on dm.
//
// If one or more read locks are already in use, it will grant another lock.
// Otherwise the calling go routine blocks until the mutex is available.
func (dm *DRWMutex) RLock(id, source string) {
	isReadLock := true
	dm.lockBlocking(context.Background(), nil, id, source, isReadLock, Options{
		Timeout: drwMutexInfinite,
	})
}

// GetRLock tries to get a read lock on dm before the timeout elapses.
//
// If one or more read locks are already in use, it will grant another lock.
// Otherwise the calling go routine blocks until either the mutex becomes
// available and return success or more time has passed than the timeout
// value and return false.
func (dm *DRWMutex) GetRLock(ctx context.Context, cancel context.CancelFunc, id, source string, opts Options) (locked bool) {
	isReadLock := true
	return dm.lockBlocking(ctx, cancel, id, source, isReadLock, opts)
}

// lockBlocking will try to acquire either a read or a write lock
//
// The function will loop using a built-in timing randomized back-off
// algorithm until either the lock is acquired successfully or more
// time has elapsed than the timeout value.
func (dm *DRWMutex) lockBlocking(ctx context.Context, lockLossCallback func(), id, source string, isReadLock bool, opts Options) (locked bool) {
	restClnts, _ := dm.clnt.GetLockers()

	// Create lock array to capture the successful lockers
	locks := make([]string, len(restClnts))

	// Add total timeout
	ctx, cancel := context.WithTimeout(ctx, opts.Timeout)
	defer cancel()

	// Tolerance is not set, defaults to half of the locker clients.
	tolerance := len(restClnts) / 2

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

	log("lockBlocking %s/%s for %#v: lockType readLock(%t), additional opts: %#v, quorum: %d, tolerance: %d, lockClients: %d\n", id, source, dm.Names, isReadLock, opts, quorum, tolerance, len(restClnts))

	tolerance = len(restClnts) - quorum
	attempt := uint(0)

	for {
		select {
		case <-ctx.Done():
			return false
		default:
			// Try to acquire the lock.
			if locked = lock(ctx, dm.clnt, &locks, id, source, isReadLock, tolerance, quorum, dm.Names...); locked {
				dm.m.Lock()

				// If success, copy array to object
				if isReadLock {
					copy(dm.readLocks, locks)
				} else {
					copy(dm.writeLocks, locks)
				}

				dm.m.Unlock()
				log("lockBlocking %s/%s for %#v: granted\n", id, source, dm.Names)

				// Refresh lock continuously and cancel if there is no quorum in the lock anymore
				dm.startContinuousLockRefresh(lockLossCallback, id, source, quorum)

				return locked
			}

			switch {
			case opts.RetryInterval < 0:
				return false
			case opts.RetryInterval > 0:
				time.Sleep(opts.RetryInterval)
			default:
				attempt++
				time.Sleep(lockRetryBackOff(dm.rng, attempt))
			}
		}
	}
}

func (dm *DRWMutex) startContinuousLockRefresh(lockLossCallback func(), id, source string, quorum int) {
	ctx, cancel := context.WithCancel(context.Background())

	dm.m.Lock()
	dm.cancelRefresh = cancel
	dm.m.Unlock()

	go func() {
		defer cancel()

		refreshTimer := time.NewTimer(dm.refreshInterval)
		defer refreshTimer.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-refreshTimer.C:
				noQuorum, err := refreshLock(ctx, dm.clnt, id, source, quorum)
				if err == nil && noQuorum {
					// Clean the lock locally and in remote nodes
					forceUnlock(ctx, dm.clnt, id)
					// Execute the caller lock loss callback
					if lockLossCallback != nil {
						lockLossCallback()
					}
					return
				}

				refreshTimer.Reset(dm.refreshInterval)
			}
		}
	}()
}

func forceUnlock(ctx context.Context, ds *Dsync, id string) {
	ctx, cancel := context.WithTimeout(ctx, ds.Timeouts.ForceUnlockCall)
	defer cancel()

	restClnts, _ := ds.GetLockers()

	args := LockArgs{
		UID: id,
	}

	var wg sync.WaitGroup
	for index, c := range restClnts {
		wg.Add(1)
		// Send refresh request to all nodes
		go func(index int, c NetLocker) {
			defer wg.Done()
			c.ForceUnlock(ctx, args)
		}(index, c)
	}
	wg.Wait()
}

type refreshResult struct {
	offline   bool
	refreshed bool
}

// Refresh the given lock in all nodes, return true to indicate if a lock
// does not exist in enough quorum nodes.
func refreshLock(ctx context.Context, ds *Dsync, id, source string, quorum int) (bool, error) {
	restClnts, _ := ds.GetLockers()

	// Create buffered channel of size equal to total number of nodes.
	ch := make(chan refreshResult, len(restClnts))
	var wg sync.WaitGroup

	args := LockArgs{
		UID: id,
	}

	for index, c := range restClnts {
		wg.Add(1)
		// Send refresh request to all nodes
		go func(index int, c NetLocker) {
			defer wg.Done()

			if c == nil {
				ch <- refreshResult{offline: true}
				return
			}

			ctx, cancel := context.WithTimeout(ctx, ds.Timeouts.RefreshCall)
			defer cancel()

			refreshed, err := c.Refresh(ctx, args)
			if err != nil {
				ch <- refreshResult{offline: true}
				log("dsync: Unable to call Refresh failed with %s for %#v at %s\n", err, args, c)
			} else {
				ch <- refreshResult{refreshed: refreshed}
				log("dsync: Refresh returned false for %#v at %s\n", args, c)
			}
		}(index, c)
	}

	// Wait until we have either
	//
	// a) received all refresh responses
	// b) received too many refreshed for quorum to be still possible
	// c) timed out
	//
	lockNotFound, lockRefreshed := 0, 0
	done := false

	for range len(restClnts) {
		select {
		case refreshResult := <-ch:
			if refreshResult.offline {
				continue
			}
			if refreshResult.refreshed {
				lockRefreshed++
			} else {
				lockNotFound++
			}
			if lockRefreshed >= quorum || lockNotFound > len(restClnts)-quorum {
				done = true
			}
		case <-ctx.Done():
			// Refreshing is canceled
			return false, ctx.Err()
		}
		if done {
			break
		}
	}

	// We may have some unused results in ch, release them async.
	go func() {
		wg.Wait()
		xioutil.SafeClose(ch)
		for range ch {
		}
	}()

	noQuorum := lockNotFound > len(restClnts)-quorum
	return noQuorum, nil
}

// lock tries to acquire the distributed lock, returning true or false.
func lock(ctx context.Context, ds *Dsync, locks *[]string, id, source string, isReadLock bool, tolerance, quorum int, names ...string) bool {
	for i := range *locks {
		(*locks)[i] = ""
	}

	restClnts, owner := ds.GetLockers()

	// Create buffered channel of size equal to total number of nodes.
	ch := make(chan Granted, len(restClnts))
	var wg sync.WaitGroup

	args := LockArgs{
		Owner:     owner,
		UID:       id,
		Resources: names,
		Source:    source,
		Quorum:    &quorum,
	}

	// Combined timeout for the lock attempt.
	ctx, cancel := context.WithTimeout(ctx, ds.Timeouts.Acquire)
	defer cancel()

	// Special context for NetLockers - do not use timeouts.
	// Also, pass the trace context info if found for debugging
	netLockCtx := context.Background()

	tc, ok := ctx.Value(mcontext.ContextTraceKey).(*mcontext.TraceCtxt)
	if ok {
		netLockCtx = context.WithValue(netLockCtx, mcontext.ContextTraceKey, tc)
	}

	for index, c := range restClnts {
		wg.Add(1)
		// broadcast lock request to all nodes
		go func(index int, isReadLock bool, c NetLocker) {
			defer wg.Done()

			g := Granted{index: index}
			if c == nil {
				log("dsync: nil locker\n")
				ch <- g
				return
			}

			var locked bool
			var err error
			if isReadLock {
				if locked, err = c.RLock(netLockCtx, args); err != nil {
					log("dsync: Unable to call RLock failed with %s for %#v at %s\n", err, args, c)
				}
			} else {
				if locked, err = c.Lock(netLockCtx, args); err != nil {
					log("dsync: Unable to call Lock failed with %s for %#v at %s\n", err, args, c)
				}
			}
			if locked {
				g.lockUID = args.UID
			}
			ch <- g
		}(index, isReadLock, c)
	}

	// Wait until we have either
	//
	// a) received all lock responses
	// b) received too many 'non-'locks for quorum to be still possible
	// c) timed out
	//
	i, locksFailed := 0, 0
	done := false

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
				}
			}
		case <-ctx.Done():
			// Capture timedout locks as failed or took too long
			locksFailed++
			if locksFailed > tolerance {
				// We know that we are not going to get the lock anymore,
				// so exit out and release any locks that did get acquired
				done = true
			}
		}

		if done {
			break
		}
	}

	quorumLocked := checkQuorumLocked(locks, quorum) && locksFailed <= tolerance
	if !quorumLocked {
		log("dsync: Unable to acquire lock in quorum %#v\n", args)
		// Release all acquired locks without quorum.
		if !releaseAll(ctx, ds, tolerance, owner, locks, isReadLock, restClnts, names...) {
			log("Unable to release acquired locks, these locks will expire automatically %#v\n", args)
		}
	}

	// We may have some unused results in ch, release them async.
	go func() {
		wg.Wait()
		xioutil.SafeClose(ch)
		for grantToBeReleased := range ch {
			if grantToBeReleased.isLocked() {
				// release abandoned lock
				log("Releasing abandoned lock\n")
				sendRelease(ctx, ds, restClnts[grantToBeReleased.index],
					owner, grantToBeReleased.lockUID, isReadLock, names...)
			}
		}
	}()

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
		// In case of split brain scenarios where
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
func releaseAll(ctx context.Context, ds *Dsync, tolerance int, owner string, locks *[]string, isReadLock bool, restClnts []NetLocker, names ...string) bool {
	var wg sync.WaitGroup
	for lockID := range restClnts {
		wg.Add(1)
		go func(lockID int) {
			defer wg.Done()
			if sendRelease(ctx, ds, restClnts[lockID], owner, (*locks)[lockID], isReadLock, names...) {
				(*locks)[lockID] = ""
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
func (dm *DRWMutex) Unlock(ctx context.Context) {
	dm.m.Lock()
	dm.cancelRefresh()
	dm.m.Unlock()

	restClnts, owner := dm.clnt.GetLockers()
	// create temp array on stack
	locks := make([]string, len(restClnts))

	{
		dm.m.Lock()
		defer dm.m.Unlock()

		// Check if minimally a single bool is set in the writeLocks array
		lockFound := slices.ContainsFunc(dm.writeLocks, isLocked)
		if !lockFound {
			panic("Trying to Unlock() while no Lock() is active")
		}

		// Copy write locks to stack array
		copy(locks, dm.writeLocks)
	}

	// Tolerance is not set, defaults to half of the locker clients.
	tolerance := len(restClnts) / 2

	isReadLock := false
	started := time.Now()
	// Do async unlocking.
	// This means unlock will no longer block on the network or missing quorum.
	go func() {
		ctx, done := context.WithTimeout(ctx, drwMutexUnlockCallTimeout)
		defer done()
		for !releaseAll(ctx, dm.clnt, tolerance, owner, &locks, isReadLock, restClnts, dm.Names...) {
			time.Sleep(time.Duration(dm.rng.Float64() * float64(dm.lockRetryMinInterval)))
			if time.Since(started) > dm.clnt.Timeouts.UnlockCall {
				return
			}
		}
	}()
}

// RUnlock releases a read lock held on dm.
//
// It is a run-time error if dm is not locked on entry to RUnlock.
func (dm *DRWMutex) RUnlock(ctx context.Context) {
	dm.m.Lock()
	dm.cancelRefresh()
	dm.m.Unlock()

	restClnts, owner := dm.clnt.GetLockers()
	// create temp array on stack
	locks := make([]string, len(restClnts))

	{
		dm.m.Lock()
		defer dm.m.Unlock()

		// Check if minimally a single bool is set in the writeLocks array
		lockFound := slices.ContainsFunc(dm.readLocks, isLocked)
		if !lockFound {
			panic("Trying to RUnlock() while no RLock() is active")
		}

		// Copy write locks to stack array
		copy(locks, dm.readLocks)
	}

	// Tolerance is not set, defaults to half of the locker clients.
	tolerance := len(restClnts) / 2
	isReadLock := true
	started := time.Now()
	// Do async unlocking.
	// This means unlock will no longer block on the network or missing quorum.
	go func() {
		for !releaseAll(ctx, dm.clnt, tolerance, owner, &locks, isReadLock, restClnts, dm.Names...) {
			time.Sleep(time.Duration(dm.rng.Float64() * float64(dm.lockRetryMinInterval)))
			// If we have been waiting for more than the force unlock timeout, return
			// Remotes will have canceled due to the missing refreshes anyway.
			if time.Since(started) > dm.clnt.Timeouts.UnlockCall {
				return
			}
		}
	}()
}

// sendRelease sends a release message to a node that previously granted a lock
func sendRelease(ctx context.Context, ds *Dsync, c NetLocker, owner string, uid string, isReadLock bool, names ...string) bool {
	if c == nil {
		log("Unable to call RUnlock failed with %s\n", errors.New("netLocker is offline"))
		return false
	}

	if len(uid) == 0 {
		return false
	}

	args := LockArgs{
		Owner:     owner,
		UID:       uid,
		Resources: names,
	}

	netLockCtx, cancel := context.WithTimeout(context.Background(), ds.Timeouts.UnlockCall)
	defer cancel()

	tc, ok := ctx.Value(mcontext.ContextTraceKey).(*mcontext.TraceCtxt)
	if ok {
		netLockCtx = context.WithValue(netLockCtx, mcontext.ContextTraceKey, tc)
	}

	if isReadLock {
		if _, err := c.RUnlock(netLockCtx, args); err != nil {
			log("dsync: Unable to call RUnlock failed with %s for %#v at %s\n", err, args, c)
			return false
		}
	} else {
		if _, err := c.Unlock(netLockCtx, args); err != nil {
			log("dsync: Unable to call Unlock failed with %s for %#v at %s\n", err, args, c)
			return false
		}
	}

	return true
}

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
	cryptorand "crypto/rand"
	"fmt"
	"log"
	"math"
	"math/rand"
	"net"
	"os"
	"sync"
	"time"
)

// Indicator if logging is enabled.
var dsyncLog bool

func init() {
	// Check for DSYNC_LOG env variable, if set logging will be enabled for failed RPC operations.
	dsyncLog = os.Getenv("DSYNC_LOG") == "1"
}

// DRWMutexAcquireTimeout - tolerance limit to wait for lock acquisition before.
const DRWMutexAcquireTimeout = 25 * time.Millisecond // 25ms.

// A DRWMutex is a distributed mutual exclusion lock.
type DRWMutex struct {
	Name         string
	writeLocks   []string   // Array of nodes that granted a write lock
	readersLocks [][]string // Array of array of nodes that granted reader locks
	m            sync.Mutex // Mutex to prevent multiple simultaneous locks from this node
}

type Granted struct {
	index   int
	lockUid string // Locked if set with UID string, unlocked if empty
}

func (g *Granted) isLocked() bool {
	return isLocked(g.lockUid)
}

func isLocked(uid string) bool {
	return len(uid) > 0
}

type LockArgs struct {
	Token     string
	Timestamp time.Time
	Name      string
	Node      string
	RPCPath   string
	UID       string
}

func (l *LockArgs) SetToken(token string) {
	l.Token = token
}

func (l *LockArgs) SetTimestamp(tstamp time.Time) {
	l.Timestamp = tstamp
}

func NewDRWMutex(name string) *DRWMutex {
	return &DRWMutex{
		Name:       name,
		writeLocks: make([]string, dnodeCount),
	}
}

// Lock holds a write lock on dm.
//
// If the lock is already in use, the calling go routine
// blocks until the mutex is available.
func (dm *DRWMutex) Lock() {

	isReadLock := false
	dm.lockBlocking(isReadLock)
}

// RLock holds a read lock on dm.
//
// If one or more read lock are already in use, it will grant another lock.
// Otherwise the calling go routine blocks until the mutex is available.
func (dm *DRWMutex) RLock() {

	isReadLock := true
	dm.lockBlocking(isReadLock)
}

// lockBlocking will acquire either a read or a write lock
//
// The call will block until the lock is granted using a built-in
// timing randomized back-off algorithm to try again until successful
func (dm *DRWMutex) lockBlocking(isReadLock bool) {

	runs, backOff := 1, 1

	for {
		// create temp array on stack
		locks := make([]string, dnodeCount)

		// try to acquire the lock
		success := lock(clnts, &locks, dm.Name, isReadLock)
		if success {
			dm.m.Lock()
			defer dm.m.Unlock()

			// if success, copy array to object
			if isReadLock {
				// append new array of strings at the end
				dm.readersLocks = append(dm.readersLocks, make([]string, dnodeCount))
				// and copy stack array into last spot
				copy(dm.readersLocks[len(dm.readersLocks)-1], locks[:])
			} else {
				copy(dm.writeLocks, locks[:])
			}

			return
		}

		// We timed out on the previous lock, incrementally wait for a longer back-off time,
		// and try again afterwards
		time.Sleep(time.Duration(backOff) * time.Millisecond)

		backOff += int(rand.Float64() * math.Pow(2, float64(runs)))
		if backOff > 1024 {
			backOff = backOff % 64

			runs = 1 // reset runs
		} else if runs < 10 {
			runs++
		}
	}
}

// lock tries to acquire the distributed lock, returning true or false
//
func lock(clnts []RPC, locks *[]string, lockName string, isReadLock bool) bool {

	// Create buffered channel of size equal to total number of nodes.
	ch := make(chan Granted, dnodeCount)

	for index, c := range clnts {

		// broadcast lock request to all nodes
		go func(index int, isReadLock bool, c RPC) {
			// All client methods issuing RPCs are thread-safe and goroutine-safe,
			// i.e. it is safe to call them from multiple concurrently running go routines.
			var locked bool
			bytesUid := [16]byte{}
			cryptorand.Read(bytesUid[:])
			uid := fmt.Sprintf("%X", bytesUid[:])
			args := LockArgs{Name: lockName, Node: clnts[ownNode].Node(), RPCPath: clnts[ownNode].RPCPath(), UID: uid}
			if isReadLock {
				if err := c.Call("Dsync.RLock", &args, &locked); err != nil {
					if dsyncLog {
						log.Println("Unable to call Dsync.RLock", err)
					}
				}
			} else {
				if err := c.Call("Dsync.Lock", &args, &locked); err != nil {
					if dsyncLog {
						log.Println("Unable to call Dsync.Lock", err)
					}
				}
			}

			g := Granted{index: index}
			if locked {
				g.lockUid = args.UID
			}
			ch <- g

		}(index, isReadLock, c)
	}

	quorum := false

	var wg sync.WaitGroup
	wg.Add(1)
	go func(isReadLock bool) {

		// Wait until we have either a) received all lock responses, b) received too many 'non-'locks for quorum to be or c) time out
		i, locksFailed := 0, 0
		done := false
		timeout := time.After(DRWMutexAcquireTimeout)

		for ; i < dnodeCount; i++ { // Loop until we acquired all locks

			select {
			case grant := <-ch:
				if grant.isLocked() {
					// Mark that this node has acquired the lock
					(*locks)[grant.index] = grant.lockUid
				} else {
					locksFailed++
					if !isReadLock && locksFailed > dnodeCount-dquorum ||
						isReadLock && locksFailed > dnodeCount-dquorumReads {
						// We know that we are not going to get the lock anymore, so exit out
						// and release any locks that did get acquired
						done = true
						// Increment the number of grants received from the buffered channel.
						i++
						releaseAll(clnts, locks, lockName, isReadLock)
					}
				}

			case <-timeout:
				done = true
				// timeout happened, maybe one of the nodes is slow, count
				// number of locks to check whether we have quorum or not
				if !quorumMet(locks, isReadLock) {
					releaseAll(clnts, locks, lockName, isReadLock)
				}
			}

			if done {
				break
			}
		}

		// Count locks in order to determine whterh we have quorum or not
		quorum = quorumMet(locks, isReadLock)

		// Signal that we have the quorum
		wg.Done()

		// Wait for the other responses and immediately release the locks
		// (do not add them to the locks array because the DRWMutex could
		//  already has been unlocked again by the original calling thread)
		for ; i < dnodeCount; i++ {
			grantToBeReleased := <-ch
			if grantToBeReleased.isLocked() {
				// release lock
				sendRelease(clnts[grantToBeReleased.index], lockName, grantToBeReleased.lockUid, isReadLock)
			}
		}
	}(isReadLock)

	wg.Wait()

	// Verify that localhost server is actively participating in the lock (the lock maintenance relies on this fact)
	if quorum && !isLocked((*locks)[ownNode]) {
		// If not, release lock (and try again later)
		releaseAll(clnts, locks, lockName, isReadLock)
		quorum = false
	}

	return quorum
}

// quorumMet determines whether we have acquired the required quorum of underlying locks or not
func quorumMet(locks *[]string, isReadLock bool) bool {

	count := 0
	for _, uid := range *locks {
		if isLocked(uid) {
			count++
		}
	}

	if isReadLock {
		return count >= dquorumReads
	} else {
		return count >= dquorum
	}
}

// releaseAll releases all locks that are marked as locked
func releaseAll(clnts []RPC, locks *[]string, lockName string, isReadLock bool) {
	for lock := 0; lock < dnodeCount; lock++ {
		if isLocked((*locks)[lock]) {
			sendRelease(clnts[lock], lockName, (*locks)[lock], isReadLock)
			(*locks)[lock] = ""
		}
	}
}

// Unlock unlocks the write lock.
//
// It is a run-time error if dm is not locked on entry to Unlock.
func (dm *DRWMutex) Unlock() {

	// create temp array on stack
	locks := make([]string, dnodeCount)

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
		dm.writeLocks = make([]string, dnodeCount)
	}

	isReadLock := false
	unlock(locks, dm.Name, isReadLock)
}

// RUnlock releases a read lock held on dm.
//
// It is a run-time error if dm is not locked on entry to RUnlock.
func (dm *DRWMutex) RUnlock() {

	// create temp array on stack
	locks := make([]string, dnodeCount)

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
	unlock(locks, dm.Name, isReadLock)
}

func unlock(locks []string, name string, isReadLock bool) {

	// We don't need to synchronously wait until we have released all the locks (or the quorum)
	// (a subsequent lock will retry automatically in case it would fail to get quorum)

	for index, c := range clnts {

		if isLocked(locks[index]) {
			// broadcast lock release to all nodes that granted the lock
			sendRelease(c, name, locks[index], isReadLock)
		}
	}
}

// ForceUnlock will forcefully clear a write or read lock.
func (dm *DRWMutex) ForceUnlock() {

	{
		dm.m.Lock()
		defer dm.m.Unlock()

		// Clear write locks array
		dm.writeLocks = make([]string, dnodeCount)
		// Clear read locks array
		dm.readersLocks = nil
	}

	for _, c := range clnts {
		// broadcast lock release to all nodes that granted the lock
		sendRelease(c, dm.Name, "", false)
	}
}

// sendRelease sends a release message to a node that previously granted a lock
func sendRelease(c RPC, name, uid string, isReadLock bool) {

	backOffArray := []time.Duration{
		30 * time.Second, // 30secs.
		1 * time.Minute,  // 1min.
		3 * time.Minute,  // 3min.
		10 * time.Minute, // 10min.
		30 * time.Minute, // 30min.
		1 * time.Hour,    // 1hr.
	}

	go func(c RPC, name string) {

		for _, backOff := range backOffArray {

			// All client methods issuing RPCs are thread-safe and goroutine-safe,
			// i.e. it is safe to call them from multiple concurrently running goroutines.
			var unlocked bool
			args := LockArgs{Name: name, UID: uid} // Just send name & uid (and leave out node and rpcPath; unimportant for unlocks)
			if len(uid) == 0 {
				if err := c.Call("Dsync.ForceUnlock", &args, &unlocked); err == nil {
					// ForceUnlock delivered, exit out
					return
				} else if err != nil {
					if dsyncLog {
						log.Println("Unable to call Dsync.ForceUnlock", err)
					}
					if nErr, ok := err.(net.Error); ok && nErr.Timeout() {
						// ForceUnlock possibly failed with server timestamp mismatch, server may have restarted.
						return
					}
				}
			} else if isReadLock {
				if err := c.Call("Dsync.RUnlock", &args, &unlocked); err == nil {
					// RUnlock delivered, exit out
					return
				} else if err != nil {
					if dsyncLog {
						log.Println("Unable to call Dsync.RUnlock", err)
					}
					if nErr, ok := err.(net.Error); ok && nErr.Timeout() {
						// RUnlock possibly failed with server timestamp mismatch, server may have restarted.
						return
					}
				}
			} else {
				if err := c.Call("Dsync.Unlock", &args, &unlocked); err == nil {
					// Unlock delivered, exit out
					return
				} else if err != nil {
					if dsyncLog {
						log.Println("Unable to call Dsync.Unlock", err)
					}
					if nErr, ok := err.(net.Error); ok && nErr.Timeout() {
						// Unlock possibly failed with server timestamp mismatch, server may have restarted.
						return
					}
				}
			}

			// Wait..
			time.Sleep(backOff)
		}
	}(c, name)
}

// DRLocker returns a sync.Locker interface that implements
// the Lock and Unlock methods by calling drw.RLock and drw.RUnlock.
func (dm *DRWMutex) DRLocker() sync.Locker {
	return (*drlocker)(dm)
}

type drlocker DRWMutex

func (dr *drlocker) Lock()   { (*DRWMutex)(dr).RLock() }
func (dr *drlocker) Unlock() { (*DRWMutex)(dr).RUnlock() }

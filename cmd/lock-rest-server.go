/*
 * Minio Cloud Storage, (C) 2019 Minio, Inc.
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

package cmd

import (
	"bufio"
	"context"
	"errors"
	"math/rand"
	"net/http"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/gorilla/mux"
	"github.com/minio/minio/pkg/dsync"
)

const (
	// Lock maintenance interval.
	lockMaintenanceInterval = 10 * time.Second

	// Lock validity check interval.
	lockValidityCheckInterval = 30 * time.Second
)

// To abstract a node over network.
type lockRESTServer struct {
	ll *localLocker
}

func (l *lockRESTServer) writeErrorResponse(w http.ResponseWriter, err error) {
	w.WriteHeader(http.StatusForbidden)
	w.Write([]byte(err.Error()))
}

// IsValid - To authenticate and verify the time difference.
func (l *lockRESTServer) IsValid(w http.ResponseWriter, r *http.Request) bool {
	if l.ll == nil {
		l.writeErrorResponse(w, errLockNotInitialized)
		return false
	}

	if err := storageServerRequestValidate(r); err != nil {
		l.writeErrorResponse(w, err)
		return false
	}
	return true
}

func getLockArgs(r *http.Request) (args dsync.LockArgs, err error) {
	quorum, err := strconv.Atoi(r.URL.Query().Get(lockRESTQuorum))
	if err != nil {
		return args, err
	}

	args = dsync.LockArgs{
		Owner:  r.URL.Query().Get(lockRESTOwner),
		UID:    r.URL.Query().Get(lockRESTUID),
		Source: r.URL.Query().Get(lockRESTSource),
		Quorum: quorum,
	}

	var resources []string
	bio := bufio.NewScanner(r.Body)
	for bio.Scan() {
		resources = append(resources, bio.Text())
	}

	if err := bio.Err(); err != nil {
		return args, err
	}

	sort.Strings(resources)
	args.Resources = resources
	return args, nil
}

// HealthHandler returns success if request is authenticated.
func (l *lockRESTServer) HealthHandler(w http.ResponseWriter, r *http.Request) {
	l.IsValid(w, r)
}

// LockHandler - Acquires a lock.
func (l *lockRESTServer) LockHandler(w http.ResponseWriter, r *http.Request) {
	if !l.IsValid(w, r) {
		l.writeErrorResponse(w, errors.New("invalid request"))
		return
	}

	args, err := getLockArgs(r)
	if err != nil {
		l.writeErrorResponse(w, err)
		return
	}

	success, err := l.ll.Lock(r.Context(), args)
	if err == nil && !success {
		err = errLockConflict
	}
	if err != nil {
		l.writeErrorResponse(w, err)
		return
	}
}

// UnlockHandler - releases the acquired lock.
func (l *lockRESTServer) UnlockHandler(w http.ResponseWriter, r *http.Request) {
	if !l.IsValid(w, r) {
		l.writeErrorResponse(w, errors.New("invalid request"))
		return
	}

	args, err := getLockArgs(r)
	if err != nil {
		l.writeErrorResponse(w, err)
		return
	}

	_, err = l.ll.Unlock(args)
	// Ignore the Unlock() "reply" return value because if err == nil, "reply" is always true
	// Consequently, if err != nil, reply is always false
	if err != nil {
		l.writeErrorResponse(w, err)
		return
	}
}

// LockHandler - Acquires an RLock.
func (l *lockRESTServer) RLockHandler(w http.ResponseWriter, r *http.Request) {
	if !l.IsValid(w, r) {
		l.writeErrorResponse(w, errors.New("invalid request"))
		return
	}

	args, err := getLockArgs(r)
	if err != nil {
		l.writeErrorResponse(w, err)
		return
	}

	success, err := l.ll.RLock(r.Context(), args)
	if err == nil && !success {
		err = errLockConflict
	}
	if err != nil {
		l.writeErrorResponse(w, err)
		return
	}
}

// RUnlockHandler - releases the acquired read lock.
func (l *lockRESTServer) RUnlockHandler(w http.ResponseWriter, r *http.Request) {
	if !l.IsValid(w, r) {
		l.writeErrorResponse(w, errors.New("invalid request"))
		return
	}

	args, err := getLockArgs(r)
	if err != nil {
		l.writeErrorResponse(w, err)
		return
	}

	// Ignore the RUnlock() "reply" return value because if err == nil, "reply" is always true.
	// Consequently, if err != nil, reply is always false
	if _, err = l.ll.RUnlock(args); err != nil {
		l.writeErrorResponse(w, err)
		return
	}
}

// ForceUnlockHandler - query expired lock status.
func (l *lockRESTServer) ForceUnlockHandler(w http.ResponseWriter, r *http.Request) {
	if !l.IsValid(w, r) {
		l.writeErrorResponse(w, errors.New("invalid request"))
		return
	}

	args, err := getLockArgs(r)
	if err != nil {
		l.writeErrorResponse(w, err)
		return
	}

	if _, err = l.ll.ForceUnlock(r.Context(), args); err != nil {
		l.writeErrorResponse(w, err)
		return
	}
}

// ExpiredHandler - query expired lock status.
func (l *lockRESTServer) ExpiredHandler(w http.ResponseWriter, r *http.Request) {
	if !l.IsValid(w, r) {
		l.writeErrorResponse(w, errors.New("invalid request"))
		return
	}

	args, err := getLockArgs(r)
	if err != nil {
		l.writeErrorResponse(w, err)
		return
	}

	expired, err := l.ll.Expired(r.Context(), args)
	if err != nil {
		l.writeErrorResponse(w, err)
		return
	}
	if !expired {
		l.writeErrorResponse(w, errLockNotExpired)
		return
	}
}

// getLongLivedLocks returns locks that are older than a certain time and
// have not been 'checked' for validity too soon enough
func getLongLivedLocks(interval time.Duration) []lockRequesterInfo {
	lrips := []lockRequesterInfo{}
	globalLockServer.mutex.Lock()
	for _, lriArray := range globalLockServer.lockMap {
		for idx := range lriArray {
			// Check whether enough time has gone by since last check
			if time.Since(lriArray[idx].TimeLastCheck) >= interval {
				lrips = append(lrips, lriArray[idx])
				lriArray[idx].TimeLastCheck = UTCNow()
			}
		}
	}
	globalLockServer.mutex.Unlock()
	return lrips
}

// lockMaintenance loops over locks that have been active for some time and checks back
// with the original server whether it is still alive or not
//
// Following logic inside ignores the errors generated for Dsync.Active operation.
// - server at client down
// - some network error (and server is up normally)
//
// We will ignore the error, and we will retry later to get a resolve on this lock
func lockMaintenance(ctx context.Context, interval time.Duration) error {
	objAPI := newObjectLayerFn()
	if objAPI == nil {
		return nil
	}

	z, ok := objAPI.(*erasureServerPools)
	if !ok {
		return nil
	}

	type nlock struct {
		locks  int
		writer bool
	}

	updateNlocks := func(nlripsMap map[string]nlock, name string, writer bool) {
		nlk, ok := nlripsMap[name]
		if ok {
			nlk.locks++
			nlripsMap[name] = nlk
		} else {
			nlripsMap[name] = nlock{
				locks:  1,
				writer: writer,
			}
		}
	}

	// Validate if long lived locks are indeed clean.
	// Get list of long lived locks to check for staleness.
	lrips := getLongLivedLocks(interval)
	lripsMap := make(map[string]nlock, len(lrips))
	var mutex sync.Mutex // mutex for lripsMap updates
	for _, lrip := range lrips {
		// fetch the lockers participated in handing
		// out locks for `nlrip.name`
		var lockers []dsync.NetLocker
		if lrip.Group {
			lockers, _ = z.serverPools[0].getHashedSet("").getLockers()
		} else {
			lockers, _ = z.serverPools[0].getHashedSet(lrip.Name).getLockers()
		}
		var wg sync.WaitGroup
		wg.Add(len(lockers))
		for _, c := range lockers {
			go func(lrip lockRequesterInfo, c dsync.NetLocker) {
				defer wg.Done()
				ctx, cancel := context.WithTimeout(GlobalContext, 3*time.Second)

				// Call back to all participating servers, verify
				// if each of those servers think lock is still
				// active, if not expire it.
				expired, err := c.Expired(ctx, dsync.LockArgs{
					Owner:     lrip.Owner,
					UID:       lrip.UID,
					Resources: []string{lrip.Name},
				})
				cancel()

				if err != nil {
					mutex.Lock()
					updateNlocks(lripsMap, lrip.Name, lrip.Writer)
					mutex.Unlock()
					return
				}

				if !expired {
					mutex.Lock()
					updateNlocks(lripsMap, lrip.Name, lrip.Writer)
					mutex.Unlock()
				}
			}(lrip, c)
		}
		wg.Wait()

		// less than the quorum, we have locks expired.
		if lripsMap[lrip.Name].locks < lrip.Quorum {
			// Purge the stale entry if it exists.
			globalLockServer.removeEntryIfExists(lrip)
		}
	}

	return nil
}

// Start lock maintenance from all lock servers.
func startLockMaintenance(ctx context.Context) {
	// Wait until the object API is ready
	// no need to start the lock maintenance
	// if ObjectAPI is not initialized.
	for {
		objAPI := newObjectLayerFn()
		if objAPI == nil {
			time.Sleep(time.Second)
			continue
		}
		break
	}

	// Initialize a new ticker with a minute between each ticks.
	ticker := time.NewTicker(lockMaintenanceInterval)
	// Stop the timer upon service closure and cleanup the go-routine.
	defer ticker.Stop()

	r := rand.New(rand.NewSource(UTCNow().UnixNano()))
	for {
		// Verifies every minute for locks held more than 2 minutes.
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Start with random sleep time, so as to avoid
			// "synchronous checks" between servers
			duration := time.Duration(r.Float64() * float64(lockMaintenanceInterval))
			time.Sleep(duration)
			if err := lockMaintenance(ctx, lockValidityCheckInterval); err != nil {
				// Sleep right after an error.
				duration := time.Duration(r.Float64() * float64(lockMaintenanceInterval))
				time.Sleep(duration)
			}
		}
	}
}

// registerLockRESTHandlers - register lock rest router.
func registerLockRESTHandlers(router *mux.Router) {
	lockServer := &lockRESTServer{
		ll: newLocker(),
	}

	subrouter := router.PathPrefix(lockRESTPrefix).Subrouter()
	subrouter.Methods(http.MethodPost).Path(lockRESTVersionPrefix + lockRESTMethodHealth).HandlerFunc(httpTraceHdrs(lockServer.HealthHandler))
	subrouter.Methods(http.MethodPost).Path(lockRESTVersionPrefix + lockRESTMethodLock).HandlerFunc(httpTraceHdrs(lockServer.LockHandler))
	subrouter.Methods(http.MethodPost).Path(lockRESTVersionPrefix + lockRESTMethodRLock).HandlerFunc(httpTraceHdrs(lockServer.RLockHandler))
	subrouter.Methods(http.MethodPost).Path(lockRESTVersionPrefix + lockRESTMethodUnlock).HandlerFunc(httpTraceHdrs(lockServer.UnlockHandler))
	subrouter.Methods(http.MethodPost).Path(lockRESTVersionPrefix + lockRESTMethodRUnlock).HandlerFunc(httpTraceHdrs(lockServer.RUnlockHandler))
	subrouter.Methods(http.MethodPost).Path(lockRESTVersionPrefix + lockRESTMethodExpired).HandlerFunc(httpTraceAll(lockServer.ExpiredHandler))
	subrouter.Methods(http.MethodPost).Path(lockRESTVersionPrefix + lockRESTMethodForceUnlock).HandlerFunc(httpTraceAll(lockServer.ForceUnlockHandler))

	globalLockServer = lockServer.ll

	go startLockMaintenance(GlobalContext)
}

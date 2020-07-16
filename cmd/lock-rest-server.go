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
	"path"
	"sort"
	"time"

	"github.com/gorilla/mux"
	"github.com/minio/minio/pkg/dsync"
)

const (
	// Lock maintenance interval.
	lockMaintenanceInterval = 1 * time.Minute

	// Lock validity check interval.
	lockValidityCheckInterval = 2 * time.Minute
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
	if err := storageServerRequestValidate(r); err != nil {
		l.writeErrorResponse(w, err)
		return false
	}
	return true
}

func getLockArgs(r *http.Request) (args dsync.LockArgs, err error) {
	args = dsync.LockArgs{
		UID:    r.URL.Query().Get(lockRESTUID),
		Source: r.URL.Query().Get(lockRESTSource),
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
		l.writeErrorResponse(w, errors.New("Invalid request"))
		return
	}

	args, err := getLockArgs(r)
	if err != nil {
		l.writeErrorResponse(w, err)
		return
	}

	success, err := l.ll.Lock(args)
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
		l.writeErrorResponse(w, errors.New("Invalid request"))
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
		l.writeErrorResponse(w, errors.New("Invalid request"))
		return
	}

	args, err := getLockArgs(r)
	if err != nil {
		l.writeErrorResponse(w, err)
		return
	}

	success, err := l.ll.RLock(args)
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
		l.writeErrorResponse(w, errors.New("Invalid request"))
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

// ExpiredHandler - query expired lock status.
func (l *lockRESTServer) ExpiredHandler(w http.ResponseWriter, r *http.Request) {
	if !l.IsValid(w, r) {
		l.writeErrorResponse(w, errors.New("Invalid request"))
		return
	}

	args, err := getLockArgs(r)
	if err != nil {
		l.writeErrorResponse(w, err)
		return
	}

	l.ll.mutex.Lock()
	defer l.ll.mutex.Unlock()

	// Lock found, proceed to verify if belongs to given uid.
	for _, resource := range args.Resources {
		if lri, ok := l.ll.lockMap[resource]; ok {
			// Check whether uid is still active
			for _, entry := range lri {
				if entry.UID == args.UID {
					l.writeErrorResponse(w, errLockNotExpired)
					return
				}
			}
		}
	}
}

// nameLockRequesterInfoPair is a helper type for lock maintenance
type nameLockRequesterInfoPair struct {
	name string
	lri  lockRequesterInfo
}

// getLongLivedLocks returns locks that are older than a certain time and
// have not been 'checked' for validity too soon enough
func getLongLivedLocks(interval time.Duration) map[Endpoint][]nameLockRequesterInfoPair {
	nlripMap := make(map[Endpoint][]nameLockRequesterInfoPair)
	for endpoint, locker := range globalLockServers {
		rslt := []nameLockRequesterInfoPair{}
		locker.mutex.Lock()
		for name, lriArray := range locker.lockMap {
			for idx := range lriArray {
				// Check whether enough time has gone by since last check
				if time.Since(lriArray[idx].TimeLastCheck) >= interval {
					rslt = append(rslt, nameLockRequesterInfoPair{name: name, lri: lriArray[idx]})
					lriArray[idx].TimeLastCheck = UTCNow()
				}
			}
		}
		nlripMap[endpoint] = rslt
		locker.mutex.Unlock()
	}
	return nlripMap
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
	// Validate if long lived locks are indeed clean.
	// Get list of long lived locks to check for staleness.
	for lendpoint, nlrips := range getLongLivedLocks(interval) {
		nlripsMap := make(map[string]int, len(nlrips))
		for _, nlrip := range nlrips {
			// Locks are only held on first zone, make sure that
			// we only look for ownership of locks from endpoints
			// on first zone.
			for _, endpoint := range globalEndpoints[0].Endpoints {
				c := newLockAPI(endpoint)
				if !c.IsOnline() {
					nlripsMap[nlrip.name]++
					continue
				}

				// Call back to original server verify whether the lock is
				// still active (based on name & uid)
				expired, err := c.Expired(dsync.LockArgs{
					UID:       nlrip.lri.UID,
					Resources: []string{nlrip.name},
				})
				if err != nil {
					nlripsMap[nlrip.name]++
					c.Close()
					continue
				}

				if !expired {
					nlripsMap[nlrip.name]++
				}

				// Close the connection regardless of the call response.
				c.Close()
			}

			// Read locks we assume quorum for be N/2 success
			quorum := globalErasureSetDriveCount / 2
			if nlrip.lri.Writer {
				// For write locks we need N/2+1 success
				quorum = globalErasureSetDriveCount/2 + 1
			}

			// less than the quorum, we have locks expired.
			if nlripsMap[nlrip.name] < quorum {
				// The lock is no longer active at server that originated
				// the lock, attempt to remove the lock.
				globalLockServers[lendpoint].mutex.Lock()
				// Purge the stale entry if it exists.
				globalLockServers[lendpoint].removeEntryIfExists(nlrip)
				globalLockServers[lendpoint].mutex.Unlock()
			}

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
		objAPI := newObjectLayerWithoutSafeModeFn()
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
func registerLockRESTHandlers(router *mux.Router, endpointZones EndpointZones) {
	queries := restQueries(lockRESTUID, lockRESTSource)
	for _, ep := range endpointZones {
		for _, endpoint := range ep.Endpoints {
			if !endpoint.IsLocal {
				continue
			}

			lockServer := &lockRESTServer{
				ll: newLocker(endpoint),
			}

			subrouter := router.PathPrefix(path.Join(lockRESTPrefix, endpoint.Path)).Subrouter()
			subrouter.Methods(http.MethodPost).Path(lockRESTVersionPrefix + lockRESTMethodHealth).HandlerFunc(httpTraceHdrs(lockServer.HealthHandler)).Queries(queries...)
			subrouter.Methods(http.MethodPost).Path(lockRESTVersionPrefix + lockRESTMethodLock).HandlerFunc(httpTraceHdrs(lockServer.LockHandler)).Queries(queries...)
			subrouter.Methods(http.MethodPost).Path(lockRESTVersionPrefix + lockRESTMethodRLock).HandlerFunc(httpTraceHdrs(lockServer.RLockHandler)).Queries(queries...)
			subrouter.Methods(http.MethodPost).Path(lockRESTVersionPrefix + lockRESTMethodUnlock).HandlerFunc(httpTraceHdrs(lockServer.UnlockHandler)).Queries(queries...)
			subrouter.Methods(http.MethodPost).Path(lockRESTVersionPrefix + lockRESTMethodRUnlock).HandlerFunc(httpTraceHdrs(lockServer.RUnlockHandler)).Queries(queries...)
			subrouter.Methods(http.MethodPost).Path(lockRESTVersionPrefix + lockRESTMethodExpired).HandlerFunc(httpTraceAll(lockServer.ExpiredHandler)).Queries(queries...)

			globalLockServers[endpoint] = lockServer.ll
		}
	}

	go startLockMaintenance(GlobalContext)
}

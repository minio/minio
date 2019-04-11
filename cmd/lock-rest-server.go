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
	"context"
	"encoding/gob"
	"errors"
	"math/rand"
	"net/http"
	"time"

	"github.com/gorilla/mux"
	"github.com/minio/dsync"
	"github.com/minio/minio/cmd/logger"
	xnet "github.com/minio/minio/pkg/net"
)

const (
	// Lock rpc server endpoint.
	lockServiceSubPath = "/lock"

	// Lock maintenance interval.
	lockMaintenanceInterval = 1 * time.Minute

	// Lock validity check interval.
	lockValidityCheckInterval = 2 * time.Minute
)

// To abstract a node over network.
type lockRESTServer struct {
	ll localLocker
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

// LockHandler - Acquires a lock.
func (l *lockRESTServer) LockHandler(w http.ResponseWriter, r *http.Request) {
	if !l.IsValid(w, r) {
		l.writeErrorResponse(w, errors.New("Invalid request"))
		return
	}

	ctx := newContext(r, w, "Lock")

	var lockArgs dsync.LockArgs
	if r.ContentLength < 0 {
		l.writeErrorResponse(w, errInvalidArgument)
		return
	}

	err := gob.NewDecoder(r.Body).Decode(&lockArgs)
	if err != nil {
		l.writeErrorResponse(w, err)
		return
	}

	success, err := l.ll.Lock(lockArgs)
	if err != nil {
		l.writeErrorResponse(w, err)
		return
	}
	resp := lockResponse{Success: success}
	logger.LogIf(ctx, gob.NewEncoder(w).Encode(resp))
	w.(http.Flusher).Flush()
}

// UnlockHandler - releases the acquired lock.
func (l *lockRESTServer) UnlockHandler(w http.ResponseWriter, r *http.Request) {
	if !l.IsValid(w, r) {
		l.writeErrorResponse(w, errors.New("Invalid request"))
		return
	}

	ctx := newContext(r, w, "Unlock")

	var lockArgs dsync.LockArgs
	if r.ContentLength < 0 {
		l.writeErrorResponse(w, errInvalidArgument)
		return
	}

	err := gob.NewDecoder(r.Body).Decode(&lockArgs)
	if err != nil {
		l.writeErrorResponse(w, err)
		return
	}

	success, err := l.ll.Unlock(lockArgs)
	if err != nil {
		l.writeErrorResponse(w, err)
		return
	}
	resp := lockResponse{Success: success}
	logger.LogIf(ctx, gob.NewEncoder(w).Encode(resp))
	w.(http.Flusher).Flush()
}

// LockHandler - Acquires an RLock.
func (l *lockRESTServer) RLockHandler(w http.ResponseWriter, r *http.Request) {
	if !l.IsValid(w, r) {
		l.writeErrorResponse(w, errors.New("Invalid request"))
		return
	}

	ctx := newContext(r, w, "RLock")
	var lockArgs dsync.LockArgs
	if r.ContentLength < 0 {
		l.writeErrorResponse(w, errInvalidArgument)
		return
	}

	err := gob.NewDecoder(r.Body).Decode(&lockArgs)
	if err != nil {
		l.writeErrorResponse(w, err)
		return
	}

	success, err := l.ll.RLock(lockArgs)
	if err != nil {
		l.writeErrorResponse(w, err)
		return
	}
	resp := lockResponse{Success: success}
	logger.LogIf(ctx, gob.NewEncoder(w).Encode(resp))
	w.(http.Flusher).Flush()
}

// RUnlockHandler - releases the acquired read lock.
func (l *lockRESTServer) RUnlockHandler(w http.ResponseWriter, r *http.Request) {
	if !l.IsValid(w, r) {
		l.writeErrorResponse(w, errors.New("Invalid request"))
		return
	}

	ctx := newContext(r, w, "RUnlock")
	var lockArgs dsync.LockArgs
	if r.ContentLength < 0 {
		l.writeErrorResponse(w, errInvalidArgument)
		return
	}

	err := gob.NewDecoder(r.Body).Decode(&lockArgs)
	if err != nil {
		l.writeErrorResponse(w, err)
		return
	}

	success, err := l.ll.RUnlock(lockArgs)
	if err != nil {
		l.writeErrorResponse(w, err)
		return
	}
	resp := lockResponse{Success: success}
	logger.LogIf(ctx, gob.NewEncoder(w).Encode(resp))
	w.(http.Flusher).Flush()
}

// ForceUnlockHandler - force releases the acquired lock.
func (l *lockRESTServer) ForceUnlockHandler(w http.ResponseWriter, r *http.Request) {
	if !l.IsValid(w, r) {
		l.writeErrorResponse(w, errors.New("Invalid request"))
		return
	}

	ctx := newContext(r, w, "ForceUnlock")

	var lockArgs dsync.LockArgs
	if r.ContentLength < 0 {
		l.writeErrorResponse(w, errInvalidArgument)
		return
	}

	err := gob.NewDecoder(r.Body).Decode(&lockArgs)
	if err != nil {
		l.writeErrorResponse(w, err)
		return
	}

	success, err := l.ll.ForceUnlock(lockArgs)
	if err != nil {
		l.writeErrorResponse(w, err)
		return
	}
	resp := lockResponse{Success: success}
	logger.LogIf(ctx, gob.NewEncoder(w).Encode(resp))
	w.(http.Flusher).Flush()
}

// ExpiredHandler - query expired lock status.
func (l *lockRESTServer) ExpiredHandler(w http.ResponseWriter, r *http.Request) {
	if !l.IsValid(w, r) {
		l.writeErrorResponse(w, errors.New("Invalid request"))
		return
	}

	ctx := newContext(r, w, "Expired")

	var lockArgs dsync.LockArgs
	if r.ContentLength < 0 {
		l.writeErrorResponse(w, errInvalidArgument)
		return
	}

	err := gob.NewDecoder(r.Body).Decode(&lockArgs)
	if err != nil {
		l.writeErrorResponse(w, err)
		return
	}
	success := true
	l.ll.mutex.Lock()
	defer l.ll.mutex.Unlock()
	// Lock found, proceed to verify if belongs to given uid.
	if lri, ok := l.ll.lockMap[lockArgs.Resource]; ok {
		// Check whether uid is still active
		for _, entry := range lri {
			if entry.UID == lockArgs.UID {
				success = false // When uid found, lock is still active so return not expired.
				break
			}
		}
	}
	// When we get here lock is no longer active due to either dsync.LockArgs.Resource
	// being absent from map or uid not found for given dsync.LockArgs.Resource
	resp := lockResponse{Success: success}
	logger.LogIf(ctx, gob.NewEncoder(w).Encode(resp))
	w.(http.Flusher).Flush()
}

// lockMaintenance loops over locks that have been active for some time and checks back
// with the original server whether it is still alive or not
//
// Following logic inside ignores the errors generated for Dsync.Active operation.
// - server at client down
// - some network error (and server is up normally)
//
// We will ignore the error, and we will retry later to get a resolve on this lock
func (l *lockRESTServer) lockMaintenance(interval time.Duration) {
	l.ll.mutex.Lock()
	// Get list of long lived locks to check for staleness.
	nlripLongLived := getLongLivedLocks(l.ll.lockMap, interval)
	l.ll.mutex.Unlock()

	// Validate if long lived locks are indeed clean.
	for _, nlrip := range nlripLongLived {
		// Initialize client based on the long live locks.
		host, err := xnet.ParseHost(nlrip.lri.Node)
		if err != nil {
			logger.LogIf(context.Background(), err)
			continue
		}
		c := newlockRESTClient(host)
		if !c.connected {
			continue
		}

		// Call back to original server verify whether the lock is still active (based on name & uid)
		expired, _ := c.Expired(dsync.LockArgs{
			UID:      nlrip.lri.UID,
			Resource: nlrip.name,
		})

		// Close the connection regardless of the call response.
		c.Close()

		// For successful response, verify if lock is indeed active or stale.
		if expired {
			// The lock is no longer active at server that originated the lock
			// So remove the lock from the map.
			l.ll.mutex.Lock()
			l.ll.removeEntryIfExists(nlrip) // Purge the stale entry if it exists.
			l.ll.mutex.Unlock()
		}
	}
}

// Start lock maintenance from all lock servers.
func startLockMaintenance(lkSrv *lockRESTServer) {
	// Initialize a new ticker with a minute between each ticks.
	ticker := time.NewTicker(lockMaintenanceInterval)
	// Stop the timer upon service closure and cleanup the go-routine.
	defer ticker.Stop()

	// Start with random sleep time, so as to avoid "synchronous checks" between servers
	time.Sleep(time.Duration(rand.Float64() * float64(lockMaintenanceInterval)))
	for {
		// Verifies every minute for locks held more than 2 minutes.
		select {
		case <-GlobalServiceDoneCh:
			return
		case <-ticker.C:
			lkSrv.lockMaintenance(lockValidityCheckInterval)
		}
	}
}

// registerLockRESTHandlers - register lock rest router.
func registerLockRESTHandlers(router *mux.Router) {
	subrouter := router.PathPrefix(lockRESTPath).Subrouter()
	subrouter.Methods(http.MethodPost).Path("/" + lockRESTMethodLock).HandlerFunc(httpTraceHdrs(globalLockServer.LockHandler))
	subrouter.Methods(http.MethodPost).Path("/" + lockRESTMethodRLock).HandlerFunc(httpTraceHdrs(globalLockServer.RLockHandler))
	subrouter.Methods(http.MethodPost).Path("/" + lockRESTMethodUnlock).HandlerFunc(httpTraceHdrs(globalLockServer.UnlockHandler))
	subrouter.Methods(http.MethodPost).Path("/" + lockRESTMethodRUnlock).HandlerFunc(httpTraceHdrs(globalLockServer.RUnlockHandler))
	subrouter.Methods(http.MethodPost).Path("/" + lockRESTMethodForceUnlock).HandlerFunc(httpTraceHdrs(globalLockServer.ForceUnlockHandler))
	subrouter.Methods(http.MethodPost).Path("/" + lockRESTMethodExpired).HandlerFunc(httpTraceAll(globalLockServer.ExpiredHandler))
	router.NotFoundHandler = http.HandlerFunc(httpTraceAll(notFoundHandler))

	// Start lock maintenance from all lock servers.
	go startLockMaintenance(globalLockServer)
}

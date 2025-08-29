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
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"time"

	"github.com/minio/mux"
)

const numberOfNodes = 5

var (
	ds          *Dsync
	nodes       = make([]*httptest.Server, numberOfNodes) // list of node IP addrs or hostname with ports.
	lockServers = make([]*lockServer, numberOfNodes)
)

func getLockArgs(r *http.Request) (args LockArgs, err error) {
	buf, err := io.ReadAll(r.Body)
	if err != nil {
		return args, err
	}
	_, err = args.UnmarshalMsg(buf)
	return args, err
}

type lockServerHandler struct {
	lsrv *lockServer
}

func (lh *lockServerHandler) writeErrorResponse(w http.ResponseWriter, err error) {
	w.WriteHeader(http.StatusForbidden)
	w.Write([]byte(err.Error()))
}

func (lh *lockServerHandler) ForceUnlockHandler(w http.ResponseWriter, r *http.Request) {
	args, err := getLockArgs(r)
	if err != nil {
		lh.writeErrorResponse(w, err)
		return
	}

	if _, err = lh.lsrv.ForceUnlock(&args); err != nil {
		lh.writeErrorResponse(w, err)
		return
	}
}

func (lh *lockServerHandler) RefreshHandler(w http.ResponseWriter, r *http.Request) {
	args, err := getLockArgs(r)
	if err != nil {
		lh.writeErrorResponse(w, err)
		return
	}

	reply, err := lh.lsrv.Refresh(&args)
	if err != nil {
		lh.writeErrorResponse(w, err)
		return
	}

	if !reply {
		lh.writeErrorResponse(w, errLockNotFound)
		return
	}
}

func (lh *lockServerHandler) LockHandler(w http.ResponseWriter, r *http.Request) {
	args, err := getLockArgs(r)
	if err != nil {
		lh.writeErrorResponse(w, err)
		return
	}
	reply, err := lh.lsrv.Lock(&args)
	if err == nil && !reply {
		err = errLockConflict
	}
	if err != nil {
		lh.writeErrorResponse(w, err)
		return
	}
}

func (lh *lockServerHandler) UnlockHandler(w http.ResponseWriter, r *http.Request) {
	args, err := getLockArgs(r)
	if err != nil {
		lh.writeErrorResponse(w, err)
		return
	}
	_, err = lh.lsrv.Unlock(&args)
	if err != nil {
		lh.writeErrorResponse(w, err)
		return
	}
}

func (lh *lockServerHandler) RUnlockHandler(w http.ResponseWriter, r *http.Request) {
	args, err := getLockArgs(r)
	if err != nil {
		lh.writeErrorResponse(w, err)
		return
	}
	_, err = lh.lsrv.RUnlock(&args)
	if err != nil {
		lh.writeErrorResponse(w, err)
		return
	}
}

func (lh *lockServerHandler) HealthHandler(w http.ResponseWriter, r *http.Request) {}

func (lh *lockServerHandler) RLockHandler(w http.ResponseWriter, r *http.Request) {
	args, err := getLockArgs(r)
	if err != nil {
		lh.writeErrorResponse(w, err)
		return
	}

	reply, err := lh.lsrv.RLock(&args)
	if err == nil && !reply {
		err = errLockConflict
	}
	if err != nil {
		lh.writeErrorResponse(w, err)
		return
	}
}

func stopLockServers() {
	for i := range numberOfNodes {
		nodes[i].Close()
	}
}

func startLockServers() {
	for i := range numberOfNodes {
		lsrv := &lockServer{
			mutex:   sync.Mutex{},
			lockMap: make(map[string]int64),
		}
		lockServer := lockServerHandler{
			lsrv: lsrv,
		}
		lockServers[i] = lsrv

		router := mux.NewRouter().SkipClean(true)
		subrouter := router.PathPrefix("/").Subrouter()
		subrouter.Methods(http.MethodPost).Path("/v1/health").HandlerFunc(lockServer.HealthHandler)
		subrouter.Methods(http.MethodPost).Path("/v1/refresh").HandlerFunc(lockServer.RefreshHandler)
		subrouter.Methods(http.MethodPost).Path("/v1/lock").HandlerFunc(lockServer.LockHandler)
		subrouter.Methods(http.MethodPost).Path("/v1/rlock").HandlerFunc(lockServer.RLockHandler)
		subrouter.Methods(http.MethodPost).Path("/v1/unlock").HandlerFunc(lockServer.UnlockHandler)
		subrouter.Methods(http.MethodPost).Path("/v1/runlock").HandlerFunc(lockServer.RUnlockHandler)
		subrouter.Methods(http.MethodPost).Path("/v1/force-unlock").HandlerFunc(lockServer.ForceUnlockHandler)

		nodes[i] = httptest.NewServer(router)
	}
}

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

func (l *lockServer) Lock(args *LockArgs) (reply bool, err error) {
	if d := atomic.LoadInt64(&l.responseDelay); d != 0 {
		time.Sleep(time.Duration(d))
	}

	l.mutex.Lock()
	defer l.mutex.Unlock()
	if _, reply = l.lockMap[args.Resources[0]]; !reply {
		l.lockMap[args.Resources[0]] = WriteLock // No locks held on the given name, so claim write lock
	}
	reply = !reply // Negate *reply to return true when lock is granted or false otherwise
	return reply, nil
}

func (l *lockServer) Unlock(args *LockArgs) (reply bool, err error) {
	if d := atomic.LoadInt64(&l.responseDelay); d != 0 {
		time.Sleep(time.Duration(d))
	}

	l.mutex.Lock()
	defer l.mutex.Unlock()
	var locksHeld int64
	if locksHeld, reply = l.lockMap[args.Resources[0]]; !reply { // No lock is held on the given name
		return false, fmt.Errorf("Unlock attempted on an unlocked entity: %s", args.Resources[0])
	}
	if reply = locksHeld == WriteLock; !reply { // Unless it is a write lock
		return false, fmt.Errorf("Unlock attempted on a read locked entity: %s (%d read locks active)", args.Resources[0], locksHeld)
	}
	delete(l.lockMap, args.Resources[0]) // Remove the write lock
	return true, nil
}

const ReadLock = 1

func (l *lockServer) RLock(args *LockArgs) (reply bool, err error) {
	if d := atomic.LoadInt64(&l.responseDelay); d != 0 {
		time.Sleep(time.Duration(d))
	}

	l.mutex.Lock()
	defer l.mutex.Unlock()
	var locksHeld int64
	if locksHeld, reply = l.lockMap[args.Resources[0]]; !reply {
		l.lockMap[args.Resources[0]] = ReadLock // No locks held on the given name, so claim (first) read lock
		reply = true
	} else if reply = locksHeld != WriteLock; reply { // Unless there is a write lock
		l.lockMap[args.Resources[0]] = locksHeld + ReadLock // Grant another read lock
	}
	return reply, nil
}

func (l *lockServer) RUnlock(args *LockArgs) (reply bool, err error) {
	if d := atomic.LoadInt64(&l.responseDelay); d != 0 {
		time.Sleep(time.Duration(d))
	}

	l.mutex.Lock()
	defer l.mutex.Unlock()
	var locksHeld int64
	if locksHeld, reply = l.lockMap[args.Resources[0]]; !reply { // No lock is held on the given name
		return false, fmt.Errorf("RUnlock attempted on an unlocked entity: %s", args.Resources[0])
	}
	if reply = locksHeld != WriteLock; !reply { // A write-lock is held, cannot release a read lock
		return false, fmt.Errorf("RUnlock attempted on a write locked entity: %s", args.Resources[0])
	}
	if locksHeld > ReadLock {
		l.lockMap[args.Resources[0]] = locksHeld - ReadLock // Remove one of the read locks held
	} else {
		delete(l.lockMap, args.Resources[0]) // Remove the (last) read lock
	}
	return reply, nil
}

func (l *lockServer) Refresh(args *LockArgs) (reply bool, err error) {
	if d := atomic.LoadInt64(&l.responseDelay); d != 0 {
		time.Sleep(time.Duration(d))
	}

	l.mutex.Lock()
	defer l.mutex.Unlock()
	reply = !l.lockNotFound
	return reply, nil
}

func (l *lockServer) ForceUnlock(args *LockArgs) (reply bool, err error) {
	if d := atomic.LoadInt64(&l.responseDelay); d != 0 {
		time.Sleep(time.Duration(d))
	}

	l.mutex.Lock()
	defer l.mutex.Unlock()
	if len(args.UID) != 0 {
		return false, fmt.Errorf("ForceUnlock called with non-empty UID: %s", args.UID)
	}
	delete(l.lockMap, args.Resources[0]) // Remove the lock (irrespective of write or read lock)
	reply = true
	return reply, nil
}

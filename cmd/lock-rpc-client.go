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

package cmd

import (
	"fmt"
	"github.com/minio/dsync"
	"net"
	"time"
)

// So we try arguing lock 2 times more often than we check the disk status
const lockTimeoutRetryTime = globalStorageHealthCheckInterval / 2

// LockRPCClient is authenticable lock RPC client compatible to dsync.NetLocker
type LockRPCClient struct {
	*AuthRPCClient
	lastTimeoutError time.Time
}

// newLockRPCClient returns new lock RPC client object.
func newLockRPCClient(config authConfig) *LockRPCClient {
	return &LockRPCClient{newAuthRPCClient(config), time.Time{}}
}

// RLock calls read lock RPC.
func (lockRPCClient *LockRPCClient) RLock(args dsync.LockArgs) (reply bool, err error) {
	lockArgs := newLockArgs(args)
	err = lockRPCClient.call("Dsync.RLock", &lockArgs, &reply)
	return reply, err
}

// Lock calls write lock RPC.
func (lockRPCClient *LockRPCClient) Lock(args dsync.LockArgs) (reply bool, err error) {
	lockArgs := newLockArgs(args)
	err = lockRPCClient.call("Dsync.Lock", &lockArgs, &reply)
	return reply, err
}

// RUnlock calls read unlock RPC.
func (lockRPCClient *LockRPCClient) RUnlock(args dsync.LockArgs) (reply bool, err error) {
	lockArgs := newLockArgs(args)
	err = lockRPCClient.call("Dsync.RUnlock", &lockArgs, &reply)
	return reply, err
}

// Unlock calls write unlock RPC.
func (lockRPCClient *LockRPCClient) Unlock(args dsync.LockArgs) (reply bool, err error) {
	lockArgs := newLockArgs(args)
	err = lockRPCClient.call("Dsync.Unlock", &lockArgs, &reply)
	return reply, err
}

// ForceUnlock calls force unlock RPC.
func (lockRPCClient *LockRPCClient) ForceUnlock(args dsync.LockArgs) (reply bool, err error) {
	lockArgs := newLockArgs(args)
	err = lockRPCClient.call("Dsync.ForceUnlock", &lockArgs, &reply)
	return reply, err
}

// Expired calls expired RPC.
func (lockRPCClient *LockRPCClient) Expired(args dsync.LockArgs) (reply bool, err error) {
	lockArgs := newLockArgs(args)
	err = lockRPCClient.call("Dsync.Expired", &lockArgs, &reply)
	return reply, err
}

// Make RPC call with timeout error cooldown check.
func (lockRPCClient *LockRPCClient) call(serviceMethod string, args interface {
	SetAuthToken(authToken string)
}, reply interface{}) (err error) {

	if lockRPCClient.lastTimeoutError.Add(lockTimeoutRetryTime).After(time.Now()) {
		// we are still in cooldown period
		return &net.OpError{
			Op:   "dial-http",
			Net:  lockRPCClient.config.serverAddr,
			Addr: nil,
			Err:  fmt.Errorf("%s rpc connection for locking timeouted in near history, next retry in %v seconds", lockRPCClient.config.serverAddr, lockRPCClient.lastTimeoutError.Add(lockTimeoutRetryTime).Sub(time.Now()).Seconds()),
		}
	}

	err = lockRPCClient.AuthRPCClient.Call(serviceMethod, args, reply)

	// if it's timeouterror, mark time and start cooldown for this rpc connection
	switch err.(type) {
	case *net.OpError:
		if err.(*net.OpError).Timeout() {
			lockRPCClient.lastTimeoutError = time.Now()

			log.Printf("%s: RPC connection for locking timeouted, Retrying next time to that server in %v seconds\n", lockRPCClient.config.serverAddr, lockTimeoutRetryTime.Seconds())
		}
	}

	return err
}

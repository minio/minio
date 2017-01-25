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

import "github.com/minio/dsync"

// LockRPCClient is authenticable lock RPC client compatible to dsync.NetLocker
type LockRPCClient struct {
	*AuthRPCClient
}

// newLockRPCClient returns new lock RPC client object.
func newLockRPCClient(config authConfig) *LockRPCClient {
	return &LockRPCClient{newAuthRPCClient(config)}
}

// RLock calls read lock RPC.
func (lockRPCClient *LockRPCClient) RLock(args dsync.LockArgs) (reply bool, err error) {
	lockArgs := newLockArgs(args)
	err = lockRPCClient.AuthRPCClient.Call("Dsync.RLock", &lockArgs, &reply)
	return reply, err
}

// Lock calls write lock RPC.
func (lockRPCClient *LockRPCClient) Lock(args dsync.LockArgs) (reply bool, err error) {
	lockArgs := newLockArgs(args)
	err = lockRPCClient.AuthRPCClient.Call("Dsync.Lock", &lockArgs, &reply)
	return reply, err
}

// RUnlock calls read unlock RPC.
func (lockRPCClient *LockRPCClient) RUnlock(args dsync.LockArgs) (reply bool, err error) {
	lockArgs := newLockArgs(args)
	err = lockRPCClient.AuthRPCClient.Call("Dsync.RUnlock", &lockArgs, &reply)
	return reply, err
}

// Unlock calls write unlock RPC.
func (lockRPCClient *LockRPCClient) Unlock(args dsync.LockArgs) (reply bool, err error) {
	lockArgs := newLockArgs(args)
	err = lockRPCClient.AuthRPCClient.Call("Dsync.Unlock", &lockArgs, &reply)
	return reply, err
}

// ForceUnlock calls force unlock RPC.
func (lockRPCClient *LockRPCClient) ForceUnlock(args dsync.LockArgs) (reply bool, err error) {
	lockArgs := newLockArgs(args)
	err = lockRPCClient.AuthRPCClient.Call("Dsync.ForceUnlock", &lockArgs, &reply)
	return reply, err
}

// Expired calls expired RPC.
func (lockRPCClient *LockRPCClient) Expired(args dsync.LockArgs) (reply bool, err error) {
	lockArgs := newLockArgs(args)
	err = lockRPCClient.AuthRPCClient.Call("Dsync.Expired", &lockArgs, &reply)
	return reply, err
}

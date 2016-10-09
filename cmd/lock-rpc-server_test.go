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
	"sync"
	"testing"
	"time"
)

// Helper function to test equality of locks (without taking timing info into account)
func testLockEquality(lriLeft, lriRight []lockRequesterInfo) bool {

	if len(lriLeft) != len(lriRight) {
		return false
	}

	for i := 0; i < len(lriLeft); i++ {
		if lriLeft[i].writer != lriRight[i].writer ||
			lriLeft[i].node != lriRight[i].node ||
			lriLeft[i].rpcPath != lriRight[i].rpcPath ||
			lriLeft[i].uid != lriRight[i].uid {
			return false
		}
	}
	return true
}

// Test Lock functionality
func TestLockRpcServerLock(t *testing.T) {

	tsValidateArgs := time.Now().UTC()
	locker := &lockServer{
		rpcPath:   "rpc-path",
		mutex:     sync.Mutex{},
		lockMap:   make(map[string][]lockRequesterInfo),
		timestamp: tsValidateArgs,
	}

	la := LockArgs{
		Name:      "name",
		Token:     "token",
		Timestamp: tsValidateArgs,
		Node:      "node",
		RPCPath:   "rpc-path",
		UID:       "0123-4567",
	}

	var result bool
	err := locker.Lock(&la, &result)
	if err != nil {
		t.Errorf("Expected %#v, got %#v", nil, err)
	} else {
		if !result {
			t.Errorf("Expected %#v, got %#v", true, result)
		} else {
			gotLri, _ := locker.lockMap["name"]
			expectedLri := []lockRequesterInfo{
				lockRequesterInfo{
					writer:        true,
					node:          "node",
					rpcPath:       "rpc-path",
					uid:           "0123-4567",
					timestamp:     tsValidateArgs,
					timeLastCheck: tsValidateArgs,
				},
			}
			if !testLockEquality(expectedLri, gotLri) {
				t.Errorf("Expected %#v, got %#v", expectedLri, gotLri)
			}
		}
	}

	la2 := LockArgs{
		Name:      "name",
		Token:     "token",
		Timestamp: tsValidateArgs,
		Node:      "node",
		RPCPath:   "rpc-path",
		UID:       "89ab-cdef",
	}
	err = locker.Lock(&la2, &result)
	if err != nil {
		t.Errorf("Expected %#v, got %#v", nil, err)
	} else {
		if result {
			t.Errorf("Expected %#v, got %#v", false, result)
		}
	}
}

// Test Unlock functionality
func TestLockRpcServerUnlock(t *testing.T) {

	tsValidateArgs := time.Now().UTC()
	locker := &lockServer{
		rpcPath:   "rpc-path",
		mutex:     sync.Mutex{},
		lockMap:   make(map[string][]lockRequesterInfo),
		timestamp: tsValidateArgs,
	}

	la := LockArgs{
		Name:      "name",
		Token:     "token",
		Timestamp: tsValidateArgs,
		Node:      "node",
		RPCPath:   "rpc-path",
		UID:       "0123-4567",
	}

	// First test return of error when attempting to unlock a lock that does not exist
	var result bool
	err := locker.Unlock(&la, &result)
	if err == nil {
		t.Errorf("Expected error, got %#v", nil)
	}

	// Create lock (so that we can release)
	err = locker.Lock(&la, &result)
	if err != nil {
		t.Errorf("Expected %#v, got %#v", nil, err)
	} else if !result {
		t.Errorf("Expected %#v, got %#v", true, result)
	}

	err = locker.Unlock(&la, &result)
	if err != nil {
		t.Errorf("Expected %#v, got %#v", nil, err)
	} else {
		if !result {
			t.Errorf("Expected %#v, got %#v", true, result)
		} else {
			gotLri, _ := locker.lockMap["name"]
			expectedLri := []lockRequesterInfo(nil)
			if !testLockEquality(expectedLri, gotLri) {
				t.Errorf("Expected %#v, got %#v", expectedLri, gotLri)
			}
		}
	}
}

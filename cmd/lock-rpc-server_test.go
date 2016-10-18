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
	"runtime"
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

// Helper function to create a lock server for testing
func createLockTestServer(t *testing.T) (string, *lockServer, string) {
	testPath, err := newTestConfig("us-east-1")
	if err != nil {
		t.Fatalf("unable initialize config file, %s", err)
	}

	jwt, err := newJWT(defaultJWTExpiry)
	if err != nil {
		t.Fatalf("unable to get new JWT, %s", err)
	}

	err = jwt.Authenticate(serverConfig.GetCredential().AccessKeyID, serverConfig.GetCredential().SecretAccessKey)
	if err != nil {
		t.Fatalf("unable for JWT to authenticate, %s", err)
	}

	token, err := jwt.GenerateToken(serverConfig.GetCredential().AccessKeyID)
	if err != nil {
		t.Fatalf("unable for JWT to generate token, %s", err)
	}

	locker := &lockServer{
		rpcPath: "rpc-path",
		mutex:   sync.Mutex{},
		lockMap: make(map[string][]lockRequesterInfo),
	}

	return testPath, locker, token
}

// Test Lock functionality
func TestLockRpcServerLock(t *testing.T) {

	timestamp := time.Now().UTC()
	testPath, locker, token := createLockTestServer(t)
	defer removeAll(testPath)

	la := LockArgs{
		Name:      "name",
		Token:     token,
		Timestamp: timestamp,
		Node:      "node",
		RPCPath:   "rpc-path",
		UID:       "0123-4567",
	}

	// Claim a lock
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
				{
					writer:  true,
					node:    "node",
					rpcPath: "rpc-path",
					uid:     "0123-4567",
				},
			}
			if !testLockEquality(expectedLri, gotLri) {
				t.Errorf("Expected %#v, got %#v", expectedLri, gotLri)
			}
		}
	}

	// Try to claim same lock again (will fail)
	la2 := LockArgs{
		Name:      "name",
		Token:     token,
		Timestamp: timestamp,
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

	timestamp := time.Now().UTC()
	testPath, locker, token := createLockTestServer(t)
	defer removeAll(testPath)

	la := LockArgs{
		Name:      "name",
		Token:     token,
		Timestamp: timestamp,
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

	// Finally test successful release of lock
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

// Test RLock functionality
func TestLockRpcServerRLock(t *testing.T) {

	timestamp := time.Now().UTC()
	testPath, locker, token := createLockTestServer(t)
	defer removeAll(testPath)

	la := LockArgs{
		Name:      "name",
		Token:     token,
		Timestamp: timestamp,
		Node:      "node",
		RPCPath:   "rpc-path",
		UID:       "0123-4567",
	}

	// Claim a lock
	var result bool
	err := locker.RLock(&la, &result)
	if err != nil {
		t.Errorf("Expected %#v, got %#v", nil, err)
	} else {
		if !result {
			t.Errorf("Expected %#v, got %#v", true, result)
		} else {
			gotLri, _ := locker.lockMap["name"]
			expectedLri := []lockRequesterInfo{
				{
					writer:  false,
					node:    "node",
					rpcPath: "rpc-path",
					uid:     "0123-4567",
				},
			}
			if !testLockEquality(expectedLri, gotLri) {
				t.Errorf("Expected %#v, got %#v", expectedLri, gotLri)
			}
		}
	}

	// Try to claim same again (will succeed)
	la2 := LockArgs{
		Name:      "name",
		Token:     token,
		Timestamp: timestamp,
		Node:      "node",
		RPCPath:   "rpc-path",
		UID:       "89ab-cdef",
	}
	err = locker.RLock(&la2, &result)
	if err != nil {
		t.Errorf("Expected %#v, got %#v", nil, err)
	} else {
		if !result {
			t.Errorf("Expected %#v, got %#v", true, result)
		}
	}
}

// Test RUnlock functionality
func TestLockRpcServerRUnlock(t *testing.T) {

	timestamp := time.Now().UTC()
	testPath, locker, token := createLockTestServer(t)
	defer removeAll(testPath)

	la := LockArgs{
		Name:      "name",
		Token:     token,
		Timestamp: timestamp,
		Node:      "node",
		RPCPath:   "rpc-path",
		UID:       "0123-4567",
	}

	// First test return of error when attempting to unlock a read-lock that does not exist
	var result bool
	err := locker.Unlock(&la, &result)
	if err == nil {
		t.Errorf("Expected error, got %#v", nil)
	}

	// Create first lock ... (so that we can release)
	err = locker.RLock(&la, &result)
	if err != nil {
		t.Errorf("Expected %#v, got %#v", nil, err)
	} else if !result {
		t.Errorf("Expected %#v, got %#v", true, result)
	}

	la2 := LockArgs{
		Name:      "name",
		Token:     token,
		Timestamp: timestamp,
		Node:      "node",
		RPCPath:   "rpc-path",
		UID:       "89ab-cdef",
	}

	// ... and create a second lock on same resource
	err = locker.RLock(&la2, &result)
	if err != nil {
		t.Errorf("Expected %#v, got %#v", nil, err)
	} else if !result {
		t.Errorf("Expected %#v, got %#v", true, result)
	}

	// Test successful release of first read lock
	err = locker.RUnlock(&la, &result)
	if err != nil {
		t.Errorf("Expected %#v, got %#v", nil, err)
	} else {
		if !result {
			t.Errorf("Expected %#v, got %#v", true, result)
		} else {
			gotLri, _ := locker.lockMap["name"]
			expectedLri := []lockRequesterInfo{
				{
					writer:  false,
					node:    "node",
					rpcPath: "rpc-path",
					uid:     "89ab-cdef",
				},
			}
			if !testLockEquality(expectedLri, gotLri) {
				t.Errorf("Expected %#v, got %#v", expectedLri, gotLri)
			}

		}
	}

	// Finally test successful release of second (and last) read lock
	err = locker.RUnlock(&la2, &result)
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

// Test ForceUnlock functionality
func TestLockRpcServerForceUnlock(t *testing.T) {

	timestamp := time.Now().UTC()
	testPath, locker, token := createLockTestServer(t)
	defer removeAll(testPath)

	laForce := LockArgs{
		Name:      "name",
		Token:     token,
		Timestamp: timestamp,
		Node:      "node",
		RPCPath:   "rpc-path",
		UID:       "1234-5678",
	}

	// First test that UID should be empty
	var result bool
	err := locker.ForceUnlock(&laForce, &result)
	if err == nil {
		t.Errorf("Expected error, got %#v", nil)
	}

	// Then test force unlock of a lock that does not exist (not returning an error)
	laForce.UID = ""
	err = locker.ForceUnlock(&laForce, &result)
	if err != nil {
		t.Errorf("Expected no error, got %#v", err)
	}

	la := LockArgs{
		Name:      "name",
		Token:     token,
		Timestamp: timestamp,
		Node:      "node",
		RPCPath:   "rpc-path",
		UID:       "0123-4567",
	}

	// Create lock ... (so that we can force unlock)
	err = locker.Lock(&la, &result)
	if err != nil {
		t.Errorf("Expected %#v, got %#v", nil, err)
	} else if !result {
		t.Errorf("Expected %#v, got %#v", true, result)
	}

	// Forcefully unlock the lock (not returning an error)
	err = locker.ForceUnlock(&laForce, &result)
	if err != nil {
		t.Errorf("Expected no error, got %#v", err)
	}

	// Try to get lock again (should be granted)
	err = locker.Lock(&la, &result)
	if err != nil {
		t.Errorf("Expected %#v, got %#v", nil, err)
	} else if !result {
		t.Errorf("Expected %#v, got %#v", true, result)
	}

	// Finally forcefully unlock the lock once again
	err = locker.ForceUnlock(&laForce, &result)
	if err != nil {
		t.Errorf("Expected no error, got %#v", err)
	}
}

// Test Expired functionality
func TestLockRpcServerExpired(t *testing.T) {
	timestamp := time.Now().UTC()
	testPath, locker, token := createLockTestServer(t)
	defer removeAll(testPath)

	la := LockArgs{
		Name:      "name",
		Token:     token,
		Timestamp: timestamp,
		Node:      "node",
		RPCPath:   "rpc-path",
		UID:       "0123-4567",
	}

	// Unknown lock at server will return expired = true
	var expired bool
	err := locker.Expired(&la, &expired)
	if err != nil {
		t.Errorf("Expected no error, got %#v", err)
	} else {
		if !expired {
			t.Errorf("Expected %#v, got %#v", true, expired)
		}
	}

	// Create lock (so that we can test that it is not expired)
	var result bool
	err = locker.Lock(&la, &result)
	if err != nil {
		t.Errorf("Expected %#v, got %#v", nil, err)
	} else if !result {
		t.Errorf("Expected %#v, got %#v", true, result)
	}

	err = locker.Expired(&la, &expired)
	if err != nil {
		t.Errorf("Expected no error, got %#v", err)
	} else {
		if expired {
			t.Errorf("Expected %#v, got %#v", false, expired)
		}
	}
}

// Test initialization of lock servers.
func TestLockServers(t *testing.T) {
	if runtime.GOOS == "windows" {
		return
	}
	testCases := []struct {
		srvCmdConfig     serverCmdConfig
		totalLockServers int
	}{
		// Test - 1 one lock server initialized.
		{
			srvCmdConfig: serverCmdConfig{
				isDistXL: true,
				endPoints: []storageEndPoint{
					{"localhost", 9000, "/mnt/disk1"},
					{"1.1.1.2", 9000, "/mnt/disk2"},
					{"1.1.2.1", 9000, "/mnt/disk3"},
					{"1.1.2.2", 9000, "/mnt/disk4"},
				},
			},
			totalLockServers: 1,
		},
		// Test - 2 two servers possible, 1 ignored.
		{
			srvCmdConfig: serverCmdConfig{
				isDistXL: true,
				endPoints: []storageEndPoint{
					{"localhost", 9000, "/mnt/disk1"},
					{"localhost", 9000, "/mnt/disk2"},
					{"1.1.2.1", 9000, "/mnt/disk3"},
					{"1.1.2.2", 9000, "/mnt/disk4"},
				},
				ignoredEndPoints: []storageEndPoint{
					{"localhost", 9000, "/mnt/disk2"},
				},
			},
			totalLockServers: 1,
		},
	}

	// Validates lock server initialization.
	for i, testCase := range testCases {
		lockServers := newLockServers(testCase.srvCmdConfig)
		if len(lockServers) != testCase.totalLockServers {
			t.Fatalf("Test %d: Expected total %d, got %d", i+1, testCase.totalLockServers, len(lockServers))
		}
	}
}

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
	"testing"
	"time"
)

type lockStateCase struct {
	volume      string
	path        string
	lockOrigin  string
	opsID       string
	readLock    bool // lock type.
	setBlocked  bool // initialize the initial state to blocked.
	expectedErr error
	// expected global lock stats.
	expectedLockStatus string // Status of the lock Blocked/Running.

	expectedGlobalLockCount  int // Total number of locks held across the system, includes blocked + held locks.
	expectedBlockedLockCount int // Total blocked lock across the system.
	expectedRunningLockCount int // Total successfully held locks (non-blocking).
	// expected lock statu for given <volume, path> pair.
	expectedVolPathLockCount    int // Total locks held for given <volume,path> pair, includes blocked locks.
	expectedVolPathRunningCount int // Total succcesfully held locks for given <volume, path> pair.
	expectedVolPathBlockCount   int // Total locks blocked on the given <volume, path> pair.
}

// Used for validating the Lock info obtaining from contol RPC end point for obtaining lock related info.
func verifyRPCLockInfoResponse(l lockStateCase, rpcLockInfoResponse SystemLockState, t TestErrHandler, testNum int) {
	// Assert the total number of locks (locked + acquired) in the system.
	if rpcLockInfoResponse.TotalLocks != int64(l.expectedGlobalLockCount) {
		t.Fatalf("Test %d: Expected the global lock counter to be %v, but got %v", testNum, int64(l.expectedGlobalLockCount),
			rpcLockInfoResponse.TotalLocks)
	}

	// verify the count for total blocked locks.
	if rpcLockInfoResponse.TotalBlockedLocks != int64(l.expectedBlockedLockCount) {
		t.Fatalf("Test %d: Expected the total blocked lock counter to be %v, but got %v", testNum, int64(l.expectedBlockedLockCount),
			rpcLockInfoResponse.TotalBlockedLocks)
	}

	// verify the count for total running locks.
	if rpcLockInfoResponse.TotalAcquiredLocks != int64(l.expectedRunningLockCount) {
		t.Fatalf("Test %d: Expected the total running lock counter to be %v, but got %v", testNum, int64(l.expectedRunningLockCount),
			rpcLockInfoResponse.TotalAcquiredLocks)
	}

	for _, locksInfoPerObject := range rpcLockInfoResponse.LocksInfoPerObject {
		// See whether the entry for the <bucket, object> exists in the RPC response.
		if locksInfoPerObject.Bucket == l.volume && locksInfoPerObject.Object == l.path {
			// Assert the total number of locks (blocked + acquired) for the given <buckt, object> pair.
			if locksInfoPerObject.LocksOnObject != int64(l.expectedVolPathLockCount) {
				t.Errorf("Test %d: Expected the total lock count for bucket: \"%s\", object: \"%s\" to be %v, but got %v", testNum,
					l.volume, l.path, int64(l.expectedVolPathLockCount), locksInfoPerObject.LocksOnObject)
			}
			// Assert the total number of acquired locks for the given <buckt, object> pair.
			if locksInfoPerObject.LocksAcquiredOnObject != int64(l.expectedVolPathRunningCount) {
				t.Errorf("Test %d: Expected the acquired lock count for bucket: \"%s\", object: \"%s\" to be %v, but got %v", testNum,
					l.volume, l.path, int64(l.expectedVolPathRunningCount), locksInfoPerObject.LocksAcquiredOnObject)
			}
			// Assert the total number of blocked locks for the given <buckt, object> pair.
			if locksInfoPerObject.TotalBlockedLocks != int64(l.expectedVolPathBlockCount) {
				t.Errorf("Test %d: Expected the blocked lock count for bucket: \"%s\", object: \"%s\" to be %v, but got %v", testNum,
					l.volume, l.path, int64(l.expectedVolPathBlockCount), locksInfoPerObject.TotalBlockedLocks)
			}
			// Flag to mark whether there's an entry in the RPC lock info response for given opsID.
			var opsIDfound bool
			for _, opsLockState := range locksInfoPerObject.LockDetailsOnObject {
				// first check whether the entry for the given operation ID exists.
				if opsLockState.OperationID == l.opsID {
					opsIDfound = true
					// asserting the type  of lock (RLock/WLock) from the RPC lock info response.
					if l.readLock {
						if opsLockState.LockType != debugRLockStr {
							t.Errorf("Test case %d: Expected the lock type to be \"%s\"", testNum, debugRLockStr)
						}
					} else {
						if opsLockState.LockType != debugWLockStr {
							t.Errorf("Test case %d: Expected the lock type to be \"%s\"", testNum, debugWLockStr)
						}
					}

					if opsLockState.Status != l.expectedLockStatus {
						t.Errorf("Test case %d: Expected the  status of the operation to be \"%s\", got \"%s\"", testNum, l.expectedLockStatus, opsLockState.Status)
					}

					// if opsLockState.LockOrigin != l.lockOrigin {
					// 	t.Fatalf("Test case %d: Expected the origin of the lock to be \"%s\", got \"%s\"", testNum, opsLockState.LockOrigin, l.lockOrigin)
					// }
					// all check satisfied, return here.
					// Any mismatch in the earlier checks would have ended the tests due to `Fatalf`,
					// control reaching here implies that all checks are satisfied.
					return
				}
			}
			// opsID not found.
			// No entry for an operation with given operation ID exists.
			if !opsIDfound {
				t.Fatalf("Test case %d: Entry for OpsId: \"%s\" not found in <bucket>: \"%s\", <path>: \"%s\" doesn't exist in the RPC response", testNum, l.opsID, l.volume, l.path)
			}
		}
	}
	// No entry exists for given <bucket, object> pair in the RPC response.
	t.Errorf("Test case %d: Entry for <bucket>: \"%s\", <object>: \"%s\" doesn't exist in the RPC response", testNum, l.volume, l.path)
}

// Asserts the lock counter from the global nsMutex inmemory lock with the expected one.
func verifyGlobalLockStats(l lockStateCase, t *testing.T, testNum int) {
	nsMutex.lockMapMutex.Lock()

	// Verifying the lock stats.
	if nsMutex.globalLockCounter != int64(l.expectedGlobalLockCount) {
		t.Errorf("Test %d: Expected the global lock counter to be %v, but got %v", testNum, int64(l.expectedGlobalLockCount),
			nsMutex.globalLockCounter)
	}
	// verify the count for total blocked locks.
	if nsMutex.blockedCounter != int64(l.expectedBlockedLockCount) {
		t.Errorf("Test %d: Expected the total blocked lock counter to be %v, but got %v", testNum, int64(l.expectedBlockedLockCount),
			nsMutex.blockedCounter)
	}
	// verify the count for total running locks.
	if nsMutex.runningLockCounter != int64(l.expectedRunningLockCount) {
		t.Errorf("Test %d: Expected the total running lock counter to be %v, but got %v", testNum, int64(l.expectedRunningLockCount),
			nsMutex.runningLockCounter)
	}
	nsMutex.lockMapMutex.Unlock()
	// Verifying again with the JSON response of the lock info.
	// Verifying the lock stats.
	sysLockState, err := generateSystemLockResponse()
	if err != nil {
		t.Fatalf("Obtaining lock info failed with <ERROR> %s", err)

	}
	if sysLockState.TotalLocks != int64(l.expectedGlobalLockCount) {
		t.Errorf("Test %d: Expected the global lock counter to be %v, but got %v", testNum, int64(l.expectedGlobalLockCount),
			sysLockState.TotalLocks)
	}
	// verify the count for total blocked locks.
	if sysLockState.TotalBlockedLocks != int64(l.expectedBlockedLockCount) {
		t.Errorf("Test %d: Expected the total blocked lock counter to be %v, but got %v", testNum, int64(l.expectedBlockedLockCount),
			sysLockState.TotalBlockedLocks)
	}
	// verify the count for total running locks.
	if sysLockState.TotalAcquiredLocks != int64(l.expectedRunningLockCount) {
		t.Errorf("Test %d: Expected the total running lock counter to be %v, but got %v", testNum, int64(l.expectedRunningLockCount),
			sysLockState.TotalAcquiredLocks)
	}
}

// Verify the lock counter for entries of given <volume, path> pair.
func verifyLockStats(l lockStateCase, t *testing.T, testNum int) {
	nsMutex.lockMapMutex.Lock()
	defer nsMutex.lockMapMutex.Unlock()
	param := nsParam{l.volume, l.path}

	// Verify the total locks (blocked+running) for given <vol,path> pair.
	if nsMutex.debugLockMap[param].ref != int64(l.expectedVolPathLockCount) {
		t.Errorf("Test %d: Expected the total lock count for volume: \"%s\", path: \"%s\" to be %v, but got %v", testNum,
			param.volume, param.path, int64(l.expectedVolPathLockCount), nsMutex.debugLockMap[param].ref)
	}
	// Verify the total running locks for given <volume, path> pair.
	if nsMutex.debugLockMap[param].running != int64(l.expectedVolPathRunningCount) {
		t.Errorf("Test %d: Expected the total running locks for volume: \"%s\", path: \"%s\" to be %v, but got %v", testNum, param.volume, param.path,
			int64(l.expectedVolPathRunningCount), nsMutex.debugLockMap[param].running)
	}
	// Verify the total blocked locks for givne <volume, path> pair.
	if nsMutex.debugLockMap[param].blocked != int64(l.expectedVolPathBlockCount) {
		t.Errorf("Test %d:  Expected the total blocked locks for volume: \"%s\", path: \"%s\"  to be %v, but got %v", testNum, param.volume, param.path,
			int64(l.expectedVolPathBlockCount), nsMutex.debugLockMap[param].blocked)
	}
}

// verifyLockState - function which asserts the expected lock info in the system with the actual values in the nsMutex.
func verifyLockState(l lockStateCase, t *testing.T, testNum int) {
	param := nsParam{l.volume, l.path}

	verifyGlobalLockStats(l, t, testNum)
	nsMutex.lockMapMutex.Lock()
	// Verifying the lock statuS fields.
	if debugLockMap, ok := nsMutex.debugLockMap[param]; ok {
		if lockInfo, ok := debugLockMap.lockInfo[l.opsID]; ok {
			// Validating the lock type filed in the debug lock information.
			if l.readLock {
				if lockInfo.lockType != debugRLockStr {
					t.Errorf("Test case %d: Expected the lock type in the lock debug info to be \"%s\"", testNum, debugRLockStr)
				}
			} else {
				if lockInfo.lockType != debugWLockStr {
					t.Errorf("Test case %d: Expected the lock type in the lock debug info to be \"%s\"", testNum, debugWLockStr)
				}
			}

			// // validating the lock origin.
			// if l.lockOrigin != lockInfo.lockOrigin {
			// 	t.Fatalf("Test %d: Expected the lock origin info to be \"%s\", but got \"%s\"", testNum, l.lockOrigin, lockInfo.lockOrigin)
			// }
			// validating the status of the lock.
			if lockInfo.status != l.expectedLockStatus {
				t.Errorf("Test %d: Expected the status of the lock to be \"%s\", but got \"%s\"", testNum, l.expectedLockStatus, lockInfo.status)
			}
		} else {
			// Stop the tests if lock debug entry for given <volume, path> pair is not found.
			t.Errorf("Test case %d: Expected an debug lock entry for opsID \"%s\"", testNum, l.opsID)
		}
	} else {
		// To change the status the entry for given <volume, path> should exist in the lock info struct.
		t.Errorf("Test case %d: Debug lock entry for volume: %s, path: %s doesn't exist", testNum, param.volume, param.path)
	}
	// verifyLockStats holds its own lock.
	nsMutex.lockMapMutex.Unlock()

	// verify the lock count.
	verifyLockStats(l, t, testNum)
}

// TestNewDebugLockInfoPerVolumePath -  Validates the values initialized by newDebugLockInfoPerVolumePath().
func TestNewDebugLockInfoPerVolumePath(t *testing.T) {
	lockInfo := newDebugLockInfoPerVolumePath()

	if lockInfo.ref != 0 {
		t.Errorf("Expected initial reference value of total locks to be 0, got %d", lockInfo.ref)
	}
	if lockInfo.blocked != 0 {
		t.Errorf("Expected initial reference of blocked locks to be 0, got %d", lockInfo.blocked)
	}
	if lockInfo.running != 0 {
		t.Errorf("Expected initial reference value of held locks to be 0, got %d", lockInfo.running)
	}
}

// TestNsLockMapStatusBlockedToRunning - Validates the function for changing the lock state from blocked to running.
func TestNsLockMapStatusBlockedToRunning(t *testing.T) {

	testCases := []struct {
		volume      string
		path        string
		lockOrigin  string
		opsID       string
		readLock    bool // lock type.
		setBlocked  bool // initialize the initial state to blocked.
		expectedErr error
	}{
		// Test case - 1.
		{

			volume:     "my-bucket",
			path:       "my-object",
			lockOrigin: "/home/vadmeste/work/go/src/github.com/minio/minio/xl-v1-object.go:683 +0x2a",
			opsID:      "abcd1234",
			readLock:   true,
			setBlocked: true,
			// expected metrics.
			expectedErr: nil,
		},
		// Test case - 2.
		// No entry for <volume, path> pair.
		// So an attempt to change the state of the lock from `Blocked`->`Running` should fail.
		{

			volume:     "my-bucket",
			path:       "my-object-2",
			lockOrigin: "/home/vadmeste/work/go/src/github.com/minio/minio/xl-v1-object.go:683 +0x2a",
			opsID:      "abcd1234",
			readLock:   false,
			setBlocked: false,
			// expected metrics.
			expectedErr: LockInfoVolPathMssing{"my-bucket", "my-object-2"},
		},
		// Test case - 3.
		// Entry for the given operationID doesn't exist in the lock state info.
		{
			volume:     "my-bucket",
			path:       "my-object",
			lockOrigin: "/home/vadmeste/work/go/src/github.com/minio/minio/xl-v1-object.go:683 +0x2a",
			opsID:      "ops-Id-not-registered",
			readLock:   true,
			setBlocked: false,
			// expected metrics.
			expectedErr: LockInfoOpsIDNotFound{"my-bucket", "my-object", "ops-Id-not-registered"},
		},
		// Test case - 4.
		// Test case with non-existent lock origin.
		{
			volume:     "my-bucket",
			path:       "my-object",
			lockOrigin: "Bad Origin",
			opsID:      "abcd1234",
			readLock:   true,
			setBlocked: false,
			// expected metrics.
			expectedErr: LockInfoOriginNotFound{"my-bucket", "my-object", "abcd1234", "Bad Origin"},
		},
		// Test case - 5.
		// Test case with write lock.
		{

			volume:     "my-bucket",
			path:       "my-object",
			lockOrigin: "/home/vadmeste/work/go/src/github.com/minio/minio/xl-v1-object.go:683 +0x2a",
			opsID:      "abcd1234",
			readLock:   false,
			setBlocked: true,
			// expected metrics.
			expectedErr: nil,
		},
	}

	param := nsParam{testCases[0].volume, testCases[0].path}
	// Testing before the initialization done.
	// Since the data structures for
	actualErr := nsMutex.statusBlockedToRunning(param, testCases[0].lockOrigin,
		testCases[0].opsID, testCases[0].readLock)

	expectedNilErr := LockInfoNil{}
	if actualErr != expectedNilErr {
		t.Fatalf("Errors mismatch: Expected \"%s\", got \"%s\"", expectedNilErr, actualErr)
	}

	nsMutex = &nsLockMap{
		// entries of <volume,path> -> stateInfo of locks, for instrumentation purpose.
		debugLockMap: make(map[nsParam]*debugLockInfoPerVolumePath),
		lockMap:      make(map[nsParam]*nsLock),
	}
	// Entry for <volume, path> pair is set to nil.
	// Should fail with `LockInfoNil{}`.
	nsMutex.debugLockMap[param] = nil
	actualErr = nsMutex.statusBlockedToRunning(param, testCases[0].lockOrigin,
		testCases[0].opsID, testCases[0].readLock)

	expectedNilErr = LockInfoNil{}
	if actualErr != expectedNilErr {
		t.Fatalf("Errors mismatch: Expected \"%s\", got \"%s\"", expectedNilErr, actualErr)
	}

	// Setting the lock info the be `nil`.
	nsMutex.debugLockMap[param] = &debugLockInfoPerVolumePath{
		lockInfo: nil, // setting the lockinfo to nil.
		ref:      0,
		blocked:  0,
		running:  0,
	}

	actualErr = nsMutex.statusBlockedToRunning(param, testCases[0].lockOrigin,
		testCases[0].opsID, testCases[0].readLock)

	expectedOpsErr := LockInfoOpsIDNotFound{testCases[0].volume, testCases[0].path, testCases[0].opsID}
	if actualErr != expectedOpsErr {
		t.Fatalf("Errors mismatch: Expected \"%s\", got \"%s\"", expectedOpsErr, actualErr)
	}

	// Next case: ase whether an attempt to change the state of the lock to "Running" done,
	// but the initial state if already "Running". Such an attempt should fail
	nsMutex.debugLockMap[param] = &debugLockInfoPerVolumePath{
		lockInfo: make(map[string]debugLockInfo),
		ref:      0,
		blocked:  0,
		running:  0,
	}

	// Setting the status of the lock to be "Running".
	// The initial state of the lock should set to "Blocked", otherwise its not possible to change the state from "Blocked" -> "Running".
	nsMutex.debugLockMap[param].lockInfo[testCases[0].opsID] = debugLockInfo{
		lockOrigin: "/home/vadmeste/work/go/src/github.com/minio/minio/xl-v1-object.go:683 +0x2a",
		status:     "Running", // State set to "Running". Should fail with `LockInfoStateNotBlocked`.
		since:      time.Now().UTC(),
	}

	actualErr = nsMutex.statusBlockedToRunning(param, testCases[0].lockOrigin,
		testCases[0].opsID, testCases[0].readLock)

	expectedBlockErr := LockInfoStateNotBlocked{testCases[0].volume, testCases[0].path, testCases[0].opsID}
	if actualErr != expectedBlockErr {
		t.Fatalf("Errors mismatch: Expected: \"%s\", got: \"%s\"", expectedBlockErr, actualErr)
	}

	// enabling lock instrumentation.
	globalDebugLock = true
	// initializing the locks.
	initNSLock(false)
	// set debug lock info  to `nil` so that the next tests have to initialize them again.
	defer func() {
		globalDebugLock = false
		nsMutex.debugLockMap = nil
	}()
	// Iterate over the cases and assert the result.
	for i, testCase := range testCases {
		param := nsParam{testCase.volume, testCase.path}
		// status of the lock to be set to "Blocked", before setting Blocked->Running.
		if testCase.setBlocked {
			nsMutex.lockMapMutex.Lock()
			err := nsMutex.statusNoneToBlocked(param, testCase.lockOrigin, testCase.opsID, testCase.readLock)
			if err != nil {
				t.Fatalf("Test %d: Initializing the initial state to Blocked failed <ERROR> %s", i+1, err)
			}
			nsMutex.lockMapMutex.Unlock()
		}
		// invoking the method under test.
		actualErr = nsMutex.statusBlockedToRunning(param, testCase.lockOrigin, testCase.opsID, testCase.readLock)
		if actualErr != testCase.expectedErr {
			t.Fatalf("Test %d: Errors mismatch: Expected: \"%s\", got: \"%s\"", i+1, testCase.expectedErr, actualErr)
		}
		// In case of no error proceed with validating the lock state information.
		if actualErr == nil {
			// debug entry for given <volume, path> pair should exist.
			if debugLockMap, ok := nsMutex.debugLockMap[param]; ok {
				if lockInfo, ok := debugLockMap.lockInfo[testCase.opsID]; ok {
					// Validating the lock type filed in the debug lock information.
					if testCase.readLock {
						if lockInfo.lockType != debugRLockStr {
							t.Errorf("Test case %d: Expected the lock type in the lock debug info to be \"%s\"", i+1, debugRLockStr)
						}
					} else {
						if lockInfo.lockType != debugWLockStr {
							t.Errorf("Test case %d: Expected the lock type in the lock debug info to be \"%s\"", i+1, debugWLockStr)
						}
					}

					// validating the lock origin.
					if testCase.lockOrigin != lockInfo.lockOrigin {
						t.Errorf("Test %d: Expected the lock origin info to be \"%s\", but got \"%s\"", i+1, testCase.lockOrigin, lockInfo.lockOrigin)
					}
					// validating the status of the lock.
					if lockInfo.status != "Running" {
						t.Errorf("Test %d: Expected the status of the lock to be \"%s\", but got \"%s\"", i+1, "Running", lockInfo.status)
					}
				} else {
					// Stop the tests if lock debug entry for given <volume, path> pair is not found.
					t.Fatalf("Test case %d: Expected an debug lock entry for opsID \"%s\"", i+1, testCase.opsID)
				}
			} else {
				// To change the status the entry for given <volume, path> should exist in the lock info struct.
				t.Fatalf("Test case %d: Debug lock entry for  volume: %s, path: %s doesn't exist", i+1, param.volume, param.path)
			}
		}
	}

}

// TestNsLockMapStatusNoneToBlocked - Validates the function for changing the lock state to blocked
func TestNsLockMapStatusNoneToBlocked(t *testing.T) {

	testCases := []lockStateCase{
		// Test case - 1.
		{

			volume:     "my-bucket",
			path:       "my-object",
			lockOrigin: "/home/vadmeste/work/go/src/github.com/minio/minio/xl-v1-object.go:683 +0x2a",
			opsID:      "abcd1234",
			readLock:   true,
			// expected metrics.
			expectedErr:        nil,
			expectedLockStatus: "Blocked",

			expectedGlobalLockCount:  1,
			expectedRunningLockCount: 0,
			expectedBlockedLockCount: 1,

			expectedVolPathLockCount:    1,
			expectedVolPathRunningCount: 0,
			expectedVolPathBlockCount:   1,
		},
		// Test case - 2.
		// No entry for <volume, path> pair.
		// So an attempt to change the state of the lock from `Blocked`->`Running` should fail.
		{

			volume:     "my-bucket",
			path:       "my-object-2",
			lockOrigin: "/home/vadmeste/work/go/src/github.com/minio/minio/xl-v1-object.go:683 +0x2a",
			opsID:      "abcd1234",
			readLock:   false,
			// expected metrics.
			expectedErr:        nil,
			expectedLockStatus: "Blocked",

			expectedGlobalLockCount:  2,
			expectedRunningLockCount: 0,
			expectedBlockedLockCount: 2,

			expectedVolPathLockCount:    1,
			expectedVolPathRunningCount: 0,
			expectedVolPathBlockCount:   1,
		},
		// Test case - 3.
		// Entry for the given operationID doesn't exist in the lock state info.
		// The entry should be created and relevant counters should be set.
		{
			volume:     "my-bucket",
			path:       "my-object",
			lockOrigin: "/home/vadmeste/work/go/src/github.com/minio/minio/xl-v1-object.go:683 +0x2a",
			opsID:      "ops-Id-not-registered",
			readLock:   true,
			// expected metrics.
			expectedErr:        nil,
			expectedLockStatus: "Blocked",

			expectedGlobalLockCount:  3,
			expectedRunningLockCount: 0,
			expectedBlockedLockCount: 3,

			expectedVolPathLockCount:    2,
			expectedVolPathRunningCount: 0,
			expectedVolPathBlockCount:   2,
		},
	}

	param := nsParam{testCases[0].volume, testCases[0].path}
	// Testing before the initialization done.
	// Since the data structures for
	actualErr := nsMutex.statusBlockedToRunning(param, testCases[0].lockOrigin,
		testCases[0].opsID, testCases[0].readLock)

	expectedNilErr := LockInfoNil{}
	if actualErr != expectedNilErr {
		t.Fatalf("Errors mismatch: Expected \"%s\", got \"%s\"", expectedNilErr, actualErr)
	}
	// enabling lock instrumentation.
	globalDebugLock = true
	// initializing the locks.
	initNSLock(false)
	// set debug lock info  to `nil` so that the next tests have to initialize them again.
	defer func() {
		globalDebugLock = false
		nsMutex.debugLockMap = nil
	}()
	// Iterate over the cases and assert the result.
	for i, testCase := range testCases {
		nsMutex.lockMapMutex.Lock()
		param := nsParam{testCase.volume, testCase.path}
		actualErr := nsMutex.statusNoneToBlocked(param, testCase.lockOrigin, testCase.opsID, testCase.readLock)
		if actualErr != testCase.expectedErr {
			t.Fatalf("Test %d: Errors mismatch: Expected: \"%s\", got: \"%s\"", i+1, testCase.expectedErr, actualErr)
		}
		nsMutex.lockMapMutex.Unlock()
		if actualErr == nil {
			verifyLockState(testCase, t, i+1)
		}
	}
}

// TestNsLockMapDeleteLockInfoEntryForOps - Validates the removal of entry for given Operational ID from the lock info.
func TestNsLockMapDeleteLockInfoEntryForOps(t *testing.T) {
	testCases := []lockStateCase{
		// Test case - 1.
		{
			volume:     "my-bucket",
			path:       "my-object",
			lockOrigin: "/home/vadmeste/work/go/src/github.com/minio/minio/xl-v1-object.go:683 +0x2a",
			opsID:      "abcd1234",
			readLock:   true,
			// expected metrics.
		},
	}
	// case  - 1.
	// Testing the case where delete lock info is attempted even before the lock is initialized.
	param := nsParam{testCases[0].volume, testCases[0].path}
	// Testing before the initialization done.

	actualErr := nsMutex.deleteLockInfoEntryForOps(param, testCases[0].opsID)

	expectedNilErr := LockInfoNil{}
	if actualErr != expectedNilErr {
		t.Fatalf("Errors mismatch: Expected \"%s\", got \"%s\"", expectedNilErr, actualErr)
	}

	// enabling lock instrumentation.
	globalDebugLock = true
	// initializing the locks.
	initNSLock(false)
	// set debug lock info  to `nil` so that the next tests have to initialize them again.
	defer func() {
		globalDebugLock = false
		nsMutex.debugLockMap = nil
	}()
	// case - 2.
	// Case where an attempt to delete the entry for non-existent <volume, path> pair is done.
	// Set the status of the lock to blocked and then to running.
	nonExistParam := nsParam{volume: "non-exist-volume", path: "non-exist-path"}
	actualErr = nsMutex.deleteLockInfoEntryForOps(nonExistParam, testCases[0].opsID)

	expectedVolPathErr := LockInfoVolPathMssing{nonExistParam.volume, nonExistParam.path}
	if actualErr != expectedVolPathErr {
		t.Fatalf("Errors mismatch: Expected \"%s\", got \"%s\"", expectedVolPathErr, actualErr)
	}

	// Case - 3.
	// Lock state is set to Running and then an attempt to delete the info for non-existent opsID done.
	nsMutex.lockMapMutex.Lock()
	err := nsMutex.statusNoneToBlocked(param, testCases[0].lockOrigin, testCases[0].opsID, testCases[0].readLock)
	if err != nil {
		t.Fatalf("Setting lock status to Blocked failed: <ERROR> %s", err)
	}
	nsMutex.lockMapMutex.Unlock()
	err = nsMutex.statusBlockedToRunning(param, testCases[0].lockOrigin, testCases[0].opsID, testCases[0].readLock)
	if err != nil {
		t.Fatalf("Setting lock status to Running failed: <ERROR> %s", err)
	}
	actualErr = nsMutex.deleteLockInfoEntryForOps(param, "non-existent-OpsID")

	expectedOpsIDErr := LockInfoOpsIDNotFound{param.volume, param.path, "non-existent-OpsID"}
	if actualErr != expectedOpsIDErr {
		t.Fatalf("Errors mismatch: Expected \"%s\", got \"%s\"", expectedOpsIDErr, actualErr)
	}
	// case - 4.
	// Attempt to delete an registered entry is done.
	// All metrics should be 0 after deleting the entry.

	// Verify that the entry the opsID exists.
	if debugLockMap, ok := nsMutex.debugLockMap[param]; ok {
		if _, ok := debugLockMap.lockInfo[testCases[0].opsID]; !ok {
			t.Fatalf("Entry for OpsID \"%s\" in <volume> %s, <path> %s should have existed. ", testCases[0].opsID, param.volume, param.path)
		}
	} else {
		t.Fatalf("Entry for <volume> %s, <path> %s should have existed. ", param.volume, param.path)
	}

	actualErr = nsMutex.deleteLockInfoEntryForOps(param, testCases[0].opsID)
	if actualErr != nil {
		t.Fatalf("Expected the error to be <nil>, but got <ERROR> %s", actualErr)
	}

	// Verify that the entry for the opsId doesn't exists.
	if debugLockMap, ok := nsMutex.debugLockMap[param]; ok {
		if _, ok := debugLockMap.lockInfo[testCases[0].opsID]; ok {
			t.Fatalf("The entry for opsID \"%s\" should have been deleted", testCases[0].opsID)
		}
	} else {
		t.Fatalf("Entry for <volume> %s, <path> %s should have existed. ", param.volume, param.path)
	}
	if nsMutex.runningLockCounter != int64(0) {
		t.Errorf("Expected the count of total running locks to be %v, but got %v", int64(0), nsMutex.runningLockCounter)
	}
	if nsMutex.blockedCounter != int64(0) {
		t.Errorf("Expected the count of total blocked locks to be %v, but got %v", int64(0), nsMutex.blockedCounter)
	}
	if nsMutex.globalLockCounter != int64(0) {
		t.Errorf("Expected the count of all locks to be %v, but got %v", int64(0), nsMutex.globalLockCounter)
	}
}

// TestNsLockMapDeleteLockInfoEntryForVolumePath - Tests validate the logic for removal
// of entry for given <volume, path> pair from lock info.
func TestNsLockMapDeleteLockInfoEntryForVolumePath(t *testing.T) {
	testCases := []lockStateCase{
		// Test case - 1.
		{
			volume:     "my-bucket",
			path:       "my-object",
			lockOrigin: "/home/vadmeste/work/go/src/github.com/minio/minio/xl-v1-object.go:683 +0x2a",
			opsID:      "abcd1234",
			readLock:   true,
			// expected metrics.
		},
	}
	// case  - 1.
	// Testing the case where delete lock info is attempted even before the lock is initialized.
	param := nsParam{testCases[0].volume, testCases[0].path}
	// Testing before the initialization done.

	actualErr := nsMutex.deleteLockInfoEntryForVolumePath(param)

	expectedNilErr := LockInfoNil{}
	if actualErr != expectedNilErr {
		t.Fatalf("Errors mismatch: Expected \"%s\", got \"%s\"", expectedNilErr, actualErr)
	}

	// enabling lock instrumentation.
	globalDebugLock = true
	// initializing the locks.
	initNSLock(false)
	// set debug lock info  to `nil` so that the next tests have to initialize them again.
	defer func() {
		globalDebugLock = false
		nsMutex.debugLockMap = nil
	}()
	// case - 2.
	// Case where an attempt to delete the entry for non-existent <volume, path> pair is done.
	// Set the status of the lock to blocked and then to running.
	nonExistParam := nsParam{volume: "non-exist-volume", path: "non-exist-path"}
	actualErr = nsMutex.deleteLockInfoEntryForVolumePath(nonExistParam)

	expectedVolPathErr := LockInfoVolPathMssing{nonExistParam.volume, nonExistParam.path}
	if actualErr != expectedVolPathErr {
		t.Fatalf("Errors mismatch: Expected \"%s\", got \"%s\"", expectedVolPathErr, actualErr)
	}

	// case - 3.
	// Attempt to delete an registered entry is done.
	// All metrics should be 0 after deleting the entry.

	// Registering the entry first.
	nsMutex.lockMapMutex.Lock()
	err := nsMutex.statusNoneToBlocked(param, testCases[0].lockOrigin, testCases[0].opsID, testCases[0].readLock)
	if err != nil {
		t.Fatalf("Setting lock status to Blocked failed: <ERROR> %s", err)
	}
	nsMutex.lockMapMutex.Unlock()
	err = nsMutex.statusBlockedToRunning(param, testCases[0].lockOrigin, testCases[0].opsID, testCases[0].readLock)
	if err != nil {
		t.Fatalf("Setting lock status to Running failed: <ERROR> %s", err)
	}
	// Verify that the entry the for given <volume, path> exists.
	if _, ok := nsMutex.debugLockMap[param]; !ok {
		t.Fatalf("Entry for <volume> %s, <path> %s should have existed.", param.volume, param.path)
	}
	// first delete the entry for the operation ID.
	_ = nsMutex.deleteLockInfoEntryForOps(param, testCases[0].opsID)
	actualErr = nsMutex.deleteLockInfoEntryForVolumePath(param)
	if actualErr != nil {
		t.Fatalf("Expected the error to be <nil>, but got <ERROR> %s", actualErr)
	}

	// Verify that the entry for the opsId doesn't exists.
	if _, ok := nsMutex.debugLockMap[param]; ok {
		t.Fatalf("Entry for <volume> %s, <path> %s should have been deleted. ", param.volume, param.path)
	}
	// The lock count values should be 0.
	if nsMutex.runningLockCounter != int64(0) {
		t.Errorf("Expected the count of total running locks to be %v, but got %v", int64(0), nsMutex.runningLockCounter)
	}
	if nsMutex.blockedCounter != int64(0) {
		t.Errorf("Expected the count of total blocked locks to be %v, but got %v", int64(0), nsMutex.blockedCounter)
	}
	if nsMutex.globalLockCounter != int64(0) {
		t.Errorf("Expected the count of all locks to be %v, but got %v", int64(0), nsMutex.globalLockCounter)
	}
}

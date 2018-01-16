/*
 * Minio Cloud Storage, (C) 2016, 2017 Minio, Inc.
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

	"github.com/minio/minio/pkg/errors"
)

type lockStateCase struct {
	volume      string
	path        string
	lockSource  string
	opsID       string
	readLock    bool // lock type.
	setBlocked  bool // initialize the initial state to blocked.
	expectedErr error
	// Expected global lock stats.
	expectedLockStatus statusType // Status of the lock Blocked/Running.

	expectedGlobalLockCount  int // Total number of locks held across the system, includes blocked + held locks.
	expectedBlockedLockCount int // Total blocked lock across the system.
	expectedRunningLockCount int // Total successfully held locks (non-blocking).
	// Expected lock status for given <volume, path> pair.
	expectedVolPathLockCount    int // Total locks held for given <volume,path> pair, includes blocked locks.
	expectedVolPathRunningCount int // Total succcesfully held locks for given <volume, path> pair.
	expectedVolPathBlockCount   int // Total locks blocked on the given <volume, path> pair.
}

// Read entire state of the locks in the system and return.
func getSystemLockState() (SystemLockState, error) {
	globalNSMutex.lockMapMutex.Lock()
	defer globalNSMutex.lockMapMutex.Unlock()

	lockState := SystemLockState{}

	lockState.TotalBlockedLocks = globalNSMutex.counters.blocked
	lockState.TotalLocks = globalNSMutex.counters.total
	lockState.TotalAcquiredLocks = globalNSMutex.counters.granted

	for param, debugLock := range globalNSMutex.debugLockMap {
		volLockInfo := VolumeLockInfo{}
		volLockInfo.Bucket = param.volume
		volLockInfo.Object = param.path
		volLockInfo.LocksOnObject = debugLock.counters.total
		volLockInfo.TotalBlockedLocks = debugLock.counters.blocked
		volLockInfo.LocksAcquiredOnObject = debugLock.counters.granted
		for opsID, lockInfo := range debugLock.lockInfo {
			volLockInfo.LockDetailsOnObject = append(volLockInfo.LockDetailsOnObject, OpsLockState{
				OperationID: opsID,
				LockSource:  lockInfo.lockSource,
				LockType:    lockInfo.lType,
				Status:      lockInfo.status,
				Since:       lockInfo.since,
			})
		}
		lockState.LocksInfoPerObject = append(lockState.LocksInfoPerObject, volLockInfo)
	}
	return lockState, nil
}

// Asserts the lock counter from the global globalNSMutex inmemory lock with the expected one.
func verifyGlobalLockStats(l lockStateCase, t *testing.T, testNum int) {
	globalNSMutex.lockMapMutex.Lock()

	// Verifying the lock stats.
	if globalNSMutex.counters.total != int64(l.expectedGlobalLockCount) {
		t.Errorf("Test %d: Expected the global lock counter to be %v, but got %v", testNum, int64(l.expectedGlobalLockCount),
			globalNSMutex.counters.total)
	}
	// verify the count for total blocked locks.
	if globalNSMutex.counters.blocked != int64(l.expectedBlockedLockCount) {
		t.Errorf("Test %d: Expected the total blocked lock counter to be %v, but got %v", testNum, int64(l.expectedBlockedLockCount),
			globalNSMutex.counters.blocked)
	}
	// verify the count for total running locks.
	if globalNSMutex.counters.granted != int64(l.expectedRunningLockCount) {
		t.Errorf("Test %d: Expected the total running lock counter to be %v, but got %v", testNum, int64(l.expectedRunningLockCount),
			globalNSMutex.counters.granted)
	}
	globalNSMutex.lockMapMutex.Unlock()
	// Verifying again with the JSON response of the lock info.
	// Verifying the lock stats.
	sysLockState, err := getSystemLockState()
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
	globalNSMutex.lockMapMutex.Lock()
	defer globalNSMutex.lockMapMutex.Unlock()
	param := nsParam{l.volume, l.path}

	// Verify the total locks (blocked+running) for given <vol,path> pair.
	if globalNSMutex.debugLockMap[param].counters.total != int64(l.expectedVolPathLockCount) {
		t.Errorf("Test %d: Expected the total lock count for volume: \"%s\", path: \"%s\" to be %v, but got %v", testNum,
			param.volume, param.path, int64(l.expectedVolPathLockCount), globalNSMutex.debugLockMap[param].counters.total)
	}
	// Verify the total running locks for given <volume, path> pair.
	if globalNSMutex.debugLockMap[param].counters.granted != int64(l.expectedVolPathRunningCount) {
		t.Errorf("Test %d: Expected the total running locks for volume: \"%s\", path: \"%s\" to be %v, but got %v", testNum, param.volume, param.path,
			int64(l.expectedVolPathRunningCount), globalNSMutex.debugLockMap[param].counters.granted)
	}
	// Verify the total blocked locks for givne <volume, path> pair.
	if globalNSMutex.debugLockMap[param].counters.blocked != int64(l.expectedVolPathBlockCount) {
		t.Errorf("Test %d:  Expected the total blocked locks for volume: \"%s\", path: \"%s\"  to be %v, but got %v", testNum, param.volume, param.path,
			int64(l.expectedVolPathBlockCount), globalNSMutex.debugLockMap[param].counters.blocked)
	}
}

// verifyLockState - function which asserts the expected lock info in the system with the actual values in the globalNSMutex.
func verifyLockState(l lockStateCase, t *testing.T, testNum int) {
	param := nsParam{l.volume, l.path}

	verifyGlobalLockStats(l, t, testNum)
	globalNSMutex.lockMapMutex.Lock()
	// Verifying the lock statuS fields.
	if debugLockMap, ok := globalNSMutex.debugLockMap[param]; ok {
		if lockInfo, ok := debugLockMap.lockInfo[l.opsID]; ok {
			// Validating the lock type filed in the debug lock information.
			if l.readLock {
				if lockInfo.lType != debugRLockStr {
					t.Errorf("Test case %d: Expected the lock type in the lock debug info to be \"%s\"", testNum, debugRLockStr)
				}
			} else {
				if lockInfo.lType != debugWLockStr {
					t.Errorf("Test case %d: Expected the lock type in the lock debug info to be \"%s\"", testNum, debugWLockStr)
				}
			}

			// // validating the lock origin.
			// if l.lockSource != lockInfo.lockSource {
			// 	t.Fatalf("Test %d: Expected the lock origin info to be \"%s\", but got \"%s\"", testNum, l.lockSource, lockInfo.lockSource)
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
	globalNSMutex.lockMapMutex.Unlock()

	// verify the lock count.
	verifyLockStats(l, t, testNum)
}

// TestNewDebugLockInfoPerVolumePath -  Validates the values initialized by newDebugLockInfoPerVolumePath().
func TestNewDebugLockInfoPerVolumePath(t *testing.T) {
	lockInfo := &debugLockInfoPerVolumePath{
		lockInfo: make(map[string]debugLockInfo),
		counters: &lockStat{},
	}

	if lockInfo.counters.total != 0 {
		t.Errorf("Expected initial reference value of total locks to be 0, got %d", lockInfo.counters.total)
	}
	if lockInfo.counters.blocked != 0 {
		t.Errorf("Expected initial reference of blocked locks to be 0, got %d", lockInfo.counters.blocked)
	}
	if lockInfo.counters.granted != 0 {
		t.Errorf("Expected initial reference value of held locks to be 0, got %d", lockInfo.counters.granted)
	}
}

// TestNsLockMapStatusBlockedToRunning - Validates the function for changing the lock state from blocked to running.
func TestNsLockMapStatusBlockedToRunning(t *testing.T) {

	testCases := []struct {
		volume      string
		path        string
		lockSource  string
		opsID       string
		readLock    bool // Read lock type.
		setBlocked  bool // Initialize the initial state to blocked.
		expectedErr error
	}{
		// Test case - 1.
		{
			volume:     "my-bucket",
			path:       "my-object",
			lockSource: "/home/vadmeste/work/go/src/github.com/minio/minio/xl-v1-object.go:683 +0x2a",
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
			lockSource: "/home/vadmeste/work/go/src/github.com/minio/minio/xl-v1-object.go:683 +0x2a",
			opsID:      "abcd1234",
			readLock:   false,
			setBlocked: false,
			// expected metrics.
			expectedErr: LockInfoVolPathMissing{"my-bucket", "my-object-2"},
		},
		// Test case - 3.
		// Entry for the given operationID doesn't exist in the lock state info.
		{
			volume:     "my-bucket",
			path:       "my-object",
			lockSource: "/home/vadmeste/work/go/src/github.com/minio/minio/xl-v1-object.go:683 +0x2a",
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
			lockSource: "Bad Origin",
			opsID:      "abcd1234",
			readLock:   true,
			setBlocked: false,
			// expected metrics.
			expectedErr: LockInfoOriginMismatch{"my-bucket", "my-object", "abcd1234", "Bad Origin"},
		},
		// Test case - 5.
		// Test case with write lock.
		{
			volume:     "my-bucket",
			path:       "my-object",
			lockSource: "/home/vadmeste/work/go/src/github.com/minio/minio/xl-v1-object.go:683 +0x2a",
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
	actualErr := globalNSMutex.statusBlockedToRunning(param, testCases[0].lockSource,
		testCases[0].opsID, testCases[0].readLock)

	expectedErr := LockInfoVolPathMissing{testCases[0].volume, testCases[0].path}
	if errors.Cause(actualErr) != expectedErr {
		t.Fatalf("Errors mismatch: Expected \"%s\", got \"%s\"", expectedErr, actualErr)
	}

	globalNSMutex = &nsLockMap{
		// entries of <volume,path> -> stateInfo of locks, for instrumentation purpose.
		debugLockMap: make(map[nsParam]*debugLockInfoPerVolumePath),
		lockMap:      make(map[nsParam]*nsLock),
	}

	// Setting the lock info the be `nil`.
	globalNSMutex.debugLockMap[param] = &debugLockInfoPerVolumePath{
		lockInfo: nil, // setting the lockinfo to nil.
		counters: &lockStat{},
	}

	actualErr = globalNSMutex.statusBlockedToRunning(param, testCases[0].lockSource,
		testCases[0].opsID, testCases[0].readLock)

	expectedOpsErr := LockInfoOpsIDNotFound{testCases[0].volume, testCases[0].path, testCases[0].opsID}
	if errors.Cause(actualErr) != expectedOpsErr {
		t.Fatalf("Errors mismatch: Expected \"%s\", got \"%s\"", expectedOpsErr, actualErr)
	}

	// Next case: ase whether an attempt to change the state of the lock to "Running" done,
	// but the initial state if already "Running". Such an attempt should fail
	globalNSMutex.debugLockMap[param] = &debugLockInfoPerVolumePath{
		lockInfo: make(map[string]debugLockInfo),
		counters: &lockStat{},
	}

	// Setting the status of the lock to be "Running".
	// The initial state of the lock should set to "Blocked", otherwise its not possible to change the state from "Blocked" -> "Running".
	globalNSMutex.debugLockMap[param].lockInfo[testCases[0].opsID] = debugLockInfo{
		lockSource: "/home/vadmeste/work/go/src/github.com/minio/minio/xl-v1-object.go:683 +0x2a",
		status:     "Running", // State set to "Running". Should fail with `LockInfoStateNotBlocked`.
		since:      UTCNow(),
	}

	actualErr = globalNSMutex.statusBlockedToRunning(param, testCases[0].lockSource,
		testCases[0].opsID, testCases[0].readLock)

	expectedBlockErr := LockInfoStateNotBlocked{testCases[0].volume, testCases[0].path, testCases[0].opsID}
	if errors.Cause(actualErr) != expectedBlockErr {
		t.Fatalf("Errors mismatch: Expected: \"%s\", got: \"%s\"", expectedBlockErr, actualErr)
	}

	// initializing the locks.
	initNSLock(false)

	// Iterate over the cases and assert the result.
	for i, testCase := range testCases {
		param := nsParam{testCase.volume, testCase.path}
		// status of the lock to be set to "Blocked", before setting Blocked->Running.
		if testCase.setBlocked {
			globalNSMutex.lockMapMutex.Lock()
			err := globalNSMutex.statusNoneToBlocked(param, testCase.lockSource, testCase.opsID, testCase.readLock)
			if err != nil {
				t.Fatalf("Test %d: Initializing the initial state to Blocked failed <ERROR> %s", i+1, err)
			}
			globalNSMutex.lockMapMutex.Unlock()
		}
		// invoking the method under test.
		actualErr = globalNSMutex.statusBlockedToRunning(param, testCase.lockSource, testCase.opsID, testCase.readLock)
		if errors.Cause(actualErr) != testCase.expectedErr {
			t.Fatalf("Test %d: Errors mismatch: Expected: \"%s\", got: \"%s\"", i+1, testCase.expectedErr, actualErr)
		}
		// In case of no error proceed with validating the lock state information.
		if actualErr == nil {
			// debug entry for given <volume, path> pair should exist.
			if debugLockMap, ok := globalNSMutex.debugLockMap[param]; ok {
				if lockInfo, ok := debugLockMap.lockInfo[testCase.opsID]; ok {
					// Validating the lock type filed in the debug lock information.
					if testCase.readLock {
						if lockInfo.lType != debugRLockStr {
							t.Errorf("Test case %d: Expected the lock type in the lock debug info to be \"%s\"", i+1, debugRLockStr)
						}
					} else {
						if lockInfo.lType != debugWLockStr {
							t.Errorf("Test case %d: Expected the lock type in the lock debug info to be \"%s\"", i+1, debugWLockStr)
						}
					}

					// validating the lock origin.
					if testCase.lockSource != lockInfo.lockSource {
						t.Errorf("Test %d: Expected the lock origin info to be \"%s\", but got \"%s\"", i+1, testCase.lockSource, lockInfo.lockSource)
					}
					// validating the status of the lock.
					if lockInfo.status != runningStatus {
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
			lockSource: "/home/vadmeste/work/go/src/github.com/minio/minio/xl-v1-object.go:683 +0x2a",
			opsID:      "abcd1234",
			readLock:   true,
			// expected metrics.
			expectedErr:        nil,
			expectedLockStatus: blockedStatus,

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
			lockSource: "/home/vadmeste/work/go/src/github.com/minio/minio/xl-v1-object.go:683 +0x2a",
			opsID:      "abcd1234",
			readLock:   false,
			// expected metrics.
			expectedErr:        nil,
			expectedLockStatus: blockedStatus,

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
			lockSource: "/home/vadmeste/work/go/src/github.com/minio/minio/xl-v1-object.go:683 +0x2a",
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

	// initializing the locks.
	initNSLock(false)

	param := nsParam{testCases[0].volume, testCases[0].path}
	// Testing before the initialization done.
	// Since the data structures for
	actualErr := globalNSMutex.statusBlockedToRunning(param, testCases[0].lockSource,
		testCases[0].opsID, testCases[0].readLock)

	expectedErr := LockInfoVolPathMissing{testCases[0].volume, testCases[0].path}
	if errors.Cause(actualErr) != expectedErr {
		t.Fatalf("Errors mismatch: Expected \"%s\", got \"%s\"", expectedErr, actualErr)
	}

	// Iterate over the cases and assert the result.
	for i, testCase := range testCases {
		globalNSMutex.lockMapMutex.Lock()
		param := nsParam{testCase.volume, testCase.path}
		actualErr := globalNSMutex.statusNoneToBlocked(param, testCase.lockSource, testCase.opsID, testCase.readLock)
		if actualErr != testCase.expectedErr {
			t.Fatalf("Test %d: Errors mismatch: Expected: \"%s\", got: \"%s\"", i+1, testCase.expectedErr, actualErr)
		}
		globalNSMutex.lockMapMutex.Unlock()
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
			lockSource: "/home/vadmeste/work/go/src/github.com/minio/minio/xl-v1-object.go:683 +0x2a",
			opsID:      "abcd1234",
			readLock:   true,
			// expected metrics.
		},
	}

	// initializing the locks.
	initNSLock(false)

	// case  - 1.
	// Testing the case where delete lock info is attempted even before the lock is initialized.
	param := nsParam{testCases[0].volume, testCases[0].path}
	// Testing before the initialization done.

	actualErr := globalNSMutex.deleteLockInfoEntryForOps(param, testCases[0].opsID)

	expectedErr := LockInfoVolPathMissing{testCases[0].volume, testCases[0].path}
	if errors.Cause(actualErr) != expectedErr {
		t.Fatalf("Errors mismatch: Expected \"%s\", got \"%s\"", expectedErr, actualErr)
	}

	// Case - 2.
	// Lock state is set to Running and then an attempt to delete the info for non-existent opsID done.
	globalNSMutex.lockMapMutex.Lock()
	err := globalNSMutex.statusNoneToBlocked(param, testCases[0].lockSource, testCases[0].opsID, testCases[0].readLock)
	if err != nil {
		t.Fatalf("Setting lock status to Blocked failed: <ERROR> %s", err)
	}
	globalNSMutex.lockMapMutex.Unlock()
	err = globalNSMutex.statusBlockedToRunning(param, testCases[0].lockSource, testCases[0].opsID, testCases[0].readLock)
	if err != nil {
		t.Fatalf("Setting lock status to Running failed: <ERROR> %s", err)
	}
	actualErr = globalNSMutex.deleteLockInfoEntryForOps(param, "non-existent-OpsID")

	expectedOpsIDErr := LockInfoOpsIDNotFound{param.volume, param.path, "non-existent-OpsID"}
	if errors.Cause(actualErr) != expectedOpsIDErr {
		t.Fatalf("Errors mismatch: Expected \"%s\", got \"%s\"", expectedOpsIDErr, actualErr)
	}
	// case - 4.
	// Attempt to delete an registered entry is done.
	// All metrics should be 0 after deleting the entry.

	// Verify that the entry the opsID exists.
	if debugLockMap, ok := globalNSMutex.debugLockMap[param]; ok {
		if _, ok := debugLockMap.lockInfo[testCases[0].opsID]; !ok {
			t.Fatalf("Entry for OpsID \"%s\" in <volume> %s, <path> %s should have existed. ", testCases[0].opsID, param.volume, param.path)
		}
	} else {
		t.Fatalf("Entry for <volume> %s, <path> %s should have existed. ", param.volume, param.path)
	}

	actualErr = globalNSMutex.deleteLockInfoEntryForOps(param, testCases[0].opsID)
	if actualErr != nil {
		t.Fatalf("Expected the error to be <nil>, but got <ERROR> %s", actualErr)
	}

	// Verify that the entry for the opsId doesn't exists.
	if debugLockMap, ok := globalNSMutex.debugLockMap[param]; ok {
		if _, ok := debugLockMap.lockInfo[testCases[0].opsID]; ok {
			t.Fatalf("The entry for opsID \"%s\" should have been deleted", testCases[0].opsID)
		}
	} else {
		t.Fatalf("Entry for <volume> %s, <path> %s should have existed. ", param.volume, param.path)
	}
	if globalNSMutex.counters.granted != 0 {
		t.Errorf("Expected the count of total running locks to be %v, but got %v", 0, globalNSMutex.counters.granted)
	}
	if globalNSMutex.counters.blocked != 0 {
		t.Errorf("Expected the count of total blocked locks to be %v, but got %v", 0, globalNSMutex.counters.blocked)
	}
	if globalNSMutex.counters.total != 0 {
		t.Errorf("Expected the count of all locks to be %v, but got %v", 0, globalNSMutex.counters.total)
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
			lockSource: "/home/vadmeste/work/go/src/github.com/minio/minio/xl-v1-object.go:683 +0x2a",
			opsID:      "abcd1234",
			readLock:   true,
			// expected metrics.
		},
	}

	// initializing the locks.
	initNSLock(false)

	// case  - 1.
	// Case where an attempt to delete the entry for non-existent <volume, path> pair is done.
	// Set the status of the lock to blocked and then to running.
	param := nsParam{testCases[0].volume, testCases[0].path}
	actualErr := globalNSMutex.deleteLockInfoEntryForVolumePath(param)
	expectedNilErr := LockInfoVolPathMissing{param.volume, param.path}
	if errors.Cause(actualErr) != expectedNilErr {
		t.Fatalf("Errors mismatch: Expected \"%s\", got \"%s\"", expectedNilErr, actualErr)
	}

	// case - 2.
	// Attempt to delete an registered entry is done.
	// All metrics should be 0 after deleting the entry.

	// Registering the entry first.
	globalNSMutex.lockMapMutex.Lock()
	err := globalNSMutex.statusNoneToBlocked(param, testCases[0].lockSource, testCases[0].opsID, testCases[0].readLock)
	if err != nil {
		t.Fatalf("Setting lock status to Blocked failed: <ERROR> %s", err)
	}
	globalNSMutex.lockMapMutex.Unlock()
	err = globalNSMutex.statusBlockedToRunning(param, testCases[0].lockSource, testCases[0].opsID, testCases[0].readLock)
	if err != nil {
		t.Fatalf("Setting lock status to Running failed: <ERROR> %s", err)
	}
	// Verify that the entry the for given <volume, path> exists.
	if _, ok := globalNSMutex.debugLockMap[param]; !ok {
		t.Fatalf("Entry for <volume> %s, <path> %s should have existed.", param.volume, param.path)
	}
	// first delete the entry for the operation ID.
	_ = globalNSMutex.deleteLockInfoEntryForOps(param, testCases[0].opsID)
	actualErr = globalNSMutex.deleteLockInfoEntryForVolumePath(param)
	if actualErr != nil {
		t.Fatalf("Expected the error to be <nil>, but got <ERROR> %s", actualErr)
	}

	// Verify that the entry for the opsId doesn't exists.
	if _, ok := globalNSMutex.debugLockMap[param]; ok {
		t.Fatalf("Entry for <volume> %s, <path> %s should have been deleted. ", param.volume, param.path)
	}
	// The lock count values should be 0.
	if globalNSMutex.counters.granted != 0 {
		t.Errorf("Expected the count of total running locks to be %v, but got %v", 0, globalNSMutex.counters.granted)
	}
	if globalNSMutex.counters.blocked != 0 {
		t.Errorf("Expected the count of total blocked locks to be %v, but got %v", 0, globalNSMutex.counters.blocked)
	}
	if globalNSMutex.counters.total != 0 {
		t.Errorf("Expected the count of all locks to be %v, but got %v", 0, globalNSMutex.counters.total)
	}
}

// Test to assert that status change from blocked to none shouldn't remove lock info entry for ops
// Ref: Logs from https://github.com/minio/minio/issues/5311
func TestStatusBlockedToNone(t *testing.T) {
	// Initialize namespace lock subsystem
	initNSLock(false)

	ns := globalNSMutex

	volume, path := "bucket", "object"
	param := nsParam{volume: volume, path: path}
	lockSrc := "main.go:1"
	opsID := "1"

	err := ns.statusNoneToBlocked(param, lockSrc, opsID, false)
	if err != nil {
		t.Fatal("Failed to mark lock state to blocked")
	}

	err = ns.statusBlockedToNone(param, lockSrc, opsID, false)
	if err != nil {
		t.Fatal("Failed to mark lock state to none")
	}

	err = ns.deleteLockInfoEntryForOps(param, opsID)
	if err != nil {
		t.Fatalf("Expected deleting of lock entry for %s to pass but got %v", opsID, err)
	}

}

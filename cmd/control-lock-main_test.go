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

// Test print systemState.
func TestPrintLockState(t *testing.T) {
	testLock := nsMutex.NewNSLock("testbucket", "1.txt")
	testLock.Lock()
	sysLockState, err := getSystemLockState()
	if err != nil {
		t.Fatal(err)
	}
	testLock.Unlock()
	sysLockStateMap := map[string]SystemLockState{}
	sysLockStateMap["bucket"] = sysLockState

	// Print lock state.
	printLockState(sysLockStateMap, 0)

	// Print lock state verbose.
	printLockStateVerbose(sysLockStateMap, 0)

	// Does not print any lock state in normal print mode.
	printLockState(sysLockStateMap, 10*time.Second)

	// Does not print any lock state in debug print mode.
	printLockStateVerbose(sysLockStateMap, 10*time.Second)
}

// Helper function to test equality of locks (without taking timing info into account)
func testLockStateEquality(vliLeft, vliRight VolumeLockInfo) bool {

	if vliLeft.Bucket != vliRight.Bucket ||
		vliLeft.Object != vliRight.Object ||
		vliLeft.LocksOnObject != vliRight.LocksOnObject ||
		vliLeft.LocksAcquiredOnObject != vliRight.LocksAcquiredOnObject ||
		vliLeft.TotalBlockedLocks != vliRight.TotalBlockedLocks {
		return false
	}
	return true
}

// Test clearing of locks.
func TestLockStateClear(t *testing.T) {

	// Helper function to circumvent RPC call to LockClear and call msMutex.ForceUnlock immediately.
	f := func(bucket, object string) {
		nsMutex.ForceUnlock(bucket, object)
	}

	testLock := nsMutex.NewNSLock("testbucket", "1.txt")
	testLock.Lock()

	sysLockState, err := getSystemLockState()
	if err != nil {
		t.Fatal(err)
	}

	expectedVli := VolumeLockInfo{
		Bucket:                "testbucket",
		Object:                "1.txt",
		LocksOnObject:         1,
		LocksAcquiredOnObject: 1,
		TotalBlockedLocks:     0,
	}

	// Test initial condition.
	if !testLockStateEquality(expectedVli, sysLockState.LocksInfoPerObject[0]) {
		t.Errorf("Expected %#v, got %#v", expectedVli, sysLockState.LocksInfoPerObject[0])
	}

	sysLockStateMap := map[string]SystemLockState{}
	sysLockStateMap["testnode1"] = sysLockState

	// Clear locks that are 10 seconds old (which is a no-op in this case)
	clearLockState(f, sysLockStateMap, 10*time.Second, "", false)

	if sysLockState, err = getSystemLockState(); err != nil {
		t.Fatal(err)
	}
	if !testLockStateEquality(expectedVli, sysLockState.LocksInfoPerObject[0]) {
		t.Errorf("Expected %#v, got %#v", expectedVli, sysLockState.LocksInfoPerObject[0])
	}

	// Clear all locks (older than 0 seconds)
	clearLockState(f, sysLockStateMap, 0, "", false)

	// Verify that there are no locks
	if sysLockState, err = getSystemLockState(); err != nil {
		t.Fatal(err)
	}
	if len(sysLockState.LocksInfoPerObject) != 0 {
		t.Errorf("Expected no locks, got %#v", sysLockState.LocksInfoPerObject)
	}

	// Create another lock
	blobLock := nsMutex.NewNSLock("testbucket", "blob.txt")
	blobLock.RLock()

	if sysLockState, err = getSystemLockState(); err != nil {
		t.Fatal(err)
	}
	sysLockStateMap["testnode1"] = sysLockState

	// Correct wildcard match but bad age.
	clearLockState(f, sysLockStateMap, 10*time.Second, "testbucket/blob", true)

	// Ensure lock is still there.
	if sysLockState, err = getSystemLockState(); err != nil {
		t.Fatal(err)
	}
	expectedVli.Object = "blob.txt"
	if !testLockStateEquality(expectedVli, sysLockState.LocksInfoPerObject[0]) {
		t.Errorf("Expected %#v, got %#v", expectedVli, sysLockState.LocksInfoPerObject[0])
	}

	// Clear lock based on wildcard match.
	clearLockState(f, sysLockStateMap, 0, "testbucket/blob", true)

	// Verify that there are no locks
	if sysLockState, err = getSystemLockState(); err != nil {
		t.Fatal(err)
	}
	if len(sysLockState.LocksInfoPerObject) != 0 {
		t.Errorf("Expected no locks, got %#v", sysLockState.LocksInfoPerObject)
	}

	// Create yet another lock
	exactLock := nsMutex.NewNSLock("testbucket", "exact.txt")
	exactLock.RLock()

	if sysLockState, err = getSystemLockState(); err != nil {
		t.Fatal(err)
	}
	sysLockStateMap["testnode1"] = sysLockState

	// Make sure that exact match can fail.
	clearLockState(f, sysLockStateMap, 0, "testbucket/exact.txT", false)

	// Ensure lock is still there.
	if sysLockState, err = getSystemLockState(); err != nil {
		t.Fatal(err)
	}
	expectedVli.Object = "exact.txt"
	if !testLockStateEquality(expectedVli, sysLockState.LocksInfoPerObject[0]) {
		t.Errorf("Expected %#v, got %#v", expectedVli, sysLockState.LocksInfoPerObject[0])
	}

	// Clear lock based on exact match.
	clearLockState(f, sysLockStateMap, 0, "testbucket/exact.txt", false)

	// Verify that there are no locks
	if sysLockState, err = getSystemLockState(); err != nil {
		t.Fatal(err)
	}
	if len(sysLockState.LocksInfoPerObject) != 0 {
		t.Errorf("Expected no locks, got %#v", sysLockState.LocksInfoPerObject)
	}

	// reset lock states for further tests
	initNSLock(false)
}

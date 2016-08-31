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
	"strconv"
	"sync"
	"testing"
	"time"
)

// Tests functionality provided by namespace lock.
func TestNamespaceLockTest(t *testing.T) {
	// List of test cases.
	testCases := []struct {
		lk               func(s1, s2, s3 string)
		unlk             func(s1, s2, s3 string)
		rlk              func(s1, s2, s3 string)
		runlk            func(s1, s2, s3 string)
		lkCount          int
		lockedRefCount   uint
		unlockedRefCount uint
		shouldPass       bool
	}{
		{
			lk:               nsMutex.Lock,
			unlk:             nsMutex.Unlock,
			lockedRefCount:   1,
			unlockedRefCount: 0,
			shouldPass:       true,
		},
		{
			rlk:              nsMutex.RLock,
			runlk:            nsMutex.RUnlock,
			lockedRefCount:   4,
			unlockedRefCount: 2,
			shouldPass:       true,
		},
		{
			rlk:              nsMutex.RLock,
			runlk:            nsMutex.RUnlock,
			lockedRefCount:   1,
			unlockedRefCount: 0,
			shouldPass:       true,
		},
	}

	// Run all test cases.

	// Write lock tests.
	testCase := testCases[0]
	testCase.lk("a", "b", "c") // lock once.
	nsLk, ok := nsMutex.lockMap[nsParam{"a", "b"}]
	if !ok && testCase.shouldPass {
		t.Errorf("Lock in map missing.")
	}
	// Validate loced ref count.
	if testCase.lockedRefCount != nsLk.ref && testCase.shouldPass {
		t.Errorf("Test %d fails, expected to pass. Wanted ref count is %d, got %d", 1, testCase.lockedRefCount, nsLk.ref)
	}
	testCase.unlk("a", "b", "c") // unlock once.
	if testCase.unlockedRefCount != nsLk.ref && testCase.shouldPass {
		t.Errorf("Test %d fails, expected to pass. Wanted ref count is %d, got %d", 1, testCase.unlockedRefCount, nsLk.ref)
	}
	_, ok = nsMutex.lockMap[nsParam{"a", "b"}]
	if ok && !testCase.shouldPass {
		t.Errorf("Lock map found after unlock.")
	}

	// Read lock tests.
	testCase = testCases[1]
	testCase.rlk("a", "b", "c") // lock once.
	testCase.rlk("a", "b", "c") // lock second time.
	testCase.rlk("a", "b", "c") // lock third time.
	testCase.rlk("a", "b", "c") // lock fourth time.
	nsLk, ok = nsMutex.lockMap[nsParam{"a", "b"}]
	if !ok && testCase.shouldPass {
		t.Errorf("Lock in map missing.")
	}
	// Validate loced ref count.
	if testCase.lockedRefCount != nsLk.ref && testCase.shouldPass {
		t.Errorf("Test %d fails, expected to pass. Wanted ref count is %d, got %d", 1, testCase.lockedRefCount, nsLk.ref)
	}

	testCase.runlk("a", "b", "c") // unlock once.
	testCase.runlk("a", "b", "c") // unlock second time.
	if testCase.unlockedRefCount != nsLk.ref && testCase.shouldPass {
		t.Errorf("Test %d fails, expected to pass. Wanted ref count is %d, got %d", 2, testCase.unlockedRefCount, nsLk.ref)
	}
	_, ok = nsMutex.lockMap[nsParam{"a", "b"}]
	if !ok && testCase.shouldPass {
		t.Errorf("Lock map not found.")
	}

	// Read lock 0 ref count.
	testCase = testCases[2]
	testCase.rlk("a", "c", "d") // lock once.

	nsLk, ok = nsMutex.lockMap[nsParam{"a", "c"}]
	if !ok && testCase.shouldPass {
		t.Errorf("Lock in map missing.")
	}
	// Validate loced ref count.
	if testCase.lockedRefCount != nsLk.ref && testCase.shouldPass {
		t.Errorf("Test %d fails, expected to pass. Wanted ref count is %d, got %d", 3, testCase.lockedRefCount, nsLk.ref)
	}
	testCase.runlk("a", "c", "d") // unlock once.
	if testCase.unlockedRefCount != nsLk.ref && testCase.shouldPass {
		t.Errorf("Test %d fails, expected to pass. Wanted ref count is %d, got %d", 3, testCase.unlockedRefCount, nsLk.ref)
	}
	_, ok = nsMutex.lockMap[nsParam{"a", "c"}]
	if ok && !testCase.shouldPass {
		t.Errorf("Lock map not found.")
	}
}

func TestLockStats(t *testing.T) {

	expectedResult := []lockStateCase{
		{

			volume: "my-bucket",
			path:   "my-object",
			// expected metrics.
			expectedErr:        nil,
			expectedLockStatus: "Blocked",

			expectedGlobalLockCount:  10,
			expectedRunningLockCount: 10,
			expectedBlockedLockCount: 0,

			expectedVolPathLockCount:    10,
			expectedVolPathRunningCount: 10,
			expectedVolPathBlockCount:   0,
		},
		{

			volume: "my-bucket",
			path:   "my-object",
			// expected metrics.
			expectedErr:        nil,
			expectedLockStatus: "Blocked",

			expectedGlobalLockCount:  5,
			expectedRunningLockCount: 5,
			expectedBlockedLockCount: 0,

			expectedVolPathLockCount:    5,
			expectedVolPathRunningCount: 5,
			expectedVolPathBlockCount:   0,
		},
		{

			volume: "my-bucket",
			path:   "my-object",
			// expected metrics.
			expectedErr:        nil,
			expectedLockStatus: "Blocked",

			expectedGlobalLockCount:  2,
			expectedRunningLockCount: 1,
			expectedBlockedLockCount: 1,

			expectedVolPathLockCount:    2,
			expectedVolPathRunningCount: 1,
			expectedVolPathBlockCount:   1,
		},
		{

			volume: "my-bucket",
			path:   "my-object",
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
		{

			volume: "my-bucket",
			path:   "my-object",
			// expected metrics.
			expectedErr:        nil,
			expectedLockStatus: "Blocked",

			expectedGlobalLockCount:  1,
			expectedRunningLockCount: 1,
			expectedBlockedLockCount: 0,

			expectedVolPathLockCount:    1,
			expectedVolPathRunningCount: 1,
			expectedVolPathBlockCount:   0,
		},
		{

			volume: "my-bucket",
			path:   "my-object",
			// expected metrics.
			expectedErr:        nil,
			expectedLockStatus: "Blocked",

			expectedGlobalLockCount:  7,
			expectedRunningLockCount: 5,
			expectedBlockedLockCount: 2,

			expectedVolPathLockCount:    7,
			expectedVolPathRunningCount: 5,
			expectedVolPathBlockCount:   2,
		},
		{
			volume: "my-bucket",
			path:   "my-object",
			// expected metrics.
			expectedErr:        nil,
			expectedLockStatus: "Blocked",

			expectedGlobalLockCount:  2,
			expectedRunningLockCount: 0,
			expectedBlockedLockCount: 2,

			expectedVolPathLockCount:    2,
			expectedVolPathRunningCount: 0,
			expectedVolPathBlockCount:   2,
		},
		{

			volume: "my-bucket",
			path:   "my-object",
			// expected metrics.
			expectedErr:        nil,
			expectedLockStatus: "Blocked",

			expectedGlobalLockCount:  0,
			expectedRunningLockCount: 0,
			expectedBlockedLockCount: 0,
		},
	}
	var wg sync.WaitGroup
	// enabling lock instrumentation.
	globalDebugLock = true
	// initializing the locks.
	initNSLock()
	// hold 10 read locks.
	for i := 0; i < 10; i++ {
		nsMutex.RLock("my-bucket", "my-object", strconv.Itoa(i))
	}
	// expected lock info.
	expectedLockStats := expectedResult[0]
	// verify the actual lock info with the expected one.
	verifyLockStats(expectedLockStats, t, 1)
	// unlock 5 readlock.
	for i := 0; i < 5; i++ {
		nsMutex.RUnlock("my-bucket", "my-object", strconv.Itoa(i))
	}

	expectedLockStats = expectedResult[1]
	// verify the actual lock info with the expected one.
	verifyLockStats(expectedLockStats, t, 1)

	syncChan := make(chan struct{}, 1)
	wg.Add(1)
	go func() {
		defer wg.Done()
		// blocks till all read locks are released.
		nsMutex.Lock("my-bucket", "my-object", strconv.Itoa(10))
		// Once the above attempt to lock is unblocked/acquired, we verify the stats and release the lock.
		expectedWLockStats := expectedResult[2]
		// Since the write lock acquired here, the number of blocked locks should reduce by 1 and
		// count of running locks should increase by 1.
		verifyLockStats(expectedWLockStats, t, 4)
		// release the write lock.
		nsMutex.Unlock("my-bucket", "my-object", strconv.Itoa(10))
		// The number of running locks should decrease by 1.
		expectedWLockStats = expectedResult[3]
		verifyLockStats(expectedWLockStats, t, 4)
		// Take the lock stats after the first write lock is unlocked.
		// Only then unlock then second write lock.
		syncChan <- struct{}{}
	}()
	// waiting so that the write locks in the above go routines are held.
	// sleeping so that we can predict the order of the write locks held.
	time.Sleep(100 * time.Millisecond)

	// since there are 5 more readlocks still held on <"my-bucket","my-object">,
	// an attempt to hold write locks blocks. So its run in a new go routine.
	wg.Add(1)
	go func() {
		defer wg.Done()
		// blocks till all read locks are released.
		nsMutex.Lock("my-bucket", "my-object", strconv.Itoa(11))
		// Once the above attempt to lock is unblocked/acquired, we release the lock.
		// Unlock the second write lock only after lock stats for first write lock release is taken.
		<-syncChan
		// The number of running locks should decrease by 1.
		expectedWLockStats := expectedResult[4]
		verifyLockStats(expectedWLockStats, t, 5)
		nsMutex.Unlock("my-bucket", "my-object", strconv.Itoa(11))
	}()

	expectedLockStats = expectedResult[5]

	time.Sleep(1 * time.Second)
	// verify the actual lock info with the expected one.
	verifyLockStats(expectedLockStats, t, 2)

	// unlock 5 readlock.
	for i := 0; i < 5; i++ {
		nsMutex.RUnlock("my-bucket", "my-object", strconv.Itoa(i+5))
	}

	expectedLockStats = expectedResult[6]
	// verify the actual lock info with the expected one.
	verifyLockStats(expectedLockStats, t, 3)
	wg.Wait()

	expectedLockStats = expectedResult[7]
	// verify the actual lock info with the expected one.
	verifyGlobalLockStats(expectedLockStats, t, 6)

}

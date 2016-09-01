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
		// Test case - 1.
		// Case where 10 read locks are held.
		// Entry for any of the 10 reads locks has to be found.
		// Since they held in a loop, Lock origin for first 10 read locks (opsID 0-9) should be the same.
		{

			volume:     "my-bucket",
			path:       "my-object",
			opsID:      "0",
			readLock:   true,
			lockOrigin: "[lock held] in github.com/minio/minio/cmd.TestLockStats[/Users/hackintoshrao/mycode/go/src/github.com/minio/minio/cmd/namespace-lock_test.go:298]",
			// expected metrics.
			expectedErr:        nil,
			expectedLockStatus: "Running",

			expectedGlobalLockCount:  10,
			expectedRunningLockCount: 10,
			expectedBlockedLockCount: 0,

			expectedVolPathLockCount:    10,
			expectedVolPathRunningCount: 10,
			expectedVolPathBlockCount:   0,
		},
		// Test case - 2.
		// Case where the first 5 read locks are released.
		// Entry for any  of the 6-10th "Running" reads lock has to be found.
		{
			volume:     "my-bucket",
			path:       "my-object",
			opsID:      "6",
			readLock:   true,
			lockOrigin: "[lock held] in github.com/minio/minio/cmd.TestLockStats[/Users/hackintoshrao/mycode/go/src/github.com/minio/minio/cmd/namespace-lock_test.go:298]",
			// expected metrics.
			expectedErr:        nil,
			expectedLockStatus: "Running",

			expectedGlobalLockCount:  5,
			expectedRunningLockCount: 5,
			expectedBlockedLockCount: 0,

			expectedVolPathLockCount:    5,
			expectedVolPathRunningCount: 5,
			expectedVolPathBlockCount:   0,
		},
		// Test case - 3.
		{

			volume:     "my-bucket",
			path:       "my-object",
			opsID:      "10",
			readLock:   false,
			lockOrigin: "[lock held] in github.com/minio/minio/cmd.TestLockStats[/Users/hackintoshrao/mycode/go/src/github.com/minio/minio/cmd/namespace-lock_test.go:298]",
			// expected metrics.
			expectedErr:        nil,
			expectedLockStatus: "Running",

			expectedGlobalLockCount:  2,
			expectedRunningLockCount: 1,
			expectedBlockedLockCount: 1,

			expectedVolPathLockCount:    2,
			expectedVolPathRunningCount: 1,
			expectedVolPathBlockCount:   1,
		},
		// Test case - 4.
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
		// Test case - 5.
		{

			volume:     "my-bucket",
			path:       "my-object",
			opsID:      "11",
			readLock:   false,
			lockOrigin: "[lock held] in github.com/minio/minio/cmd.TestLockStats[/Users/hackintoshrao/mycode/go/src/github.com/minio/minio/cmd/namespace-lock_test.go:298]",
			// expected metrics.
			expectedErr:        nil,
			expectedLockStatus: "Running",

			expectedGlobalLockCount:  1,
			expectedRunningLockCount: 1,
			expectedBlockedLockCount: 0,

			expectedVolPathLockCount:    1,
			expectedVolPathRunningCount: 1,
			expectedVolPathBlockCount:   0,
		},
		// Test case - 6.
		// Case where in the first 5 read locks are released, but 2 write locks are
		// blocked waiting for the remaining 5 read locks locks to be released (10 read locks were held initially).
		// We check the entry for the first blocked write call here.
		{

			volume:   "my-bucket",
			path:     "my-object",
			opsID:    "10",
			readLock: false,
			// write lock is held at line 318.
			// this confirms that we are looking the right write lock.
			lockOrigin: "[lock held] in github.com/minio/minio/cmd.TestLockStats.func2[/Users/hackintoshrao/mycode/go/src/github.com/minio/minio/cmd/namespace-lock_test.go:318]",
			// expected metrics.
			expectedErr:        nil,
			expectedLockStatus: "Blocked",

			// count of held(running) + blocked locks.
			expectedGlobalLockCount: 7,
			// count of acquired locks.
			expectedRunningLockCount: 5,
			//  2 write calls are blocked, waiting for the remaining 5 read locks.
			expectedBlockedLockCount: 2,

			expectedVolPathLockCount:    7,
			expectedVolPathRunningCount: 5,
			expectedVolPathBlockCount:   2,
		},
		// Test case - 7.
		// Case where in 9 out of 10 read locks are released.
		// Since there's one more pending read lock, the 2 write locks are still blocked.
		// Testing the entry for the last read lock.
		{volume: "my-bucket",
			path:       "my-object",
			opsID:      "9",
			readLock:   true,
			lockOrigin: "[lock held] in github.com/minio/minio/cmd.TestLockStats.func2[/Users/hackintoshrao/mycode/go/src/github.com/minio/minio/cmd/namespace-lock_test.go:318]",
			// expected metrics.
			expectedErr:        nil,
			expectedLockStatus: "Running",

			// Total running + blocked locks.
			// 2 blocked write lock.
			expectedGlobalLockCount:  3,
			expectedRunningLockCount: 1,
			expectedBlockedLockCount: 2,

			expectedVolPathLockCount:    3,
			expectedVolPathRunningCount: 1,
			expectedVolPathBlockCount:   2,
		},
		// Test case - 8.
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
	initNSLock(false)

	// set debug lock info  to `nil` so that the next tests have to initialize them again.
	defer func() {
		globalDebugLock = false
		nsMutex.debugLockMap = nil
	}()

	// hold 10 read locks.
	for i := 0; i < 10; i++ {
		nsMutex.RLock("my-bucket", "my-object", strconv.Itoa(i))
	}
	// expected lock info.
	expectedLockStats := expectedResult[0]
	// verify the actual lock info with the expected one.
	verifyLockState(expectedLockStats, t, 1)
	// unlock 5 readlock.
	for i := 0; i < 5; i++ {
		nsMutex.RUnlock("my-bucket", "my-object", strconv.Itoa(i))
	}

	expectedLockStats = expectedResult[1]
	// verify the actual lock info with the expected one.
	verifyLockState(expectedLockStats, t, 2)

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
		verifyLockState(expectedWLockStats, t, 3)
		// release the write lock.
		nsMutex.Unlock("my-bucket", "my-object", strconv.Itoa(10))
		// The number of running locks should decrease by 1.
		// expectedWLockStats = expectedResult[3]
		// verifyLockState(expectedWLockStats, t, 4)
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
		verifyLockState(expectedWLockStats, t, 5)
		nsMutex.Unlock("my-bucket", "my-object", strconv.Itoa(11))
	}()

	expectedLockStats = expectedResult[5]

	time.Sleep(1 * time.Second)
	// verify the actual lock info with the expected one.
	verifyLockState(expectedLockStats, t, 6)

	// unlock 4 out of remaining 5 read locks.
	for i := 0; i < 4; i++ {
		nsMutex.RUnlock("my-bucket", "my-object", strconv.Itoa(i+5))
	}

	// verify the entry for one remaining read lock and count of blocked write locks.
	expectedLockStats = expectedResult[6]
	// verify the actual lock info with the expected one.
	verifyLockState(expectedLockStats, t, 7)

	// Releasing the last read lock.
	nsMutex.RUnlock("my-bucket", "my-object", strconv.Itoa(9))
	wg.Wait()
	expectedLockStats = expectedResult[7]
	// verify the actual lock info with the expected one.
	verifyGlobalLockStats(expectedLockStats, t, 8)

}

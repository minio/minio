/*
 * Minio Cloud Storage, (C) 2015, 2016 Minio, Inc.
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
	"path"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// API suite container common to both FS and XL.
type TestRPCControllerSuite struct {
	serverType   string
	testServer   TestServer
	testAuthConf *authConfig
}

// Setting up the test suite.
// Starting the Test server with temporary FS backend.
func (s *TestRPCControllerSuite) SetUpSuite(c *testing.T) {
	s.testServer = StartTestRPCServer(c, s.serverType)
	s.testAuthConf = &authConfig{
		address:     s.testServer.Server.Listener.Addr().String(),
		accessKey:   s.testServer.AccessKey,
		secretKey:   s.testServer.SecretKey,
		path:        path.Join(reservedBucket, controlPath),
		loginMethod: "Controller.LoginHandler",
	}
}

// No longer used with gocheck, but used in explicit teardown code in
// each test function. // Called implicitly by "gopkg.in/check.v1"
// after all tests are run.
func (s *TestRPCControllerSuite) TearDownSuite(c *testing.T) {
	s.testServer.Stop()
}

func TestRPCControlLock(t *testing.T) {
	//setup code
	s := &TestRPCControllerSuite{serverType: "XL"}
	s.SetUpSuite(t)

	//run test
	s.testRPCControlLock(t)

	//teardown code
	s.TearDownSuite(t)
}

// Tests to validate the correctness of lock instrumentation control RPC end point.
func (s *TestRPCControllerSuite) testRPCControlLock(c *testing.T) {
	// enabling lock instrumentation.
	globalDebugLock = true
	// initializing the locks.
	initNSLock(false)
	// set debug lock info to `nil` so that the next tests have to
	// initialize them again.
	defer func() {
		globalDebugLock = false
		nsMutex.debugLockMap = nil
	}()

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
		// Test case 2.
		// Testing the existence of entry for the last read lock (read lock with opsID "9").
		{

			volume:     "my-bucket",
			path:       "my-object",
			opsID:      "9",
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

		// Test case 3.
		// Hold a write lock, and it should block since 10 read locks
		// on <"my-bucket", "my-object"> are still held.
		{

			volume:     "my-bucket",
			path:       "my-object",
			opsID:      "10",
			readLock:   false,
			lockOrigin: "[lock held] in github.com/minio/minio/cmd.TestLockStats[/Users/hackintoshrao/mycode/go/src/github.com/minio/minio/cmd/namespace-lock_test.go:298]",
			// expected metrics.
			expectedErr:        nil,
			expectedLockStatus: "Blocked",

			expectedGlobalLockCount:  11,
			expectedRunningLockCount: 10,
			expectedBlockedLockCount: 1,

			expectedVolPathLockCount:    11,
			expectedVolPathRunningCount: 10,
			expectedVolPathBlockCount:   1,
		},

		// Test case 4.
		// Expected result when all the read locks are released and the blocked write lock acquires the lock.
		{

			volume:     "my-bucket",
			path:       "my-object",
			opsID:      "10",
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
		// Test case - 5.
		// At the end after locks are released, its verified whether the counters are set to 0.
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

	// used to make sure that the tests don't end till locks held in other go routines are released.
	var wg sync.WaitGroup

	// Hold 5 read locks. We should find the info about these in the RPC response.

	// hold 10 read locks.
	// Then call the RPC control end point for obtaining lock instrumentation info.

	for i := 0; i < 10; i++ {
		nsMutex.RLock("my-bucket", "my-object", strconv.Itoa(i))
	}

	client := newAuthClient(s.testAuthConf)
	defer client.Close()

	args := &GenericArgs{}
	reply := &SystemLockState{}
	// Call the lock instrumentation RPC end point.
	err := client.Call("Controller.LockInfo", args, reply)
	if err != nil {
		c.Errorf("Add: expected no error but got string %q", err.Error())
	}
	// expected lock info.
	expectedLockStats := expectedResult[0]
	// verify the actual lock info with the expected one.
	// verify the existence entry for first read lock (read lock with opsID "0").
	verifyRPCLockInfoResponse(expectedLockStats, *reply, c, 1)
	expectedLockStats = expectedResult[1]
	// verify the actual lock info with the expected one.
	// verify the existence entry for last read lock (read lock with opsID "9").
	verifyRPCLockInfoResponse(expectedLockStats, *reply, c, 2)

	// now hold a write lock in a different go routine and it should block since 10 read locks are
	// still held.
	wg.Add(1)
	go func() {
		defer wg.Done()
		// blocks till all read locks are released.
		nsMutex.Lock("my-bucket", "my-object", strconv.Itoa(10))
		// Once the above attempt to lock is unblocked/acquired, we verify the stats and release the lock.
		expectedWLockStats := expectedResult[3]
		// Since the write lock acquired here, the number of blocked locks should reduce by 1 and
		// count of running locks should increase by 1.

		// Call the RPC control handle to fetch the lock instrumentation info.
		reply = &SystemLockState{}
		// Call the lock instrumentation RPC end point.
		err = client.Call("Controller.LockInfo", args, reply)
		if err != nil {
			c.Errorf("Add: expected no error but got string %q", err.Error())
		}
		verifyRPCLockInfoResponse(expectedWLockStats, *reply, c, 4)

		// release the write lock.
		nsMutex.Unlock("my-bucket", "my-object", strconv.Itoa(10))

	}()
	// waiting for a second so that the attempt to acquire the write lock in
	// the above go routines gets blocked.
	time.Sleep(1 * time.Second)
	// The write lock should have got blocked by now,
	// check whether the entry for one blocked lock exists.
	expectedLockStats = expectedResult[2]

	// Call the RPC control handle to fetch the lock instrumentation info.
	reply = &SystemLockState{}
	// Call the lock instrumentation RPC end point.
	err = client.Call("Controller.LockInfo", args, reply)
	if err != nil {
		c.Errorf("Add: expected no error but got string %q", err.Error())
	}
	verifyRPCLockInfoResponse(expectedLockStats, *reply, c, 3)
	// Release all the read locks held.
	// the blocked write lock in the above go routines should get unblocked.
	for i := 0; i < 10; i++ {
		nsMutex.RUnlock("my-bucket", "my-object", strconv.Itoa(i))
	}
	wg.Wait()
	// Since all the locks are released. There should not be any entry in the lock info.
	// and all the counters should be set to 0.
	reply = &SystemLockState{}
	// Call the lock instrumentation RPC end point.
	err = client.Call("Controller.LockInfo", args, reply)
	if err != nil {
		c.Errorf("Add: expected no error but got string %q", err.Error())
	}

	if reply.TotalAcquiredLocks != 0 && reply.TotalLocks != 0 && reply.TotalBlockedLocks != 0 {
		c.Fatalf("The counters are not reset properly after all locks are released")
	}
	if len(reply.LocksInfoPerObject) != 0 {
		c.Fatalf("Since all locks are released there shouldn't have been any lock info entry, but found %d", len(reply.LocksInfoPerObject))
	}
}

func TestControllerHealDiskMetadataH(t *testing.T) {
	//setup code
	s := &TestRPCControllerSuite{serverType: "XL"}
	s.SetUpSuite(t)

	//run test
	s.testControllerHealDiskMetadataH(t)

	//teardown code
	s.TearDownSuite(t)
}

// TestControllerHandlerHealDiskMetadata - Registers and call the `HealDiskMetadataHandler`,
// asserts to validate the success.
func (s *TestRPCControllerSuite) testControllerHealDiskMetadataH(c *testing.T) {
	// The suite has already started the test RPC server, just send RPC calls.
	client := newAuthClient(s.testAuthConf)
	defer client.Close()

	args := &GenericArgs{}
	reply := &GenericReply{}
	err := client.Call("Controller.HealDiskMetadataHandler", args, reply)

	if err != nil {
		c.Errorf("Control.HealDiskMetadataH - test failed with <ERROR> %s",
			err.Error())
	}
}

func TestControllerHealObjectH(t *testing.T) {
	//setup code
	s := &TestRPCControllerSuite{serverType: "XL"}
	s.SetUpSuite(t)

	//run test
	s.testControllerHealObjectH(t)

	//teardown code
	s.TearDownSuite(t)
}

func (s *TestRPCControllerSuite) testControllerHealObjectH(t *testing.T) {
	client := newAuthClient(s.testAuthConf)
	defer client.Close()

	err := s.testServer.Obj.MakeBucket("testbucket")
	if err != nil {
		t.Fatalf(
			"Controller.HealObjectH - create bucket failed with <ERROR> %s",
			err.Error(),
		)
	}

	datum := strings.NewReader("a")
	_, err = s.testServer.Obj.PutObject("testbucket", "testobject", 1,
		datum, nil)
	if err != nil {
		t.Fatalf("Controller.HealObjectH - put object failed with <ERROR> %s",
			err.Error())
	}

	args := &HealObjectArgs{GenericArgs{}, "testbucket", "testobject"}
	reply := &GenericReply{}
	err = client.Call("Controller.HealObjectHandler", args, reply)

	if err != nil {
		t.Errorf("Controller.HealObjectH - test failed with <ERROR> %s",
			err.Error())
	}
}

func TestControllerListObjectsHealH(t *testing.T) {
	//setup code
	s := &TestRPCControllerSuite{serverType: "XL"}
	s.SetUpSuite(t)

	//run test
	s.testControllerListObjectsHealH(t)

	//teardown code
	s.TearDownSuite(t)
}

func (s *TestRPCControllerSuite) testControllerListObjectsHealH(t *testing.T) {
	client := newAuthClient(s.testAuthConf)
	defer client.Close()

	// careate a bucket
	err := s.testServer.Obj.MakeBucket("testbucket")
	if err != nil {
		t.Fatalf(
			"Controller.ListObjectsHealH - create bucket failed - %s",
			err.Error(),
		)
	}

	r := strings.NewReader("0")
	_, err = s.testServer.Obj.PutObject(
		"testbucket", "testObj-0", 1, r, nil,
	)
	if err != nil {
		t.Fatalf("Controller.ListObjectsHealH - object creation failed - %s",
			err.Error())
	}

	args := &HealListArgs{
		GenericArgs{}, "testbucket", "testObj-",
		"", "", 100,
	}
	reply := &GenericReply{}
	err = client.Call("Controller.ListObjectsHealHandler", args, reply)

	if err != nil {
		t.Errorf("Controller.ListObjectsHealHandler - test failed - %s",
			err.Error())
	}
}

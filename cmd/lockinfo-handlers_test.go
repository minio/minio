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
	"os"
	"testing"
	"time"
)

// TestListLocksInfo - Test for listLocksInfo.
func TestListLocksInfo(t *testing.T) {
	// reset global variables to start afresh.
	resetTestGlobals()
	// Initialize minio server config.
	rootPath, err := newTestConfig(globalMinioDefaultRegion)
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(rootPath)

	// Initializing new XL objectLayer.
	objAPI, _, xlErr := initTestXLObjLayer()
	if xlErr != nil {
		t.Fatalf("failed to init object layer")
	}
	// Make objLayer available to all internal services via globalObjectAPI.
	globalObjLayerMutex.Lock()
	globalObjectAPI = objAPI
	globalObjLayerMutex.Unlock()
	// Set globalIsXL to indicate that the setup uses an erasure code backend.
	// initialize NSLock.
	isDistXL := false
	initNSLock(isDistXL)

	var nsMutex *nsLockMap

	nsMutex = objAPI.(*xlSets).sets[0].nsMutex

	// Acquire a few locks to populate lock instrumentation.
	// Take 10 read locks on bucket1/prefix1/obj1
	for i := 0; i < 10; i++ {
		readLk := nsMutex.NewNSLock("bucket1", "prefix1/obj1")
		if readLk.GetRLock(newDynamicTimeout(60*time.Second, time.Second)) != nil {
			t.Errorf("Failed to get read lock on iteration %d", i)
		}
	}

	// Take write locks on bucket1/prefix/obj{11..19}
	for i := 0; i < 10; i++ {
		wrLk := nsMutex.NewNSLock("bucket1", fmt.Sprintf("prefix1/obj%d", 10+i))
		if wrLk.GetLock(newDynamicTimeout(60*time.Second, time.Second)) != nil {
			t.Errorf("Failed to get write lock on iteration %d", i)
		}
	}

	testCases := []struct {
		bucket   string
		prefix   string
		duration time.Duration
		numLocks int
	}{
		// Test 1 - Matches all the locks acquired above.
		{
			bucket:   "bucket1",
			prefix:   "prefix1",
			duration: time.Duration(0 * time.Second),
			numLocks: 20,
		},
		// Test 2 - Bucket doesn't match.
		{
			bucket:   "bucket",
			prefix:   "prefix1",
			duration: time.Duration(0 * time.Second),
			numLocks: 0,
		},
		// Test 3 - Prefix doesn't match.
		{
			bucket:   "bucket1",
			prefix:   "prefix11",
			duration: time.Duration(0 * time.Second),
			numLocks: 0,
		},
	}

	for i, test := range testCases {
		actual := listLocksInfo(test.bucket, test.prefix, test.duration)
		if len(actual) != test.numLocks {
			t.Errorf("Test %d - Expected %d locks but observed %d locks",
				i+1, test.numLocks, len(actual))
		}
	}
}

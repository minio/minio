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
	"fmt"
	"os"
	"testing"
)

// Tests heal message to be correct and properly formatted.
func TestHealMsg(t *testing.T) {
	rootPath, err := newTestConfig(globalMinioDefaultRegion)
	if err != nil {
		t.Fatal("Unable to initialize test config", err)
	}
	defer os.RemoveAll(rootPath)
	storageDisks, fsDirs := prepareXLStorageDisks(t)
	errs := make([]error, len(storageDisks))
	defer removeRoots(fsDirs)
	nilDisks := deepCopyStorageDisks(storageDisks)
	nilDisks[5] = nil
	authErrs := make([]error, len(storageDisks))
	authErrs[5] = errAuthentication

	args := []string{}
	for i := range storageDisks {
		args = append(args, fmt.Sprintf("http://10.1.10.%d:9000/d1", i+1))
	}
	endpoints := mustGetNewEndpointList(args...)

	testCases := []struct {
		endPoints    EndpointList
		storageDisks []StorageAPI
		serrs        []error
	}{
		// Test - 1 for valid disks and errors.
		{endpoints, storageDisks, errs},
		// Test - 2 for one of the disks is nil.
		{endpoints, nilDisks, errs},
	}

	for i, testCase := range testCases {
		msg := getHealMsg(testCase.endPoints, testCase.storageDisks)
		if msg == "" {
			t.Fatalf("Test: %d Unable to get heal message.", i+1)
		}
		msg = getStorageInitMsg("init", testCase.endPoints, testCase.storageDisks)
		if msg == "" {
			t.Fatalf("Test: %d Unable to get regular message.", i+1)
		}
	}
}

// Tests disk info, validates if we do return proper disk info structure
// even in case of certain disks not available.
func TestDisksInfo(t *testing.T) {
	storageDisks, fsDirs := prepareXLStorageDisks(t)
	defer removeRoots(fsDirs)

	testCases := []struct {
		storageDisks []StorageAPI
		onlineDisks  int
		offlineDisks int
	}{
		{
			storageDisks: storageDisks,
			onlineDisks:  16,
			offlineDisks: 0,
		},
		{
			storageDisks: prepareNOfflineDisks(deepCopyStorageDisks(storageDisks), 4, t),
			onlineDisks:  12,
			offlineDisks: 4,
		},
		{
			storageDisks: prepareNOfflineDisks(deepCopyStorageDisks(storageDisks), 16, t),
			onlineDisks:  0,
			offlineDisks: 16,
		},
	}

	for i, testCase := range testCases {
		_, onlineDisks, offlineDisks := getDisksInfo(testCase.storageDisks)
		if testCase.onlineDisks != onlineDisks {
			t.Errorf("Test %d: Expected online disks %d, got %d", i+1, testCase.onlineDisks, onlineDisks)
		}
		if testCase.offlineDisks != offlineDisks {
			t.Errorf("Test %d: Expected offline disks %d, got %d", i+1, testCase.offlineDisks, offlineDisks)
		}
	}

}

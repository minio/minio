/*
 * MinIO Cloud Storage, (C) 2019 MinIO, Inc.
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
	"os"
	"reflect"
	"sync"
	"testing"

	"github.com/minio/minio/pkg/dsync"
)

// Helper function to create a lock server for testing
func createLockTestServer(t *testing.T) (string, *lockRESTServer, string) {
	obj, fsDir, err := prepareFS()
	if err != nil {
		t.Fatal(err)
	}
	if err = newTestConfig(globalMinioDefaultRegion, obj); err != nil {
		t.Fatalf("unable initialize config file, %s", err)
	}

	locker := &lockRESTServer{
		ll: &localLocker{
			mutex:   sync.Mutex{},
			lockMap: make(map[string][]lockRequesterInfo),
		},
	}
	creds := globalActiveCred
	token, err := authenticateNode(creds.AccessKey, creds.SecretKey, "")
	if err != nil {
		t.Fatal(err)
	}
	return fsDir, locker, token
}

// Test function to remove lock entries from map based on name & uid combination
func TestLockRpcServerRemoveEntry(t *testing.T) {
	testPath, locker, _ := createLockTestServer(t)
	defer os.RemoveAll(testPath)

	lockRequesterInfo1 := lockRequesterInfo{
		Owner:         "owner",
		Writer:        true,
		UID:           "0123-4567",
		Timestamp:     UTCNow(),
		TimeLastCheck: UTCNow(),
	}
	lockRequesterInfo2 := lockRequesterInfo{
		Owner:         "owner",
		Writer:        true,
		UID:           "89ab-cdef",
		Timestamp:     UTCNow(),
		TimeLastCheck: UTCNow(),
	}

	locker.ll.lockMap["name"] = []lockRequesterInfo{
		lockRequesterInfo1,
		lockRequesterInfo2,
	}

	lri := locker.ll.lockMap["name"]

	// test unknown uid
	if locker.ll.removeEntry("name", dsync.LockArgs{
		Owner: "owner",
		UID:   "unknown-uid",
	}, &lri) {
		t.Errorf("Expected %#v, got %#v", false, true)
	}

	if !locker.ll.removeEntry("name", dsync.LockArgs{
		Owner: "owner",
		UID:   "0123-4567",
	}, &lri) {
		t.Errorf("Expected %#v, got %#v", true, false)
	} else {
		gotLri := locker.ll.lockMap["name"]
		expectedLri := []lockRequesterInfo{lockRequesterInfo2}
		if !reflect.DeepEqual(expectedLri, gotLri) {
			t.Errorf("Expected %#v, got %#v", expectedLri, gotLri)
		}
	}

	if !locker.ll.removeEntry("name", dsync.LockArgs{
		Owner: "owner",
		UID:   "89ab-cdef",
	}, &lri) {
		t.Errorf("Expected %#v, got %#v", true, false)
	} else {
		gotLri := locker.ll.lockMap["name"]
		expectedLri := []lockRequesterInfo(nil)
		if !reflect.DeepEqual(expectedLri, gotLri) {
			t.Errorf("Expected %#v, got %#v", expectedLri, gotLri)
		}
	}
}

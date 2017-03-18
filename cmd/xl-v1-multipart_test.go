/*
 * Minio Cloud Storage, (C) 2014, 2015, 2016, 2017 Minio, Inc.
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

func TestUpdateUploadJSON(t *testing.T) {
	// Initialize configuration
	root, err := newTestConfig(globalMinioDefaultRegion)
	if err != nil {
		t.Fatalf("%s", err)
	}
	defer removeAll(root)

	// Create an instance of xl backend
	obj, fsDirs, err := prepareXL()
	if err != nil {
		t.Fatal(err)
	}
	// Defer cleanup of backend directories
	defer removeRoots(fsDirs)

	bucket, object := "bucket", "object"
	err = obj.MakeBucket(bucket)
	if err != nil {
		t.Fatal(err)
	}

	testCases := []struct {
		uploadID  string
		initiated time.Time
		isRemove  bool
		errVal    error
	}{
		{"111abc", UTCNow(), false, nil},
		{"222abc", UTCNow(), false, nil},
		{"111abc", time.Time{}, true, nil},
	}

	xl := obj.(*xlObjects)
	for i, test := range testCases {
		testErrVal := xl.updateUploadJSON(bucket, object, test.uploadID, test.initiated, test.isRemove)
		if testErrVal != test.errVal {
			t.Errorf("Test %d: Expected error value %v, but got %v",
				i+1, test.errVal, testErrVal)
		}
	}

	// make some disks faulty to simulate a failure.
	for i := range xl.storageDisks[:9] {
		xl.storageDisks[i] = newNaughtyDisk(xl.storageDisks[i].(*retryStorage), nil, errFaultyDisk)
	}

	testErrVal := xl.updateUploadJSON(bucket, object, "222abc", UTCNow(), false)
	if testErrVal == nil || testErrVal.Error() != errXLWriteQuorum.Error() {
		t.Errorf("Expected write quorum error, but got: %v", testErrVal)
	}
}

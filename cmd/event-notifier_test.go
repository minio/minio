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

import "testing"

// Tests various forms of inititalization of event notifier.
func TestInitEventNotifier(t *testing.T) {
	fs, disk, err := getSingleNodeObjectLayer()
	if err != nil {
		t.Fatal("Unable to initialize FS backend.", err)
	}
	xl, disks, err := getXLObjectLayer()
	if err != nil {
		t.Fatal("Unable to initialize XL backend.", err)
	}

	disks = append(disks, disk)
	for _, d := range disks {
		defer removeAll(d)
	}

	// Collection of test cases for inititalizing event notifier.
	testCases := []struct {
		objAPI  ObjectLayer
		configs map[string]*notificationConfig
		err     error
	}{
		// Test 1 - invalid arguments.
		{
			objAPI: nil,
			err:    errInvalidArgument,
		},
		// Test 2 - valid FS object layer but no bucket notifications.
		{
			objAPI: fs,
			err:    nil,
		},
		// Test 3 - valid XL object layer but no bucket notifications.
		{
			objAPI: xl,
			err:    nil,
		},
	}

	// Validate if event notifier is properly initialized.
	for i, testCase := range testCases {
		err = initEventNotifier(testCase.objAPI)
		if err != testCase.err {
			t.Errorf("Test %d: Expected %s, but got: %s", i+1, testCase.err, err)
		}
	}
}

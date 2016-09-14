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
	"reflect"
	"testing"
)

// Tests event notify.
func TestEventNotify(t *testing.T) {
	ExecObjectLayerTest(t, testEventNotify)
}

func testEventNotify(obj ObjectLayer, instanceType string, t TestErrHandler) {
	bucketName := getRandomBucketName()

	// initialize the server and obtain the credentials and root.
	// credentials are necessary to sign the HTTP request.
	rootPath, err := newTestConfig("us-east-1")
	if err != nil {
		t.Fatalf("Init Test config failed")
	}
	// remove the root folder after the test ends.
	defer removeAll(rootPath)

	initEventNotifier(obj)

	// Notify object created event.
	eventNotify(eventData{
		Type:   ObjectCreatedPost,
		Bucket: bucketName,
		ObjInfo: ObjectInfo{
			Bucket: bucketName,
			Name:   "object1",
		},
		ReqParams: map[string]string{
			"sourceIPAddress": "localhost:1337",
		},
	})

	if err := globalEventNotifier.SetBucketNotificationConfig(bucketName, nil); err != errInvalidArgument {
		t.Errorf("Expected error %s, got %s", errInvalidArgument, err)
	}

	if err := globalEventNotifier.SetBucketNotificationConfig(bucketName, &notificationConfig{}); err != nil {
		t.Errorf("Expected error to be nil, got %s", err)
	}

	if !globalEventNotifier.IsBucketNotificationSet(bucketName) {
		t.Errorf("Notification expected to be set, but notification not set.")
	}

	nConfig := globalEventNotifier.GetBucketNotificationConfig(bucketName)
	if !reflect.DeepEqual(nConfig, &notificationConfig{}) {
		t.Errorf("Mismatching notification configs.")
	}

	// Notify object created event.
	eventNotify(eventData{
		Type:   ObjectCreatedPost,
		Bucket: bucketName,
		ObjInfo: ObjectInfo{
			Bucket: bucketName,
			Name:   "object1",
		},
		ReqParams: map[string]string{
			"sourceIPAddress": "localhost:1337",
		},
	})
}

// Tests various forms of inititalization of event notifier.
func TestInitEventNotifier(t *testing.T) {
	disk, err := getRandomDisks(1)
	if err != nil {
		t.Fatal("Unable to create directories for FS backend. ", err)
	}
	fs, err := getSingleNodeObjectLayer(disk[0])
	if err != nil {
		t.Fatal("Unable to initialize FS backend.", err)
	}
	nDisks := 16
	disks, err := getRandomDisks(nDisks)
	if err != nil {
		t.Fatal("Unable to create directories for XL backend. ", err)
	}
	xl, err := getXLObjectLayer(disks)
	if err != nil {
		t.Fatal("Unable to initialize XL backend.", err)
	}

	disks = append(disks, disk...)
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

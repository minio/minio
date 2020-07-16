/*
 * MinIO Cloud Storage, (C) 2018 MinIO, Inc.
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
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"testing"
)

// Wrapper for calling DeleteObject tests for both Erasure multiple disks and single node setup.
func TestDeleteObject(t *testing.T) {
	ExecObjectLayerTest(t, testDeleteObject)
}

// Unit test for DeleteObject in general.
func testDeleteObject(obj ObjectLayer, instanceType string, t TestErrHandler) {

	type objectUpload struct {
		name    string
		content string
	}

	testCases := []struct {
		bucketName         string
		objectToUploads    []objectUpload
		pathToDelete       string
		objectsAfterDelete []string
	}{
		// Test 1: removes an object and checks it is the only object
		// that has been deleted.
		{
			"bucket1",
			[]objectUpload{{"object0", "content"}, {"object1", "content"}},
			"object0",
			[]string{"object1"},
		},
		// Test 2: remove an object inside a directory and checks it is deleted
		// with its parent since this former becomes empty
		{
			"bucket2",
			[]objectUpload{{"object0", "content"}, {"dir/object1", "content"}},
			"dir/object1",
			[]string{"object0"},
		},
		// Test 3: remove an object inside a directory and checks if it is deleted
		// but other sibling object in the same directory still exists
		{
			"bucket3",
			[]objectUpload{{"dir/object1", "content"}, {"dir/object2", "content"}},
			"dir/object1",
			[]string{"dir/object2"},
		},
		// Test 4: remove a non empty directory and checks it has no effect
		{
			"bucket4",
			[]objectUpload{{"object0", "content"}, {"dir/object1", "content"}},
			"dir/",
			[]string{"dir/object1", "object0"},
		},
		// Test 5: Remove an empty directory and checks it is really removed
		{
			"bucket5",
			[]objectUpload{{"object0", "content"}, {"dir/", ""}},
			"dir/",
			[]string{"object0"},
		},
	}

	for i, testCase := range testCases {
		err := obj.MakeBucketWithLocation(context.Background(), testCase.bucketName, BucketOptions{})
		if err != nil {
			t.Fatalf("%s : %s", instanceType, err.Error())
		}

		for _, object := range testCase.objectToUploads {
			md5Bytes := md5.Sum([]byte(object.content))
			_, err = obj.PutObject(context.Background(), testCase.bucketName, object.name, mustGetPutObjReader(t, bytes.NewBufferString(object.content),
				int64(len(object.content)), hex.EncodeToString(md5Bytes[:]), ""), ObjectOptions{})
			if err != nil {
				t.Fatalf("%s : %s", instanceType, err.Error())
			}
		}

		_, _ = obj.DeleteObject(context.Background(), testCase.bucketName, testCase.pathToDelete, ObjectOptions{})

		result, err := obj.ListObjects(context.Background(), testCase.bucketName, "", "", "", 1000)
		if err != nil {
			t.Errorf("Test %d: %s:  Expected to pass, but failed with: <ERROR> %s", i+1, instanceType, err.Error())
			continue
		}

		if len(result.Objects) != len(testCase.objectsAfterDelete) {
			t.Errorf("Test %d: %s: mismatch number of objects after delete, expected = %v, found = %v", i+1, instanceType, testCase.objectsAfterDelete, result.Objects)
			continue
		}

		for idx := range result.Objects {
			if result.Objects[idx].Name != testCase.objectsAfterDelete[idx] {
				t.Errorf("Test %d: %s: Unexpected object found after delete, found = `%v`", i+1, instanceType, result.Objects[idx].Name)
			}
		}
	}
}

/*
 * Minio Cloud Storage, (C) 2017 Minio, Inc.
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
	"net/http"
	"testing"
)

// Tests headerToObjectInfo
func TestHeaderToObjectInfo(t *testing.T) {
	testCases := []struct {
		bucket, object string
		header         http.Header
		objInfo        ObjectInfo
	}{
		{
			bucket: "bucket",
			object: "object",
			header: http.Header{
				"Content-Length":         []string{"10"},
				"Content-Type":           []string{"application/javascript"},
				"X-Bz-Upload-Timestamp":  []string{"1000"},
				"X-Bz-Info-X-Amz-Meta-1": []string{"test1"},
				"X-Bz-File-Id":           []string{"xxxxx"},
			},
			objInfo: ObjectInfo{
				Bucket:      "bucket",
				Name:        "object",
				ContentType: "application/javascript",
				Size:        10,
				UserDefined: map[string]string{
					"X-Amz-Meta-1": "test1",
				},
				ETag: "xxxxx",
			},
		},
	}
	for i, testCase := range testCases {
		gotObjInfo, err := headerToObjectInfo(testCase.bucket, testCase.object, testCase.header)
		if err != nil {
			t.Fatalf("Test %d: %s", i+1, err)
		}
		if gotObjInfo.Bucket != testCase.objInfo.Bucket {
			t.Errorf("Test %d: Expected %s, got %s", i+1, testCase.objInfo.Bucket, gotObjInfo.Bucket)
		}
		if gotObjInfo.Name != testCase.objInfo.Name {
			t.Errorf("Test %d: Expected %s, got %s", i+1, testCase.objInfo.Name, gotObjInfo.Name)
		}
		if gotObjInfo.ContentType != testCase.objInfo.ContentType {
			t.Errorf("Test %d: Expected %s, got %s", i+1, testCase.objInfo.ContentType, gotObjInfo.ContentType)
		}
		if gotObjInfo.ETag != testCase.objInfo.ETag {
			t.Errorf("Test %d: Expected %s, got %s", i+1, testCase.objInfo.ETag, gotObjInfo.ETag)
		}
	}
}

// Tests mkRange test.
func TestMkRange(t *testing.T) {
	testCases := []struct {
		offset, size int64
		expectedRng  string
	}{
		// No offset set, size not set.
		{
			offset:      0,
			size:        0,
			expectedRng: "",
		},
		// Offset set, size not set.
		{
			offset:      10,
			size:        0,
			expectedRng: "bytes=10-",
		},
		// Offset set, size set.
		{
			offset:      10,
			size:        11,
			expectedRng: "bytes=10-20",
		},
	}
	for i, testCase := range testCases {
		gotRng := mkRange(testCase.offset, testCase.size)
		if gotRng != testCase.expectedRng {
			t.Errorf("Test %d: expected %s, got %s", i+1, testCase.expectedRng, gotRng)
		}
	}
}

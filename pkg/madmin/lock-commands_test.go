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
 *
 */

package madmin

import (
	"bytes"
	"encoding/json"
	"reflect"
	"testing"
)

// Test for getLockInfos helper function.
func TestGetLockInfos(t *testing.T) {
	testCases := []struct {
		// Used to construct a io.Reader holding xml serialized lock information
		inputLocks []VolumeLockInfo
	}{
		// To build a reader with _no_ lock information.
		{
			inputLocks: []VolumeLockInfo{},
		},
		// To build a reader with _one_ lock information.
		{
			inputLocks: []VolumeLockInfo{{Bucket: "bucket", Object: "object"}},
		},
	}
	for i, test := range testCases {
		jsonBytes, err := json.Marshal(test.inputLocks)
		if err != nil {
			t.Fatalf("Test %d - Failed to marshal input lockInfos - %v", i+1, err)
		}
		actualLocks, err := getLockInfos(bytes.NewReader(jsonBytes))
		if err != nil {
			t.Fatalf("Test %d - Failed to get lock information - %v", i+1, err)
		}
		if !reflect.DeepEqual(actualLocks, test.inputLocks) {
			t.Errorf("Test %d - Expected %v but received %v", i+1, test.inputLocks, actualLocks)
		}
	}

	// Invalid json representation of []VolumeLockInfo
	_, err := getLockInfos(bytes.NewReader([]byte("invalidBytes")))
	if err == nil {
		t.Errorf("Test expected to fail, but passed")
	}
}

/*
 * MinIO Cloud Storage, (C) 2017 MinIO, Inc.
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

package sys

import "testing"

// Test get max open file limit.
func TestGetMaxOpenFileLimit(t *testing.T) {
	_, _, err := GetMaxOpenFileLimit()
	if err != nil {
		t.Errorf("expected: nil, got: %v", err)
	}
}

// Test set open file limit
func TestSetMaxOpenFileLimit(t *testing.T) {
	curLimit, maxLimit, err := GetMaxOpenFileLimit()
	if err != nil {
		t.Fatalf("Unable to get max open file limit. %v", err)
	}

	err = SetMaxOpenFileLimit(curLimit, maxLimit)
	if err != nil {
		t.Errorf("expected: nil, got: %v", err)
	}
}

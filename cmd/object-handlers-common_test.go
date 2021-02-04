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
	"testing"
)

// Tests - canonicalizeETag()
func TestCanonicalizeETag(t *testing.T) {
	testCases := []struct {
		etag              string
		canonicalizedETag string
	}{
		{
			etag:              "\"\"\"",
			canonicalizedETag: "",
		},
		{
			etag:              "\"\"\"abc\"",
			canonicalizedETag: "abc",
		},
		{
			etag:              "abcd",
			canonicalizedETag: "abcd",
		},
		{
			etag:              "abcd\"\"",
			canonicalizedETag: "abcd",
		},
	}
	for _, test := range testCases {
		etag := canonicalizeETag(test.etag)
		if test.canonicalizedETag != etag {
			t.Fatalf("Expected %s , got %s", test.canonicalizedETag, etag)

		}
	}
}

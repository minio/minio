/*
 * MinIO Cloud Storage, (C) 2015, 2016, 2017 MinIO, Inc.
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

func TestNewRequestID(t *testing.T) {
	// Ensure that it returns an alphanumeric result of length 16.
	var id = mustGetRequestID(UTCNow())

	if len(id) != 16 {
		t.Fail()
	}

	var e rune
	for _, char := range id {
		e = rune(char)

		// Ensure that it is alphanumeric, in this case, between 0-9 and A-Z.
		if !(('0' <= e && e <= '9') || ('A' <= e && e <= 'Z')) {
			t.Fail()
		}
	}
}

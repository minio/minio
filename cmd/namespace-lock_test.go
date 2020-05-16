/*
 * MinIO Cloud Storage, (C) 2016 MinIO, Inc.
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

// WARNING:
//
// Expected source line number is hard coded, 31, in the
// following test. Adding new code before this test or changing its
// position will cause the line number to change and the test to FAIL
// Tests getSource().
func TestGetSource(t *testing.T) {
	currentSource := func() string { return getSource(2) }
	gotSource := currentSource()
	// Hard coded line number, 31, in the "expectedSource" value
	expectedSource := "[namespace-lock_test.go:31:TestGetSource()]"
	if gotSource != expectedSource {
		t.Errorf("expected : %s, got : %s", expectedSource, gotSource)
	}
}

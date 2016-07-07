/*
 * Minio Cloud Storage, (C) 2015, 2016 Minio, Inc.
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

package main

import "testing"
import "syscall"

// Test for reduceErrs.
func TestReduceErrs(t *testing.T) {
	testCases := []struct {
		errs []error
		err  error
	}{
		{[]error{errDiskNotFound, errDiskNotFound, errDiskFull}, errDiskNotFound},
		{[]error{errDiskNotFound, errDiskFull, errDiskFull}, errDiskFull},
		{[]error{errDiskFull, errDiskNotFound, errDiskFull}, errDiskFull},
		// A case for error not in the known errors list (refer to func reduceErrs)
		{[]error{errDiskFull, syscall.EEXIST, syscall.EEXIST}, syscall.EEXIST},
		{[]error{errDiskNotFound, errDiskNotFound, nil}, nil},
		{[]error{nil, nil, nil}, nil},
	}
	for i, testCase := range testCases {
		expected := testCase.err
		got := reduceErrs(testCase.errs)
		if expected != got {
			t.Errorf("Test %d : expected %s, got %s", i+1, expected, got)
		}
	}
}

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

package errors

import (
	"fmt"
	"go/build"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// Test trace errors.
func TestTrace(t *testing.T) {
	var errExpectedCause = fmt.Errorf("traceable error")
	var testCases = []struct {
		expectedCauseErr error
	}{
		{
			expectedCauseErr: nil,
		},
		{
			expectedCauseErr: errExpectedCause,
		},
		{
			expectedCauseErr: Trace(errExpectedCause),
		},
	}
	for i, testCase := range testCases {
		if err := Trace(testCase.expectedCauseErr); err != nil {
			if errGotCause := Cause(err); errGotCause != Cause(testCase.expectedCauseErr) {
				t.Errorf("Test: %d Expected %s, got %s", i+1, testCase.expectedCauseErr, errGotCause)
			}
		}
	}
}

// Test if isErrIgnored works correctly.
func TestIsErrIgnored(t *testing.T) {
	var errIgnored = fmt.Errorf("ignored error")
	var testCases = []struct {
		err     error
		ignored bool
	}{
		{
			err:     nil,
			ignored: false,
		},
		{
			err:     errIgnored,
			ignored: true,
		},
		{
			err:     Trace(errIgnored),
			ignored: true,
		},
	}
	for i, testCase := range testCases {
		if ok := IsErrIgnored(testCase.err, errIgnored); ok != testCase.ignored {
			t.Errorf("Test: %d, Expected %t, got %t", i+1, testCase.ignored, ok)
		}
	}
}

// Tests if pkgPath is set properly in init.
func TestInit(t *testing.T) {
	Init("/home/test/go", "test")
	if filepath.ToSlash(pkgPath) != "/home/test/go/src/test/" {
		t.Fatalf("Expected pkgPath to be \"/home/test/go/src/test/\", found %s", pkgPath)
	}
}

// Tests stack output.
func TestStack(t *testing.T) {
	Init(build.Default.GOPATH, "github.com/minio/minio")

	err := Trace(fmt.Errorf("traceable error"))
	if terr, ok := err.(*Error); ok {
		if !strings.HasSuffix(terr.Stack()[0], "TestStack()") {
			t.Errorf("Expected suffix \"TestStack()\", got %s", terr.Stack()[0])
		}
	}
	// Test if the cause error is returned properly with the underlying string.
	if err.Error() != "traceable error" {
		t.Errorf("Expected \"traceable error\", got %s", err.Error())
	}
}

// Tests converting error causes.
func TestErrCauses(t *testing.T) {
	errTraceableError := fmt.Errorf("traceable error")
	var errs = []error{
		errTraceableError,
		errTraceableError,
		errTraceableError,
	}
	var terrs []error
	for _, err := range errs {
		terrs = append(terrs, Trace(err))
	}
	cerrs := Causes(terrs)
	if !reflect.DeepEqual(errs, cerrs) {
		t.Errorf("Expected %#v, got %#v", errs, cerrs)
	}
}

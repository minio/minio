// Copyright (c) 2015-2021 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package iampolicy

import (
	"testing"
)

func TestActionIsObjectAction(t *testing.T) {
	testCases := []struct {
		action         Action
		expectedResult bool
	}{
		{AbortMultipartUploadAction, true},
		{DeleteObjectAction, true},
		{GetObjectAction, true},
		{ListMultipartUploadPartsAction, true},
		{PutObjectAction, true},
		{CreateBucketAction, false},
	}

	for i, testCase := range testCases {
		result := testCase.action.isObjectAction()

		if testCase.expectedResult != result {
			t.Fatalf("case %v: expected: %v, got: %v", i+1, testCase.expectedResult, result)
		}
	}
}

func TestActionIsValid(t *testing.T) {
	testCases := []struct {
		action         Action
		expectedResult bool
	}{
		{PutObjectAction, true},
		{AbortMultipartUploadAction, true},
		{Action("foo"), false},
	}

	for i, testCase := range testCases {
		result := testCase.action.IsValid()

		if testCase.expectedResult != result {
			t.Fatalf("case %v: expected: %v, got: %v", i+1, testCase.expectedResult, result)
		}
	}
}

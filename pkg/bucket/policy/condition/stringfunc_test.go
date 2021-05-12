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

package condition

import (
	"encoding/base64"
	"reflect"
	"testing"

	"github.com/minio/minio-go/v7/pkg/set"
)

func TestStringEqualsFuncEvaluate(t *testing.T) {
	case1Function, err := newStringEqualsFunc(S3XAmzCopySource.ToKey(), NewValueSet(NewStringValue("mybucket/myobject")), "")
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case2Function, err := newStringEqualsFunc(S3LocationConstraint.ToKey(), NewValueSet(NewStringValue("eu-west-1"), NewStringValue("ap-southeast-1")), "")
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case3Function, err := newStringEqualsFunc(JWTGroups.ToKey(), NewValueSet(NewStringValue("prod"), NewStringValue("art")), forAllValues)
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case4Function, err := newStringEqualsFunc(JWTGroups.ToKey(), NewValueSet(NewStringValue("prod"), NewStringValue("art")), forAnyValue)
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case5Function, err := newStringEqualsFunc(S3LocationConstraint.ToKey(), NewValueSet(NewStringValue(S3LocationConstraint.ToKey().VarName())), "")
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case6Function, err := newStringEqualsFunc(NewKey(ExistingObjectTag, "security"), NewValueSet(NewStringValue("public")), "")
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	testCases := []struct {
		function       Function
		values         map[string][]string
		expectedResult bool
	}{
		{case1Function, map[string][]string{"x-amz-copy-source": {"mybucket/myobject"}}, true},
		{case1Function, map[string][]string{"x-amz-copy-source": {"yourbucket/myobject"}}, false},
		{case1Function, map[string][]string{}, false},
		{case1Function, map[string][]string{"delimiter": {"/"}}, false},

		{case2Function, map[string][]string{"LocationConstraint": {"eu-west-1"}}, true},
		{case2Function, map[string][]string{"LocationConstraint": {"ap-southeast-1"}}, true},
		{case2Function, map[string][]string{"LocationConstraint": {"us-east-1"}}, false},
		{case2Function, map[string][]string{}, false},
		{case2Function, map[string][]string{"delimiter": {"/"}}, false},

		{case3Function, map[string][]string{"groups": {"prod", "art"}}, true},
		{case3Function, map[string][]string{"groups": {"art"}}, false},
		{case3Function, map[string][]string{}, true},
		{case3Function, map[string][]string{"delimiter": {"/"}}, true},

		{case4Function, map[string][]string{"groups": {"prod", "art"}}, true},
		{case4Function, map[string][]string{"groups": {"art"}}, true},
		{case4Function, map[string][]string{}, false},
		{case4Function, map[string][]string{"delimiter": {"/"}}, false},

		{case5Function, map[string][]string{"LocationConstraint": {"us-west-1"}}, true},

		{case6Function, map[string][]string{"ExistingObjectTag/security": {"public"}}, true},
		{case6Function, map[string][]string{"ExistingObjectTag/security": {"private"}}, false},
		{case6Function, map[string][]string{"ExistingObjectTag/project": {"foo"}}, false},
	}

	for i, testCase := range testCases {
		result := testCase.function.evaluate(testCase.values)

		if result != testCase.expectedResult {
			t.Fatalf("case %v: expected: %v, got: %v\n", i+1, testCase.expectedResult, result)
		}
	}
}

func TestStringNotEqualsFuncEvaluate(t *testing.T) {
	case1Function, err := newStringNotEqualsFunc(S3XAmzCopySource.ToKey(), NewValueSet(NewStringValue("mybucket/myobject")), "")
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case2Function, err := newStringNotEqualsFunc(S3LocationConstraint.ToKey(), NewValueSet(NewStringValue("eu-west-1"), NewStringValue("ap-southeast-1")), "")
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case3Function, err := newStringNotEqualsFunc(JWTGroups.ToKey(), NewValueSet(NewStringValue("prod"), NewStringValue("art")), forAllValues)
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case4Function, err := newStringNotEqualsFunc(JWTGroups.ToKey(), NewValueSet(NewStringValue("prod"), NewStringValue("art")), forAnyValue)
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	testCases := []struct {
		function       Function
		values         map[string][]string
		expectedResult bool
	}{
		{case1Function, map[string][]string{"x-amz-copy-source": {"mybucket/myobject"}}, false},
		{case1Function, map[string][]string{"x-amz-copy-source": {"yourbucket/myobject"}}, true},
		{case1Function, map[string][]string{}, true},
		{case1Function, map[string][]string{"delimiter": {"/"}}, true},

		{case2Function, map[string][]string{"LocationConstraint": {"eu-west-1"}}, false},
		{case2Function, map[string][]string{"LocationConstraint": {"ap-southeast-1"}}, false},
		{case2Function, map[string][]string{"LocationConstraint": {"us-east-1"}}, true},
		{case2Function, map[string][]string{}, true},
		{case2Function, map[string][]string{"delimiter": {"/"}}, true},

		{case3Function, map[string][]string{"groups": {"prod", "art"}}, false},
		{case3Function, map[string][]string{"groups": {"art"}}, true},
		{case3Function, map[string][]string{}, false},
		{case3Function, map[string][]string{"delimiter": {"/"}}, false},

		{case4Function, map[string][]string{"groups": {"prod", "art"}}, false},
		{case4Function, map[string][]string{"groups": {"art"}}, false},
		{case4Function, map[string][]string{}, true},
		{case4Function, map[string][]string{"delimiter": {"/"}}, true},
	}

	for i, testCase := range testCases {
		result := testCase.function.evaluate(testCase.values)

		if result != testCase.expectedResult {
			t.Fatalf("case %v: expected: %v, got: %v\n", i+1, testCase.expectedResult, result)
		}
	}
}

func TestStringEqualsIgnoreCaseFuncEvaluate(t *testing.T) {
	case1Function, err := newStringEqualsIgnoreCaseFunc(S3XAmzCopySource.ToKey(), NewValueSet(NewStringValue("mybucket/MYOBJECT")), "")
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case2Function, err := newStringEqualsIgnoreCaseFunc(S3LocationConstraint.ToKey(), NewValueSet(NewStringValue("EU-WEST-1"), NewStringValue("AP-southeast-1")), "")
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case3Function, err := newStringEqualsIgnoreCaseFunc(JWTGroups.ToKey(), NewValueSet(NewStringValue("Prod"), NewStringValue("Art")), forAllValues)
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case4Function, err := newStringEqualsIgnoreCaseFunc(JWTGroups.ToKey(), NewValueSet(NewStringValue("Prod"), NewStringValue("Art")), forAnyValue)
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	testCases := []struct {
		function       Function
		values         map[string][]string
		expectedResult bool
	}{
		{case1Function, map[string][]string{"x-amz-copy-source": {"mybucket/myobject"}}, true},
		{case1Function, map[string][]string{"x-amz-copy-source": {"yourbucket/myobject"}}, false},
		{case1Function, map[string][]string{}, false},
		{case1Function, map[string][]string{"delimiter": {"/"}}, false},

		{case2Function, map[string][]string{"LocationConstraint": {"eu-west-1"}}, true},
		{case2Function, map[string][]string{"LocationConstraint": {"ap-southeast-1"}}, true},
		{case2Function, map[string][]string{"LocationConstraint": {"us-east-1"}}, false},
		{case2Function, map[string][]string{}, false},
		{case2Function, map[string][]string{"delimiter": {"/"}}, false},

		{case3Function, map[string][]string{"groups": {"prod", "art"}}, true},
		{case3Function, map[string][]string{"groups": {"art"}}, false},
		{case3Function, map[string][]string{}, true},
		{case3Function, map[string][]string{"delimiter": {"/"}}, true},

		{case4Function, map[string][]string{"groups": {"prod", "art"}}, true},
		{case4Function, map[string][]string{"groups": {"art"}}, true},
		{case4Function, map[string][]string{}, false},
		{case4Function, map[string][]string{"delimiter": {"/"}}, false},
	}

	for i, testCase := range testCases {
		result := testCase.function.evaluate(testCase.values)

		if result != testCase.expectedResult {
			t.Fatalf("case %v: expected: %v, got: %v\n", i+1, testCase.expectedResult, result)
		}
	}
}

func TestStringNotEqualsIgnoreCaseFuncEvaluate(t *testing.T) {
	case1Function, err := newStringNotEqualsIgnoreCaseFunc(S3XAmzCopySource.ToKey(), NewValueSet(NewStringValue("mybucket/MYOBJECT")), "")
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case2Function, err := newStringNotEqualsIgnoreCaseFunc(S3LocationConstraint.ToKey(), NewValueSet(NewStringValue("EU-WEST-1"), NewStringValue("AP-southeast-1")), "")
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case3Function, err := newStringNotEqualsIgnoreCaseFunc(JWTGroups.ToKey(), NewValueSet(NewStringValue("Prod"), NewStringValue("Art")), forAllValues)
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case4Function, err := newStringNotEqualsIgnoreCaseFunc(JWTGroups.ToKey(), NewValueSet(NewStringValue("Prod"), NewStringValue("Art")), forAnyValue)
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	testCases := []struct {
		function       Function
		values         map[string][]string
		expectedResult bool
	}{
		{case1Function, map[string][]string{"x-amz-copy-source": {"mybucket/myobject"}}, false},
		{case1Function, map[string][]string{"x-amz-copy-source": {"yourbucket/myobject"}}, true},
		{case1Function, map[string][]string{}, true},
		{case1Function, map[string][]string{"delimiter": {"/"}}, true},

		{case2Function, map[string][]string{"LocationConstraint": {"eu-west-1"}}, false},
		{case2Function, map[string][]string{"LocationConstraint": {"ap-southeast-1"}}, false},
		{case2Function, map[string][]string{"LocationConstraint": {"us-east-1"}}, true},
		{case2Function, map[string][]string{}, true},
		{case2Function, map[string][]string{"delimiter": {"/"}}, true},

		{case3Function, map[string][]string{"groups": {"prod", "art"}}, false},
		{case3Function, map[string][]string{"groups": {"art"}}, true},
		{case3Function, map[string][]string{}, false},
		{case3Function, map[string][]string{"delimiter": {"/"}}, false},

		{case4Function, map[string][]string{"groups": {"prod", "art"}}, false},
		{case4Function, map[string][]string{"groups": {"art"}}, false},
		{case4Function, map[string][]string{}, true},
		{case4Function, map[string][]string{"delimiter": {"/"}}, true},
	}

	for i, testCase := range testCases {
		result := testCase.function.evaluate(testCase.values)

		if result != testCase.expectedResult {
			t.Fatalf("case %v: expected: %v, got: %v\n", i+1, testCase.expectedResult, result)
		}
	}
}

func TestBinaryEqualsFuncEvaluate(t *testing.T) {
	case1Function, err := newBinaryEqualsFunc(
		S3XAmzCopySource.ToKey(),
		NewValueSet(NewStringValue(base64.StdEncoding.EncodeToString([]byte("mybucket/myobject")))),
		"",
	)
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case2Function, err := newBinaryEqualsFunc(
		S3LocationConstraint.ToKey(),
		NewValueSet(
			NewStringValue(base64.StdEncoding.EncodeToString([]byte("eu-west-1"))),
			NewStringValue(base64.StdEncoding.EncodeToString([]byte("ap-southeast-1"))),
		),
		"",
	)
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case3Function, err := newBinaryEqualsFunc(
		JWTGroups.ToKey(),
		NewValueSet(
			NewStringValue(base64.StdEncoding.EncodeToString([]byte("prod"))),
			NewStringValue(base64.StdEncoding.EncodeToString([]byte("art"))),
		),
		forAllValues,
	)
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case4Function, err := newBinaryEqualsFunc(
		JWTGroups.ToKey(),
		NewValueSet(NewStringValue(
			base64.StdEncoding.EncodeToString([]byte("prod"))),
			NewStringValue(base64.StdEncoding.EncodeToString([]byte("art"))),
		),
		forAnyValue,
	)
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	testCases := []struct {
		function       Function
		values         map[string][]string
		expectedResult bool
	}{
		{case1Function, map[string][]string{"x-amz-copy-source": {"mybucket/myobject"}}, true},
		{case1Function, map[string][]string{"x-amz-copy-source": {"yourbucket/myobject"}}, false},
		{case1Function, map[string][]string{}, false},
		{case1Function, map[string][]string{"delimiter": {"/"}}, false},

		{case2Function, map[string][]string{"LocationConstraint": {"eu-west-1"}}, true},
		{case2Function, map[string][]string{"LocationConstraint": {"ap-southeast-1"}}, true},
		{case2Function, map[string][]string{"LocationConstraint": {"us-east-1"}}, false},
		{case2Function, map[string][]string{}, false},
		{case2Function, map[string][]string{"delimiter": {"/"}}, false},

		{case3Function, map[string][]string{"groups": {"prod", "art"}}, true},
		{case3Function, map[string][]string{"groups": {"art"}}, false},
		{case3Function, map[string][]string{}, true},
		{case3Function, map[string][]string{"delimiter": {"/"}}, true},

		{case4Function, map[string][]string{"groups": {"prod", "art"}}, true},
		{case4Function, map[string][]string{"groups": {"art"}}, true},
		{case4Function, map[string][]string{}, false},
		{case4Function, map[string][]string{"delimiter": {"/"}}, false},
	}

	for i, testCase := range testCases {
		result := testCase.function.evaluate(testCase.values)

		if result != testCase.expectedResult {
			t.Fatalf("case %v: expected: %v, got: %v\n", i+1, testCase.expectedResult, result)
		}
	}
}

func TestStringLikeFuncEvaluate(t *testing.T) {
	case1Function, err := newStringLikeFunc(S3XAmzCopySource.ToKey(), NewValueSet(NewStringValue("mybucket/myobject")), "")
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case2Function, err := newStringLikeFunc(S3LocationConstraint.ToKey(), NewValueSet(NewStringValue("eu-west-*"), NewStringValue("ap-southeast-1")), "")
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case3Function, err := newStringLikeFunc(JWTGroups.ToKey(), NewValueSet(NewStringValue("prod"), NewStringValue("art*")), forAllValues)
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case4Function, err := newStringLikeFunc(JWTGroups.ToKey(), NewValueSet(NewStringValue("prod*"), NewStringValue("art")), forAnyValue)
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	testCases := []struct {
		function       Function
		values         map[string][]string
		expectedResult bool
	}{
		{case1Function, map[string][]string{"x-amz-copy-source": {"mybucket/myobject"}}, true},
		{case1Function, map[string][]string{"x-amz-copy-source": {"yourbucket/myobject"}}, false},
		{case1Function, map[string][]string{}, false},
		{case1Function, map[string][]string{"delimiter": {"/"}}, false},

		{case2Function, map[string][]string{"LocationConstraint": {"eu-west-1"}}, true},
		{case2Function, map[string][]string{"LocationConstraint": {"eu-west-2"}}, true},
		{case2Function, map[string][]string{"LocationConstraint": {"ap-southeast-1"}}, true},
		{case2Function, map[string][]string{"LocationConstraint": {"us-east-1"}}, false},
		{case2Function, map[string][]string{}, false},
		{case2Function, map[string][]string{"delimiter": {"/"}}, false},

		{case3Function, map[string][]string{"groups": {"prod", "arts"}}, true},
		{case3Function, map[string][]string{"groups": {"art"}}, false},
		{case3Function, map[string][]string{}, true},
		{case3Function, map[string][]string{"delimiter": {"/"}}, true},

		{case4Function, map[string][]string{"groups": {"prods", "art"}}, true},
		{case4Function, map[string][]string{"groups": {"art"}}, true},
		{case4Function, map[string][]string{}, false},
		{case4Function, map[string][]string{"delimiter": {"/"}}, false},
	}

	for i, testCase := range testCases {
		result := testCase.function.evaluate(testCase.values)

		if result != testCase.expectedResult {
			t.Fatalf("case %v: expected: %v, got: %v\n", i+1, testCase.expectedResult, result)
		}
	}
}

func TestStringNotLikeFuncEvaluate(t *testing.T) {
	case1Function, err := newStringNotLikeFunc(S3XAmzCopySource.ToKey(), NewValueSet(NewStringValue("mybucket/myobject")), "")
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case2Function, err := newStringNotLikeFunc(S3LocationConstraint.ToKey(), NewValueSet(NewStringValue("eu-west-*"), NewStringValue("ap-southeast-1")), "")
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case3Function, err := newStringNotLikeFunc(JWTGroups.ToKey(), NewValueSet(NewStringValue("prod"), NewStringValue("art*")), forAllValues)
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case4Function, err := newStringNotLikeFunc(JWTGroups.ToKey(), NewValueSet(NewStringValue("prod*"), NewStringValue("art")), forAnyValue)
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	testCases := []struct {
		function       Function
		values         map[string][]string
		expectedResult bool
	}{
		{case1Function, map[string][]string{"x-amz-copy-source": {"mybucket/myobject"}}, false},
		{case1Function, map[string][]string{"x-amz-copy-source": {"yourbucket/myobject"}}, true},
		{case1Function, map[string][]string{}, true},
		{case1Function, map[string][]string{"delimiter": {"/"}}, true},

		{case2Function, map[string][]string{"LocationConstraint": {"eu-west-1"}}, false},
		{case2Function, map[string][]string{"LocationConstraint": {"eu-west-2"}}, false},
		{case2Function, map[string][]string{"LocationConstraint": {"ap-southeast-1"}}, false},
		{case2Function, map[string][]string{"LocationConstraint": {"us-east-1"}}, true},
		{case2Function, map[string][]string{}, true},
		{case2Function, map[string][]string{"delimiter": {"/"}}, true},

		{case3Function, map[string][]string{"groups": {"prod", "arts"}}, false},
		{case3Function, map[string][]string{"groups": {"art"}}, true},
		{case3Function, map[string][]string{}, false},
		{case3Function, map[string][]string{"delimiter": {"/"}}, false},

		{case4Function, map[string][]string{"groups": {"prods", "art"}}, false},
		{case4Function, map[string][]string{"groups": {"art"}}, false},
		{case4Function, map[string][]string{}, true},
		{case4Function, map[string][]string{"delimiter": {"/"}}, true},
	}

	for i, testCase := range testCases {
		result := testCase.function.evaluate(testCase.values)

		if result != testCase.expectedResult {
			t.Fatalf("case %v: expected: %v, got: %v\n", i+1, testCase.expectedResult, result)
		}
	}
}

func TestStringFuncKey(t *testing.T) {
	case1Function, err := newStringEqualsFunc(S3XAmzCopySource.ToKey(), NewValueSet(NewStringValue("mybucket/myobject")), "")
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	testCases := []struct {
		function       Function
		expectedResult Key
	}{
		{case1Function, S3XAmzCopySource.ToKey()},
	}

	for i, testCase := range testCases {
		result := testCase.function.key()

		if result != testCase.expectedResult {
			t.Fatalf("case %v: expected: %v, got: %v\n", i+1, testCase.expectedResult, result)
		}
	}
}

func TestStringFuncName(t *testing.T) {
	case1Function, err := newStringEqualsFunc(S3XAmzCopySource.ToKey(), NewValueSet(NewStringValue("mybucket/myobject")), "")
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case2Function, err := newStringNotEqualsFunc(S3XAmzCopySource.ToKey(), NewValueSet(NewStringValue("mybucket/myobject")), "")
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case3Function, err := newStringEqualsIgnoreCaseFunc(S3XAmzCopySource.ToKey(), NewValueSet(NewStringValue("mybucket/MYOBJECT")), "")
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case4Function, err := newStringNotEqualsIgnoreCaseFunc(S3XAmzCopySource.ToKey(), NewValueSet(NewStringValue("mybucket/MYOBJECT")), "")
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case5Function, err := newBinaryEqualsFunc(
		S3XAmzCopySource.ToKey(),
		NewValueSet(NewStringValue(base64.StdEncoding.EncodeToString([]byte("mybucket/myobject")))),
		"",
	)
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case6Function, err := newStringLikeFunc(S3XAmzCopySource.ToKey(), NewValueSet(NewStringValue("mybucket/myobject")), "")
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case7Function, err := newStringNotLikeFunc(S3XAmzCopySource.ToKey(), NewValueSet(NewStringValue("mybucket/myobject")), "")
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case8Function, err := newStringLikeFunc(S3XAmzCopySource.ToKey(), NewValueSet(NewStringValue("mybucket/myobject")), forAllValues)
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case9Function, err := newStringNotLikeFunc(S3XAmzCopySource.ToKey(), NewValueSet(NewStringValue("mybucket/myobject")), forAnyValue)
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	testCases := []struct {
		function       Function
		expectedResult name
	}{
		{case1Function, name{name: stringEquals}},
		{case2Function, name{name: stringNotEquals}},
		{case3Function, name{name: stringEqualsIgnoreCase}},
		{case4Function, name{name: stringNotEqualsIgnoreCase}},
		{case5Function, name{name: binaryEquals}},
		{case6Function, name{name: stringLike}},
		{case7Function, name{name: stringNotLike}},
		{case8Function, name{qualifier: forAllValues, name: stringLike}},
		{case9Function, name{qualifier: forAnyValue, name: stringNotLike}},
	}

	for i, testCase := range testCases {
		result := testCase.function.name()

		if result != testCase.expectedResult {
			t.Fatalf("case %v: expected: %v, got: %v\n", i+1, testCase.expectedResult, result)
		}
	}
}

func TestStringEqualsFuncToMap(t *testing.T) {
	case1Function, err := newStringEqualsFunc(S3XAmzCopySource.ToKey(), NewValueSet(NewStringValue("mybucket/myobject")), "")
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case1Result := map[Key]ValueSet{
		S3XAmzCopySource.ToKey(): NewValueSet(NewStringValue("mybucket/myobject")),
	}

	case2Function, err := newStringEqualsFunc(S3XAmzCopySource.ToKey(),
		NewValueSet(
			NewStringValue("mybucket/myobject"),
			NewStringValue("yourbucket/myobject"),
		),
		"",
	)
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case2Result := map[Key]ValueSet{
		S3XAmzCopySource.ToKey(): NewValueSet(
			NewStringValue("mybucket/myobject"),
			NewStringValue("yourbucket/myobject"),
		),
	}

	case3Function, err := newStringEqualsIgnoreCaseFunc(S3XAmzCopySource.ToKey(), NewValueSet(NewStringValue("mybucket/MYOBJECT")), "")
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case3Result := map[Key]ValueSet{
		S3XAmzCopySource.ToKey(): NewValueSet(NewStringValue("mybucket/MYOBJECT")),
	}

	case4Function, err := newBinaryEqualsFunc(
		S3XAmzCopySource.ToKey(),
		NewValueSet(NewStringValue(base64.StdEncoding.EncodeToString([]byte("mybucket/myobject")))),
		"",
	)
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case4Result := map[Key]ValueSet{
		S3XAmzCopySource.ToKey(): NewValueSet(NewStringValue(base64.StdEncoding.EncodeToString([]byte("mybucket/myobject")))),
	}

	testCases := []struct {
		f              Function
		expectedResult map[Key]ValueSet
	}{
		{case1Function, case1Result},
		{case2Function, case2Result},
		{case3Function, case3Result},
		{case4Function, case4Result},
		{&stringFunc{}, nil},
	}

	for i, testCase := range testCases {
		result := testCase.f.toMap()

		if !reflect.DeepEqual(result, testCase.expectedResult) {
			t.Fatalf("case %v: result: expected: %v, got: %v\n", i+1, testCase.expectedResult, result)
		}
	}
}

func TestStringFuncClone(t *testing.T) {
	case1Function, err := newStringEqualsFunc(S3XAmzCopySource.ToKey(), NewValueSet(NewStringValue("mybucket/myobject")), "")
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case1Result := &stringFunc{
		n:          name{name: stringEquals},
		k:          S3XAmzCopySource.ToKey(),
		values:     set.CreateStringSet("mybucket/myobject"),
		ignoreCase: false,
		base64:     false,
		negate:     false,
	}

	case2Function, err := newStringNotEqualsFunc(S3XAmzCopySource.ToKey(), NewValueSet(NewStringValue("mybucket/myobject")), "")
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case2Result := &stringFunc{
		n:          name{name: stringNotEquals},
		k:          S3XAmzCopySource.ToKey(),
		values:     set.CreateStringSet("mybucket/myobject"),
		ignoreCase: false,
		base64:     false,
		negate:     true,
	}

	case3Function, err := newStringEqualsIgnoreCaseFunc(S3XAmzCopySource.ToKey(), NewValueSet(NewStringValue("mybucket/MYOBJECT")), "")
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case3Result := &stringFunc{
		n:          name{name: stringEqualsIgnoreCase},
		k:          S3XAmzCopySource.ToKey(),
		values:     set.CreateStringSet("mybucket/MYOBJECT"),
		ignoreCase: true,
		base64:     false,
		negate:     false,
	}

	case4Function, err := newStringNotEqualsIgnoreCaseFunc(S3XAmzCopySource.ToKey(), NewValueSet(NewStringValue("mybucket/MYOBJECT")), "")
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case4Result := &stringFunc{
		n:          name{name: stringNotEqualsIgnoreCase},
		k:          S3XAmzCopySource.ToKey(),
		values:     set.CreateStringSet("mybucket/MYOBJECT"),
		ignoreCase: true,
		base64:     false,
		negate:     true,
	}

	case5Function, err := newBinaryEqualsFunc(
		S3XAmzCopySource.ToKey(),
		NewValueSet(NewStringValue(base64.StdEncoding.EncodeToString([]byte("mybucket/myobject")))),
		"",
	)
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case5Result := &stringFunc{
		n:          name{name: binaryEquals},
		k:          S3XAmzCopySource.ToKey(),
		values:     set.CreateStringSet("mybucket/myobject"),
		ignoreCase: false,
		base64:     true,
		negate:     false,
	}

	case6Function, err := newStringLikeFunc(S3XAmzCopySource.ToKey(), NewValueSet(NewStringValue("mybucket/myobject")), "")
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case6Result := &stringLikeFunc{stringFunc{
		n:          name{name: stringLike},
		k:          S3XAmzCopySource.ToKey(),
		values:     set.CreateStringSet("mybucket/myobject"),
		ignoreCase: false,
		base64:     false,
		negate:     false,
	}}

	case7Function, err := newStringNotLikeFunc(S3XAmzCopySource.ToKey(), NewValueSet(NewStringValue("mybucket/myobject")), "")
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case7Result := &stringLikeFunc{stringFunc{
		n:          name{name: stringNotLike},
		k:          S3XAmzCopySource.ToKey(),
		values:     set.CreateStringSet("mybucket/myobject"),
		ignoreCase: false,
		base64:     false,
		negate:     true,
	}}

	case8Function, err := newStringLikeFunc(S3XAmzCopySource.ToKey(), NewValueSet(NewStringValue("mybucket/myobject")), forAllValues)
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case8Result := &stringLikeFunc{stringFunc{
		n:          name{qualifier: forAllValues, name: stringLike},
		k:          S3XAmzCopySource.ToKey(),
		values:     set.CreateStringSet("mybucket/myobject"),
		ignoreCase: false,
		base64:     false,
		negate:     false,
	}}

	case9Function, err := newStringNotLikeFunc(S3XAmzCopySource.ToKey(), NewValueSet(NewStringValue("mybucket/myobject")), forAnyValue)
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case9Result := &stringLikeFunc{stringFunc{
		n:          name{qualifier: forAnyValue, name: stringNotLike},
		k:          S3XAmzCopySource.ToKey(),
		values:     set.CreateStringSet("mybucket/myobject"),
		ignoreCase: false,
		base64:     false,
		negate:     true,
	}}

	testCases := []struct {
		function       Function
		expectedResult Function
	}{
		{case1Function, case1Result},
		{case2Function, case2Result},
		{case3Function, case3Result},
		{case4Function, case4Result},
		{case5Function, case5Result},
		{case6Function, case6Result},
		{case7Function, case7Result},
		{case8Function, case8Result},
		{case9Function, case9Result},
	}

	for i, testCase := range testCases {
		result := testCase.function.clone()

		if !reflect.DeepEqual(result, testCase.expectedResult) {
			t.Fatalf("case %v: expected: %v, got: %v\n", i+1, testCase.expectedResult, result)
		}
	}
}

func TestNewStringFuncError(t *testing.T) {
	testCases := []struct {
		key       Key
		values    ValueSet
		qualifier string
	}{
		{S3XAmzCopySource.ToKey(), NewValueSet(NewStringValue("mybucket/myobject"), NewIntValue(7)), ""},
		{S3XAmzCopySource.ToKey(), NewValueSet(NewStringValue("mybucket")), ""},
		{S3XAmzServerSideEncryption.ToKey(), NewValueSet(NewStringValue("SSE-C")), ""},
		{S3XAmzServerSideEncryptionCustomerAlgorithm.ToKey(), NewValueSet(NewStringValue("SSE-C")), ""},
		{S3XAmzMetadataDirective.ToKey(), NewValueSet(NewStringValue("DUPLICATE")), ""},
		{S3XAmzContentSha256.ToKey(), NewValueSet(NewStringValue("")), ""},
		{S3XAmzCopySource.ToKey(), NewValueSet(NewStringValue("mybucket/myobject")), "For_All_Values"},
	}

	for i, testCase := range testCases {
		if _, err := newStringEqualsFunc(testCase.key, testCase.values, testCase.qualifier); err == nil {
			t.Errorf("case %v: error expected", i+1)
		}
	}

	if _, err := newBinaryEqualsFunc(S3XAmzCopySource.ToKey(), NewValueSet(NewStringValue("mybucket/myobject")), ""); err == nil {
		t.Errorf("error expected")
	}

	if _, err := newStringLikeFunc(S3XAmzCopySource.ToKey(), NewValueSet(NewStringValue("mybucket")), ""); err == nil {
		t.Errorf("error expected")
	}
}

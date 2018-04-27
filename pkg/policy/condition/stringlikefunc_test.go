/*
 * Minio Cloud Storage, (C) 2018 Minio, Inc.
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

package condition

import (
	"reflect"
	"testing"
)

func TestStringLikeFuncEvaluate(t *testing.T) {
	case1Function, err := newStringLikeFunc(S3XAmzCopySource, NewValueSet(NewStringValue("mybucket/myobject*")))
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case2Function, err := newStringLikeFunc(S3XAmzCopySource, NewValueSet(NewStringValue("mybucket/myobject")))
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case3Function, err := newStringLikeFunc(S3XAmzServerSideEncryption, NewValueSet(NewStringValue("AES*")))
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case4Function, err := newStringLikeFunc(S3XAmzServerSideEncryption, NewValueSet(NewStringValue("AES256")))
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case5Function, err := newStringLikeFunc(S3XAmzMetadataDirective, NewValueSet(NewStringValue("REPL*")))
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case6Function, err := newStringLikeFunc(S3XAmzMetadataDirective, NewValueSet(NewStringValue("REPLACE")))
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case7Function, err := newStringLikeFunc(S3LocationConstraint, NewValueSet(NewStringValue("eu-west-*")))
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case8Function, err := newStringLikeFunc(S3LocationConstraint, NewValueSet(NewStringValue("eu-west-1")))
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	testCases := []struct {
		function       Function
		values         map[string][]string
		expectedResult bool
	}{
		{case1Function, map[string][]string{"x-amz-copy-source": {"mybucket/myobject"}}, true},
		{case1Function, map[string][]string{"x-amz-copy-source": {"mybucket/myobject.png"}}, true},
		{case1Function, map[string][]string{"x-amz-copy-source": {"yourbucket/myobject"}}, false},
		{case1Function, map[string][]string{}, false},
		{case1Function, map[string][]string{"delimiter": {"/"}}, false},

		{case2Function, map[string][]string{"x-amz-copy-source": {"mybucket/myobject"}}, true},
		{case2Function, map[string][]string{"x-amz-copy-source": {"mybucket/myobject.png"}}, false},
		{case2Function, map[string][]string{"x-amz-copy-source": {"yourbucket/myobject"}}, false},
		{case2Function, map[string][]string{}, false},
		{case2Function, map[string][]string{"delimiter": {"/"}}, false},

		{case3Function, map[string][]string{"x-amz-server-side-encryption": {"AES256"}}, true},
		{case3Function, map[string][]string{"x-amz-server-side-encryption": {"AES512"}}, true},
		{case3Function, map[string][]string{"x-amz-server-side-encryption": {"aws:kms"}}, false},
		{case3Function, map[string][]string{}, false},
		{case3Function, map[string][]string{"delimiter": {"/"}}, false},

		{case4Function, map[string][]string{"x-amz-server-side-encryption": {"AES256"}}, true},
		{case4Function, map[string][]string{"x-amz-server-side-encryption": {"AES512"}}, false},
		{case4Function, map[string][]string{"x-amz-server-side-encryption": {"aws:kms"}}, false},
		{case4Function, map[string][]string{}, false},
		{case4Function, map[string][]string{"delimiter": {"/"}}, false},

		{case5Function, map[string][]string{"x-amz-metadata-directive": {"REPLACE"}}, true},
		{case5Function, map[string][]string{"x-amz-metadata-directive": {"REPLACE/COPY"}}, true},
		{case5Function, map[string][]string{"x-amz-metadata-directive": {"COPY"}}, false},
		{case5Function, map[string][]string{}, false},
		{case5Function, map[string][]string{"delimiter": {"/"}}, false},

		{case6Function, map[string][]string{"x-amz-metadata-directive": {"REPLACE"}}, true},
		{case6Function, map[string][]string{"x-amz-metadata-directive": {"REPLACE/COPY"}}, false},
		{case6Function, map[string][]string{"x-amz-metadata-directive": {"COPY"}}, false},
		{case6Function, map[string][]string{}, false},
		{case6Function, map[string][]string{"delimiter": {"/"}}, false},

		{case7Function, map[string][]string{"LocationConstraint": {"eu-west-1"}}, true},
		{case7Function, map[string][]string{"LocationConstraint": {"eu-west-2"}}, true},
		{case7Function, map[string][]string{"LocationConstraint": {"us-east-1"}}, false},
		{case7Function, map[string][]string{}, false},
		{case7Function, map[string][]string{"delimiter": {"/"}}, false},

		{case8Function, map[string][]string{"LocationConstraint": {"eu-west-1"}}, true},
		{case8Function, map[string][]string{"LocationConstraint": {"eu-west-2"}}, false},
		{case8Function, map[string][]string{"LocationConstraint": {"us-east-1"}}, false},
		{case8Function, map[string][]string{}, false},
		{case8Function, map[string][]string{"delimiter": {"/"}}, false},
	}

	for i, testCase := range testCases {
		result := testCase.function.evaluate(testCase.values)

		if result != testCase.expectedResult {
			t.Fatalf("case %v: expected: %v, got: %v\n", i+1, testCase.expectedResult, result)
		}
	}
}

func TestStringLikeFuncKey(t *testing.T) {
	case1Function, err := newStringLikeFunc(S3XAmzCopySource, NewValueSet(NewStringValue("mybucket/myobject")))
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case2Function, err := newStringLikeFunc(S3XAmzServerSideEncryption, NewValueSet(NewStringValue("AES256")))
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case3Function, err := newStringLikeFunc(S3XAmzMetadataDirective, NewValueSet(NewStringValue("REPLACE")))
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case4Function, err := newStringLikeFunc(S3LocationConstraint, NewValueSet(NewStringValue("eu-west-1")))
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	testCases := []struct {
		function       Function
		expectedResult Key
	}{
		{case1Function, S3XAmzCopySource},
		{case2Function, S3XAmzServerSideEncryption},
		{case3Function, S3XAmzMetadataDirective},
		{case4Function, S3LocationConstraint},
	}

	for i, testCase := range testCases {
		result := testCase.function.key()

		if result != testCase.expectedResult {
			t.Fatalf("case %v: expected: %v, got: %v\n", i+1, testCase.expectedResult, result)
		}
	}
}

func TestStringLikeFuncToMap(t *testing.T) {
	case1Function, err := newStringLikeFunc(S3XAmzCopySource, NewValueSet(NewStringValue("mybucket/*")))
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case1Result := map[Key]ValueSet{
		S3XAmzCopySource: NewValueSet(NewStringValue("mybucket/*")),
	}

	case2Function, err := newStringLikeFunc(S3XAmzCopySource,
		NewValueSet(
			NewStringValue("mybucket/*"),
			NewStringValue("yourbucket/myobject*"),
		),
	)
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case2Result := map[Key]ValueSet{
		S3XAmzCopySource: NewValueSet(
			NewStringValue("mybucket/*"),
			NewStringValue("yourbucket/myobject*"),
		),
	}

	case3Function, err := newStringLikeFunc(S3XAmzServerSideEncryption, NewValueSet(NewStringValue("AES*")))
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case3Result := map[Key]ValueSet{
		S3XAmzServerSideEncryption: NewValueSet(NewStringValue("AES*")),
	}

	case4Function, err := newStringLikeFunc(S3XAmzServerSideEncryption,
		NewValueSet(
			NewStringValue("AES*"),
			NewStringValue("aws:*"),
		),
	)
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case4Result := map[Key]ValueSet{
		S3XAmzServerSideEncryption: NewValueSet(
			NewStringValue("AES*"),
			NewStringValue("aws:*"),
		),
	}

	case5Function, err := newStringLikeFunc(S3XAmzMetadataDirective, NewValueSet(NewStringValue("REPL*")))
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case5Result := map[Key]ValueSet{
		S3XAmzMetadataDirective: NewValueSet(NewStringValue("REPL*")),
	}

	case6Function, err := newStringLikeFunc(S3XAmzMetadataDirective,
		NewValueSet(
			NewStringValue("REPL*"),
			NewStringValue("COPY*"),
		),
	)
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case6Result := map[Key]ValueSet{
		S3XAmzMetadataDirective: NewValueSet(
			NewStringValue("REPL*"),
			NewStringValue("COPY*"),
		),
	}

	case7Function, err := newStringLikeFunc(S3LocationConstraint, NewValueSet(NewStringValue("eu-west-*")))
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case7Result := map[Key]ValueSet{
		S3LocationConstraint: NewValueSet(NewStringValue("eu-west-*")),
	}

	case8Function, err := newStringLikeFunc(S3LocationConstraint,
		NewValueSet(
			NewStringValue("eu-west-*"),
			NewStringValue("us-west-*"),
		),
	)
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case8Result := map[Key]ValueSet{
		S3LocationConstraint: NewValueSet(
			NewStringValue("eu-west-*"),
			NewStringValue("us-west-*"),
		),
	}

	testCases := []struct {
		f              Function
		expectedResult map[Key]ValueSet
	}{
		{case1Function, case1Result},
		{case2Function, case2Result},
		{case3Function, case3Result},
		{case4Function, case4Result},
		{case5Function, case5Result},
		{case6Function, case6Result},
		{case7Function, case7Result},
		{case8Function, case8Result},
		{&stringLikeFunc{}, nil},
	}

	for i, testCase := range testCases {
		result := testCase.f.toMap()

		if !reflect.DeepEqual(result, testCase.expectedResult) {
			t.Fatalf("case %v: result: expected: %v, got: %v\n", i+1, testCase.expectedResult, result)
		}
	}
}

func TestStringNotLikeFuncEvaluate(t *testing.T) {
	case1Function, err := newStringNotLikeFunc(S3XAmzCopySource, NewValueSet(NewStringValue("mybucket/myobject*")))
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case2Function, err := newStringNotLikeFunc(S3XAmzCopySource, NewValueSet(NewStringValue("mybucket/myobject")))
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case3Function, err := newStringNotLikeFunc(S3XAmzServerSideEncryption, NewValueSet(NewStringValue("AES*")))
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case4Function, err := newStringNotLikeFunc(S3XAmzServerSideEncryption, NewValueSet(NewStringValue("AES256")))
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case5Function, err := newStringNotLikeFunc(S3XAmzMetadataDirective, NewValueSet(NewStringValue("REPL*")))
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case6Function, err := newStringNotLikeFunc(S3XAmzMetadataDirective, NewValueSet(NewStringValue("REPLACE")))
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case7Function, err := newStringNotLikeFunc(S3LocationConstraint, NewValueSet(NewStringValue("eu-west-*")))
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case8Function, err := newStringNotLikeFunc(S3LocationConstraint, NewValueSet(NewStringValue("eu-west-1")))
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	testCases := []struct {
		function       Function
		values         map[string][]string
		expectedResult bool
	}{
		{case1Function, map[string][]string{"x-amz-copy-source": {"mybucket/myobject"}}, false},
		{case1Function, map[string][]string{"x-amz-copy-source": {"mybucket/myobject.png"}}, false},
		{case1Function, map[string][]string{"x-amz-copy-source": {"yourbucket/myobject"}}, true},
		{case1Function, map[string][]string{}, true},
		{case1Function, map[string][]string{"delimiter": {"/"}}, true},

		{case2Function, map[string][]string{"x-amz-copy-source": {"mybucket/myobject"}}, false},
		{case2Function, map[string][]string{"x-amz-copy-source": {"mybucket/myobject.png"}}, true},
		{case2Function, map[string][]string{"x-amz-copy-source": {"yourbucket/myobject"}}, true},
		{case2Function, map[string][]string{}, true},
		{case2Function, map[string][]string{"delimiter": {"/"}}, true},

		{case3Function, map[string][]string{"x-amz-server-side-encryption": {"AES256"}}, false},
		{case3Function, map[string][]string{"x-amz-server-side-encryption": {"AES512"}}, false},
		{case3Function, map[string][]string{"x-amz-server-side-encryption": {"aws:kms"}}, true},
		{case3Function, map[string][]string{}, true},
		{case3Function, map[string][]string{"delimiter": {"/"}}, true},

		{case4Function, map[string][]string{"x-amz-server-side-encryption": {"AES256"}}, false},
		{case4Function, map[string][]string{"x-amz-server-side-encryption": {"AES512"}}, true},
		{case4Function, map[string][]string{"x-amz-server-side-encryption": {"aws:kms"}}, true},
		{case4Function, map[string][]string{}, true},
		{case4Function, map[string][]string{"delimiter": {"/"}}, true},

		{case5Function, map[string][]string{"x-amz-metadata-directive": {"REPLACE"}}, false},
		{case5Function, map[string][]string{"x-amz-metadata-directive": {"REPLACE/COPY"}}, false},
		{case5Function, map[string][]string{"x-amz-metadata-directive": {"COPY"}}, true},
		{case5Function, map[string][]string{}, true},
		{case5Function, map[string][]string{"delimiter": {"/"}}, true},

		{case6Function, map[string][]string{"x-amz-metadata-directive": {"REPLACE"}}, false},
		{case6Function, map[string][]string{"x-amz-metadata-directive": {"REPLACE/COPY"}}, true},
		{case6Function, map[string][]string{"x-amz-metadata-directive": {"COPY"}}, true},
		{case6Function, map[string][]string{}, true},
		{case6Function, map[string][]string{"delimiter": {"/"}}, true},

		{case7Function, map[string][]string{"LocationConstraint": {"eu-west-1"}}, false},
		{case7Function, map[string][]string{"LocationConstraint": {"eu-west-2"}}, false},
		{case7Function, map[string][]string{"LocationConstraint": {"us-east-1"}}, true},
		{case7Function, map[string][]string{}, true},
		{case7Function, map[string][]string{"delimiter": {"/"}}, true},

		{case8Function, map[string][]string{"LocationConstraint": {"eu-west-1"}}, false},
		{case8Function, map[string][]string{"LocationConstraint": {"eu-west-2"}}, true},
		{case8Function, map[string][]string{"LocationConstraint": {"us-east-1"}}, true},
		{case8Function, map[string][]string{}, true},
		{case8Function, map[string][]string{"delimiter": {"/"}}, true},
	}

	for i, testCase := range testCases {
		result := testCase.function.evaluate(testCase.values)

		if result != testCase.expectedResult {
			t.Fatalf("case %v: expected: %v, got: %v\n", i+1, testCase.expectedResult, result)
		}
	}
}

func TestStringNotLikeFuncKey(t *testing.T) {
	case1Function, err := newStringNotLikeFunc(S3XAmzCopySource, NewValueSet(NewStringValue("mybucket/myobject")))
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case2Function, err := newStringNotLikeFunc(S3XAmzServerSideEncryption, NewValueSet(NewStringValue("AES256")))
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case3Function, err := newStringNotLikeFunc(S3XAmzMetadataDirective, NewValueSet(NewStringValue("REPLACE")))
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case4Function, err := newStringNotLikeFunc(S3LocationConstraint, NewValueSet(NewStringValue("eu-west-1")))
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	testCases := []struct {
		function       Function
		expectedResult Key
	}{
		{case1Function, S3XAmzCopySource},
		{case2Function, S3XAmzServerSideEncryption},
		{case3Function, S3XAmzMetadataDirective},
		{case4Function, S3LocationConstraint},
	}

	for i, testCase := range testCases {
		result := testCase.function.key()

		if result != testCase.expectedResult {
			t.Fatalf("case %v: expected: %v, got: %v\n", i+1, testCase.expectedResult, result)
		}
	}
}

func TestStringNotLikeFuncToMap(t *testing.T) {
	case1Function, err := newStringNotLikeFunc(S3XAmzCopySource, NewValueSet(NewStringValue("mybucket/*")))
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case1Result := map[Key]ValueSet{
		S3XAmzCopySource: NewValueSet(NewStringValue("mybucket/*")),
	}

	case2Function, err := newStringNotLikeFunc(S3XAmzCopySource,
		NewValueSet(
			NewStringValue("mybucket/*"),
			NewStringValue("yourbucket/myobject*"),
		),
	)
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case2Result := map[Key]ValueSet{
		S3XAmzCopySource: NewValueSet(
			NewStringValue("mybucket/*"),
			NewStringValue("yourbucket/myobject*"),
		),
	}

	case3Function, err := newStringNotLikeFunc(S3XAmzServerSideEncryption, NewValueSet(NewStringValue("AES*")))
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case3Result := map[Key]ValueSet{
		S3XAmzServerSideEncryption: NewValueSet(NewStringValue("AES*")),
	}

	case4Function, err := newStringNotLikeFunc(S3XAmzServerSideEncryption,
		NewValueSet(
			NewStringValue("AES*"),
			NewStringValue("aws:*"),
		),
	)
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case4Result := map[Key]ValueSet{
		S3XAmzServerSideEncryption: NewValueSet(
			NewStringValue("AES*"),
			NewStringValue("aws:*"),
		),
	}

	case5Function, err := newStringNotLikeFunc(S3XAmzMetadataDirective, NewValueSet(NewStringValue("REPL*")))
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case5Result := map[Key]ValueSet{
		S3XAmzMetadataDirective: NewValueSet(NewStringValue("REPL*")),
	}

	case6Function, err := newStringNotLikeFunc(S3XAmzMetadataDirective,
		NewValueSet(
			NewStringValue("REPL*"),
			NewStringValue("COPY*"),
		),
	)
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case6Result := map[Key]ValueSet{
		S3XAmzMetadataDirective: NewValueSet(
			NewStringValue("REPL*"),
			NewStringValue("COPY*"),
		),
	}

	case7Function, err := newStringNotLikeFunc(S3LocationConstraint, NewValueSet(NewStringValue("eu-west-*")))
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case7Result := map[Key]ValueSet{
		S3LocationConstraint: NewValueSet(NewStringValue("eu-west-*")),
	}

	case8Function, err := newStringNotLikeFunc(S3LocationConstraint,
		NewValueSet(
			NewStringValue("eu-west-*"),
			NewStringValue("us-west-*"),
		),
	)
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case8Result := map[Key]ValueSet{
		S3LocationConstraint: NewValueSet(
			NewStringValue("eu-west-*"),
			NewStringValue("us-west-*"),
		),
	}

	testCases := []struct {
		f              Function
		expectedResult map[Key]ValueSet
	}{
		{case1Function, case1Result},
		{case2Function, case2Result},
		{case3Function, case3Result},
		{case4Function, case4Result},
		{case5Function, case5Result},
		{case6Function, case6Result},
		{case7Function, case7Result},
		{case8Function, case8Result},
		{&stringNotLikeFunc{}, nil},
	}

	for i, testCase := range testCases {
		result := testCase.f.toMap()

		if !reflect.DeepEqual(result, testCase.expectedResult) {
			t.Fatalf("case %v: result: expected: %v, got: %v\n", i+1, testCase.expectedResult, result)
		}
	}
}

func TestNewStringLikeFunc(t *testing.T) {
	case1Function, err := newStringLikeFunc(S3XAmzCopySource, NewValueSet(NewStringValue("mybucket/*")))
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case2Function, err := newStringLikeFunc(S3XAmzCopySource,
		NewValueSet(
			NewStringValue("mybucket/*"),
			NewStringValue("yourbucket/myobject*"),
		),
	)
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case3Function, err := newStringLikeFunc(S3XAmzServerSideEncryption, NewValueSet(NewStringValue("AES*")))
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case4Function, err := newStringLikeFunc(S3XAmzServerSideEncryption,
		NewValueSet(
			NewStringValue("AES*"),
			NewStringValue("aws:*"),
		),
	)
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case5Function, err := newStringLikeFunc(S3XAmzMetadataDirective, NewValueSet(NewStringValue("REPL*")))
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case6Function, err := newStringLikeFunc(S3XAmzMetadataDirective,
		NewValueSet(
			NewStringValue("REPL*"),
			NewStringValue("COPY*"),
		),
	)
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case7Function, err := newStringLikeFunc(S3LocationConstraint, NewValueSet(NewStringValue("eu-west-*")))
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case8Function, err := newStringLikeFunc(S3LocationConstraint,
		NewValueSet(
			NewStringValue("eu-west-*"),
			NewStringValue("us-west-*"),
		),
	)
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	testCases := []struct {
		key            Key
		values         ValueSet
		expectedResult Function
		expectErr      bool
	}{
		{S3XAmzCopySource, NewValueSet(NewStringValue("mybucket/*")), case1Function, false},
		{S3XAmzCopySource,
			NewValueSet(
				NewStringValue("mybucket/*"),
				NewStringValue("yourbucket/myobject*"),
			), case2Function, false},

		{S3XAmzServerSideEncryption, NewValueSet(NewStringValue("AES*")), case3Function, false},
		{S3XAmzServerSideEncryption,
			NewValueSet(
				NewStringValue("AES*"),
				NewStringValue("aws:*"),
			), case4Function, false},

		{S3XAmzMetadataDirective, NewValueSet(NewStringValue("REPL*")), case5Function, false},
		{S3XAmzMetadataDirective,
			NewValueSet(
				NewStringValue("REPL*"),
				NewStringValue("COPY*"),
			), case6Function, false},

		{S3LocationConstraint, NewValueSet(NewStringValue("eu-west-*")), case7Function, false},
		{S3LocationConstraint,
			NewValueSet(
				NewStringValue("eu-west-*"),
				NewStringValue("us-west-*"),
			), case8Function, false},

		// Unsupported value error.
		{S3XAmzCopySource, NewValueSet(NewStringValue("mybucket/myobject"), NewIntValue(7)), nil, true},
		{S3XAmzServerSideEncryption, NewValueSet(NewStringValue("AES256"), NewIntValue(7)), nil, true},
		{S3XAmzMetadataDirective, NewValueSet(NewStringValue("REPLACE"), NewIntValue(7)), nil, true},
		{S3LocationConstraint, NewValueSet(NewStringValue("eu-west-1"), NewIntValue(7)), nil, true},

		// Invalid value error.
		{S3XAmzCopySource, NewValueSet(NewStringValue("mybucket")), nil, true},
	}

	for i, testCase := range testCases {
		result, err := newStringLikeFunc(testCase.key, testCase.values)
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			t.Fatalf("case %v: error: expected: %v, got: %v\n", i+1, testCase.expectErr, expectErr)
		}

		if !testCase.expectErr {
			if !reflect.DeepEqual(result, testCase.expectedResult) {
				t.Fatalf("case %v: result: expected: %v, got: %v\n", i+1, testCase.expectedResult, result)
			}
		}
	}
}

func TestNewStringNotLikeFunc(t *testing.T) {
	case1Function, err := newStringNotLikeFunc(S3XAmzCopySource, NewValueSet(NewStringValue("mybucket/*")))
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case2Function, err := newStringNotLikeFunc(S3XAmzCopySource,
		NewValueSet(
			NewStringValue("mybucket/*"),
			NewStringValue("yourbucket/myobject*"),
		),
	)
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case3Function, err := newStringNotLikeFunc(S3XAmzServerSideEncryption, NewValueSet(NewStringValue("AES*")))
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case4Function, err := newStringNotLikeFunc(S3XAmzServerSideEncryption,
		NewValueSet(
			NewStringValue("AES*"),
			NewStringValue("aws:*"),
		),
	)
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case5Function, err := newStringNotLikeFunc(S3XAmzMetadataDirective, NewValueSet(NewStringValue("REPL*")))
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case6Function, err := newStringNotLikeFunc(S3XAmzMetadataDirective,
		NewValueSet(
			NewStringValue("REPL*"),
			NewStringValue("COPY*"),
		),
	)
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case7Function, err := newStringNotLikeFunc(S3LocationConstraint, NewValueSet(NewStringValue("eu-west-*")))
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case8Function, err := newStringNotLikeFunc(S3LocationConstraint,
		NewValueSet(
			NewStringValue("eu-west-*"),
			NewStringValue("us-west-*"),
		),
	)
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	testCases := []struct {
		key            Key
		values         ValueSet
		expectedResult Function
		expectErr      bool
	}{
		{S3XAmzCopySource, NewValueSet(NewStringValue("mybucket/*")), case1Function, false},
		{S3XAmzCopySource,
			NewValueSet(
				NewStringValue("mybucket/*"),
				NewStringValue("yourbucket/myobject*"),
			), case2Function, false},

		{S3XAmzServerSideEncryption, NewValueSet(NewStringValue("AES*")), case3Function, false},
		{S3XAmzServerSideEncryption,
			NewValueSet(
				NewStringValue("AES*"),
				NewStringValue("aws:*"),
			), case4Function, false},

		{S3XAmzMetadataDirective, NewValueSet(NewStringValue("REPL*")), case5Function, false},
		{S3XAmzMetadataDirective,
			NewValueSet(
				NewStringValue("REPL*"),
				NewStringValue("COPY*"),
			), case6Function, false},

		{S3LocationConstraint, NewValueSet(NewStringValue("eu-west-*")), case7Function, false},
		{S3LocationConstraint,
			NewValueSet(
				NewStringValue("eu-west-*"),
				NewStringValue("us-west-*"),
			), case8Function, false},

		// Unsupported value error.
		{S3XAmzCopySource, NewValueSet(NewStringValue("mybucket/myobject"), NewIntValue(7)), nil, true},
		{S3XAmzServerSideEncryption, NewValueSet(NewStringValue("AES256"), NewIntValue(7)), nil, true},
		{S3XAmzMetadataDirective, NewValueSet(NewStringValue("REPLACE"), NewIntValue(7)), nil, true},
		{S3LocationConstraint, NewValueSet(NewStringValue("eu-west-1"), NewIntValue(7)), nil, true},

		// Invalid value error.
		{S3XAmzCopySource, NewValueSet(NewStringValue("mybucket")), nil, true},
	}

	for i, testCase := range testCases {
		result, err := newStringNotLikeFunc(testCase.key, testCase.values)
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			t.Fatalf("case %v: error: expected: %v, got: %v\n", i+1, testCase.expectErr, expectErr)
		}

		if !testCase.expectErr {
			if !reflect.DeepEqual(result, testCase.expectedResult) {
				t.Fatalf("case %v: result: expected: %v, got: %v\n", i+1, testCase.expectedResult, result)
			}
		}
	}
}

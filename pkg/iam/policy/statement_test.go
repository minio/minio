/*
 * MinIO Cloud Storage, (C) 2018 MinIO, Inc.
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

package iampolicy

import (
	"encoding/json"
	"net"
	"reflect"
	"testing"

	"github.com/minio/minio/pkg/bucket/policy"
	"github.com/minio/minio/pkg/bucket/policy/condition"
)

func TestStatementIsAllowed(t *testing.T) {
	case1Statement := NewStatement(
		policy.Allow,
		NewActionSet(GetBucketLocationAction, PutObjectAction),
		NewResourceSet(NewResource("*", "")),
		condition.NewFunctions(),
	)

	case2Statement := NewStatement(
		policy.Allow,
		NewActionSet(GetObjectAction, PutObjectAction),
		NewResourceSet(NewResource("mybucket", "/myobject*")),
		condition.NewFunctions(),
	)

	_, IPNet1, err := net.ParseCIDR("192.168.1.0/24")
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}
	func1, err := condition.NewIPAddressFunc(
		condition.AWSSourceIP,
		IPNet1,
	)
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case3Statement := NewStatement(
		policy.Allow,
		NewActionSet(GetObjectAction, PutObjectAction),
		NewResourceSet(NewResource("mybucket", "/myobject*")),
		condition.NewFunctions(func1),
	)

	case4Statement := NewStatement(
		policy.Deny,
		NewActionSet(GetObjectAction, PutObjectAction),
		NewResourceSet(NewResource("mybucket", "/myobject*")),
		condition.NewFunctions(func1),
	)

	anonGetBucketLocationArgs := Args{
		AccountName:     "Q3AM3UQ867SPQQA43P2F",
		Action:          GetBucketLocationAction,
		BucketName:      "mybucket",
		ConditionValues: map[string][]string{},
	}

	anonPutObjectActionArgs := Args{
		AccountName: "Q3AM3UQ867SPQQA43P2F",
		Action:      PutObjectAction,
		BucketName:  "mybucket",
		ConditionValues: map[string][]string{
			"x-amz-copy-source": {"mybucket/myobject"},
			"SourceIp":          {"192.168.1.10"},
		},
		ObjectName: "myobject",
	}

	anonGetObjectActionArgs := Args{
		AccountName:     "Q3AM3UQ867SPQQA43P2F",
		Action:          GetObjectAction,
		BucketName:      "mybucket",
		ConditionValues: map[string][]string{},
		ObjectName:      "myobject",
	}

	getBucketLocationArgs := Args{
		AccountName:     "Q3AM3UQ867SPQQA43P2F",
		Action:          GetBucketLocationAction,
		BucketName:      "mybucket",
		ConditionValues: map[string][]string{},
	}

	putObjectActionArgs := Args{
		AccountName: "Q3AM3UQ867SPQQA43P2F",
		Action:      PutObjectAction,
		BucketName:  "mybucket",
		ConditionValues: map[string][]string{
			"x-amz-copy-source": {"mybucket/myobject"},
			"SourceIp":          {"192.168.1.10"},
		},
		ObjectName: "myobject",
	}

	getObjectActionArgs := Args{
		AccountName:     "Q3AM3UQ867SPQQA43P2F",
		Action:          GetObjectAction,
		BucketName:      "mybucket",
		ConditionValues: map[string][]string{},
		ObjectName:      "myobject",
	}

	testCases := []struct {
		statement      Statement
		args           Args
		expectedResult bool
	}{
		{case1Statement, anonGetBucketLocationArgs, true},
		{case1Statement, anonPutObjectActionArgs, true},
		{case1Statement, anonGetObjectActionArgs, false},
		{case1Statement, getBucketLocationArgs, true},
		{case1Statement, putObjectActionArgs, true},
		{case1Statement, getObjectActionArgs, false},

		{case2Statement, anonGetBucketLocationArgs, false},
		{case2Statement, anonPutObjectActionArgs, true},
		{case2Statement, anonGetObjectActionArgs, true},
		{case2Statement, getBucketLocationArgs, false},
		{case2Statement, putObjectActionArgs, true},
		{case2Statement, getObjectActionArgs, true},

		{case3Statement, anonGetBucketLocationArgs, false},
		{case3Statement, anonPutObjectActionArgs, true},
		{case3Statement, anonGetObjectActionArgs, false},
		{case3Statement, getBucketLocationArgs, false},
		{case3Statement, putObjectActionArgs, true},
		{case3Statement, getObjectActionArgs, false},

		{case4Statement, anonGetBucketLocationArgs, true},
		{case4Statement, anonPutObjectActionArgs, false},
		{case4Statement, anonGetObjectActionArgs, true},
		{case4Statement, getBucketLocationArgs, true},
		{case4Statement, putObjectActionArgs, false},
		{case4Statement, getObjectActionArgs, true},
	}

	for i, testCase := range testCases {
		result := testCase.statement.IsAllowed(testCase.args)

		if result != testCase.expectedResult {
			t.Fatalf("case %v: expected: %v, got: %v\n", i+1, testCase.expectedResult, result)
		}
	}
}

func TestStatementIsValid(t *testing.T) {
	_, IPNet1, err := net.ParseCIDR("192.168.1.0/24")
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}
	func1, err := condition.NewIPAddressFunc(
		condition.AWSSourceIP,
		IPNet1,
	)
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	func2, err := condition.NewStringEqualsFunc(
		condition.S3XAmzCopySource,
		"mybucket/myobject",
	)
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	func3, err := condition.NewStringEqualsFunc(
		condition.AWSUserAgent,
		"NSPlayer",
	)
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	testCases := []struct {
		statement Statement
		expectErr bool
	}{
		// Invalid effect error.
		{NewStatement(
			policy.Effect("foo"),
			NewActionSet(GetBucketLocationAction, PutObjectAction),
			NewResourceSet(NewResource("*", "")),
			condition.NewFunctions(),
		), true},
		// Empty actions error.
		{NewStatement(
			policy.Allow,
			NewActionSet(),
			NewResourceSet(NewResource("*", "")),
			condition.NewFunctions(),
		), true},
		// Empty resources error.
		{NewStatement(
			policy.Allow,
			NewActionSet(GetBucketLocationAction, PutObjectAction),
			NewResourceSet(),
			condition.NewFunctions(),
		), true},
		// Unsupported conditions for GetObject
		{NewStatement(
			policy.Allow,
			NewActionSet(GetObjectAction, PutObjectAction),
			NewResourceSet(NewResource("mybucket", "myobject*")),
			condition.NewFunctions(func1, func2),
		), true},
		{NewStatement(
			policy.Allow,
			NewActionSet(GetBucketLocationAction, PutObjectAction),
			NewResourceSet(NewResource("mybucket", "myobject*")),
			condition.NewFunctions(),
		), false},
		{NewStatement(
			policy.Allow,
			NewActionSet(GetBucketLocationAction, PutObjectAction),
			NewResourceSet(NewResource("mybucket", "")),
			condition.NewFunctions(),
		), false},
		{NewStatement(
			policy.Deny,
			NewActionSet(GetObjectAction, PutObjectAction),
			NewResourceSet(NewResource("mybucket", "myobject*")),
			condition.NewFunctions(func1),
		), false},
		{NewStatement(
			policy.Allow,
			NewActionSet(CreateUserAdminAction, DeleteUserAdminAction),
			nil,
			condition.NewFunctions(func2, func3),
		), true},
		{NewStatement(
			policy.Allow,
			NewActionSet(CreateUserAdminAction, DeleteUserAdminAction),
			nil,
			condition.NewFunctions(),
		), false},
	}

	for i, testCase := range testCases {
		err := testCase.statement.isValid()
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			t.Fatalf("case %v: error: expected: %v, got: %v", i+1, testCase.expectErr, expectErr)
		}
	}
}

func TestStatementUnmarshalJSONAndValidate(t *testing.T) {
	case1Data := []byte(`{
    "Sid": "SomeId1",
    "Effect": "Allow",
    "Action": "s3:PutObject",
    "Resource": "arn:aws:s3:::mybucket/myobject*"
}`)
	case1Statement := NewStatement(
		policy.Allow,
		NewActionSet(PutObjectAction),
		NewResourceSet(NewResource("mybucket", "/myobject*")),
		condition.NewFunctions(),
	)
	case1Statement.SID = "SomeId1"

	case2Data := []byte(`{
    "Effect": "Allow",
    "Action": "s3:PutObject",
    "Resource": "arn:aws:s3:::mybucket/myobject*",
    "Condition": {
        "Null": {
            "s3:x-amz-copy-source": true
        }
    }
}`)
	func1, err := condition.NewNullFunc(
		condition.S3XAmzCopySource,
		true,
	)
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}
	case2Statement := NewStatement(
		policy.Allow,
		NewActionSet(PutObjectAction),
		NewResourceSet(NewResource("mybucket", "/myobject*")),
		condition.NewFunctions(func1),
	)

	case3Data := []byte(`{
    "Effect": "Deny",
    "Action": [
        "s3:PutObject",
        "s3:GetObject"
    ],
    "Resource": "arn:aws:s3:::mybucket/myobject*",
    "Condition": {
        "Null": {
            "s3:x-amz-server-side-encryption": "false"
        }
    }
}`)
	func2, err := condition.NewNullFunc(
		condition.S3XAmzServerSideEncryption,
		false,
	)
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}
	case3Statement := NewStatement(
		policy.Deny,
		NewActionSet(PutObjectAction, GetObjectAction),
		NewResourceSet(NewResource("mybucket", "/myobject*")),
		condition.NewFunctions(func2),
	)

	case4Data := []byte(`{
    "Effect": "Allow",
    "Action": "s3:PutObjec,
    "Resource": "arn:aws:s3:::mybucket/myobject*"
}`)

	case5Data := []byte(`{
    "Action": "s3:PutObject",
    "Resource": "arn:aws:s3:::mybucket/myobject*"
}`)

	case7Data := []byte(`{
    "Effect": "Allow",
    "Resource": "arn:aws:s3:::mybucket/myobject*"
}`)

	case8Data := []byte(`{
    "Effect": "Allow",
    "Action": "s3:PutObject"
}`)

	case9Data := []byte(`{
    "Effect": "Allow",
    "Action": "s3:PutObject",
    "Resource": "arn:aws:s3:::mybucket/myobject*",
    "Condition": {
    }
}`)

	case10Data := []byte(`{
    "Effect": "Deny",
    "Action": [
        "s3:PutObject",
        "s3:GetObject"
    ],
    "Resource": "arn:aws:s3:::mybucket/myobject*",
    "Condition": {
        "StringEquals": {
            "s3:x-amz-copy-source": "yourbucket/myobject*"
        }
    }
}`)

	testCases := []struct {
		data                []byte
		expectedResult      Statement
		expectUnmarshalErr  bool
		expectValidationErr bool
	}{
		{case1Data, case1Statement, false, false},
		{case2Data, case2Statement, false, false},
		{case3Data, case3Statement, false, false},
		// JSON unmarshaling error.
		{case4Data, Statement{}, true, true},
		// Invalid effect error.
		{case5Data, Statement{}, false, true},
		// Empty action error.
		{case7Data, Statement{}, false, true},
		// Empty resource error.
		{case8Data, Statement{}, false, true},
		// Empty condition error.
		{case9Data, Statement{}, true, false},
		// Unsupported condition key error.
		{case10Data, Statement{}, false, true},
	}

	for i, testCase := range testCases {
		var result Statement
		expectErr := (json.Unmarshal(testCase.data, &result) != nil)

		if expectErr != testCase.expectUnmarshalErr {
			t.Fatalf("case %v: error during unmarshal: expected: %v, got: %v", i+1, testCase.expectUnmarshalErr, expectErr)
		}

		expectErr = (result.Validate() != nil)
		if expectErr != testCase.expectValidationErr {
			t.Fatalf("case %v: error during validation: expected: %v, got: %v", i+1, testCase.expectValidationErr, expectErr)
		}

		if !testCase.expectUnmarshalErr && !testCase.expectValidationErr {
			if !reflect.DeepEqual(result, testCase.expectedResult) {
				t.Fatalf("case %v: result: expected: %v, got: %v", i+1, testCase.expectedResult, result)
			}
		}
	}
}

func TestStatementValidate(t *testing.T) {
	case1Statement := NewStatement(
		policy.Allow,
		NewActionSet(PutObjectAction),
		NewResourceSet(NewResource("mybucket", "/myobject*")),
		condition.NewFunctions(),
	)

	func1, err := condition.NewNullFunc(
		condition.S3XAmzCopySource,
		true,
	)
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}
	func2, err := condition.NewNullFunc(
		condition.S3XAmzServerSideEncryption,
		false,
	)
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}
	case2Statement := NewStatement(
		policy.Allow,
		NewActionSet(GetObjectAction, PutObjectAction),
		NewResourceSet(NewResource("mybucket", "myobject*")),
		condition.NewFunctions(func1, func2),
	)

	testCases := []struct {
		statement Statement
		expectErr bool
	}{
		{case1Statement, false},
		{case2Statement, true},
	}

	for i, testCase := range testCases {
		err := testCase.statement.Validate()
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			t.Fatalf("case %v: error: expected: %v, got: %v", i+1, testCase.expectErr, expectErr)
		}
	}
}

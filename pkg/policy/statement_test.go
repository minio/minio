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

package policy

import (
	"encoding/json"
	"net"
	"reflect"
	"testing"

	"github.com/minio/minio/pkg/policy/condition"
)

func TestStatementIsAllowed(t *testing.T) {
	case1Statement := NewStatement(
		Allow,
		NewPrincipal("*"),
		NewActionSet(GetBucketLocationAction, PutObjectAction),
		NewResourceSet(NewResource("*", "")),
		condition.NewFunctions(),
	)

	case2Statement := NewStatement(
		Allow,
		NewPrincipal("*"),
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
		Allow,
		NewPrincipal("*"),
		NewActionSet(GetObjectAction, PutObjectAction),
		NewResourceSet(NewResource("mybucket", "/myobject*")),
		condition.NewFunctions(func1),
	)

	case4Statement := NewStatement(
		Deny,
		NewPrincipal("*"),
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
		IsOwner:         true,
	}

	putObjectActionArgs := Args{
		AccountName: "Q3AM3UQ867SPQQA43P2F",
		Action:      PutObjectAction,
		BucketName:  "mybucket",
		ConditionValues: map[string][]string{
			"x-amz-copy-source": {"mybucket/myobject"},
			"SourceIp":          {"192.168.1.10"},
		},
		IsOwner:    true,
		ObjectName: "myobject",
	}

	getObjectActionArgs := Args{
		AccountName:     "Q3AM3UQ867SPQQA43P2F",
		Action:          GetObjectAction,
		BucketName:      "mybucket",
		ConditionValues: map[string][]string{},
		IsOwner:         true,
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

	testCases := []struct {
		statement Statement
		expectErr bool
	}{
		// Invalid effect error.
		{NewStatement(
			Effect("foo"),
			NewPrincipal("*"),
			NewActionSet(GetBucketLocationAction, PutObjectAction),
			NewResourceSet(NewResource("*", "")),
			condition.NewFunctions(),
		), true},
		// Invalid principal error.
		{NewStatement(
			Allow,
			NewPrincipal(),
			NewActionSet(GetBucketLocationAction, PutObjectAction),
			NewResourceSet(NewResource("*", "")),
			condition.NewFunctions(),
		), true},
		// Empty actions error.
		{NewStatement(
			Allow,
			NewPrincipal("*"),
			NewActionSet(),
			NewResourceSet(NewResource("*", "")),
			condition.NewFunctions(),
		), true},
		// Empty resources error.
		{NewStatement(
			Allow,
			NewPrincipal("*"),
			NewActionSet(GetBucketLocationAction, PutObjectAction),
			NewResourceSet(),
			condition.NewFunctions(),
		), true},
		// Unsupported resource found for object action.
		{NewStatement(
			Allow,
			NewPrincipal("*"),
			NewActionSet(GetBucketLocationAction, PutObjectAction),
			NewResourceSet(NewResource("mybucket", "")),
			condition.NewFunctions(),
		), true},
		// Unsupported resource found for bucket action.
		{NewStatement(
			Allow,
			NewPrincipal("*"),
			NewActionSet(GetBucketLocationAction, PutObjectAction),
			NewResourceSet(NewResource("mybucket", "myobject*")),
			condition.NewFunctions(),
		), true},
		// Unsupported condition key for action.
		{NewStatement(
			Allow,
			NewPrincipal("*"),
			NewActionSet(GetObjectAction, PutObjectAction),
			NewResourceSet(NewResource("mybucket", "myobject*")),
			condition.NewFunctions(func1, func2),
		), true},
		{NewStatement(
			Deny,
			NewPrincipal("*"),
			NewActionSet(GetObjectAction, PutObjectAction),
			NewResourceSet(NewResource("mybucket", "myobject*")),
			condition.NewFunctions(func1),
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

func TestStatementMarshalJSON(t *testing.T) {
	case1Statement := NewStatement(
		Allow,
		NewPrincipal("*"),
		NewActionSet(PutObjectAction),
		NewResourceSet(NewResource("mybucket", "/myobject*")),
		condition.NewFunctions(),
	)
	case1Statement.SID = "SomeId1"
	case1Data := []byte(`{"Sid":"SomeId1","Effect":"Allow","Principal":{"AWS":["*"]},"Action":["s3:PutObject"],"Resource":["arn:aws:s3:::mybucket/myobject*"]}`)

	func1, err := condition.NewNullFunc(
		condition.S3XAmzCopySource,
		true,
	)
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}
	case2Statement := NewStatement(
		Allow,
		NewPrincipal("*"),
		NewActionSet(PutObjectAction),
		NewResourceSet(NewResource("mybucket", "/myobject*")),
		condition.NewFunctions(func1),
	)
	case2Data := []byte(`{"Effect":"Allow","Principal":{"AWS":["*"]},"Action":["s3:PutObject"],"Resource":["arn:aws:s3:::mybucket/myobject*"],"Condition":{"Null":{"s3:x-amz-copy-source":[true]}}}`)

	func2, err := condition.NewNullFunc(
		condition.S3XAmzServerSideEncryption,
		false,
	)
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}
	case3Statement := NewStatement(
		Deny,
		NewPrincipal("*"),
		NewActionSet(GetObjectAction),
		NewResourceSet(NewResource("mybucket", "/myobject*")),
		condition.NewFunctions(func2),
	)
	case3Data := []byte(`{"Effect":"Deny","Principal":{"AWS":["*"]},"Action":["s3:GetObject"],"Resource":["arn:aws:s3:::mybucket/myobject*"],"Condition":{"Null":{"s3:x-amz-server-side-encryption":[false]}}}`)

	case4Statement := NewStatement(
		Allow,
		NewPrincipal("*"),
		NewActionSet(GetObjectAction, PutObjectAction),
		NewResourceSet(NewResource("mybucket", "myobject*")),
		condition.NewFunctions(func1, func2),
	)

	testCases := []struct {
		statement      Statement
		expectedResult []byte
		expectErr      bool
	}{
		{case1Statement, case1Data, false},
		{case2Statement, case2Data, false},
		{case3Statement, case3Data, false},
		// Invalid statement error.
		{case4Statement, nil, true},
	}

	for i, testCase := range testCases {
		result, err := json.Marshal(testCase.statement)
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			t.Fatalf("case %v: error: expected: %v, got: %v", i+1, testCase.expectErr, expectErr)
		}

		if !testCase.expectErr {
			if !reflect.DeepEqual(result, testCase.expectedResult) {
				t.Fatalf("case %v: result: expected: %v, got: %v", i+1, string(testCase.expectedResult), string(result))
			}
		}
	}
}

func TestStatementUnmarshalJSON(t *testing.T) {
	case1Data := []byte(`{
    "Sid": "SomeId1",
    "Effect": "Allow",
    "Principal": "*",
    "Action": "s3:PutObject",
    "Resource": "arn:aws:s3:::mybucket/myobject*"
}`)
	case1Statement := NewStatement(
		Allow,
		NewPrincipal("*"),
		NewActionSet(PutObjectAction),
		NewResourceSet(NewResource("mybucket", "/myobject*")),
		condition.NewFunctions(),
	)
	case1Statement.SID = "SomeId1"

	case2Data := []byte(`{
    "Effect": "Allow",
    "Principal": "*",
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
		Allow,
		NewPrincipal("*"),
		NewActionSet(PutObjectAction),
		NewResourceSet(NewResource("mybucket", "/myobject*")),
		condition.NewFunctions(func1),
	)

	case3Data := []byte(`{
    "Effect": "Deny",
    "Principal": {
        "AWS": "*"
    },
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
		Deny,
		NewPrincipal("*"),
		NewActionSet(PutObjectAction, GetObjectAction),
		NewResourceSet(NewResource("mybucket", "/myobject*")),
		condition.NewFunctions(func2),
	)

	case4Data := []byte(`{
    "Effect": "Allow",
    "Principal": "Q3AM3UQ867SPQQA43P2F",
    "Action": "s3:PutObject",
    "Resource": "arn:aws:s3:::mybucket/myobject*"
}`)

	case5Data := []byte(`{
    "Principal": "*",
    "Action": "s3:PutObject",
    "Resource": "arn:aws:s3:::mybucket/myobject*"
}`)

	case6Data := []byte(`{
    "Effect": "Allow",
    "Action": "s3:PutObject",
    "Resource": "arn:aws:s3:::mybucket/myobject*"
}`)

	case7Data := []byte(`{
    "Effect": "Allow",
    "Principal": "*",
    "Resource": "arn:aws:s3:::mybucket/myobject*"
}`)

	case8Data := []byte(`{
    "Effect": "Allow",
    "Principal": "*",
    "Action": "s3:PutObject"
}`)

	case9Data := []byte(`{
    "Effect": "Allow",
    "Principal": "*",
    "Action": "s3:PutObject",
    "Resource": "arn:aws:s3:::mybucket/myobject*",
    "Condition": {
    }
}`)

	case10Data := []byte(`{
    "Effect": "Deny",
    "Principal": {
        "AWS": "*"
    },
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
		data           []byte
		expectedResult Statement
		expectErr      bool
	}{
		{case1Data, case1Statement, false},
		{case2Data, case2Statement, false},
		{case3Data, case3Statement, false},
		// JSON unmarshaling error.
		{case4Data, Statement{}, true},
		// Invalid effect error.
		{case5Data, Statement{}, true},
		// empty principal error.
		{case6Data, Statement{}, true},
		// Empty action error.
		{case7Data, Statement{}, true},
		// Empty resource error.
		{case8Data, Statement{}, true},
		// Empty condition error.
		{case9Data, Statement{}, true},
		// Unsupported condition key error.
		{case10Data, Statement{}, true},
	}

	for i, testCase := range testCases {
		var result Statement
		err := json.Unmarshal(testCase.data, &result)
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			t.Fatalf("case %v: error: expected: %v, got: %v", i+1, testCase.expectErr, expectErr)
		}

		if !testCase.expectErr {
			if !reflect.DeepEqual(result, testCase.expectedResult) {
				t.Fatalf("case %v: result: expected: %v, got: %v", i+1, testCase.expectedResult, result)
			}
		}
	}
}

func TestStatementValidate(t *testing.T) {
	case1Statement := NewStatement(
		Allow,
		NewPrincipal("*"),
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
		Allow,
		NewPrincipal("*"),
		NewActionSet(GetObjectAction, PutObjectAction),
		NewResourceSet(NewResource("mybucket", "myobject*")),
		condition.NewFunctions(func1, func2),
	)

	testCases := []struct {
		statement  Statement
		bucketName string
		expectErr  bool
	}{
		{case1Statement, "mybucket", false},
		{case2Statement, "mybucket", true},
		{case1Statement, "yourbucket", true},
	}

	for i, testCase := range testCases {
		err := testCase.statement.Validate(testCase.bucketName)
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			t.Fatalf("case %v: error: expected: %v, got: %v", i+1, testCase.expectErr, expectErr)
		}
	}
}

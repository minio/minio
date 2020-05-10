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

func TestPolicyIsAllowed(t *testing.T) {
	case1Policy := Policy{
		Version: DefaultVersion,
		Statements: []Statement{
			NewStatement(
				policy.Allow,
				NewActionSet(GetBucketLocationAction, PutObjectAction),
				NewResourceSet(NewResource("*", "")),
				condition.NewFunctions(),
			)},
	}

	case2Policy := Policy{
		Version: DefaultVersion,
		Statements: []Statement{
			NewStatement(
				policy.Allow,
				NewActionSet(GetObjectAction, PutObjectAction),
				NewResourceSet(NewResource("mybucket", "/myobject*")),
				condition.NewFunctions(),
			)},
	}

	_, IPNet, err := net.ParseCIDR("192.168.1.0/24")
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}
	func1, err := condition.NewIPAddressFunc(
		condition.AWSSourceIP,
		IPNet,
	)
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case3Policy := Policy{
		Version: DefaultVersion,
		Statements: []Statement{
			NewStatement(
				policy.Allow,
				NewActionSet(GetObjectAction, PutObjectAction),
				NewResourceSet(NewResource("mybucket", "/myobject*")),
				condition.NewFunctions(func1),
			)},
	}

	case4Policy := Policy{
		Version: DefaultVersion,
		Statements: []Statement{
			NewStatement(
				policy.Deny,
				NewActionSet(GetObjectAction, PutObjectAction),
				NewResourceSet(NewResource("mybucket", "/myobject*")),
				condition.NewFunctions(func1),
			)},
	}

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
		policy         Policy
		args           Args
		expectedResult bool
	}{
		{case1Policy, anonGetBucketLocationArgs, true},
		{case1Policy, anonPutObjectActionArgs, true},
		{case1Policy, anonGetObjectActionArgs, false},
		{case1Policy, getBucketLocationArgs, true},
		{case1Policy, putObjectActionArgs, true},
		{case1Policy, getObjectActionArgs, false},

		{case2Policy, anonGetBucketLocationArgs, false},
		{case2Policy, anonPutObjectActionArgs, true},
		{case2Policy, anonGetObjectActionArgs, true},
		{case2Policy, getBucketLocationArgs, false},
		{case2Policy, putObjectActionArgs, true},
		{case2Policy, getObjectActionArgs, true},

		{case3Policy, anonGetBucketLocationArgs, false},
		{case3Policy, anonPutObjectActionArgs, true},
		{case3Policy, anonGetObjectActionArgs, false},
		{case3Policy, getBucketLocationArgs, false},
		{case3Policy, putObjectActionArgs, true},
		{case3Policy, getObjectActionArgs, false},

		{case4Policy, anonGetBucketLocationArgs, false},
		{case4Policy, anonPutObjectActionArgs, false},
		{case4Policy, anonGetObjectActionArgs, false},
		{case4Policy, getBucketLocationArgs, false},
		{case4Policy, putObjectActionArgs, false},
		{case4Policy, getObjectActionArgs, false},
	}

	for i, testCase := range testCases {
		result := testCase.policy.IsAllowed(testCase.args)

		if result != testCase.expectedResult {
			t.Errorf("case %v: expected: %v, got: %v\n", i+1, testCase.expectedResult, result)
		}
	}
}

func TestPolicyIsEmpty(t *testing.T) {
	case1Policy := Policy{
		Version: DefaultVersion,
		Statements: []Statement{
			NewStatement(
				policy.Allow,
				NewActionSet(PutObjectAction),
				NewResourceSet(NewResource("mybucket", "/myobject*")),
				condition.NewFunctions(),
			),
		},
	}

	case2Policy := Policy{
		ID:      "MyPolicyForMyBucket",
		Version: DefaultVersion,
	}

	testCases := []struct {
		policy         Policy
		expectedResult bool
	}{
		{case1Policy, false},
		{case2Policy, true},
	}

	for i, testCase := range testCases {
		result := testCase.policy.IsEmpty()

		if result != testCase.expectedResult {
			t.Fatalf("case %v: expected: %v, got: %v\n", i+1, testCase.expectedResult, result)
		}
	}
}

func TestPolicyIsValid(t *testing.T) {
	case1Policy := Policy{
		Version: DefaultVersion,
		Statements: []Statement{
			NewStatement(
				policy.Allow,
				NewActionSet(PutObjectAction),
				NewResourceSet(NewResource("mybucket", "/myobject*")),
				condition.NewFunctions(),
			),
		},
	}

	case2Policy := Policy{
		Version: DefaultVersion,
		Statements: []Statement{
			NewStatement(
				policy.Allow,
				NewActionSet(PutObjectAction),
				NewResourceSet(NewResource("mybucket", "/myobject*")),
				condition.NewFunctions(),
			),
			NewStatement(
				policy.Deny,
				NewActionSet(GetObjectAction),
				NewResourceSet(NewResource("mybucket", "/myobject*")),
				condition.NewFunctions(),
			),
		},
	}

	case3Policy := Policy{
		Version: DefaultVersion,
		Statements: []Statement{
			NewStatement(
				policy.Allow,
				NewActionSet(PutObjectAction),
				NewResourceSet(NewResource("mybucket", "/myobject*")),
				condition.NewFunctions(),
			),
			NewStatement(
				policy.Deny,
				NewActionSet(PutObjectAction),
				NewResourceSet(NewResource("mybucket", "/yourobject*")),
				condition.NewFunctions(),
			),
		},
	}

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

	case4Policy := Policy{
		Version: DefaultVersion,
		Statements: []Statement{
			NewStatement(
				policy.Allow,
				NewActionSet(PutObjectAction),
				NewResourceSet(NewResource("mybucket", "/myobject*")),
				condition.NewFunctions(func1),
			),
			NewStatement(
				policy.Deny,
				NewActionSet(PutObjectAction),
				NewResourceSet(NewResource("mybucket", "/myobject*")),
				condition.NewFunctions(func2),
			),
		},
	}

	case5Policy := Policy{
		Version: "17-10-2012",
		Statements: []Statement{
			NewStatement(
				policy.Allow,
				NewActionSet(PutObjectAction),
				NewResourceSet(NewResource("mybucket", "/myobject*")),
				condition.NewFunctions(),
			),
		},
	}

	case6Policy := Policy{
		ID:      "MyPolicyForMyBucket1",
		Version: DefaultVersion,
		Statements: []Statement{
			NewStatement(
				policy.Allow,
				NewActionSet(GetObjectAction, PutObjectAction),
				NewResourceSet(NewResource("mybucket", "myobject*")),
				condition.NewFunctions(func1, func2),
			),
		},
	}

	case7Policy := Policy{
		Version: DefaultVersion,
		Statements: []Statement{
			NewStatement(
				policy.Allow,
				NewActionSet(PutObjectAction),
				NewResourceSet(NewResource("mybucket", "/myobject*")),
				condition.NewFunctions(),
			),
			NewStatement(
				policy.Deny,
				NewActionSet(PutObjectAction),
				NewResourceSet(NewResource("mybucket", "/myobject*")),
				condition.NewFunctions(),
			),
		},
	}

	case8Policy := Policy{
		Version: DefaultVersion,
		Statements: []Statement{
			NewStatement(
				policy.Allow,
				NewActionSet(PutObjectAction),
				NewResourceSet(NewResource("mybucket", "/myobject*")),
				condition.NewFunctions(),
			),
			NewStatement(
				policy.Allow,
				NewActionSet(PutObjectAction),
				NewResourceSet(NewResource("mybucket", "/myobject*")),
				condition.NewFunctions(),
			),
		},
	}

	testCases := []struct {
		policy    Policy
		expectErr bool
	}{
		{case1Policy, false},
		// allowed duplicate principal.
		{case2Policy, false},
		// allowed duplicate principal and action.
		{case3Policy, false},
		// allowed duplicate principal, action and resource.
		{case4Policy, false},
		// Invalid version error.
		{case5Policy, true},
		// Invalid statement error.
		{case6Policy, true},
		// Duplicate statement different Effects.
		{case7Policy, false},
		// Duplicate statement same Effects, duplicate effect will be removed.
		{case8Policy, false},
	}

	for i, testCase := range testCases {
		err := testCase.policy.isValid()
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			t.Fatalf("case %v: error: expected: %v, got: %v", i+1, testCase.expectErr, expectErr)
		}
	}
}

func TestPolicyUnmarshalJSONAndValidate(t *testing.T) {
	case1Data := []byte(`{
    "ID": "MyPolicyForMyBucket1",
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "SomeId1",
            "Effect": "Allow",
            "Action": "s3:PutObject",
            "Resource": "arn:aws:s3:::mybucket/myobject*"
        }
    ]
}`)
	case1Policy := Policy{
		ID:      "MyPolicyForMyBucket1",
		Version: DefaultVersion,
		Statements: []Statement{
			NewStatement(
				policy.Allow,
				NewActionSet(PutObjectAction),
				NewResourceSet(NewResource("mybucket", "/myobject*")),
				condition.NewFunctions(),
			),
		},
	}
	case1Policy.Statements[0].SID = "SomeId1"

	case2Data := []byte(`{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": "s3:PutObject",
            "Resource": "arn:aws:s3:::mybucket/myobject*"
        },
        {
            "Effect": "Deny",
            "Action": "s3:GetObject",
            "Resource": "arn:aws:s3:::mybucket/yourobject*",
            "Condition": {
                "IpAddress": {
                    "aws:SourceIp": "192.168.1.0/24"
                }
            }
        }
    ]
}`)
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

	case2Policy := Policy{
		Version: DefaultVersion,
		Statements: []Statement{
			NewStatement(
				policy.Allow,
				NewActionSet(PutObjectAction),
				NewResourceSet(NewResource("mybucket", "/myobject*")),
				condition.NewFunctions(),
			),
			NewStatement(
				policy.Deny,
				NewActionSet(GetObjectAction),
				NewResourceSet(NewResource("mybucket", "/yourobject*")),
				condition.NewFunctions(func1),
			),
		},
	}

	case3Data := []byte(`{
    "ID": "MyPolicyForMyBucket1",
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": "s3:GetObject",
            "Resource": "arn:aws:s3:::mybucket/myobject*"
        },
        {
            "Effect": "Allow",
            "Action": "s3:PutObject",
            "Resource": "arn:aws:s3:::mybucket/myobject*"
        }
    ]
}`)
	case3Policy := Policy{
		ID:      "MyPolicyForMyBucket1",
		Version: DefaultVersion,
		Statements: []Statement{
			NewStatement(
				policy.Allow,
				NewActionSet(GetObjectAction),
				NewResourceSet(NewResource("mybucket", "/myobject*")),
				condition.NewFunctions(),
			),
			NewStatement(
				policy.Allow,
				NewActionSet(PutObjectAction),
				NewResourceSet(NewResource("mybucket", "/myobject*")),
				condition.NewFunctions(),
			),
		},
	}

	case4Data := []byte(`{
    "ID": "MyPolicyForMyBucket1",
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": "s3:PutObject",
            "Resource": "arn:aws:s3:::mybucket/myobject*"
        },
        {
            "Effect": "Allow",
            "Action": "s3:GetObject",
            "Resource": "arn:aws:s3:::mybucket/myobject*"
        }
    ]
}`)
	case4Policy := Policy{
		ID:      "MyPolicyForMyBucket1",
		Version: DefaultVersion,
		Statements: []Statement{
			NewStatement(
				policy.Allow,
				NewActionSet(PutObjectAction),
				NewResourceSet(NewResource("mybucket", "/myobject*")),
				condition.NewFunctions(),
			),
			NewStatement(
				policy.Allow,
				NewActionSet(GetObjectAction),
				NewResourceSet(NewResource("mybucket", "/myobject*")),
				condition.NewFunctions(),
			),
		},
	}

	case5Data := []byte(`{
    "ID": "MyPolicyForMyBucket1",
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": "s3:PutObject",
            "Resource": "arn:aws:s3:::mybucket/myobject*"
        },
        {
            "Effect": "Allow",
            "Action": "s3:PutObject",
            "Resource": "arn:aws:s3:::mybucket/yourobject*"
        }
    ]
}`)
	case5Policy := Policy{
		ID:      "MyPolicyForMyBucket1",
		Version: DefaultVersion,
		Statements: []Statement{
			NewStatement(
				policy.Allow,
				NewActionSet(PutObjectAction),
				NewResourceSet(NewResource("mybucket", "/myobject*")),
				condition.NewFunctions(),
			),
			NewStatement(
				policy.Allow,
				NewActionSet(PutObjectAction),
				NewResourceSet(NewResource("mybucket", "/yourobject*")),
				condition.NewFunctions(),
			),
		},
	}

	case6Data := []byte(`{
    "ID": "MyPolicyForMyBucket1",
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": "s3:PutObject",
            "Resource": "arn:aws:s3:::mybucket/myobject*",
            "Condition": {
                "IpAddress": {
                    "aws:SourceIp": "192.168.1.0/24"
                }
            }
        },
        {
            "Effect": "Allow",
            "Action": "s3:PutObject",
            "Resource": "arn:aws:s3:::mybucket/myobject*",
            "Condition": {
                "IpAddress": {
                    "aws:SourceIp": "192.168.2.0/24"
                }
            }
        }
    ]
}`)
	_, IPNet2, err := net.ParseCIDR("192.168.2.0/24")
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}
	func2, err := condition.NewIPAddressFunc(
		condition.AWSSourceIP,
		IPNet2,
	)
	if err != nil {
		t.Fatalf("unexpected error. %v\n", err)
	}

	case6Policy := Policy{
		ID:      "MyPolicyForMyBucket1",
		Version: DefaultVersion,
		Statements: []Statement{
			NewStatement(
				policy.Allow,
				NewActionSet(PutObjectAction),
				NewResourceSet(NewResource("mybucket", "/myobject*")),
				condition.NewFunctions(func1),
			),
			NewStatement(
				policy.Allow,
				NewActionSet(PutObjectAction),
				NewResourceSet(NewResource("mybucket", "/myobject*")),
				condition.NewFunctions(func2),
			),
		},
	}

	case7Data := []byte(`{
    "ID": "MyPolicyForMyBucket1",
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": "s3:GetBucketLocation",
            "Resource": "arn:aws:s3:::mybucket"
        }
    ]
}`)

	case7Policy := Policy{
		ID:      "MyPolicyForMyBucket1",
		Version: DefaultVersion,
		Statements: []Statement{
			NewStatement(
				policy.Allow,
				NewActionSet(GetBucketLocationAction),
				NewResourceSet(NewResource("mybucket", "")),
				condition.NewFunctions(),
			),
		},
	}

	case8Data := []byte(`{
    "ID": "MyPolicyForMyBucket1",
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": "s3:GetBucketLocation",
            "Resource": "arn:aws:s3:::*"
        }
    ]
}`)

	case8Policy := Policy{
		ID:      "MyPolicyForMyBucket1",
		Version: DefaultVersion,
		Statements: []Statement{
			NewStatement(
				policy.Allow,
				NewActionSet(GetBucketLocationAction),
				NewResourceSet(NewResource("*", "")),
				condition.NewFunctions(),
			),
		},
	}

	case9Data := []byte(`{
    "ID": "MyPolicyForMyBucket1",
    "Version": "17-10-2012",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": "s3:PutObject",
            "Resource": "arn:aws:s3:::mybucket/myobject*"
        }
    ]
}`)

	case10Data := []byte(`{
    "ID": "MyPolicyForMyBucket1",
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": "s3:PutObject",
            "Resource": "arn:aws:s3:::mybucket/myobject*"
        },
        {
            "Effect": "Allow",
            "Action": "s3:PutObject",
            "Resource": "arn:aws:s3:::mybucket/myobject*"
        }
    ]
}`)
	case10Policy := Policy{
		ID:      "MyPolicyForMyBucket1",
		Version: DefaultVersion,
		Statements: []Statement{
			NewStatement(
				policy.Allow,
				NewActionSet(PutObjectAction),
				NewResourceSet(NewResource("mybucket", "myobject*")),
				condition.NewFunctions(),
			),
		},
	}

	case11Data := []byte(`{
    "ID": "MyPolicyForMyBucket1",
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": "s3:PutObject",
            "Resource": "arn:aws:s3:::mybucket/myobject*"
        },
        {
            "Effect": "Deny",
            "Action": "s3:PutObject",
            "Resource": "arn:aws:s3:::mybucket/myobject*"
        }
    ]
}`)

	case11Policy := Policy{
		ID:      "MyPolicyForMyBucket1",
		Version: DefaultVersion,
		Statements: []Statement{
			NewStatement(
				policy.Allow,
				NewActionSet(PutObjectAction),
				NewResourceSet(NewResource("mybucket", "myobject*")),
				condition.NewFunctions(),
			),
			NewStatement(
				policy.Deny,
				NewActionSet(PutObjectAction),
				NewResourceSet(NewResource("mybucket", "myobject*")),
				condition.NewFunctions(),
			),
		},
	}

	testCases := []struct {
		data                []byte
		expectedResult      Policy
		expectUnmarshalErr  bool
		expectValidationErr bool
	}{
		{case1Data, case1Policy, false, false},
		{case2Data, case2Policy, false, false},
		{case3Data, case3Policy, false, false},
		{case4Data, case4Policy, false, false},
		{case5Data, case5Policy, false, false},
		{case6Data, case6Policy, false, false},
		{case7Data, case7Policy, false, false},
		{case8Data, case8Policy, false, false},
		// Invalid version error.
		{case9Data, Policy{}, false, true},
		// Duplicate statement success, duplicate statement is removed.
		{case10Data, case10Policy, false, false},
		// Duplicate statement success (Effect differs).
		{case11Data, case11Policy, false, false},
	}

	for i, testCase := range testCases {
		var result Policy
		err := json.Unmarshal(testCase.data, &result)
		expectErr := (err != nil)

		if expectErr != testCase.expectUnmarshalErr {
			t.Errorf("case %v: error during unmarshal: expected: %v, got: %v", i+1, testCase.expectUnmarshalErr, expectErr)
		}

		err = result.Validate()
		expectErr = (err != nil)

		if expectErr != testCase.expectValidationErr {
			t.Errorf("case %v: error during validation: expected: %v, got: %v", i+1, testCase.expectValidationErr, expectErr)
		}

		if !testCase.expectUnmarshalErr && !testCase.expectValidationErr {
			if !reflect.DeepEqual(result, testCase.expectedResult) {
				t.Errorf("case %v: result: expected: %v, got: %v", i+1, testCase.expectedResult, result)
			}
		}
	}
}

func TestPolicyValidate(t *testing.T) {
	case1Policy := Policy{
		Version: DefaultVersion,
		Statements: []Statement{
			NewStatement(
				policy.Allow,
				NewActionSet(PutObjectAction),
				NewResourceSet(NewResource("", "")),
				condition.NewFunctions(),
			),
		},
	}

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
	case2Policy := Policy{
		ID:      "MyPolicyForMyBucket1",
		Version: DefaultVersion,
		Statements: []Statement{
			NewStatement(
				policy.Allow,
				NewActionSet(GetObjectAction, PutObjectAction),
				NewResourceSet(NewResource("mybucket", "myobject*")),
				condition.NewFunctions(func1, func2),
			),
		},
	}

	case3Policy := Policy{
		ID:      "MyPolicyForMyBucket1",
		Version: DefaultVersion,
		Statements: []Statement{
			NewStatement(
				policy.Allow,
				NewActionSet(GetObjectAction, PutObjectAction),
				NewResourceSet(NewResource("mybucket", "myobject*")),
				condition.NewFunctions(),
			),
		},
	}

	testCases := []struct {
		policy    Policy
		expectErr bool
	}{
		{case1Policy, true},
		{case2Policy, true},
		{case3Policy, false},
	}

	for i, testCase := range testCases {
		err := testCase.policy.Validate()
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			t.Fatalf("case %v: error: expected: %v, got: %v", i+1, testCase.expectErr, expectErr)
		}
	}
}

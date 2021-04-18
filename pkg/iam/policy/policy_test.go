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
	"encoding/json"
	"net"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/minio/minio-go/v7/pkg/set"
	"github.com/minio/minio/pkg/bucket/policy"
	"github.com/minio/minio/pkg/bucket/policy/condition"
)

func TestGetPoliciesFromClaims(t *testing.T) {
	attributesArray := `{
  "exp": 1594690452,
  "iat": 1594689552,
  "auth_time": 1594689552,
  "jti": "18ed05c9-2c69-45d5-a33f-8c94aca99ad5",
  "iss": "http://localhost:8080/auth/realms/minio",
  "aud": "account",
  "sub": "7e5e2f30-1c97-4616-8623-2eae14dee9b1",
  "typ": "ID",
  "azp": "account",
  "nonce": "66ZoLzwJbjdkiedI",
  "session_state": "3df7b526-5310-4038-9f35-50ecd295a31d",
  "acr": "1",
  "upn": "harsha",
  "address": {},
  "email_verified": false,
  "groups": [
    "offline_access"
  ],
  "preferred_username": "harsha",
  "policy": [
    "readwrite",
    "readwrite,readonly",
    "  readonly",
    ""
  ]}`
	var m = make(map[string]interface{})
	if err := json.Unmarshal([]byte(attributesArray), &m); err != nil {
		t.Fatal(err)
	}
	var expectedSet = set.CreateStringSet("readwrite", "readonly")
	gotSet, ok := GetPoliciesFromClaims(m, "policy")
	if !ok {
		t.Fatal("no policy claim was found")
	}
	if gotSet.IsEmpty() {
		t.Fatal("no policies were found in policy claim")
	}
	if !gotSet.Equals(expectedSet) {
		t.Fatalf("Expected %v got %v", expectedSet, gotSet)
	}
}

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

// Parse config with location constraints
func TestPolicyParseConfig(t *testing.T) {
	policy1LocationConstraint := `{
   "Version":"2012-10-17",
   "Statement":[
      {
         "Sid":"statement1",
         "Effect":"Allow",
         "Action": "s3:CreateBucket",
         "Resource": "arn:aws:s3:::*",
         "Condition": {
             "StringLike": {
                 "s3:LocationConstraint": "us-east-1"
             }
         }
       },
      {
         "Sid":"statement2",
         "Effect":"Deny",
         "Action": "s3:CreateBucket",
         "Resource": "arn:aws:s3:::*",
         "Condition": {
             "StringNotLike": {
                 "s3:LocationConstraint": "us-east-1"
             }
         }
       }
    ]
}`
	policy2Condition := `{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "statement1",
            "Effect": "Allow",
            "Action": "s3:GetObjectVersion",
            "Resource": "arn:aws:s3:::test/HappyFace.jpg"
        },
        {
            "Sid": "statement2",
            "Effect": "Deny",
            "Action": "s3:GetObjectVersion",
            "Resource": "arn:aws:s3:::test/HappyFace.jpg",
            "Condition": {
                "StringNotEquals": {
                    "s3:versionid": "AaaHbAQitwiL_h47_44lRO2DDfLlBO5e"
                }
            }
        }
    ]
}`

	policy3ConditionActionRegex := `{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "statement2",
            "Effect": "Allow",
            "Action": "s3:Get*",
            "Resource": "arn:aws:s3:::test/HappyFace.jpg",
            "Condition": {
                "StringEquals": {
                    "s3:versionid": "AaaHbAQitwiL_h47_44lRO2DDfLlBO5e"
                }
            }
        }
    ]
}`

	policy4ConditionAction := `{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "statement2",
            "Effect": "Allow",
            "Action": "s3:GetObject",
            "Resource": "arn:aws:s3:::test/HappyFace.jpg",
            "Condition": {
                "StringEquals": {
                    "s3:versionid": "AaaHbAQitwiL_h47_44lRO2DDfLlBO5e"
                }
            }
        }
    ]
}`

	policy5ConditionCurrenTime := `{
 "Version": "2012-10-17",
 "Statement": [
  {
   "Effect": "Allow",
   "Action": [
    "s3:Get*",
    "s3:Put*"
   ],
   "Resource": [
    "arn:aws:s3:::test/*"
   ],
   "Condition": {
    "DateGreaterThan": {
     "aws:CurrentTime": [
      "2017-02-28T00:00:00Z"
     ]
    }
   }
  }
 ]
}`

	policy5ConditionCurrenTimeLesser := `{
 "Version": "2012-10-17",
 "Statement": [
  {
   "Effect": "Allow",
   "Action": [
    "s3:Get*",
    "s3:Put*"
   ],
   "Resource": [
    "arn:aws:s3:::test/*"
   ],
   "Condition": {
    "DateLessThan": {
     "aws:CurrentTime": [
      "2017-02-28T00:00:00Z"
     ]
    }
   }
  }
 ]
}`

	tests := []struct {
		p       string
		args    Args
		allowed bool
	}{
		{
			p:       policy1LocationConstraint,
			allowed: true,
			args: Args{
				AccountName:     "allowed",
				Action:          CreateBucketAction,
				BucketName:      "test",
				ConditionValues: map[string][]string{"LocationConstraint": {"us-east-1"}},
			},
		},
		{
			p:       policy1LocationConstraint,
			allowed: false,
			args: Args{
				AccountName:     "disallowed",
				Action:          CreateBucketAction,
				BucketName:      "test",
				ConditionValues: map[string][]string{"LocationConstraint": {"us-east-2"}},
			},
		},
		{
			p:       policy2Condition,
			allowed: true,
			args: Args{
				AccountName:     "allowed",
				Action:          GetObjectAction,
				BucketName:      "test",
				ObjectName:      "HappyFace.jpg",
				ConditionValues: map[string][]string{"versionid": {"AaaHbAQitwiL_h47_44lRO2DDfLlBO5e"}},
			},
		},
		{
			p:       policy2Condition,
			allowed: false,
			args: Args{
				AccountName:     "disallowed",
				Action:          GetObjectAction,
				BucketName:      "test",
				ObjectName:      "HappyFace.jpg",
				ConditionValues: map[string][]string{"versionid": {"AaaHbAQitwiL_h47_44lRO2DDfLlBO5f"}},
			},
		},
		{
			p:       policy3ConditionActionRegex,
			allowed: true,
			args: Args{
				AccountName:     "allowed",
				Action:          GetObjectAction,
				BucketName:      "test",
				ObjectName:      "HappyFace.jpg",
				ConditionValues: map[string][]string{"versionid": {"AaaHbAQitwiL_h47_44lRO2DDfLlBO5e"}},
			},
		},
		{
			p:       policy3ConditionActionRegex,
			allowed: false,
			args: Args{
				AccountName:     "disallowed",
				Action:          GetObjectAction,
				BucketName:      "test",
				ObjectName:      "HappyFace.jpg",
				ConditionValues: map[string][]string{"versionid": {"AaaHbAQitwiL_h47_44lRO2DDfLlBO5f"}},
			},
		},
		{
			p:       policy4ConditionAction,
			allowed: true,
			args: Args{
				AccountName:     "allowed",
				Action:          GetObjectAction,
				BucketName:      "test",
				ObjectName:      "HappyFace.jpg",
				ConditionValues: map[string][]string{"versionid": {"AaaHbAQitwiL_h47_44lRO2DDfLlBO5e"}},
			},
		},
		{
			p:       policy5ConditionCurrenTime,
			allowed: true,
			args: Args{
				AccountName: "allowed",
				Action:      GetObjectAction,
				BucketName:  "test",
				ObjectName:  "HappyFace.jpg",
				ConditionValues: map[string][]string{
					"CurrentTime": {time.Now().Format(time.RFC3339)},
				},
			},
		},
		{
			p:       policy5ConditionCurrenTimeLesser,
			allowed: false,
			args: Args{
				AccountName: "disallowed",
				Action:      GetObjectAction,
				BucketName:  "test",
				ObjectName:  "HappyFace.jpg",
				ConditionValues: map[string][]string{
					"CurrentTime": {time.Now().Format(time.RFC3339)},
				},
			},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.args.AccountName, func(t *testing.T) {
			ip, err := ParseConfig(strings.NewReader(test.p))
			if err != nil {
				t.Error(err)
			}
			if got := ip.IsAllowed(test.args); got != test.allowed {
				t.Errorf("Expected %t, got %t", test.allowed, got)
			}
		})
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

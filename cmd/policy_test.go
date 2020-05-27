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

package cmd

import (
	"reflect"
	"testing"

	miniogopolicy "github.com/minio/minio-go/v6/pkg/policy"
	"github.com/minio/minio-go/v6/pkg/set"
	"github.com/minio/minio/pkg/bucket/policy"
	"github.com/minio/minio/pkg/bucket/policy/condition"
)

func TestPolicySysIsAllowed(t *testing.T) {
	p := &policy.Policy{
		Version: policy.DefaultVersion,
		Statements: []policy.Statement{
			policy.NewStatement(
				policy.Allow,
				policy.NewPrincipal("*"),
				policy.NewActionSet(policy.GetBucketLocationAction),
				policy.NewResourceSet(policy.NewResource("mybucket", "")),
				condition.NewFunctions(),
			),
			policy.NewStatement(
				policy.Allow,
				policy.NewPrincipal("*"),
				policy.NewActionSet(policy.PutObjectAction),
				policy.NewResourceSet(policy.NewResource("mybucket", "/myobject*")),
				condition.NewFunctions(),
			),
		},
	}

	anonGetBucketLocationArgs := policy.Args{
		AccountName:     "Q3AM3UQ867SPQQA43P2F",
		Action:          policy.GetBucketLocationAction,
		BucketName:      "mybucket",
		ConditionValues: map[string][]string{},
	}

	anonPutObjectActionArgs := policy.Args{
		AccountName: "Q3AM3UQ867SPQQA43P2F",
		Action:      policy.PutObjectAction,
		BucketName:  "mybucket",
		ConditionValues: map[string][]string{
			"x-amz-copy-source": {"mybucket/myobject"},
			"SourceIp":          {"192.168.1.10"},
		},
		ObjectName: "myobject",
	}

	anonGetObjectActionArgs := policy.Args{
		AccountName:     "Q3AM3UQ867SPQQA43P2F",
		Action:          policy.GetObjectAction,
		BucketName:      "mybucket",
		ConditionValues: map[string][]string{},
		ObjectName:      "myobject",
	}

	getBucketLocationArgs := policy.Args{
		AccountName:     "Q3AM3UQ867SPQQA43P2F",
		Action:          policy.GetBucketLocationAction,
		BucketName:      "mybucket",
		ConditionValues: map[string][]string{},
		IsOwner:         true,
	}

	putObjectActionArgs := policy.Args{
		AccountName: "Q3AM3UQ867SPQQA43P2F",
		Action:      policy.PutObjectAction,
		BucketName:  "mybucket",
		ConditionValues: map[string][]string{
			"x-amz-copy-source": {"mybucket/myobject"},
			"SourceIp":          {"192.168.1.10"},
		},
		IsOwner:    true,
		ObjectName: "myobject",
	}

	getObjectActionArgs := policy.Args{
		AccountName:     "Q3AM3UQ867SPQQA43P2F",
		Action:          policy.GetObjectAction,
		BucketName:      "mybucket",
		ConditionValues: map[string][]string{},
		IsOwner:         true,
		ObjectName:      "myobject",
	}

	yourbucketAnonGetObjectActionArgs := policy.Args{
		AccountName:     "Q3AM3UQ867SPQQA43P2F",
		Action:          policy.GetObjectAction,
		BucketName:      "yourbucket",
		ConditionValues: map[string][]string{},
		ObjectName:      "yourobject",
	}

	yourbucketGetObjectActionArgs := policy.Args{
		AccountName:     "Q3AM3UQ867SPQQA43P2F",
		Action:          policy.GetObjectAction,
		BucketName:      "yourbucket",
		ConditionValues: map[string][]string{},
		IsOwner:         true,
		ObjectName:      "yourobject",
	}

	testCases := []struct {
		args           policy.Args
		expectedResult bool
	}{
		{anonGetBucketLocationArgs, true},
		{anonPutObjectActionArgs, true},
		{anonGetObjectActionArgs, false},
		{getBucketLocationArgs, true},
		{putObjectActionArgs, true},
		{getObjectActionArgs, true},
		{yourbucketAnonGetObjectActionArgs, false},
		{yourbucketGetObjectActionArgs, true},
	}

	for i, testCase := range testCases {
		result := p.IsAllowed(testCase.args)

		if result != testCase.expectedResult {
			t.Fatalf("case %v: expected: %v, got: %v\n", i+1, testCase.expectedResult, result)
		}
	}
}

func getReadOnlyStatement(bucketName, prefix string) []miniogopolicy.Statement {
	return []miniogopolicy.Statement{
		{
			Effect:    string(policy.Allow),
			Principal: miniogopolicy.User{AWS: set.CreateStringSet("*")},
			Resources: set.CreateStringSet(policy.NewResource(bucketName, "").String()),
			Actions:   set.CreateStringSet("s3:GetBucketLocation", "s3:ListBucket"),
		},
		{
			Effect:    string(policy.Allow),
			Principal: miniogopolicy.User{AWS: set.CreateStringSet("*")},
			Resources: set.CreateStringSet(policy.NewResource(bucketName, prefix).String()),
			Actions:   set.CreateStringSet("s3:GetObject"),
		},
	}
}

func TestPolicyToBucketAccessPolicy(t *testing.T) {
	case1Policy := &policy.Policy{
		Version: policy.DefaultVersion,
		Statements: []policy.Statement{
			policy.NewStatement(
				policy.Allow,
				policy.NewPrincipal("*"),
				policy.NewActionSet(policy.GetBucketLocationAction, policy.ListBucketAction),
				policy.NewResourceSet(policy.NewResource("mybucket", "")),
				condition.NewFunctions(),
			),
			policy.NewStatement(
				policy.Allow,
				policy.NewPrincipal("*"),
				policy.NewActionSet(policy.GetObjectAction),
				policy.NewResourceSet(policy.NewResource("mybucket", "/myobject*")),
				condition.NewFunctions(),
			),
		},
	}

	case1Result := &miniogopolicy.BucketAccessPolicy{
		Version:    policy.DefaultVersion,
		Statements: getReadOnlyStatement("mybucket", "/myobject*"),
	}

	case2Policy := &policy.Policy{
		Version:    policy.DefaultVersion,
		Statements: []policy.Statement{},
	}

	case2Result := &miniogopolicy.BucketAccessPolicy{
		Version:    policy.DefaultVersion,
		Statements: []miniogopolicy.Statement{},
	}

	case3Policy := &policy.Policy{
		Version: "12-10-2012",
		Statements: []policy.Statement{
			policy.NewStatement(
				policy.Allow,
				policy.NewPrincipal("*"),
				policy.NewActionSet(policy.PutObjectAction),
				policy.NewResourceSet(policy.NewResource("mybucket", "/myobject*")),
				condition.NewFunctions(),
			),
		},
	}

	testCases := []struct {
		bucketPolicy   *policy.Policy
		expectedResult *miniogopolicy.BucketAccessPolicy
		expectErr      bool
	}{
		{case1Policy, case1Result, false},
		{case2Policy, case2Result, false},
		{case3Policy, nil, true},
	}

	for i, testCase := range testCases {
		result, err := PolicyToBucketAccessPolicy(testCase.bucketPolicy)
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			t.Fatalf("case %v: error: expected: %v, got: %v\n", i+1, testCase.expectErr, expectErr)
		}

		if !testCase.expectErr {
			if !reflect.DeepEqual(result, testCase.expectedResult) {
				t.Fatalf("case %v: result: expected: %+v, got: %+v\n", i+1, testCase.expectedResult, result)
			}
		}
	}
}

func TestBucketAccessPolicyToPolicy(t *testing.T) {
	case1PolicyInfo := &miniogopolicy.BucketAccessPolicy{
		Version:    policy.DefaultVersion,
		Statements: getReadOnlyStatement("mybucket", "/myobject*"),
	}

	case1Result := &policy.Policy{
		Version: policy.DefaultVersion,
		Statements: []policy.Statement{
			policy.NewStatement(
				policy.Allow,
				policy.NewPrincipal("*"),
				policy.NewActionSet(policy.GetBucketLocationAction, policy.ListBucketAction),
				policy.NewResourceSet(policy.NewResource("mybucket", "")),
				condition.NewFunctions(),
			),
			policy.NewStatement(
				policy.Allow,
				policy.NewPrincipal("*"),
				policy.NewActionSet(policy.GetObjectAction),
				policy.NewResourceSet(policy.NewResource("mybucket", "/myobject*")),
				condition.NewFunctions(),
			),
		},
	}

	case2PolicyInfo := &miniogopolicy.BucketAccessPolicy{
		Version:    policy.DefaultVersion,
		Statements: []miniogopolicy.Statement{},
	}

	case2Result := &policy.Policy{
		Version:    policy.DefaultVersion,
		Statements: []policy.Statement{},
	}

	case3PolicyInfo := &miniogopolicy.BucketAccessPolicy{
		Version:    "12-10-2012",
		Statements: getReadOnlyStatement("mybucket", "/myobject*"),
	}

	testCases := []struct {
		policyInfo     *miniogopolicy.BucketAccessPolicy
		expectedResult *policy.Policy
		expectErr      bool
	}{
		{case1PolicyInfo, case1Result, false},
		{case2PolicyInfo, case2Result, false},
		{case3PolicyInfo, nil, true},
	}

	for i, testCase := range testCases {
		result, err := BucketAccessPolicyToPolicy(testCase.policyInfo)
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			t.Fatalf("case %v: error: expected: %v, got: %v\n", i+1, testCase.expectErr, expectErr)
		}

		if !testCase.expectErr {
			if !reflect.DeepEqual(result, testCase.expectedResult) {
				t.Fatalf("case %v: result: expected: %+v, got: %+v\n", i+1, testCase.expectedResult, result)
			}
		}
	}
}

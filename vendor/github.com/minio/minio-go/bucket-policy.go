/*
 * Minio Go Library for Amazon S3 Compatible Cloud Storage (C) 2015 Minio, Inc.
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

package minio

import (
	"encoding/json"
	"sort"
)

// maximum supported access policy size.
const maxAccessPolicySize = 20 * 1024 * 1024 // 20KiB.

// Resource prefix for all aws resources.
const awsResourcePrefix = "arn:aws:s3:::"

// BucketPolicy - Bucket level policy.
type BucketPolicy string

// Different types of Policies currently supported for buckets.
const (
	BucketPolicyNone      BucketPolicy = "none"
	BucketPolicyReadOnly               = "readonly"
	BucketPolicyReadWrite              = "readwrite"
	BucketPolicyWriteOnly              = "writeonly"
)

// isValidBucketPolicy - Is provided policy value supported.
func (p BucketPolicy) isValidBucketPolicy() bool {
	switch p {
	case BucketPolicyNone, BucketPolicyReadOnly, BucketPolicyReadWrite, BucketPolicyWriteOnly:
		return true
	}
	return false
}

// User - canonical users list.
type User struct {
	AWS []string
}

// Statement - minio policy statement
type Statement struct {
	Sid        string
	Effect     string
	Principal  User                         `json:"Principal"`
	Actions    []string                     `json:"Action"`
	Resources  []string                     `json:"Resource"`
	Conditions map[string]map[string]string `json:"Condition,omitempty"`
}

// BucketAccessPolicy - minio policy collection
type BucketAccessPolicy struct {
	Version    string      // date in 0000-00-00 format
	Statements []Statement `json:"Statement"`
}

// Read write actions.
var (
	readWriteBucketActions = []string{
		"s3:GetBucketLocation",
		"s3:ListBucket",
		"s3:ListBucketMultipartUploads",
		// Add more bucket level read-write actions here.
	}
	readWriteObjectActions = []string{
		"s3:AbortMultipartUpload",
		"s3:DeleteObject",
		"s3:GetObject",
		"s3:ListMultipartUploadParts",
		"s3:PutObject",
		// Add more object level read-write actions here.
	}
)

// Write only actions.
var (
	writeOnlyBucketActions = []string{
		"s3:GetBucketLocation",
		"s3:ListBucketMultipartUploads",
		// Add more bucket level write actions here.
	}
	writeOnlyObjectActions = []string{
		"s3:AbortMultipartUpload",
		"s3:DeleteObject",
		"s3:ListMultipartUploadParts",
		"s3:PutObject",
		// Add more object level write actions here.
	}
)

// Read only actions.
var (
	readOnlyBucketActions = []string{
		"s3:GetBucketLocation",
		"s3:ListBucket",
		// Add more bucket level read actions here.
	}
	readOnlyObjectActions = []string{
		"s3:GetObject",
		// Add more object level read actions here.
	}
)

// subsetActions returns true if the first array is completely
// contained in the second array. There must be at least
// the same number of duplicate values in second as there
// are in first.
func subsetActions(first, second []string) bool {
	set := make(map[string]int)
	for _, value := range second {
		set[value]++
	}
	for _, value := range first {
		if count, found := set[value]; !found {
			return false
		} else if count < 1 {
			return false
		} else {
			set[value] = count - 1
		}
	}
	return true
}

// Verifies if we have read/write policy set at bucketName, objectPrefix.
func isBucketPolicyReadWrite(statements []Statement, bucketName string, objectPrefix string) bool {
	var commonActions, readWrite bool
	sort.Strings(readWriteBucketActions)
	sort.Strings(readWriteObjectActions)
	for _, statement := range statements {
		for _, resource := range statement.Resources {
			if resource == awsResourcePrefix+bucketName {
				if subsetActions(readWriteBucketActions, statement.Actions) {
					commonActions = true
					continue
				}
			} else if resource == awsResourcePrefix+bucketName+"/"+objectPrefix+"*" {
				if subsetActions(readWriteObjectActions, statement.Actions) {
					readWrite = true
				}
			}
		}
	}
	return commonActions && readWrite
}

// Verifies if we have write only policy set at bucketName, objectPrefix.
func isBucketPolicyWriteOnly(statements []Statement, bucketName string, objectPrefix string) bool {
	var commonActions, writeOnly bool
	sort.Strings(writeOnlyBucketActions)
	sort.Strings(writeOnlyObjectActions)
	for _, statement := range statements {
		for _, resource := range statement.Resources {
			if resource == awsResourcePrefix+bucketName {
				if subsetActions(writeOnlyBucketActions, statement.Actions) {
					commonActions = true
					continue
				}
			} else if resource == awsResourcePrefix+bucketName+"/"+objectPrefix+"*" {
				if subsetActions(writeOnlyObjectActions, statement.Actions) {
					writeOnly = true
				}
			}
		}
	}
	return commonActions && writeOnly
}

// Verifies if we have read only policy set at bucketName, objectPrefix.
func isBucketPolicyReadOnly(statements []Statement, bucketName string, objectPrefix string) bool {
	var commonActions, readOnly bool
	sort.Strings(readOnlyBucketActions)
	sort.Strings(readOnlyObjectActions)
	for _, statement := range statements {
		for _, resource := range statement.Resources {
			if resource == awsResourcePrefix+bucketName {
				if subsetActions(readOnlyBucketActions, statement.Actions) {
					commonActions = true
					continue
				}
			} else if resource == awsResourcePrefix+bucketName+"/"+objectPrefix+"*" {
				if subsetActions(readOnlyObjectActions, statement.Actions) {
					readOnly = true
					break
				}
			}
		}
	}
	return commonActions && readOnly
}

// Removes read write bucket policy if found.
func removeBucketPolicyStatementReadWrite(statements []Statement, bucketName string, objectPrefix string) []Statement {
	var newStatements []Statement
	for _, statement := range statements {
		for _, resource := range statement.Resources {
			if resource == awsResourcePrefix+bucketName {
				var newActions []string
				for _, action := range statement.Actions {
					switch action {
					case "s3:GetBucketLocation", "s3:ListBucket", "s3:ListBucketMultipartUploads":
						continue
					}
					newActions = append(newActions, action)
				}
				statement.Actions = newActions
			} else if resource == awsResourcePrefix+bucketName+"/"+objectPrefix+"*" {
				var newActions []string
				for _, action := range statement.Actions {
					switch action {
					case "s3:PutObject", "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts", "s3:DeleteObject", "s3:GetObject":
						continue
					}
					newActions = append(newActions, action)
				}
				statement.Actions = newActions
			}
		}
		if len(statement.Actions) != 0 {
			newStatements = append(newStatements, statement)
		}
	}
	return newStatements
}

// Removes write only bucket policy if found.
func removeBucketPolicyStatementWriteOnly(statements []Statement, bucketName string, objectPrefix string) []Statement {
	var newStatements []Statement
	for _, statement := range statements {
		for _, resource := range statement.Resources {
			if resource == awsResourcePrefix+bucketName {
				var newActions []string
				for _, action := range statement.Actions {
					switch action {
					case "s3:GetBucketLocation", "s3:ListBucketMultipartUploads":
						continue
					}
					newActions = append(newActions, action)
				}
				statement.Actions = newActions
			} else if resource == awsResourcePrefix+bucketName+"/"+objectPrefix+"*" {
				var newActions []string
				for _, action := range statement.Actions {
					switch action {
					case "s3:PutObject", "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts", "s3:DeleteObject":
						continue
					}
					newActions = append(newActions, action)
				}
				statement.Actions = newActions
			}
		}
		if len(statement.Actions) != 0 {
			newStatements = append(newStatements, statement)
		}
	}
	return newStatements
}

// Removes read only bucket policy if found.
func removeBucketPolicyStatementReadOnly(statements []Statement, bucketName string, objectPrefix string) []Statement {
	var newStatements []Statement
	for _, statement := range statements {
		for _, resource := range statement.Resources {
			if resource == awsResourcePrefix+bucketName {
				var newActions []string
				for _, action := range statement.Actions {
					switch action {
					case "s3:GetBucketLocation", "s3:ListBucket":
						continue
					}
					newActions = append(newActions, action)
				}
				statement.Actions = newActions
			} else if resource == awsResourcePrefix+bucketName+"/"+objectPrefix+"*" {
				var newActions []string
				for _, action := range statement.Actions {
					if action == "s3:GetObject" {
						continue
					}
					newActions = append(newActions, action)
				}
				statement.Actions = newActions
			}
		}
		if len(statement.Actions) != 0 {
			newStatements = append(newStatements, statement)
		}
	}
	return newStatements
}

// Remove bucket policies based on the type.
func removeBucketPolicyStatement(statements []Statement, bucketName string, objectPrefix string) []Statement {
	// Verify type of policy to be removed.
	if isBucketPolicyReadWrite(statements, bucketName, objectPrefix) {
		statements = removeBucketPolicyStatementReadWrite(statements, bucketName, objectPrefix)
	} else if isBucketPolicyWriteOnly(statements, bucketName, objectPrefix) {
		statements = removeBucketPolicyStatementWriteOnly(statements, bucketName, objectPrefix)
	} else if isBucketPolicyReadOnly(statements, bucketName, objectPrefix) {
		statements = removeBucketPolicyStatementReadOnly(statements, bucketName, objectPrefix)
	}
	return statements
}

// Unmarshals bucket policy byte array into a structured bucket access policy.
func unMarshalBucketPolicy(bucketPolicyBuf []byte) (BucketAccessPolicy, error) {
	// Untyped lazy JSON struct.
	type bucketAccessPolicyUntyped struct {
		Version   string
		Statement []struct {
			Sid       string
			Effect    string
			Principal struct {
				AWS json.RawMessage
			}
			Action    json.RawMessage
			Resource  json.RawMessage
			Condition map[string]map[string]string
		}
	}
	var policyUntyped = bucketAccessPolicyUntyped{}
	// Unmarshal incoming policy into an untyped structure, to be
	// evaluated lazily later.
	err := json.Unmarshal(bucketPolicyBuf, &policyUntyped)
	if err != nil {
		return BucketAccessPolicy{}, err
	}
	var policy = BucketAccessPolicy{}
	policy.Version = policyUntyped.Version
	for _, stmtUntyped := range policyUntyped.Statement {
		statement := Statement{}
		// These are properly typed messages.
		statement.Sid = stmtUntyped.Sid
		statement.Effect = stmtUntyped.Effect
		statement.Conditions = stmtUntyped.Condition

		// AWS user can have two different types, either as []string
		// and either as regular 'string'. We fall back to doing this
		// since there is no other easier way to fix this.
		err = json.Unmarshal(stmtUntyped.Principal.AWS, &statement.Principal.AWS)
		if err != nil {
			var awsUser string
			err = json.Unmarshal(stmtUntyped.Principal.AWS, &awsUser)
			if err != nil {
				return BucketAccessPolicy{}, err
			}
			statement.Principal.AWS = []string{awsUser}
		}
		// Actions can have two different types, either as []string
		// and either as regular 'string'. We fall back to doing this
		// since there is no other easier way to fix this.
		err = json.Unmarshal(stmtUntyped.Action, &statement.Actions)
		if err != nil {
			var action string
			err = json.Unmarshal(stmtUntyped.Action, &action)
			if err != nil {
				return BucketAccessPolicy{}, err
			}
			statement.Actions = []string{action}
		}
		// Resources can have two different types, either as []string
		// and either as regular 'string'. We fall back to doing this
		// since there is no other easier way to fix this.
		err = json.Unmarshal(stmtUntyped.Resource, &statement.Resources)
		if err != nil {
			var resource string
			err = json.Unmarshal(stmtUntyped.Resource, &resource)
			if err != nil {
				return BucketAccessPolicy{}, err
			}
			statement.Resources = []string{resource}
		}
		// Append the typed policy.
		policy.Statements = append(policy.Statements, statement)
	}
	return policy, nil
}

/*
 * Minio Cloud Storage, (C) 2015, 2016 Minio, Inc.
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

// Package accesspolicy implements AWS Access Policy Language parser in
// accordance with http://docs.aws.amazon.com/AmazonS3/latest/dev/access-policy-language-overview.html
package accesspolicy

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

const (
	// AWSResourcePrefix - bucket policy resource prefix.
	AWSResourcePrefix = "arn:aws:s3:::"
)

// supportedActionMap - lists all the actions supported by minio.
var supportedActionMap = map[string]struct{}{
	"*":                             {},
	"s3:*":                          {},
	"s3:GetObject":                  {},
	"s3:ListBucket":                 {},
	"s3:PutObject":                  {},
	"s3:CreateBucket":               {},
	"s3:GetBucketLocation":          {},
	"s3:DeleteBucket":               {},
	"s3:DeleteObject":               {},
	"s3:AbortMultipartUpload":       {},
	"s3:ListBucketMultipartUploads": {},
	"s3:ListMultipartUploadParts":   {},
}

// User - canonical users list.
type User struct {
	AWS []string
}

// Statement - minio policy statement
type Statement struct {
	Sid        string
	Effect     string
	Principal  User
	Actions    []string                     `json:"Action"`
	Resources  []string                     `json:"Resource"`
	Conditions map[string]map[string]string `json:"Condition"`
}

// BucketPolicy - minio policy collection
type BucketPolicy struct {
	Version    string      // date in 0000-00-00 format
	Statements []Statement `json:"Statement"`
}

// supportedEffectMap - supported effects.
var supportedEffectMap = map[string]struct{}{
	"Allow": {},
	"Deny":  {},
}

// isValidActions - are actions valid.
func isValidActions(actions []string) (err error) {
	// Statement actions cannot be empty.
	if len(actions) == 0 {
		err = errors.New("Action list cannot be empty.")
		return err
	}
	for _, action := range actions {
		if _, ok := supportedActionMap[action]; !ok {
			err = errors.New("Unsupported action found: ‘" + action + "’, please validate your policy document.")
			return err
		}
	}
	return nil
}

// isValidEffect - is effect valid.
func isValidEffect(effect string) error {
	// Statement effect cannot be empty.
	if len(effect) == 0 {
		err := errors.New("Policy effect cannot be empty.")
		return err
	}
	_, ok := supportedEffectMap[effect]
	if !ok {
		err := errors.New("Unsupported Effect found: ‘" + effect + "’, please validate your policy document.")
		return err
	}
	return nil
}

// isValidResources - are valid resources.
func isValidResources(resources []string) (err error) {
	// Statement resources cannot be empty.
	if len(resources) == 0 {
		err = errors.New("Resource list cannot be empty.")
		return err
	}
	for _, resource := range resources {
		if !strings.HasPrefix(resource, AWSResourcePrefix) {
			err = errors.New("Unsupported resource style found: ‘" + resource + "’, please validate your policy document.")
			return err
		}
		resourceSuffix := strings.SplitAfter(resource, AWSResourcePrefix)[1]
		if len(resourceSuffix) == 0 || strings.HasPrefix(resourceSuffix, "/") {
			err = errors.New("Invalid resource style found: ‘" + resource + "’, please validate your policy document.")
			return err
		}
	}
	return nil
}

// isValidPrincipals - are valid principals.
func isValidPrincipals(principals []string) (err error) {
	// Statement principal should have a value.
	if len(principals) == 0 {
		err = errors.New("Principal cannot be empty.")
		return err
	}
	var ok bool
	for _, principal := range principals {
		// Minio does not support or implement IAM, "*" is the only valid value.
		if principal == "*" {
			ok = true
			continue
		}
		ok = false
	}
	if !ok {
		err = errors.New("Unsupported principal style found: ‘" + strings.Join(principals, " ") + "’, please validate your policy document.")
		return err
	}
	return nil
}

func isValidConditions(conditions map[string]map[string]string) (err error) {
	// Verify conditions should be valid.
	if len(conditions) > 0 {
		// Validate if stringEquals, stringNotEquals are present
		// if not throw an error.
		_, stringEqualsOK := conditions["StringEquals"]
		_, stringNotEqualsOK := conditions["StringNotEquals"]
		if !stringEqualsOK && !stringNotEqualsOK {
			err = fmt.Errorf("Unsupported condition type found: ‘%s’, please validate your policy document.", conditions)
			return err
		}
		// Validate s3:prefix, s3:max-keys are present if not
		// throw an error.
		if len(conditions["StringEquals"]) > 0 {
			_, s3PrefixOK := conditions["StringEquals"]["s3:prefix"]
			_, s3MaxKeysOK := conditions["StringEquals"]["s3:max-keys"]
			if !s3PrefixOK && !s3MaxKeysOK {
				err = fmt.Errorf("Unsupported condition keys found: ‘%s’, please validate your policy document.",
					conditions["StringEquals"])
				return err
			}
		}
		if len(conditions["StringNotEquals"]) > 0 {
			_, s3PrefixOK := conditions["StringNotEquals"]["s3:prefix"]
			_, s3MaxKeysOK := conditions["StringNotEquals"]["s3:max-keys"]
			if !s3PrefixOK && !s3MaxKeysOK {
				err = fmt.Errorf("Unsupported condition keys found: ‘%s’, please validate your policy document.",
					conditions["StringNotEquals"])
				return err
			}
		}
	}
	return nil
}

// Validate - validate if request body is of proper JSON and in
// accordance with policy standards.
func Validate(bucketPolicyBuf []byte) (policy BucketPolicy, err error) {
	if err = json.Unmarshal(bucketPolicyBuf, &policy); err != nil {
		return BucketPolicy{}, err
	}

	// Policy version cannot be empty.
	if len(policy.Version) == 0 {
		err = errors.New("Policy version cannot be empty.")
		return BucketPolicy{}, err
	}

	// Policy statements cannot be empty.
	if len(policy.Statements) == 0 {
		err = errors.New("Policy statement cannot be empty.")
		return BucketPolicy{}, err
	}

	// Loop through all policy statements and validate entries.
	for _, statement := range policy.Statements {
		// Statement effect should be valid.
		if err := isValidEffect(statement.Effect); err != nil {
			return BucketPolicy{}, err
		}
		// Statement principal should be supported format.
		if err := isValidPrincipals(statement.Principal.AWS); err != nil {
			return BucketPolicy{}, err
		}
		// Statement actions should be valid.
		if err := isValidActions(statement.Actions); err != nil {
			return BucketPolicy{}, err
		}
		// Statment resources should be valid.
		if err := isValidResources(statement.Resources); err != nil {
			return BucketPolicy{}, err
		}
		// Statement conditions should be valid.
		if err := isValidConditions(statement.Conditions); err != nil {
			return BucketPolicy{}, err
		}
	}
	// Return successfully parsed policy structure.
	return policy, nil
}

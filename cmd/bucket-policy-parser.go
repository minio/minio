/*
 * Minio Cloud Storage, (C) 2015, 2016, 2017 Minio, Inc.
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

// Package cmd This file implements AWS Access Policy Language parser in
// accordance with http://docs.aws.amazon.com/AmazonS3/latest/dev/access-policy-language-overview.html
package cmd

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/minio/minio-go/pkg/policy"
	"github.com/minio/minio-go/pkg/set"
)

var emptyBucketPolicy = policy.BucketAccessPolicy{}

var conditionKeyActionMap = policy.ConditionKeyMap{
	"s3:prefix": set.CreateStringSet("s3:ListBucket", "s3:ListBucketMultipartUploads"),
	"s3:max-keys": set.CreateStringSet("s3:ListBucket", "s3:ListBucketMultipartUploads",
		"s3:ListMultipartUploadParts"),
}

// supportedActionMap - lists all the actions supported by minio.
var supportedActionMap = set.CreateStringSet("*", "s3:*", "s3:GetObject",
	"s3:ListBucket", "s3:PutObject", "s3:GetBucketLocation", "s3:DeleteObject",
	"s3:AbortMultipartUpload", "s3:ListBucketMultipartUploads", "s3:ListMultipartUploadParts")

// supported Conditions type.
var supportedConditionsType = set.CreateStringSet("StringEquals", "StringNotEquals", "StringLike", "StringNotLike", "IpAddress", "NotIpAddress")

// Validate s3:prefix, s3:max-keys are present if not
// supported keys for the conditions.
var supportedConditionsKey = set.CreateStringSet("s3:prefix", "s3:max-keys", "aws:Referer", "aws:SourceIp")

// supportedEffectMap - supported effects.
var supportedEffectMap = set.CreateStringSet("Allow", "Deny")

// isValidActions - are actions valid.
func isValidActions(actions set.StringSet) (err error) {
	// Statement actions cannot be empty.
	if actions.IsEmpty() {
		err = errors.New("Action list cannot be empty")
		return err
	}
	if unsupportedActions := actions.Difference(supportedActionMap); !unsupportedActions.IsEmpty() {
		err = fmt.Errorf("Unsupported actions found: ‘%#v’, please validate your policy document",
			unsupportedActions)
		return err
	}
	return nil
}

// isValidEffect - is effect valid.
func isValidEffect(effect string) (err error) {
	// Statement effect cannot be empty.
	if effect == "" {
		err = errors.New("Policy effect cannot be empty")
		return err
	}
	if !supportedEffectMap.Contains(effect) {
		err = errors.New("Unsupported Effect found: ‘" + effect + "’, please validate your policy document")
		return err
	}
	return nil
}

// isValidResources - are valid resources.
func isValidResources(resources set.StringSet) (err error) {
	// Statement resources cannot be empty.
	if resources.IsEmpty() {
		err = errors.New("Resource list cannot be empty")
		return err
	}
	for resource := range resources {
		if !hasPrefix(resource, bucketARNPrefix) {
			err = errors.New("Unsupported resource style found: ‘" + resource + "’, please validate your policy document")
			return err
		}
		resourceSuffix := strings.SplitAfter(resource, bucketARNPrefix)[1]
		if len(resourceSuffix) == 0 || hasPrefix(resourceSuffix, "/") {
			err = errors.New("Invalid resource style found: ‘" + resource + "’, please validate your policy document")
			return err
		}
	}
	return nil
}

// isValidPrincipals - are valid principals.
func isValidPrincipals(principal policy.User) (err error) {
	if principal.AWS.IsEmpty() {
		return errors.New("Principal cannot be empty")
	}
	if diff := principal.AWS.Difference(set.CreateStringSet("*")); !diff.IsEmpty() {
		// Minio does not support or implement IAM, "*" is the only valid value.
		// Amazon s3 doc on principal:
		// http://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_elements.html#Principal
		err = fmt.Errorf("Unsupported principals found: ‘%#v’, please validate your policy document",
			diff)
		return err
	}
	return nil
}

// isValidConditions - returns nil if the given conditions valid and
// corresponding error otherwise.
func isValidConditions(actions set.StringSet, conditions policy.ConditionMap) (err error) {
	// Verify conditions should be valid. Validate if only
	// supported condition keys are present and return error
	// otherwise.
	conditionKeyVal := make(map[string]set.StringSet)
	for conditionType := range conditions {
		if !supportedConditionsType.Contains(conditionType) {
			err = fmt.Errorf("Unsupported condition type '%s', please validate your policy document", conditionType)
			return err
		}
		for key, value := range conditions[conditionType] {
			if !supportedConditionsKey.Contains(key) {
				err = fmt.Errorf("Unsupported condition key '%s', please validate your policy document", conditionType)
				return err
			}

			compatibleActions := conditionKeyActionMap[key]
			if !compatibleActions.IsEmpty() &&
				compatibleActions.Intersection(actions).IsEmpty() {
				err = fmt.Errorf("Unsupported condition key %s for the given actions %s, "+
					"please validate your policy document", key, actions)
				return err
			}

			conditionVal, ok := conditionKeyVal[key]
			if ok && !value.Intersection(conditionVal).IsEmpty() {
				err = fmt.Errorf("Ambigious condition values for key '%s', please validate your policy document", key)
				return err
			}
			conditionKeyVal[key] = value
		}
	}
	return nil
}

// List of actions for which prefixes are not allowed.
var invalidPrefixActions = set.StringSet{
	"s3:GetBucketLocation":          {},
	"s3:ListBucket":                 {},
	"s3:ListBucketMultipartUploads": {},
	// Add actions which do not honor prefixes.
}

// resourcePrefix - provides the prefix removing any wildcards.
func resourcePrefix(resource string) string {
	if strings.HasSuffix(resource, "*") {
		resource = strings.TrimSuffix(resource, "*")
	}
	return resource
}

// checkBucketPolicyResources validates Resources in unmarshalled bucket policy structure.
// - Resources are validated against the given set of Actions.
// -
func checkBucketPolicyResources(bucket string, bucketPolicy policy.BucketAccessPolicy) APIErrorCode {
	// Validate statements for special actions and collect resources
	// for others to validate nesting.
	var resourceMap = set.NewStringSet()
	for _, statement := range bucketPolicy.Statements {
		for action := range statement.Actions {
			for resource := range statement.Resources {
				resourcePrefix := strings.SplitAfter(resource, bucketARNPrefix)[1]
				if _, ok := invalidPrefixActions[action]; ok {
					// Resource prefix is not equal to bucket for
					// prefix invalid actions, reject them.
					if resourcePrefix != bucket {
						return ErrMalformedPolicy
					}
				} else {
					// For all other actions validate if resourcePrefix begins
					// with bucket name, if not reject them.
					if strings.Split(resourcePrefix, "/")[0] != bucket {
						return ErrMalformedPolicy
					}
					// All valid resources collect them separately to verify nesting.
					resourceMap.Add(resourcePrefix)
				}
			}
		}
	}

	var resources []string
	for resource := range resourceMap {
		resources = append(resources, resourcePrefix(resource))
	}

	// Sort strings as shorter first.
	sort.Strings(resources)

	for len(resources) > 1 {
		var resource string
		resource, resources = resources[0], resources[1:]
		// Loop through all resources, if one of them matches with
		// previous shorter one, it means we have detected
		// nesting. Reject such rules.
		for _, otherResource := range resources {
			// Common prefix reject such rules.
			if hasPrefix(otherResource, resource) {
				return ErrPolicyNesting
			}
		}
	}

	// No errors found.
	return ErrNone
}

// parseBucketPolicy - parses and validates if bucket policy is of
// proper JSON and follows allowed restrictions with policy standards.
func parseBucketPolicy(bucketPolicyReader io.Reader, bktPolicy *policy.BucketAccessPolicy) (err error) {
	// Parse bucket policy reader.
	decoder := json.NewDecoder(bucketPolicyReader)
	if err = decoder.Decode(bktPolicy); err != nil {
		return err
	}

	// Policy version cannot be empty.
	if len(bktPolicy.Version) == 0 {
		err = errors.New("Policy version cannot be empty")
		return err
	}

	// Policy statements cannot be empty.
	if len(bktPolicy.Statements) == 0 {
		err = errors.New("Policy statement cannot be empty")
		return err
	}

	// Loop through all policy statements and validate entries.
	for _, statement := range bktPolicy.Statements {
		// Statement effect should be valid.
		if err := isValidEffect(statement.Effect); err != nil {
			return err
		}
		// Statement principal should be supported format.
		if err := isValidPrincipals(statement.Principal); err != nil {
			return err
		}
		// Statement actions should be valid.
		if err := isValidActions(statement.Actions); err != nil {
			return err
		}
		// Statement resources should be valid.
		if err := isValidResources(statement.Resources); err != nil {
			return err
		}
		// Statement conditions should be valid.
		if err := isValidConditions(statement.Actions, statement.Conditions); err != nil {
			return err
		}
	}

	// Separate deny and allow statements, so that we can apply deny
	// statements in the beginning followed by Allow statements.
	var denyStatements []policy.Statement
	var allowStatements []policy.Statement
	for _, statement := range bktPolicy.Statements {
		if statement.Effect == "Deny" {
			denyStatements = append(denyStatements, statement)
			continue
		}

		// else if statement.Effect == "Allow"
		allowStatements = append(allowStatements, statement)
	}

	// Deny statements are enforced first once matched.
	bktPolicy.Statements = append(denyStatements, allowStatements...)

	// Return successfully parsed policy structure.
	return nil
}

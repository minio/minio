/*
 * Minio Cloud Storage, (C) 2015 Minio, Inc.
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
	"encoding/base64"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"time"
)

// toString - Safely convert interface to string without causing panic.
func toString(val interface{}) string {
	switch v := val.(type) {
	case string:
		return v
	}
	return ""
}

// toInteger _ Safely convert interface to integer without causing panic.
func toInteger(val interface{}) int {
	switch v := val.(type) {
	case int:
		return v
	}
	return 0
}

// isString - Safely check if val is of type string without causing panic.
func isString(val interface{}) bool {
	switch val.(type) {
	case string:
		return true
	}
	return false
}

// PostPolicyForm provides strict static type conversion and validation for Amazon S3's POST policy JSON string.
type PostPolicyForm struct {
	Expiration time.Time // Expiration date and time of the POST policy.
	Conditions struct {  // Conditional policy structure.
		Policies map[string]struct {
			Operator string
			Value    string
		}
		ContentLengthRange struct {
			Min int
			Max int
		}
	}
}

// parsePostPolicyFormV4 - Parse JSON policy string into typed POostPolicyForm structure.
func parsePostPolicyFormV4(policy string) (PostPolicyForm, error) {
	// Convert po into interfaces and
	// perform strict type conversion using reflection.
	var rawPolicy struct {
		Expiration string        `json:"expiration"`
		Conditions []interface{} `json:"conditions"`
	}

	err := json.Unmarshal([]byte(policy), &rawPolicy)
	if err != nil {
		return PostPolicyForm{}, err
	}

	parsedPolicy := PostPolicyForm{}

	// Parse expiry time.
	parsedPolicy.Expiration, err = time.Parse(time.RFC3339Nano, rawPolicy.Expiration)
	if err != nil {
		return PostPolicyForm{}, err
	}
	parsedPolicy.Conditions.Policies = make(map[string]struct {
		Operator string
		Value    string
	})

	// Parse conditions.
	for _, val := range rawPolicy.Conditions {
		switch condt := val.(type) {
		case map[string]interface{}: // Handle key:value map types.
			for k, v := range condt {
				if !isString(v) { // Pre-check value type.
					// All values must be of type string.
					return parsedPolicy, fmt.Errorf("Unknown type %s of conditional field value %s found in POST policy form.", reflect.TypeOf(condt).String(), condt)
				}
				// {"acl": "public-read" } is an alternate way to indicate - [ "eq", "$acl", "public-read" ]
				// In this case we will just collapse this into "eq" for all use cases.
				parsedPolicy.Conditions.Policies["$"+k] = struct {
					Operator string
					Value    string
				}{
					Operator: "eq",
					Value:    toString(v),
				}
			}
		case []interface{}: // Handle array types.
			if len(condt) != 3 { // Return error if we have insufficient elements.
				return parsedPolicy, fmt.Errorf("Malformed conditional fields %s of type %s found in POST policy form.", condt, reflect.TypeOf(condt).String())
			}
			switch toString(condt[0]) {
			case "eq", "starts-with":
				for _, v := range condt { // Pre-check all values for type.
					if !isString(v) {
						// All values must be of type string.
						return parsedPolicy, fmt.Errorf("Unknown type %s of conditional field value %s found in POST policy form.", reflect.TypeOf(condt).String(), condt)
					}
				}
				operator, matchType, value := toString(condt[0]), toString(condt[1]), toString(condt[2])
				parsedPolicy.Conditions.Policies[matchType] = struct {
					Operator string
					Value    string
				}{
					Operator: operator,
					Value:    value,
				}
			case "content-length-range":
				parsedPolicy.Conditions.ContentLengthRange = struct {
					Min int
					Max int
				}{
					Min: toInteger(condt[1]),
					Max: toInteger(condt[2]),
				}
			default:
				// Condition should be valid.
				return parsedPolicy, fmt.Errorf("Unknown type %s of conditional field value %s found in POST policy form.", reflect.TypeOf(condt).String(), condt)
			}
		default:
			return parsedPolicy, fmt.Errorf("Unknown field %s of type %s found in POST policy form.", condt, reflect.TypeOf(condt).String())
		}
	}
	return parsedPolicy, nil
}

// checkPostPolicy - apply policy conditions and validate input values.
func checkPostPolicy(formValues map[string]string) APIErrorCode {
	if formValues["X-Amz-Algorithm"] != signV4Algorithm {
		return ErrSignatureVersionNotSupported
	}
	/// Decoding policy
	policyBytes, err := base64.StdEncoding.DecodeString(formValues["Policy"])
	if err != nil {
		return ErrMalformedPOSTRequest
	}
	postPolicyForm, err := parsePostPolicyFormV4(string(policyBytes))
	if err != nil {
		return ErrMalformedPOSTRequest
	}
	if !postPolicyForm.Expiration.After(time.Now().UTC()) {
		return ErrPolicyAlreadyExpired
	}
	if postPolicyForm.Conditions.Policies["$bucket"].Operator == "eq" {
		if formValues["Bucket"] != postPolicyForm.Conditions.Policies["$bucket"].Value {
			return ErrAccessDenied
		}
	}
	if postPolicyForm.Conditions.Policies["$x-amz-date"].Operator == "eq" {
		if formValues["X-Amz-Date"] != postPolicyForm.Conditions.Policies["$x-amz-date"].Value {
			return ErrAccessDenied
		}
	}
	if postPolicyForm.Conditions.Policies["$Content-Type"].Operator == "starts-with" {
		if !strings.HasPrefix(formValues["Content-Type"], postPolicyForm.Conditions.Policies["$Content-Type"].Value) {
			return ErrAccessDenied
		}
	}
	if postPolicyForm.Conditions.Policies["$Content-Type"].Operator == "eq" {
		if formValues["Content-Type"] != postPolicyForm.Conditions.Policies["$Content-Type"].Value {
			return ErrAccessDenied
		}
	}
	if postPolicyForm.Conditions.Policies["$key"].Operator == "starts-with" {
		if !strings.HasPrefix(formValues["Key"], postPolicyForm.Conditions.Policies["$key"].Value) {
			return ErrAccessDenied
		}
	}
	if postPolicyForm.Conditions.Policies["$key"].Operator == "eq" {
		if formValues["Key"] != postPolicyForm.Conditions.Policies["$key"].Value {
			return ErrAccessDenied
		}
	}
	return ErrNone
}

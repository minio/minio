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

package cmd

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/bcicen/jstream"
	"github.com/minio/minio-go/v7/pkg/set"
)

// startWithConds - map which indicates if a given condition supports starts-with policy operator
var startsWithConds = map[string]bool{
	"$acl":                     true,
	"$bucket":                  false,
	"$cache-control":           true,
	"$content-type":            true,
	"$content-disposition":     true,
	"$content-encoding":        true,
	"$expires":                 true,
	"$key":                     true,
	"$success_action_redirect": true,
	"$redirect":                true,
	"$success_action_status":   false,
	"$x-amz-algorithm":         false,
	"$x-amz-credential":        false,
	"$x-amz-date":              false,
}

// Add policy conditionals.
const (
	policyCondEqual         = "eq"
	policyCondStartsWith    = "starts-with"
	policyCondContentLength = "content-length-range"
)

// toString - Safely convert interface to string without causing panic.
func toString(val interface{}) string {
	switch v := val.(type) {
	case string:
		return v
	default:
		return ""
	}
}

// toLowerString - safely convert interface to lower string
func toLowerString(val interface{}) string {
	return strings.ToLower(toString(val))
}

// toInteger _ Safely convert interface to integer without causing panic.
func toInteger(val interface{}) (int64, error) {
	switch v := val.(type) {
	case float64:
		return int64(v), nil
	case int64:
		return v, nil
	case int:
		return int64(v), nil
	case string:
		i, err := strconv.Atoi(v)
		return int64(i), err
	default:
		return 0, errors.New("Invalid number format")
	}
}

// isString - Safely check if val is of type string without causing panic.
func isString(val interface{}) bool {
	_, ok := val.(string)
	return ok
}

// ContentLengthRange - policy content-length-range field.
type contentLengthRange struct {
	Min   int64
	Max   int64
	Valid bool // If content-length-range was part of policy
}

// PostPolicyForm provides strict static type conversion and validation for Amazon S3's POST policy JSON string.
type PostPolicyForm struct {
	Expiration time.Time // Expiration date and time of the POST policy.
	Conditions struct {  // Conditional policy structure.
		Policies []struct {
			Operator string
			Key      string
			Value    string
		}
		ContentLengthRange contentLengthRange
	}
}

// implemented to ensure that duplicate keys in JSON
// are merged together into a single JSON key, also
// to remove any extraneous JSON bodies.
//
// Go stdlib doesn't support parsing JSON with duplicate
// keys, so we need to use this technique to merge the
// keys.
func sanitizePolicy(r io.Reader) (io.Reader, error) {
	var buf bytes.Buffer
	e := json.NewEncoder(&buf)
	d := jstream.NewDecoder(r, 0).ObjectAsKVS()
	sset := set.NewStringSet()
	for mv := range d.Stream() {
		var kvs jstream.KVS
		if mv.ValueType == jstream.Object {
			// This is a JSON object type (that preserves key order)
			kvs = mv.Value.(jstream.KVS)
			for _, kv := range kvs {
				if sset.Contains(kv.Key) {
					// Reject duplicate conditions or expiration.
					return nil, fmt.Errorf("input policy has multiple %s, please fix your client code", kv.Key)
				}
				sset.Add(kv.Key)
			}
			e.Encode(kvs)
		}
	}
	return &buf, d.Err()
}

// parsePostPolicyForm - Parse JSON policy string into typed PostPolicyForm structure.
func parsePostPolicyForm(r io.Reader) (PostPolicyForm, error) {
	reader, err := sanitizePolicy(r)
	if err != nil {
		return PostPolicyForm{}, err
	}

	d := json.NewDecoder(reader)

	// Convert po into interfaces and
	// perform strict type conversion using reflection.
	var rawPolicy struct {
		Expiration string        `json:"expiration"`
		Conditions []interface{} `json:"conditions"`
	}

	d.DisallowUnknownFields()
	if err := d.Decode(&rawPolicy); err != nil {
		return PostPolicyForm{}, err
	}

	parsedPolicy := PostPolicyForm{}

	// Parse expiry time.
	parsedPolicy.Expiration, err = time.Parse(time.RFC3339Nano, rawPolicy.Expiration)
	if err != nil {
		return PostPolicyForm{}, err
	}

	// Parse conditions.
	for _, val := range rawPolicy.Conditions {
		switch condt := val.(type) {
		case map[string]interface{}: // Handle key:value map types.
			for k, v := range condt {
				if !isString(v) { // Pre-check value type.
					// All values must be of type string.
					return parsedPolicy, fmt.Errorf("Unknown type %s of conditional field value %s found in POST policy form", reflect.TypeOf(condt).String(), condt)
				}
				// {"acl": "public-read" } is an alternate way to indicate - [ "eq", "$acl", "public-read" ]
				// In this case we will just collapse this into "eq" for all use cases.
				parsedPolicy.Conditions.Policies = append(parsedPolicy.Conditions.Policies, struct {
					Operator string
					Key      string
					Value    string
				}{
					policyCondEqual, "$" + strings.ToLower(k), toString(v),
				})
			}
		case []interface{}: // Handle array types.
			if len(condt) != 3 { // Return error if we have insufficient elements.
				return parsedPolicy, fmt.Errorf("Malformed conditional fields %s of type %s found in POST policy form", condt, reflect.TypeOf(condt).String())
			}
			switch toLowerString(condt[0]) {
			case policyCondEqual, policyCondStartsWith:
				for _, v := range condt { // Pre-check all values for type.
					if !isString(v) {
						// All values must be of type string.
						return parsedPolicy, fmt.Errorf("Unknown type %s of conditional field value %s found in POST policy form", reflect.TypeOf(condt).String(), condt)
					}
				}
				operator, matchType, value := toLowerString(condt[0]), toLowerString(condt[1]), toString(condt[2])
				if !strings.HasPrefix(matchType, "$") {
					return parsedPolicy, fmt.Errorf("Invalid according to Policy: Policy Condition failed: [%s, %s, %s]", operator, matchType, value)
				}
				parsedPolicy.Conditions.Policies = append(parsedPolicy.Conditions.Policies, struct {
					Operator string
					Key      string
					Value    string
				}{
					operator, matchType, value,
				})
			case policyCondContentLength:
				min, err := toInteger(condt[1])
				if err != nil {
					return parsedPolicy, err
				}

				max, err := toInteger(condt[2])
				if err != nil {
					return parsedPolicy, err
				}

				parsedPolicy.Conditions.ContentLengthRange = contentLengthRange{
					Min:   min,
					Max:   max,
					Valid: true,
				}
			default:
				// Condition should be valid.
				return parsedPolicy, fmt.Errorf("Unknown type %s of conditional field value %s found in POST policy form",
					reflect.TypeOf(condt).String(), condt)
			}
		default:
			return parsedPolicy, fmt.Errorf("Unknown field %s of type %s found in POST policy form",
				condt, reflect.TypeOf(condt).String())
		}
	}
	return parsedPolicy, nil
}

// checkPolicyCond returns a boolean to indicate if a condition is satisified according
// to the passed operator
func checkPolicyCond(op string, input1, input2 string) bool {
	switch op {
	case policyCondEqual:
		return input1 == input2
	case policyCondStartsWith:
		return strings.HasPrefix(input1, input2)
	}
	return false
}

// checkPostPolicy - apply policy conditions and validate input values.
// (http://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-HTTPPOSTConstructPolicy.html)
func checkPostPolicy(formValues http.Header, postPolicyForm PostPolicyForm) error {
	// Check if policy document expiry date is still not reached
	if !postPolicyForm.Expiration.After(UTCNow()) {
		return fmt.Errorf("Invalid according to Policy: Policy expired")
	}
	// map to store the metadata
	metaMap := make(map[string]string)
	for _, policy := range postPolicyForm.Conditions.Policies {
		if strings.HasPrefix(policy.Key, "$x-amz-meta-") {
			formCanonicalName := http.CanonicalHeaderKey(strings.TrimPrefix(policy.Key, "$"))
			metaMap[formCanonicalName] = policy.Value
		}
	}
	// Check if any extra metadata field is passed as input
	for key := range formValues {
		if strings.HasPrefix(key, "X-Amz-Meta-") {
			if _, ok := metaMap[key]; !ok {
				return fmt.Errorf("Invalid according to Policy: Extra input fields: %s", key)
			}
		}
	}

	// Flag to indicate if all policies conditions are satisfied
	var condPassed bool

	// Iterate over policy conditions and check them against received form fields
	for _, policy := range postPolicyForm.Conditions.Policies {
		// Form fields names are in canonical format, convert conditions names
		// to canonical for simplification purpose, so `$key` will become `Key`
		formCanonicalName := http.CanonicalHeaderKey(strings.TrimPrefix(policy.Key, "$"))
		// Operator for the current policy condition
		op := policy.Operator
		// If the current policy condition is known
		if startsWithSupported, condFound := startsWithConds[policy.Key]; condFound {
			// Check if the current condition supports starts-with operator
			if op == policyCondStartsWith && !startsWithSupported {
				return fmt.Errorf("Invalid according to Policy: Policy Condition failed")
			}
			// Check if current policy condition is satisfied
			condPassed = checkPolicyCond(op, formValues.Get(formCanonicalName), policy.Value)
			if !condPassed {
				return fmt.Errorf("Invalid according to Policy: Policy Condition failed")
			}
		} else {
			// This covers all conditions X-Amz-Meta-* and X-Amz-*
			if strings.HasPrefix(policy.Key, "$x-amz-meta-") || strings.HasPrefix(policy.Key, "$x-amz-") {
				// Check if policy condition is satisfied
				condPassed = checkPolicyCond(op, formValues.Get(formCanonicalName), policy.Value)
				if !condPassed {
					return fmt.Errorf("Invalid according to Policy: Policy Condition failed: [%s, %s, %s]", op, policy.Key, policy.Value)
				}
			}
		}
	}

	return nil
}

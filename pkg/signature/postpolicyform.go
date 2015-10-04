package signature

import (
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	"github.com/minio/minio/pkg/probe"
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

// ParsePostPolicyForm - Parse JSON policy string into typed POostPolicyForm structure.
func ParsePostPolicyForm(policy string) (PostPolicyForm, *probe.Error) {
	// Convert po into interfaces and
	// perform strict type conversion using reflection.
	var rawPolicy struct {
		Expiration string        `json:"expiration"`
		Conditions []interface{} `json:"conditions"`
	}

	e := json.Unmarshal([]byte(policy), &rawPolicy)
	if e != nil {
		return PostPolicyForm{}, probe.NewError(e)
	}

	parsedPolicy := PostPolicyForm{}

	// Parse expiry time.
	parsedPolicy.Expiration, e = time.Parse(time.RFC3339Nano, rawPolicy.Expiration)
	if e != nil {
		return PostPolicyForm{}, probe.NewError(e)
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
					return parsedPolicy, probe.NewError(fmt.Errorf("Unknown type â€˜%sâ€™ of conditional field value â€˜%sâ€™ found in POST policy form.",
						reflect.TypeOf(condt).String(), condt))
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
				return parsedPolicy, probe.NewError(fmt.Errorf("Malformed conditional fields â€˜%sâ€™ of type â€˜%sâ€™ found in POST policy form.",
					condt, reflect.TypeOf(condt).String()))
			}
			switch toString(condt[0]) {
			case "eq", "starts-with":
				for _, v := range condt { // Pre-check all values for type.
					if !isString(v) {
						// All values must be of type string.
						return parsedPolicy, probe.NewError(fmt.Errorf("Unknown type â€˜%sâ€™ of conditional field value â€˜%sâ€™ found in POST policy form.",
							reflect.TypeOf(condt).String(), condt))
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
				return parsedPolicy, probe.NewError(fmt.Errorf("Unknown type â€˜%sâ€™ of conditional field value â€˜%sâ€™ found in POST policy form.",
					reflect.TypeOf(condt).String(), condt))
			}
		default:
			return parsedPolicy, probe.NewError(fmt.Errorf("Unknown field â€˜%sâ€™ of type â€˜%sâ€™ found in POST policy form.",
				condt, reflect.TypeOf(condt).String()))
		}
	}
	return parsedPolicy, nil
}

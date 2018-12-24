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

package condition

import (
	"fmt"
	"net/http"
	"reflect"
	"strconv"
)

// nullFunc - Null condition function. It checks whether Key is not present in given
// values or not.
// For example,
//   1. if Key = S3XAmzCopySource and Value = true, at evaluate() it returns whether
//      S3XAmzCopySource is NOT in given value map or not.
//   2. if Key = S3XAmzCopySource and Value = false, at evaluate() it returns whether
//      S3XAmzCopySource is in given value map or not.
// https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_elements_condition_operators.html#Conditions_Null
type nullFunc struct {
	k     Key
	value bool
}

// evaluate() - evaluates to check whether Key is present in given values or not.
// Depending on condition boolean value, this function returns true or false.
func (f nullFunc) evaluate(values map[string][]string) bool {
	requestValue, ok := values[http.CanonicalHeaderKey(f.k.Name())]
	if !ok {
		requestValue = values[f.k.Name()]
	}

	if f.value {
		return len(requestValue) == 0
	}

	return len(requestValue) != 0
}

// key() - returns condition key which is used by this condition function.
func (f nullFunc) key() Key {
	return f.k
}

// name() - returns "Null" condition name.
func (f nullFunc) name() name {
	return null
}

func (f nullFunc) String() string {
	return fmt.Sprintf("%v:%v:%v", null, f.k, f.value)
}

// toMap - returns map representation of this function.
func (f nullFunc) toMap() map[Key]ValueSet {
	if !f.k.IsValid() {
		return nil
	}

	return map[Key]ValueSet{
		f.k: NewValueSet(NewBoolValue(f.value)),
	}
}

func newNullFunc(key Key, values ValueSet) (Function, error) {
	if len(values) != 1 {
		return nil, fmt.Errorf("only one value is allowed for Null condition")
	}

	var value bool
	for v := range values {
		switch v.GetType() {
		case reflect.Bool:
			value, _ = v.GetBool()
		case reflect.String:
			var err error
			s, _ := v.GetString()
			if value, err = strconv.ParseBool(s); err != nil {
				return nil, fmt.Errorf("value must be a boolean string for Null condition")
			}
		default:
			return nil, fmt.Errorf("value must be a boolean for Null condition")
		}
	}

	return &nullFunc{key, value}, nil
}

// NewNullFunc - returns new Null function.
func NewNullFunc(key Key, value bool) (Function, error) {
	return &nullFunc{key, value}, nil
}

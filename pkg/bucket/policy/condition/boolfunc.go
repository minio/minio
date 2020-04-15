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

package condition

import (
	"fmt"
	"net/http"
	"reflect"
	"strconv"
)

// booleanFunc - Bool condition function. It checks whether Key is true or false.
// https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_elements_condition_operators.html#Conditions_Boolean
type booleanFunc struct {
	k     Key
	value string
}

// evaluate() - evaluates to check whether Key is present in given values or not.
// Depending on condition boolean value, this function returns true or false.
func (f booleanFunc) evaluate(values map[string][]string) bool {
	requestValue, ok := values[http.CanonicalHeaderKey(f.k.Name())]
	if !ok {
		requestValue = values[f.k.Name()]
	}

	if len(requestValue) == 0 {
		return false
	}

	return f.value == requestValue[0]
}

// key() - returns condition key which is used by this condition function.
func (f booleanFunc) key() Key {
	return f.k
}

// name() - returns "Bool" condition name.
func (f booleanFunc) name() name {
	return boolean
}

func (f booleanFunc) String() string {
	return fmt.Sprintf("%v:%v:%v", boolean, f.k, f.value)
}

// toMap - returns map representation of this function.
func (f booleanFunc) toMap() map[Key]ValueSet {
	if !f.k.IsValid() {
		return nil
	}

	return map[Key]ValueSet{
		f.k: NewValueSet(NewStringValue(f.value)),
	}
}

func newBooleanFunc(key Key, values ValueSet) (Function, error) {
	if key != AWSSecureTransport {
		return nil, fmt.Errorf("only %v key is allowed for %v condition", AWSSecureTransport, boolean)
	}

	if len(values) != 1 {
		return nil, fmt.Errorf("only one value is allowed for boolean condition")
	}

	var value Value
	for v := range values {
		value = v
		switch v.GetType() {
		case reflect.Bool:
			if _, err := v.GetBool(); err != nil {
				return nil, err
			}
		case reflect.String:
			s, err := v.GetString()
			if err != nil {
				return nil, err
			}
			if _, err = strconv.ParseBool(s); err != nil {
				return nil, fmt.Errorf("value must be a boolean string for boolean condition")
			}
		default:
			return nil, fmt.Errorf("value must be a boolean for boolean condition")
		}
	}

	return &booleanFunc{key, value.String()}, nil
}

// NewBoolFunc - returns new Bool function.
func NewBoolFunc(key Key, value string) (Function, error) {
	return &booleanFunc{key, value}, nil
}

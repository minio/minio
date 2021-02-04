/*
 * MinIO Cloud Storage, (C) 2020 MinIO, Inc.
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

func toNumericEqualsFuncString(n name, key Key, value int) string {
	return fmt.Sprintf("%v:%v:%v", n, key, value)
}

// numericEqualsFunc - String equals function. It checks whether value by Key in given
// values map is in condition values.
// For example,
//   - if values = ["mybucket/foo"], at evaluate() it returns whether string
//     in value map for Key is in values.
type numericEqualsFunc struct {
	k     Key
	value int
}

// evaluate() - evaluates to check whether value by Key in given values is in
// condition values.
func (f numericEqualsFunc) evaluate(values map[string][]string) bool {
	requestValue, ok := values[http.CanonicalHeaderKey(f.k.Name())]
	if !ok {
		requestValue = values[f.k.Name()]
	}

	if len(requestValue) == 0 {
		return false
	}

	rvInt, err := strconv.Atoi(requestValue[0])
	if err != nil {
		return false
	}

	return f.value == rvInt
}

// key() - returns condition key which is used by this condition function.
func (f numericEqualsFunc) key() Key {
	return f.k
}

// name() - returns "NumericEquals" condition name.
func (f numericEqualsFunc) name() name {
	return numericEquals
}

func (f numericEqualsFunc) String() string {
	return toNumericEqualsFuncString(numericEquals, f.k, f.value)
}

// toMap - returns map representation of this function.
func (f numericEqualsFunc) toMap() map[Key]ValueSet {
	if !f.k.IsValid() {
		return nil
	}

	values := NewValueSet()
	values.Add(NewIntValue(f.value))

	return map[Key]ValueSet{
		f.k: values,
	}
}

// numericNotEqualsFunc - String not equals function. It checks whether value by Key in
// given values is NOT in condition values.
// For example,
//   - if values = ["mybucket/foo"], at evaluate() it returns whether string
//     in value map for Key is NOT in values.
type numericNotEqualsFunc struct {
	numericEqualsFunc
}

// evaluate() - evaluates to check whether value by Key in given values is NOT in
// condition values.
func (f numericNotEqualsFunc) evaluate(values map[string][]string) bool {
	return !f.numericEqualsFunc.evaluate(values)
}

// name() - returns "NumericNotEquals" condition name.
func (f numericNotEqualsFunc) name() name {
	return numericNotEquals
}

func (f numericNotEqualsFunc) String() string {
	return toNumericEqualsFuncString(numericNotEquals, f.numericEqualsFunc.k, f.numericEqualsFunc.value)
}

func valueToInt(n name, values ValueSet) (v int, err error) {
	if len(values) != 1 {
		return -1, fmt.Errorf("only one value is allowed for %s condition", n)
	}

	for vs := range values {
		switch vs.GetType() {
		case reflect.Int:
			if v, err = vs.GetInt(); err != nil {
				return -1, err
			}
		case reflect.String:
			s, err := vs.GetString()
			if err != nil {
				return -1, err
			}
			if v, err = strconv.Atoi(s); err != nil {
				return -1, fmt.Errorf("value %s must be a int for %s condition: %w", vs, n, err)
			}
		default:
			return -1, fmt.Errorf("value %s must be a int for %s condition", vs, n)
		}
	}

	return v, nil

}

// newNumericEqualsFunc - returns new NumericEquals function.
func newNumericEqualsFunc(key Key, values ValueSet) (Function, error) {
	v, err := valueToInt(numericEquals, values)
	if err != nil {
		return nil, err
	}

	return NewNumericEqualsFunc(key, v)
}

// NewNumericEqualsFunc - returns new NumericEquals function.
func NewNumericEqualsFunc(key Key, value int) (Function, error) {
	return &numericEqualsFunc{key, value}, nil
}

// newNumericNotEqualsFunc - returns new NumericNotEquals function.
func newNumericNotEqualsFunc(key Key, values ValueSet) (Function, error) {
	v, err := valueToInt(numericNotEquals, values)
	if err != nil {
		return nil, err
	}

	return NewNumericNotEqualsFunc(key, v)
}

// NewNumericNotEqualsFunc - returns new NumericNotEquals function.
func NewNumericNotEqualsFunc(key Key, value int) (Function, error) {
	return &numericNotEqualsFunc{numericEqualsFunc{key, value}}, nil
}

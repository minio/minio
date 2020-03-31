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
	"strconv"
)

func toNumericGreaterThanFuncString(n name, key Key, value int) string {
	return fmt.Sprintf("%v:%v:%v", n, key, value)
}

// numericGreaterThanFunc - String equals function. It checks whether value by Key in given
// values map is in condition values.
// For example,
//   - if values = ["mybucket/foo"], at evaluate() it returns whether string
//     in value map for Key is in values.
type numericGreaterThanFunc struct {
	k     Key
	value int
}

// evaluate() - evaluates to check whether value by Key in given values is in
// condition values.
func (f numericGreaterThanFunc) evaluate(values map[string][]string) bool {
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

	return rvInt > f.value
}

// key() - returns condition key which is used by this condition function.
func (f numericGreaterThanFunc) key() Key {
	return f.k
}

// name() - returns "NumericGreaterThan" condition name.
func (f numericGreaterThanFunc) name() name {
	return numericGreaterThan
}

func (f numericGreaterThanFunc) String() string {
	return toNumericGreaterThanFuncString(numericGreaterThan, f.k, f.value)
}

// toMap - returns map representation of this function.
func (f numericGreaterThanFunc) toMap() map[Key]ValueSet {
	if !f.k.IsValid() {
		return nil
	}

	values := NewValueSet()
	values.Add(NewIntValue(f.value))

	return map[Key]ValueSet{
		f.k: values,
	}
}

// numericGreaterThanEqualsFunc - String not equals function. It checks whether value by Key in
// given values is NOT in condition values.
// For example,
//   - if values = ["mybucket/foo"], at evaluate() it returns whether string
//     in value map for Key is NOT in values.
type numericGreaterThanEqualsFunc struct {
	numericGreaterThanFunc
}

// evaluate() - evaluates to check whether value by Key in given values is NOT in
// condition values.
func (f numericGreaterThanEqualsFunc) evaluate(values map[string][]string) bool {
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

	return rvInt >= f.value
}

// name() - returns "NumericGreaterThanEquals" condition name.
func (f numericGreaterThanEqualsFunc) name() name {
	return numericGreaterThanEquals
}

func (f numericGreaterThanEqualsFunc) String() string {
	return toNumericGreaterThanFuncString(numericGreaterThanEquals, f.numericGreaterThanFunc.k, f.numericGreaterThanFunc.value)
}

// newNumericGreaterThanFunc - returns new NumericGreaterThan function.
func newNumericGreaterThanFunc(key Key, values ValueSet) (Function, error) {
	v, err := valueToInt(numericGreaterThan, values)
	if err != nil {
		return nil, err
	}

	return NewNumericGreaterThanFunc(key, v)
}

// NewNumericGreaterThanFunc - returns new NumericGreaterThan function.
func NewNumericGreaterThanFunc(key Key, value int) (Function, error) {
	return &numericGreaterThanFunc{key, value}, nil
}

// newNumericGreaterThanEqualsFunc - returns new NumericGreaterThanEquals function.
func newNumericGreaterThanEqualsFunc(key Key, values ValueSet) (Function, error) {
	v, err := valueToInt(numericGreaterThanEquals, values)
	if err != nil {
		return nil, err
	}

	return NewNumericGreaterThanEqualsFunc(key, v)
}

// NewNumericGreaterThanEqualsFunc - returns new NumericGreaterThanEquals function.
func NewNumericGreaterThanEqualsFunc(key Key, value int) (Function, error) {
	return &numericGreaterThanEqualsFunc{numericGreaterThanFunc{key, value}}, nil
}

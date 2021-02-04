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

func toNumericLessThanFuncString(n name, key Key, value int) string {
	return fmt.Sprintf("%v:%v:%v", n, key, value)
}

// numericLessThanFunc - String equals function. It checks whether value by Key in given
// values map is in condition values.
// For example,
//   - if values = ["mybucket/foo"], at evaluate() it returns whether string
//     in value map for Key is in values.
type numericLessThanFunc struct {
	k     Key
	value int
}

// evaluate() - evaluates to check whether value by Key in given values is in
// condition values.
func (f numericLessThanFunc) evaluate(values map[string][]string) bool {
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

	return rvInt < f.value
}

// key() - returns condition key which is used by this condition function.
func (f numericLessThanFunc) key() Key {
	return f.k
}

// name() - returns "NumericLessThan" condition name.
func (f numericLessThanFunc) name() name {
	return numericLessThan
}

func (f numericLessThanFunc) String() string {
	return toNumericLessThanFuncString(numericLessThan, f.k, f.value)
}

// toMap - returns map representation of this function.
func (f numericLessThanFunc) toMap() map[Key]ValueSet {
	if !f.k.IsValid() {
		return nil
	}

	values := NewValueSet()
	values.Add(NewIntValue(f.value))

	return map[Key]ValueSet{
		f.k: values,
	}
}

// numericLessThanEqualsFunc - String not equals function. It checks whether value by Key in
// given values is NOT in condition values.
// For example,
//   - if values = ["mybucket/foo"], at evaluate() it returns whether string
//     in value map for Key is NOT in values.
type numericLessThanEqualsFunc struct {
	numericLessThanFunc
}

// evaluate() - evaluates to check whether value by Key in given values is NOT in
// condition values.
func (f numericLessThanEqualsFunc) evaluate(values map[string][]string) bool {
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

	return rvInt <= f.value
}

// name() - returns "NumericLessThanEquals" condition name.
func (f numericLessThanEqualsFunc) name() name {
	return numericLessThanEquals
}

func (f numericLessThanEqualsFunc) String() string {
	return toNumericLessThanFuncString(numericLessThanEquals, f.numericLessThanFunc.k, f.numericLessThanFunc.value)
}

// newNumericLessThanFunc - returns new NumericLessThan function.
func newNumericLessThanFunc(key Key, values ValueSet) (Function, error) {
	v, err := valueToInt(numericLessThan, values)
	if err != nil {
		return nil, err
	}

	return NewNumericLessThanFunc(key, v)
}

// NewNumericLessThanFunc - returns new NumericLessThan function.
func NewNumericLessThanFunc(key Key, value int) (Function, error) {
	return &numericLessThanFunc{key, value}, nil
}

// newNumericLessThanEqualsFunc - returns new NumericLessThanEquals function.
func newNumericLessThanEqualsFunc(key Key, values ValueSet) (Function, error) {
	v, err := valueToInt(numericLessThanEquals, values)
	if err != nil {
		return nil, err
	}

	return NewNumericLessThanEqualsFunc(key, v)
}

// NewNumericLessThanEqualsFunc - returns new NumericLessThanEquals function.
func NewNumericLessThanEqualsFunc(key Key, value int) (Function, error) {
	return &numericLessThanEqualsFunc{numericLessThanFunc{key, value}}, nil
}

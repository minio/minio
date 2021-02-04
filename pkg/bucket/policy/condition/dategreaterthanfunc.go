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
	"time"
)

func toDateGreaterThanFuncString(n name, key Key, value time.Time) string {
	return fmt.Sprintf("%v:%v:%v", n, key, value.Format(time.RFC3339))
}

// dateGreaterThanFunc - String equals function. It checks whether value by Key in given
// values map is in condition values.
// For example,
//   - if values = ["mybucket/foo"], at evaluate() it returns whether string
//     in value map for Key is in values.
type dateGreaterThanFunc struct {
	k     Key
	value time.Time
}

// evaluate() - evaluates to check whether value by Key in given values is in
// condition values.
func (f dateGreaterThanFunc) evaluate(values map[string][]string) bool {
	requestValue, ok := values[http.CanonicalHeaderKey(f.k.Name())]
	if !ok {
		requestValue = values[f.k.Name()]
	}

	if len(requestValue) == 0 {
		return false
	}

	t, err := time.Parse(time.RFC3339, requestValue[0])
	if err != nil {
		return false
	}

	return t.After(f.value)
}

// key() - returns condition key which is used by this condition function.
func (f dateGreaterThanFunc) key() Key {
	return f.k
}

// name() - returns "DateGreaterThan" condition name.
func (f dateGreaterThanFunc) name() name {
	return dateGreaterThan
}

func (f dateGreaterThanFunc) String() string {
	return toDateGreaterThanFuncString(dateGreaterThan, f.k, f.value)
}

// toMap - returns map representation of this function.
func (f dateGreaterThanFunc) toMap() map[Key]ValueSet {
	if !f.k.IsValid() {
		return nil
	}

	values := NewValueSet()
	values.Add(NewStringValue(f.value.Format(time.RFC3339)))

	return map[Key]ValueSet{
		f.k: values,
	}
}

// dateNotEqualsFunc - String not equals function. It checks whether value by Key in
// given values is NOT in condition values.
// For example,
//   - if values = ["mybucket/foo"], at evaluate() it returns whether string
//     in value map for Key is NOT in values.
type dateGreaterThanEqualsFunc struct {
	dateGreaterThanFunc
}

// evaluate() - evaluates to check whether value by Key in given values is NOT in
// condition values.
func (f dateGreaterThanEqualsFunc) evaluate(values map[string][]string) bool {
	requestValue, ok := values[http.CanonicalHeaderKey(f.k.Name())]
	if !ok {
		requestValue = values[f.k.Name()]
	}

	if len(requestValue) == 0 {
		return false
	}

	t, err := time.Parse(time.RFC3339, requestValue[0])
	if err != nil {
		return false
	}

	return t.After(f.value) || t.Equal(f.value)
}

// name() - returns "DateNotEquals" condition name.
func (f dateGreaterThanEqualsFunc) name() name {
	return dateGreaterThanEquals
}

func (f dateGreaterThanEqualsFunc) String() string {
	return toDateGreaterThanFuncString(dateNotEquals, f.dateGreaterThanFunc.k, f.dateGreaterThanFunc.value)
}

// newDateGreaterThanFunc - returns new DateGreaterThan function.
func newDateGreaterThanFunc(key Key, values ValueSet) (Function, error) {
	v, err := valueToTime(dateGreaterThan, values)
	if err != nil {
		return nil, err
	}

	return NewDateGreaterThanFunc(key, v)
}

// NewDateGreaterThanFunc - returns new DateGreaterThan function.
func NewDateGreaterThanFunc(key Key, value time.Time) (Function, error) {
	return &dateGreaterThanFunc{key, value}, nil
}

// newDateNotEqualsFunc - returns new DateNotEquals function.
func newDateGreaterThanEqualsFunc(key Key, values ValueSet) (Function, error) {
	v, err := valueToTime(dateNotEquals, values)
	if err != nil {
		return nil, err
	}

	return NewDateGreaterThanEqualsFunc(key, v)
}

// NewDateGreaterThanEqualsFunc - returns new DateNotEquals function.
func NewDateGreaterThanEqualsFunc(key Key, value time.Time) (Function, error) {
	return &dateGreaterThanEqualsFunc{dateGreaterThanFunc{key, value}}, nil
}

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

func toDateLessThanFuncString(n name, key Key, value time.Time) string {
	return fmt.Sprintf("%v:%v:%v", n, key, value.Format(time.RFC3339))
}

// dateLessThanFunc - String equals function. It checks whether value by Key in given
// values map is in condition values.
// For example,
//   - if values = ["mybucket/foo"], at evaluate() it returns whether string
//     in value map for Key is in values.
type dateLessThanFunc struct {
	k     Key
	value time.Time
}

// evaluate() - evaluates to check whether value by Key in given values is in
// condition values.
func (f dateLessThanFunc) evaluate(values map[string][]string) bool {
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

	return t.Before(f.value)
}

// key() - returns condition key which is used by this condition function.
func (f dateLessThanFunc) key() Key {
	return f.k
}

// name() - returns "DateLessThan" condition name.
func (f dateLessThanFunc) name() name {
	return dateLessThan
}

func (f dateLessThanFunc) String() string {
	return toDateLessThanFuncString(dateLessThan, f.k, f.value)
}

// toMap - returns map representation of this function.
func (f dateLessThanFunc) toMap() map[Key]ValueSet {
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
type dateLessThanEqualsFunc struct {
	dateLessThanFunc
}

// evaluate() - evaluates to check whether value by Key in given values is NOT in
// condition values.
func (f dateLessThanEqualsFunc) evaluate(values map[string][]string) bool {
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

	return t.Before(f.value) || t.Equal(f.value)
}

// name() - returns "DateNotEquals" condition name.
func (f dateLessThanEqualsFunc) name() name {
	return dateLessThanEquals
}

func (f dateLessThanEqualsFunc) String() string {
	return toDateLessThanFuncString(dateNotEquals, f.dateLessThanFunc.k, f.dateLessThanFunc.value)
}

// newDateLessThanFunc - returns new DateLessThan function.
func newDateLessThanFunc(key Key, values ValueSet) (Function, error) {
	v, err := valueToTime(dateLessThan, values)
	if err != nil {
		return nil, err
	}

	return NewDateLessThanFunc(key, v)
}

// NewDateLessThanFunc - returns new DateLessThan function.
func NewDateLessThanFunc(key Key, value time.Time) (Function, error) {
	return &dateLessThanFunc{key, value}, nil
}

// newDateNotEqualsFunc - returns new DateNotEquals function.
func newDateLessThanEqualsFunc(key Key, values ValueSet) (Function, error) {
	v, err := valueToTime(dateNotEquals, values)
	if err != nil {
		return nil, err
	}

	return NewDateLessThanEqualsFunc(key, v)
}

// NewDateLessThanEqualsFunc - returns new DateNotEquals function.
func NewDateLessThanEqualsFunc(key Key, value time.Time) (Function, error) {
	return &dateLessThanEqualsFunc{dateLessThanFunc{key, value}}, nil
}

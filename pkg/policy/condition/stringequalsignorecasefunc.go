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
	"sort"
	"strings"

	"github.com/minio/minio-go/pkg/set"
)

func toStringEqualsIgnoreCaseFuncString(n name, key Key, values set.StringSet) string {
	valueStrings := values.ToSlice()
	sort.Strings(valueStrings)

	return fmt.Sprintf("%v:%v:%v", n, key, valueStrings)
}

// stringEqualsIgnoreCaseFunc - String equals function. It checks whether value by Key in given
// values map is in condition values.
// For example,
//   - if values = ["mybucket/foo"], at evaluate() it returns whether string
//     in value map for Key is in values.
type stringEqualsIgnoreCaseFunc struct {
	k      Key
	values set.StringSet
}

// evaluate() - evaluates to check whether value by Key in given values is in
// condition values, ignores case.
func (f stringEqualsIgnoreCaseFunc) evaluate(values map[string][]string) bool {
	requestValue, ok := values[http.CanonicalHeaderKey(f.k.Name())]
	if !ok {
		requestValue = values[f.k.Name()]
	}

	for _, v := range requestValue {
		if !f.values.FuncMatch(strings.EqualFold, v).IsEmpty() {
			return true
		}
	}
	return false
}

// key() - returns condition key which is used by this condition function.
func (f stringEqualsIgnoreCaseFunc) key() Key {
	return f.k
}

// name() - returns "StringEqualsIgnoreCase" condition name.
func (f stringEqualsIgnoreCaseFunc) name() name {
	return stringEqualsIgnoreCase
}

func (f stringEqualsIgnoreCaseFunc) String() string {
	return toStringEqualsIgnoreCaseFuncString(stringEqualsIgnoreCase, f.k, f.values)
}

// toMap - returns map representation of this function.
func (f stringEqualsIgnoreCaseFunc) toMap() map[Key]ValueSet {
	if !f.k.IsValid() {
		return nil
	}

	values := NewValueSet()
	for _, value := range f.values.ToSlice() {
		values.Add(NewStringValue(value))
	}

	return map[Key]ValueSet{
		f.k: values,
	}
}

// stringNotEqualsIgnoreCaseFunc - String not equals function. It checks whether value by Key in
// given values is NOT in condition values.
// For example,
//   - if values = ["mybucket/foo"], at evaluate() it returns whether string
//     in value map for Key is NOT in values.
type stringNotEqualsIgnoreCaseFunc struct {
	stringEqualsIgnoreCaseFunc
}

// evaluate() - evaluates to check whether value by Key in given values is NOT in
// condition values.
func (f stringNotEqualsIgnoreCaseFunc) evaluate(values map[string][]string) bool {
	return !f.stringEqualsIgnoreCaseFunc.evaluate(values)
}

// name() - returns "StringNotEqualsIgnoreCase" condition name.
func (f stringNotEqualsIgnoreCaseFunc) name() name {
	return stringNotEqualsIgnoreCase
}

func (f stringNotEqualsIgnoreCaseFunc) String() string {
	return toStringEqualsIgnoreCaseFuncString(stringNotEqualsIgnoreCase, f.stringEqualsIgnoreCaseFunc.k, f.stringEqualsIgnoreCaseFunc.values)
}

func validateStringEqualsIgnoreCaseValues(n name, key Key, values set.StringSet) error {
	return validateStringEqualsValues(n, key, values)
}

// newStringEqualsIgnoreCaseFunc - returns new StringEqualsIgnoreCase function.
func newStringEqualsIgnoreCaseFunc(key Key, values ValueSet) (Function, error) {
	valueStrings, err := valuesToStringSlice(stringEqualsIgnoreCase, values)
	if err != nil {
		return nil, err
	}

	return NewStringEqualsIgnoreCaseFunc(key, valueStrings...)
}

// NewStringEqualsIgnoreCaseFunc - returns new StringEqualsIgnoreCase function.
func NewStringEqualsIgnoreCaseFunc(key Key, values ...string) (Function, error) {
	sset := set.CreateStringSet(values...)
	if err := validateStringEqualsIgnoreCaseValues(stringEqualsIgnoreCase, key, sset); err != nil {
		return nil, err
	}

	return &stringEqualsIgnoreCaseFunc{key, sset}, nil
}

// newStringNotEqualsIgnoreCaseFunc - returns new StringNotEqualsIgnoreCase function.
func newStringNotEqualsIgnoreCaseFunc(key Key, values ValueSet) (Function, error) {
	valueStrings, err := valuesToStringSlice(stringNotEqualsIgnoreCase, values)
	if err != nil {
		return nil, err
	}

	return NewStringNotEqualsIgnoreCaseFunc(key, valueStrings...)
}

// NewStringNotEqualsIgnoreCaseFunc - returns new StringNotEqualsIgnoreCase function.
func NewStringNotEqualsIgnoreCaseFunc(key Key, values ...string) (Function, error) {
	sset := set.CreateStringSet(values...)
	if err := validateStringEqualsIgnoreCaseValues(stringNotEqualsIgnoreCase, key, sset); err != nil {
		return nil, err
	}

	return &stringNotEqualsIgnoreCaseFunc{stringEqualsIgnoreCaseFunc{key, sset}}, nil
}

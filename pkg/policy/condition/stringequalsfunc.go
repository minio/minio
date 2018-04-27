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
	"sort"
	"strings"

	"github.com/minio/minio-go/pkg/set"
)

func toStringEqualsFuncString(n name, key Key, values set.StringSet) string {
	valueStrings := values.ToSlice()
	sort.Strings(valueStrings)

	return fmt.Sprintf("%v:%v:%v", n, key, valueStrings)
}

// stringEqualsFunc - String equals function. It checks whether value by Key in given
// values map is in condition values.
// For example,
//   - if values = ["mybucket/foo"], at evaluate() it returns whether string
//     in value map for Key is in values.
type stringEqualsFunc struct {
	k      Key
	values set.StringSet
}

// evaluate() - evaluates to check whether value by Key in given values is in
// condition values.
func (f stringEqualsFunc) evaluate(values map[string][]string) bool {
	requestValue := values[f.k.Name()]
	return !f.values.Intersection(set.CreateStringSet(requestValue...)).IsEmpty()
}

// key() - returns condition key which is used by this condition function.
func (f stringEqualsFunc) key() Key {
	return f.k
}

// name() - returns "StringEquals" condition name.
func (f stringEqualsFunc) name() name {
	return stringEquals
}

func (f stringEqualsFunc) String() string {
	return toStringEqualsFuncString(stringEquals, f.k, f.values)
}

// toMap - returns map representation of this function.
func (f stringEqualsFunc) toMap() map[Key]ValueSet {
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

// stringNotEqualsFunc - String not equals function. It checks whether value by Key in
// given values is NOT in condition values.
// For example,
//   - if values = ["mybucket/foo"], at evaluate() it returns whether string
//     in value map for Key is NOT in values.
type stringNotEqualsFunc struct {
	stringEqualsFunc
}

// evaluate() - evaluates to check whether value by Key in given values is NOT in
// condition values.
func (f stringNotEqualsFunc) evaluate(values map[string][]string) bool {
	return !f.stringEqualsFunc.evaluate(values)
}

// name() - returns "StringNotEquals" condition name.
func (f stringNotEqualsFunc) name() name {
	return stringNotEquals
}

func (f stringNotEqualsFunc) String() string {
	return toStringEqualsFuncString(stringNotEquals, f.stringEqualsFunc.k, f.stringEqualsFunc.values)
}

func valuesToStringSlice(n name, values ValueSet) ([]string, error) {
	valueStrings := []string{}

	for value := range values {
		// FIXME: if AWS supports non-string values, we would need to support it.
		s, err := value.GetString()
		if err != nil {
			return nil, fmt.Errorf("value must be a string for %v condition", n)
		}

		valueStrings = append(valueStrings, s)
	}

	return valueStrings, nil
}

func validateStringEqualsValues(n name, key Key, values set.StringSet) error {
	for _, s := range values.ToSlice() {
		switch key {
		case S3XAmzCopySource:
			tokens := strings.SplitN(s, "/", 2)
			if len(tokens) < 2 {
				return fmt.Errorf("invalid value '%v' for '%v' for %v condition", s, S3XAmzCopySource, n)
			}
			// FIXME: tokens[0] must be a valid bucket name.
		case S3XAmzServerSideEncryption:
			if s != "aws:kms" && s != "AES256" {
				return fmt.Errorf("invalid value '%v' for '%v' for %v condition", s, S3XAmzServerSideEncryption, n)
			}
		case S3XAmzMetadataDirective:
			if s != "COPY" && s != "REPLACE" {
				return fmt.Errorf("invalid value '%v' for '%v' for %v condition", s, S3XAmzMetadataDirective, n)
			}
		}
	}

	return nil
}

// newStringEqualsFunc - returns new StringEquals function.
func newStringEqualsFunc(key Key, values ValueSet) (Function, error) {
	valueStrings, err := valuesToStringSlice(stringEquals, values)
	if err != nil {
		return nil, err
	}

	return NewStringEqualsFunc(key, valueStrings...)
}

// NewStringEqualsFunc - returns new StringEquals function.
func NewStringEqualsFunc(key Key, values ...string) (Function, error) {
	sset := set.CreateStringSet(values...)
	if err := validateStringEqualsValues(stringEquals, key, sset); err != nil {
		return nil, err
	}

	return &stringEqualsFunc{key, sset}, nil
}

// newStringNotEqualsFunc - returns new StringNotEquals function.
func newStringNotEqualsFunc(key Key, values ValueSet) (Function, error) {
	valueStrings, err := valuesToStringSlice(stringNotEquals, values)
	if err != nil {
		return nil, err
	}

	return NewStringNotEqualsFunc(key, valueStrings...)
}

// NewStringNotEqualsFunc - returns new StringNotEquals function.
func NewStringNotEqualsFunc(key Key, values ...string) (Function, error) {
	sset := set.CreateStringSet(values...)
	if err := validateStringEqualsValues(stringNotEquals, key, sset); err != nil {
		return nil, err
	}

	return &stringNotEqualsFunc{stringEqualsFunc{key, sset}}, nil
}

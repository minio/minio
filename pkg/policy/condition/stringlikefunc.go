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
	"github.com/minio/minio/pkg/wildcard"
)

func toStringLikeFuncString(n name, key Key, values set.StringSet) string {
	valueStrings := values.ToSlice()
	sort.Strings(valueStrings)

	return fmt.Sprintf("%v:%v:%v", n, key, valueStrings)
}

// stringLikeFunc - String like function. It checks whether value by Key in given
// values map is widcard matching in condition values.
// For example,
//   - if values = ["mybucket/foo*"], at evaluate() it returns whether string
//     in value map for Key is wildcard matching in values.
type stringLikeFunc struct {
	k      Key
	values set.StringSet
}

// evaluate() - evaluates to check whether value by Key in given values is wildcard
// matching in condition values.
func (f stringLikeFunc) evaluate(values map[string][]string) bool {
	for _, v := range values[f.k.Name()] {
		if !f.values.FuncMatch(wildcard.Match, v).IsEmpty() {
			return true
		}
	}

	return false
}

// key() - returns condition key which is used by this condition function.
func (f stringLikeFunc) key() Key {
	return f.k
}

// name() - returns "StringLike" function name.
func (f stringLikeFunc) name() name {
	return stringLike
}

func (f stringLikeFunc) String() string {
	return toStringLikeFuncString(stringLike, f.k, f.values)
}

// toMap - returns map representation of this function.
func (f stringLikeFunc) toMap() map[Key]ValueSet {
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

// stringNotLikeFunc - String not like function. It checks whether value by Key in given
// values map is NOT widcard matching in condition values.
// For example,
//   - if values = ["mybucket/foo*"], at evaluate() it returns whether string
//     in value map for Key is NOT wildcard matching in values.
type stringNotLikeFunc struct {
	stringLikeFunc
}

// evaluate() - evaluates to check whether value by Key in given values is NOT wildcard
// matching in condition values.
func (f stringNotLikeFunc) evaluate(values map[string][]string) bool {
	return !f.stringLikeFunc.evaluate(values)
}

// name() - returns "StringNotLike" function name.
func (f stringNotLikeFunc) name() name {
	return stringNotLike
}

func (f stringNotLikeFunc) String() string {
	return toStringLikeFuncString(stringNotLike, f.stringLikeFunc.k, f.stringLikeFunc.values)
}

func validateStringLikeValues(n name, key Key, values set.StringSet) error {
	for _, s := range values.ToSlice() {
		switch key {
		case S3XAmzCopySource:
			tokens := strings.SplitN(s, "/", 2)
			if len(tokens) < 2 {
				return fmt.Errorf("invalid value '%v' for '%v' in %v condition", s, key, n)
			}

			// FIXME: tokens[0] must be a valid bucket name.
		}
	}

	return nil
}

// newStringLikeFunc - returns new StringLike function.
func newStringLikeFunc(key Key, values ValueSet) (Function, error) {
	valueStrings, err := valuesToStringSlice(stringLike, values)
	if err != nil {
		return nil, err
	}

	return NewStringLikeFunc(key, valueStrings...)
}

// NewStringLikeFunc - returns new StringLike function.
func NewStringLikeFunc(key Key, values ...string) (Function, error) {
	sset := set.CreateStringSet(values...)
	if err := validateStringLikeValues(stringLike, key, sset); err != nil {
		return nil, err
	}

	return &stringLikeFunc{key, sset}, nil
}

// newStringNotLikeFunc - returns new StringNotLike function.
func newStringNotLikeFunc(key Key, values ValueSet) (Function, error) {
	valueStrings, err := valuesToStringSlice(stringNotLike, values)
	if err != nil {
		return nil, err
	}

	return NewStringNotLikeFunc(key, valueStrings...)
}

// NewStringNotLikeFunc - returns new StringNotLike function.
func NewStringNotLikeFunc(key Key, values ...string) (Function, error) {
	sset := set.CreateStringSet(values...)
	if err := validateStringLikeValues(stringNotLike, key, sset); err != nil {
		return nil, err
	}

	return &stringNotLikeFunc{stringLikeFunc{key, sset}}, nil
}

// Copyright (c) 2015-2021 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package condition

import (
	"encoding/base64"
	"fmt"
	"sort"
	"strings"

	"github.com/minio/minio-go/v7/pkg/s3utils"
	"github.com/minio/minio-go/v7/pkg/set"
	"github.com/minio/minio/pkg/wildcard"
)

func substitute(values map[string][]string) func(string) string {
	return func(v string) string {
		for _, key := range CommonKeys {
			// Empty values are not supported for policy variables.
			if rvalues, ok := values[key.Name()]; ok && rvalues[0] != "" {
				v = strings.Replace(v, key.VarName(), rvalues[0], -1)
			}
		}
		return v
	}
}

type stringFunc struct {
	n          name
	k          Key
	values     set.StringSet
	ignoreCase bool
	base64     bool
	negate     bool
}

func (f stringFunc) eval(values map[string][]string) bool {
	rvalues := set.CreateStringSet(getValuesByKey(values, f.k.Name())...)
	fvalues := f.values.ApplyFunc(substitute(values))
	if f.ignoreCase {
		rvalues = rvalues.ApplyFunc(strings.ToLower)
		fvalues = fvalues.ApplyFunc(strings.ToLower)
	}
	ivalues := fvalues.Intersection(rvalues)
	if f.n.qualifier == forAllValues {
		return rvalues.IsEmpty() || fvalues.Equals(ivalues)
	}
	return !ivalues.IsEmpty()
}

func (f stringFunc) evaluate(values map[string][]string) bool {
	result := f.eval(values)
	if f.negate {
		return !result
	}
	return result
}

func (f stringFunc) key() Key {
	return f.k
}

func (f stringFunc) name() name {
	return f.n
}

func (f stringFunc) String() string {
	valueStrings := f.values.ToSlice()
	sort.Strings(valueStrings)
	return fmt.Sprintf("%v:%v:%v", f.n, f.k, valueStrings)
}

func (f stringFunc) toMap() map[Key]ValueSet {
	if !f.k.IsValid() {
		return nil
	}

	values := NewValueSet()
	for _, value := range f.values.ToSlice() {
		if f.base64 {
			values.Add(NewStringValue(base64.StdEncoding.EncodeToString([]byte(value))))
		} else {
			values.Add(NewStringValue(value))
		}
	}

	return map[Key]ValueSet{
		f.k: values,
	}
}

func (f stringFunc) copy() stringFunc {
	return stringFunc{
		n:          f.n,
		k:          f.k,
		values:     f.values.Union(set.NewStringSet()),
		ignoreCase: f.ignoreCase,
		base64:     f.base64,
		negate:     f.negate,
	}
}

func (f stringFunc) clone() Function {
	c := f.copy()
	return &c
}

// stringLikeFunc - String like function. It checks whether value by Key in given
// values map is widcard matching in condition values.
// For example,
//   - if values = ["mybucket/foo*"], at evaluate() it returns whether string
//     in value map for Key is wildcard matching in values.
type stringLikeFunc struct {
	stringFunc
}

func (f stringLikeFunc) eval(values map[string][]string) bool {
	rvalues := getValuesByKey(values, f.k.Name())
	if f.n.qualifier == forAllValues && len(rvalues) == 0 {
		return true
	}
	fvalues := f.values.ApplyFunc(substitute(values))
	if f.n.qualifier == forAllValues && len(rvalues) != len(fvalues) {
		return false
	}

	result := false
	for _, v := range rvalues {
		result = !fvalues.FuncMatch(wildcard.Match, v).IsEmpty()
		if f.n.qualifier == forAllValues {
			if !result {
				return false
			}
		} else if result {
			return true
		}
	}
	return result
}

// evaluate() - evaluates to check whether value by Key in given values is wildcard
// matching in condition values.
func (f stringLikeFunc) evaluate(values map[string][]string) bool {
	result := f.eval(values)
	if f.negate {
		return !result
	}
	return result
}

func (f stringLikeFunc) clone() Function {
	return &stringLikeFunc{stringFunc: f.copy()}
}

func valuesToStringSlice(n string, values ValueSet) ([]string, error) {
	valueStrings := []string{}

	for value := range values {
		s, err := value.GetString()
		if err != nil {
			return nil, fmt.Errorf("value must be a string for %v condition", n)
		}

		valueStrings = append(valueStrings, s)
	}

	return valueStrings, nil
}

func validateStringValues(n string, key Key, values set.StringSet) error {
	for _, s := range values.ToSlice() {
		switch key {
		case S3XAmzCopySource:
			bucket, object := path2BucketAndObject(s)
			if object == "" {
				return fmt.Errorf("invalid value '%v' for '%v' for %v condition", s, S3XAmzCopySource, n)
			}
			if err := s3utils.CheckValidBucketName(bucket); err != nil {
				return err
			}
		}

		if n == stringLike || n == stringNotLike {
			continue
		}

		switch key {
		case S3XAmzServerSideEncryption, S3XAmzServerSideEncryptionCustomerAlgorithm:
			if s != "AES256" {
				return fmt.Errorf("invalid value '%v' for '%v' for %v condition", s, S3XAmzServerSideEncryption, n)
			}
		case S3XAmzMetadataDirective:
			if s != "COPY" && s != "REPLACE" {
				return fmt.Errorf("invalid value '%v' for '%v' for %v condition", s, S3XAmzMetadataDirective, n)
			}
		case S3XAmzContentSha256:
			if s == "" {
				return fmt.Errorf("invalid empty value for '%v' for %v condition", S3XAmzContentSha256, n)
			}
		}
	}

	return nil
}

func newStringFunc(n string, key Key, values ValueSet, qualifier string, ignoreCase, base64, negate bool) (*stringFunc, error) {
	valueStrings, err := valuesToStringSlice(n, values)
	if err != nil {
		return nil, err
	}

	sset := set.CreateStringSet(valueStrings...)
	if err := validateStringValues(n, key, sset); err != nil {
		return nil, err
	}

	if _, found := qualifiers[qualifier]; qualifier != "" && !found {
		return nil, fmt.Errorf("set qualifier must be %v or %v", forAllValues, forAllValues)
	}

	return &stringFunc{
		n:          name{name: n, qualifier: qualifier},
		k:          key,
		values:     sset,
		ignoreCase: ignoreCase,
		base64:     base64,
		negate:     negate,
	}, nil
}

// newStringEqualsFunc - returns new StringEquals function.
func newStringEqualsFunc(key Key, values ValueSet, qualifier string) (Function, error) {
	return newStringFunc(stringEquals, key, values, qualifier, false, false, false)
}

// NewStringEqualsFunc - returns new StringEquals function.
func NewStringEqualsFunc(qualifier string, key Key, values ...string) (Function, error) {
	vset := NewValueSet()
	for _, value := range values {
		vset.Add(NewStringValue(value))
	}
	return newStringFunc(stringEquals, key, vset, qualifier, false, false, false)
}

// newStringNotEqualsFunc - returns new StringNotEquals function.
func newStringNotEqualsFunc(key Key, values ValueSet, qualifier string) (Function, error) {
	return newStringFunc(stringNotEquals, key, values, qualifier, false, false, true)
}

// NewStringNotEqualsFunc - returns new StringNotEquals function.
func NewStringNotEqualsFunc(qualifier string, key Key, values ...string) (Function, error) {
	vset := NewValueSet()
	for _, value := range values {
		vset.Add(NewStringValue(value))
	}
	return newStringFunc(stringNotEquals, key, vset, qualifier, false, false, true)
}

// newStringEqualsIgnoreCaseFunc - returns new StringEqualsIgnoreCase function.
func newStringEqualsIgnoreCaseFunc(key Key, values ValueSet, qualifier string) (Function, error) {
	return newStringFunc(stringEqualsIgnoreCase, key, values, qualifier, true, false, false)
}

// NewStringEqualsIgnoreCaseFunc - returns new StringEqualsIgnoreCase function.
func NewStringEqualsIgnoreCaseFunc(qualifier string, key Key, values ...string) (Function, error) {
	vset := NewValueSet()
	for _, value := range values {
		vset.Add(NewStringValue(value))
	}
	return newStringFunc(stringEqualsIgnoreCase, key, vset, qualifier, true, false, false)
}

// newStringNotEqualsIgnoreCaseFunc - returns new StringNotEqualsIgnoreCase function.
func newStringNotEqualsIgnoreCaseFunc(key Key, values ValueSet, qualifier string) (Function, error) {
	return newStringFunc(stringNotEqualsIgnoreCase, key, values, qualifier, true, false, true)
}

// NewStringNotEqualsIgnoreCaseFunc - returns new StringNotEqualsIgnoreCase function.
func NewStringNotEqualsIgnoreCaseFunc(qualifier string, key Key, values ...string) (Function, error) {
	vset := NewValueSet()
	for _, value := range values {
		vset.Add(NewStringValue(value))
	}
	return newStringFunc(stringNotEqualsIgnoreCase, key, vset, qualifier, true, false, true)
}

// newBinaryEqualsFunc - returns new BinaryEquals function.
func newBinaryEqualsFunc(key Key, values ValueSet, qualifier string) (Function, error) {
	valueStrings, err := valuesToStringSlice(binaryEquals, values)
	if err != nil {
		return nil, err
	}

	return NewBinaryEqualsFunc(qualifier, key, valueStrings...)
}

// NewBinaryEqualsFunc - returns new BinaryEquals function.
func NewBinaryEqualsFunc(qualifier string, key Key, values ...string) (Function, error) {
	vset := NewValueSet()
	for _, value := range values {
		data, err := base64.StdEncoding.DecodeString(value)
		if err != nil {
			return nil, err
		}
		vset.Add(NewStringValue(string(data)))
	}
	return newStringFunc(binaryEquals, key, vset, qualifier, false, true, false)
}

// newStringLikeFunc - returns new StringLike function.
func newStringLikeFunc(key Key, values ValueSet, qualifier string) (Function, error) {
	sf, err := newStringFunc(stringLike, key, values, qualifier, false, false, false)
	if err != nil {
		return nil, err
	}

	return &stringLikeFunc{*sf}, nil
}

// NewStringLikeFunc - returns new StringLike function.
func NewStringLikeFunc(qualifier string, key Key, values ...string) (Function, error) {
	vset := NewValueSet()
	for _, value := range values {
		vset.Add(NewStringValue(value))
	}
	return newStringLikeFunc(key, vset, qualifier)
}

// newStringNotLikeFunc - returns new StringNotLike function.
func newStringNotLikeFunc(key Key, values ValueSet, qualifier string) (Function, error) {
	sf, err := newStringFunc(stringNotLike, key, values, qualifier, false, false, true)
	if err != nil {
		return nil, err
	}

	return &stringLikeFunc{*sf}, nil
}

// NewStringNotLikeFunc - returns new StringNotLike function.
func NewStringNotLikeFunc(qualifier string, key Key, values ...string) (Function, error) {
	vset := NewValueSet()
	for _, value := range values {
		vset.Add(NewStringValue(value))
	}
	return newStringNotLikeFunc(key, vset, qualifier)
}

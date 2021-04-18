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
	"net/http"
	"sort"

	"github.com/minio/minio-go/v7/pkg/s3utils"
	"github.com/minio/minio-go/v7/pkg/set"
)

func toBinaryEqualsFuncString(n name, key Key, values set.StringSet) string {
	valueStrings := values.ToSlice()
	sort.Strings(valueStrings)

	return fmt.Sprintf("%v:%v:%v", n, key, valueStrings)
}

// binaryEqualsFunc - String equals function. It checks whether value by Key in given
// values map is in condition values.
// For example,
//   - if values = ["mybucket/foo"], at evaluate() it returns whether string
//     in value map for Key is in values.
type binaryEqualsFunc struct {
	k      Key
	values set.StringSet
}

// evaluate() - evaluates to check whether value by Key in given values is in
// condition values.
func (f binaryEqualsFunc) evaluate(values map[string][]string) bool {
	requestValue, ok := values[http.CanonicalHeaderKey(f.k.Name())]
	if !ok {
		requestValue = values[f.k.Name()]
	}

	fvalues := f.values.ApplyFunc(substFuncFromValues(values))
	return !fvalues.Intersection(set.CreateStringSet(requestValue...)).IsEmpty()
}

// key() - returns condition key which is used by this condition function.
func (f binaryEqualsFunc) key() Key {
	return f.k
}

// name() - returns "BinaryEquals" condition name.
func (f binaryEqualsFunc) name() name {
	return binaryEquals
}

func (f binaryEqualsFunc) String() string {
	return toBinaryEqualsFuncString(binaryEquals, f.k, f.values)
}

// toMap - returns map representation of this function.
func (f binaryEqualsFunc) toMap() map[Key]ValueSet {
	if !f.k.IsValid() {
		return nil
	}

	values := NewValueSet()
	for _, value := range f.values.ToSlice() {
		values.Add(NewStringValue(base64.StdEncoding.EncodeToString([]byte(value))))
	}

	return map[Key]ValueSet{
		f.k: values,
	}
}

func validateBinaryEqualsValues(n name, key Key, values set.StringSet) error {
	vslice := values.ToSlice()
	for _, s := range vslice {
		sbytes, err := base64.StdEncoding.DecodeString(s)
		if err != nil {
			return err
		}
		values.Remove(s)
		s = string(sbytes)
		switch key {
		case S3XAmzCopySource:
			bucket, object := path2BucketAndObject(s)
			if object == "" {
				return fmt.Errorf("invalid value '%v' for '%v' for %v condition", s, S3XAmzCopySource, n)
			}
			if err = s3utils.CheckValidBucketName(bucket); err != nil {
				return err
			}
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
		values.Add(s)
	}

	return nil
}

// newBinaryEqualsFunc - returns new BinaryEquals function.
func newBinaryEqualsFunc(key Key, values ValueSet) (Function, error) {
	valueStrings, err := valuesToStringSlice(binaryEquals, values)
	if err != nil {
		return nil, err
	}

	return NewBinaryEqualsFunc(key, valueStrings...)
}

// NewBinaryEqualsFunc - returns new BinaryEquals function.
func NewBinaryEqualsFunc(key Key, values ...string) (Function, error) {
	sset := set.CreateStringSet(values...)
	if err := validateBinaryEqualsValues(binaryEquals, key, sset); err != nil {
		return nil, err
	}

	return &binaryEqualsFunc{key, sset}, nil
}

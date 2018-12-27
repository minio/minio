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
	"encoding/base64"
	"fmt"
	"net/http"
	"sort"

	"github.com/minio/minio-go/pkg/s3utils"
	"github.com/minio/minio-go/pkg/set"
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

	return !f.values.Intersection(set.CreateStringSet(requestValue...)).IsEmpty()
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
		case S3XAmzServerSideEncryption:
			if s != "aws:kms" && s != "AES256" {
				return fmt.Errorf("invalid value '%v' for '%v' for %v condition", s, S3XAmzServerSideEncryption, n)
			}
		case S3XAmzMetadataDirective:
			if s != "COPY" && s != "REPLACE" {
				return fmt.Errorf("invalid value '%v' for '%v' for %v condition", s, S3XAmzMetadataDirective, n)
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

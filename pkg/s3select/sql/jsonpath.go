/*
 * MinIO Cloud Storage, (C) 2019 MinIO, Inc.
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

package sql

import (
	"errors"

	"github.com/bcicen/jstream"
)

var (
	errKeyLookup                 = errors.New("Cannot look up key in non-object value")
	errIndexLookup               = errors.New("Cannot look up array index in non-array value")
	errWildcardObjectLookup      = errors.New("Object wildcard used on non-object value")
	errWildcardArrayLookup       = errors.New("Array wildcard used on non-array value")
	errWilcardObjectUsageInvalid = errors.New("Invalid usage of object wildcard")
)

func jsonpathEval(p []*JSONPathElement, v interface{}) (r interface{}, err error) {
	// fmt.Printf("JPATHexpr: %v jsonobj: %v\n\n", p, v)
	if len(p) == 0 || v == nil {
		return v, nil
	}

	switch {
	case p[0].Key != nil:
		key := p[0].Key.keyString()

		kvs, ok := v.(jstream.KVS)
		if !ok {
			return nil, errKeyLookup
		}
		for _, kv := range kvs {
			if kv.Key == key {
				return jsonpathEval(p[1:], kv.Value)
			}
		}
		// Key not found - return nil result
		return nil, nil

	case p[0].Index != nil:
		idx := *p[0].Index

		arr, ok := v.([]interface{})
		if !ok {
			return nil, errIndexLookup
		}

		if idx >= len(arr) {
			return nil, nil
		}
		return jsonpathEval(p[1:], arr[idx])

	case p[0].ObjectWildcard:
		kvs, ok := v.(jstream.KVS)
		if !ok {
			return nil, errWildcardObjectLookup
		}

		if len(p[1:]) > 0 {
			return nil, errWilcardObjectUsageInvalid
		}

		return kvs, nil

	case p[0].ArrayWildcard:
		arr, ok := v.([]interface{})
		if !ok {
			return nil, errWildcardArrayLookup
		}

		// Lookup remainder of path in each array element and
		// make result array.
		var result []interface{}
		for _, a := range arr {
			rval, err := jsonpathEval(p[1:], a)
			if err != nil {
				return nil, err
			}

			result = append(result, rval)
		}
		return result, nil
	}
	panic("cannot reach here")
}

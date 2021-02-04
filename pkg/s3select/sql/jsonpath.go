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
	"github.com/minio/simdjson-go"
)

var (
	errKeyLookup                 = errors.New("Cannot look up key in non-object value")
	errIndexLookup               = errors.New("Cannot look up array index in non-array value")
	errWildcardObjectLookup      = errors.New("Object wildcard used on non-object value")
	errWildcardArrayLookup       = errors.New("Array wildcard used on non-array value")
	errWilcardObjectUsageInvalid = errors.New("Invalid usage of object wildcard")
)

// jsonpathEval evaluates a JSON path and returns the value at the path.
// If the value should be considered flat (from wildcards) any array returned should be considered individual values.
func jsonpathEval(p []*JSONPathElement, v interface{}) (r interface{}, flat bool, err error) {
	// fmt.Printf("JPATHexpr: %v jsonobj: %v\n\n", p, v)
	if len(p) == 0 || v == nil {
		return v, false, nil
	}

	switch {
	case p[0].Key != nil:
		key := p[0].Key.keyString()

		switch kvs := v.(type) {
		case jstream.KVS:
			for _, kv := range kvs {
				if kv.Key == key {
					return jsonpathEval(p[1:], kv.Value)
				}
			}
			// Key not found - return nil result
			return nil, false, nil
		case simdjson.Object:
			elem := kvs.FindKey(key, nil)
			if elem == nil {
				// Key not found - return nil result
				return nil, false, nil
			}
			val, err := IterToValue(elem.Iter)
			if err != nil {
				return nil, false, err
			}
			return jsonpathEval(p[1:], val)
		default:
			return nil, false, errKeyLookup
		}

	case p[0].Index != nil:
		idx := *p[0].Index

		arr, ok := v.([]interface{})
		if !ok {
			return nil, false, errIndexLookup
		}

		if idx >= len(arr) {
			return nil, false, nil
		}
		return jsonpathEval(p[1:], arr[idx])

	case p[0].ObjectWildcard:
		switch kvs := v.(type) {
		case jstream.KVS:
			if len(p[1:]) > 0 {
				return nil, false, errWilcardObjectUsageInvalid
			}

			return kvs, false, nil
		case simdjson.Object:
			if len(p[1:]) > 0 {
				return nil, false, errWilcardObjectUsageInvalid
			}

			return kvs, false, nil
		default:
			return nil, false, errWildcardObjectLookup
		}

	case p[0].ArrayWildcard:
		arr, ok := v.([]interface{})
		if !ok {
			return nil, false, errWildcardArrayLookup
		}

		// Lookup remainder of path in each array element and
		// make result array.
		var result []interface{}
		for _, a := range arr {
			rval, flatten, err := jsonpathEval(p[1:], a)
			if err != nil {
				return nil, false, err
			}

			if flatten {
				// Flatten if array.
				if arr, ok := rval.([]interface{}); ok {
					result = append(result, arr...)
					continue
				}
			}
			result = append(result, rval)
		}
		return result, true, nil
	}
	panic("cannot reach here")
}

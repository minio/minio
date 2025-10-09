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

package sql

import (
	"errors"

	"github.com/minio/minio/internal/s3select/jstream"
	"github.com/minio/simdjson-go"
)

var (
	errKeyLookup                  = errors.New("Cannot look up key in non-object value")
	errIndexLookup                = errors.New("Cannot look up array index in non-array value")
	errWildcardObjectLookup       = errors.New("Object wildcard used on non-object value")
	errWildcardArrayLookup        = errors.New("Array wildcard used on non-array value")
	errWildcardObjectUsageInvalid = errors.New("Invalid usage of object wildcard")
)

// jsonpathEval evaluates a JSON path and returns the value at the path.
// If the value should be considered flat (from wildcards) any array returned should be considered individual values.
func jsonpathEval(p []*JSONPathElement, v any) (r any, flat bool, err error) {
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
			return Missing{}, false, nil
		case simdjson.Object:
			elem := kvs.FindKey(key, nil)
			if elem == nil {
				// Key not found - return nil result
				return Missing{}, false, nil
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

		arr, ok := v.([]any)
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
				return nil, false, errWildcardObjectUsageInvalid
			}

			return kvs, false, nil
		case simdjson.Object:
			if len(p[1:]) > 0 {
				return nil, false, errWildcardObjectUsageInvalid
			}

			return kvs, false, nil
		default:
			return nil, false, errWildcardObjectLookup
		}

	case p[0].ArrayWildcard:
		arr, ok := v.([]any)
		if !ok {
			return nil, false, errWildcardArrayLookup
		}

		// Lookup remainder of path in each array element and
		// make result array.
		var result []any
		for _, a := range arr {
			rval, flatten, err := jsonpathEval(p[1:], a)
			if err != nil {
				return nil, false, err
			}

			if flatten {
				// Flatten if array.
				if arr, ok := rval.([]any); ok {
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

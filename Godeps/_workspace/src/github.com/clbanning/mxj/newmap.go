// mxj - A collection of map[string]interface{} and associated XML and JSON utilities.
// Copyright 2012-2014 Charles Banning. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file

// remap.go - build a new Map from the current Map based on keyOld:keyNew mapppings
//            keys can use dot-notation, keyOld can use wildcard, '*'
//
// Computational strategy -
// Using the key path - []string - traverse a new map[string]interface{} and
// insert the oldVal as the newVal when we arrive at the end of the path.
// If the type at the end is nil, then that is newVal
// If the type at the end is a singleton (string, float64, bool) an array is created.
// If the type at the end is an array, newVal is just appended.
// If the type at the end is a map, it is inserted if possible or the map value
//    is converted into an array if necessary.

package mxj

import (
	"errors"
	"strings"
)

// (Map)NewMap - create a new Map from data in the current Map.
//	'keypairs' are key mappings "oldKey:newKey" and specify that the current value of 'oldKey'
//	should be the value for 'newKey' in the returned Map.
//		- 'oldKey' supports dot-notation as described for (Map)ValuesForPath()
//		- 'newKey' supports dot-notation but with no wildcards, '*', or indexed arrays
//		- "oldKey" is shorthand for for the keypair value "oldKey:oldKey"
//		- "oldKey:" and ":newKey" are invalid keypair values
//		- if 'oldKey' does not exist in the current Map, it is not written to the new Map.
//		  "null" is not supported unless it is the current Map.
//		- see newmap_test.go for several syntax examples
//
//	NOTE: mv.NewMap() == mxj.New().
func (mv Map) NewMap(keypairs ...string) (Map, error) {
	n := make(map[string]interface{}, 0)
	if len(keypairs) == 0 {
		return n, nil
	}

	// loop through the pairs
	var oldKey, newKey string
	var path []string
	for _, v := range keypairs {
		if len(v) == 0 {
			continue // just skip over empty keypair arguments
		}

		// initialize oldKey, newKey and check
		vv := strings.Split(v, ":")
		if len(vv) > 2 {
			return n, errors.New("oldKey:newKey keypair value not valid - " + v)
		}
		if len(vv) == 1 {
			oldKey, newKey = vv[0], vv[0]
		} else {
			oldKey, newKey = vv[0], vv[1]
		}
		strings.TrimSpace(oldKey)
		strings.TrimSpace(newKey)
		if i := strings.Index(newKey, "*"); i > -1 {
			return n, errors.New("newKey value cannot contain wildcard character - " + v)
		}
		if i := strings.Index(newKey, "["); i > -1 {
			return n, errors.New("newKey value cannot contain indexed arrays - " + v)
		}
		if oldKey == "" || newKey == "" {
			return n, errors.New("oldKey or newKey is not specified - " + v)
		}

		// get oldKey value
		oldVal, err := mv.ValuesForPath(oldKey)
		if err != nil {
			return n, err
		}
		if len(oldVal) == 0 {
			continue // oldKey has no value, may not exist in mv
		}

		// break down path
		path = strings.Split(newKey, ".")
		if path[len(path)-1] == "" { // ignore a trailing dot in newKey spec
			path = path[:len(path)-1]
		}

		addNewVal(&n, path, oldVal)
	}

	return n, nil
}

// navigate 'n' to end of path and add val
func addNewVal(n *map[string]interface{}, path []string, val []interface{}) {
	// newVal - either singleton or array
	var newVal interface{}
	if len(val) == 1 {
		newVal = val[0] // is type interface{}
	} else {
		newVal = interface{}(val)
	}

	// walk to the position of interest, create it if necessary
	m := (*n)           // initialize map walker
	var k string        // key for m
	lp := len(path) - 1 // when to stop looking
	for i := 0; i < len(path); i++ {
		k = path[i]
		if i == lp {
			break
		}
		var nm map[string]interface{} // holds position of next-map
		switch m[k].(type) {
		case nil: // need a map for next node in path, so go there
			nm = make(map[string]interface{}, 0)
			m[k] = interface{}(nm)
			m = m[k].(map[string]interface{})
		case map[string]interface{}:
			// OK - got somewhere to walk to, go there
			m = m[k].(map[string]interface{})
		case []interface{}:
			// add a map and nm points to new map unless there's already
			// a map in the array, then nm points there
			// The placement of the next value in the array is dependent
			// on the sequence of members - could land on a map or a nil
			// value first.  TODO: how to test this.
			a := make([]interface{}, 0)
			var foundmap bool
			for _, vv := range m[k].([]interface{}) {
				switch vv.(type) {
				case nil: // doesn't appear that this occurs, need a test case
					if foundmap { // use the first one in array
						a = append(a, vv)
						continue
					}
					nm = make(map[string]interface{}, 0)
					a = append(a, interface{}(nm))
					foundmap = true
				case map[string]interface{}:
					if foundmap { // use the first one in array
						a = append(a, vv)
						continue
					}
					nm = vv.(map[string]interface{})
					a = append(a, vv)
					foundmap = true
				default:
					a = append(a, vv)
				}
			}
			// no map found in array
			if !foundmap {
				nm = make(map[string]interface{}, 0)
				a = append(a, interface{}(nm))
			}
			m[k] = interface{}(a) // must insert in map
			m = nm
		default: // it's a string, float, bool, etc.
			aa := make([]interface{}, 0)
			nm = make(map[string]interface{}, 0)
			aa = append(aa, m[k], nm)
			m[k] = interface{}(aa)
			m = nm
		}
	}

	// value is nil, array or a singleton of some kind
	// initially m.(type) == map[string]interface{}
	v := m[k]
	switch v.(type) {
	case nil: // initialized
		m[k] = newVal
	case []interface{}:
		a := m[k].([]interface{})
		a = append(a, newVal)
		m[k] = interface{}(a)
	default: // v exists:string, float64, bool, map[string]interface, etc.
		a := make([]interface{}, 0)
		a = append(a, v, newVal)
		m[k] = interface{}(a)
	}
}

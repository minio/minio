// Copyright 2012-2014 Charles Banning. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file

// updatevalues.go - modify a value based on path and possibly sub-keys

package mxj

import (
	"fmt"
	"strconv"
	"strings"
)

// Update value based on path and possible sub-key values.
// A count of the number of values changed and any error are returned.
// If the count == 0, then no path (and subkeys) matched.
//	'newVal' can be a Map or map[string]interface{} value with a single 'key' that is the key to be modified
//	             or a string value "key:value[:type]" where type is "bool" or "num" to cast the value.
//	'path' is dot-notation list of keys to traverse; last key in path can be newVal key
//	       NOTE: 'path' spec does not currently support indexed array references.
//	'subkeys' are "key:value[:type]" entries that must match for path node
//	            The subkey can be wildcarded - "key:*" - to require that it's there with some value.
//	            If a subkey is preceeded with the '!' character, the key:value[:type] entry is treated as an
//	            exclusion critera - e.g., "!author:William T. Gaddis".
func (mv Map) UpdateValuesForPath(newVal interface{}, path string, subkeys ...string) (int, error) {
	m := map[string]interface{}(mv)

	// extract the subkeys
	var subKeyMap map[string]interface{}
	if len(subkeys) > 0 {
		var err error
		subKeyMap, err = getSubKeyMap(subkeys...)
		if err != nil {
			return 0, err
		}
	}

	// extract key and value from newVal
	var key string
	var val interface{}
	switch newVal.(type) {
	case map[string]interface{}, Map:
		switch newVal.(type) { // "fallthrough is not permitted in type switch" (Spec)
		case Map:
			newVal = newVal.(Map).Old()
		}
		if len(newVal.(map[string]interface{})) != 1 {
			return 0, fmt.Errorf("newVal map can only have len == 1 - %+v", newVal)
		}
		for key, val = range newVal.(map[string]interface{}) {
		}
	case string: // split it as a key:value pair
		ss := strings.Split(newVal.(string), ":")
		n := len(ss)
		if n < 2 || n > 3 {
			return 0, fmt.Errorf("unknown newVal spec - %+v", newVal)
		}
		key = ss[0]
		if n == 2 {
			val = interface{}(ss[1])
		} else if n == 3 {
			switch ss[2] {
			case "bool", "boolean":
				nv, err := strconv.ParseBool(ss[1])
				if err != nil {
					return 0, fmt.Errorf("can't convert newVal to bool - %+v", newVal)
				}
				val = interface{}(nv)
			case "num", "numeric", "float", "int":
				nv, err := strconv.ParseFloat(ss[1], 64)
				if err != nil {
					return 0, fmt.Errorf("can't convert newVal to float64 - %+v", newVal)
				}
				val = interface{}(nv)
			default:
				return 0, fmt.Errorf("unknown type for newVal value - %+v", newVal)
			}
		}
	default:
		return 0, fmt.Errorf("invalid newVal type - %+v", newVal)
	}

	// parse path
	keys := strings.Split(path, ".")

	var count int
	updateValuesForKeyPath(key, val, m, keys, subKeyMap, &count)

	return count, nil
}

// navigate the path
func updateValuesForKeyPath(key string, value interface{}, m interface{}, keys []string, subkeys map[string]interface{}, cnt *int) {
	// ----- at end node: looking at possible node to get 'key' ----
	if len(keys) == 1 {
		updateValue(key, value, m, keys[0], subkeys, cnt)
		return
	}

	// ----- here we are navigating the path thru the penultimate node --------
	// key of interest is keys[0] - the next in the path
	switch keys[0] {
	case "*": // wildcard - scan all values
		switch m.(type) {
		case map[string]interface{}:
			for _, v := range m.(map[string]interface{}) {
				updateValuesForKeyPath(key, value, v, keys[1:], subkeys, cnt)
			}
		case []interface{}:
			for _, v := range m.([]interface{}) {
				switch v.(type) {
				// flatten out a list of maps - keys are processed
				case map[string]interface{}:
					for _, vv := range v.(map[string]interface{}) {
						updateValuesForKeyPath(key, value, vv, keys[1:], subkeys, cnt)
					}
				default:
					updateValuesForKeyPath(key, value, v, keys[1:], subkeys, cnt)
				}
			}
		}
	default: // key - must be map[string]interface{}
		switch m.(type) {
		case map[string]interface{}:
			if v, ok := m.(map[string]interface{})[keys[0]]; ok {
				updateValuesForKeyPath(key, value, v, keys[1:], subkeys, cnt)
			}
		case []interface{}: // may be buried in list
			for _, v := range m.([]interface{}) {
				switch v.(type) {
				case map[string]interface{}:
					if vv, ok := v.(map[string]interface{})[keys[0]]; ok {
						updateValuesForKeyPath(key, value, vv, keys[1:], subkeys, cnt)
					}
				}
			}
		}
	}
}

// change value if key and subkeys are present
func updateValue(key string, value interface{}, m interface{}, keys0 string, subkeys map[string]interface{}, cnt *int) {
	// there are two possible options for the value of 'keys0': map[string]interface, []interface{}
	// and 'key' is a key in the map or is a key in a map in a list.
	switch m.(type) {
	case map[string]interface{}: // gotta have the last key
		if keys0 == "*" {
			for k, _ := range m.(map[string]interface{}) {
				updateValue(key, value, m, k, subkeys, cnt)
			}
			return
		}
		endVal, _ := m.(map[string]interface{})[keys0]

		// if newV key is the end of path, replace the value for path-end
		// may be []interface{} - means replace just an entry w/ subkeys
		// otherwise replace the keys0 value if subkeys are there
		// NOTE: this will replace the subkeys, also
		if key == keys0 {
			switch endVal.(type) {
			case map[string]interface{}:
				if ok := hasSubKeys(m, subkeys); ok {
					(m.(map[string]interface{}))[keys0] = value
					(*cnt)++
				}
			case []interface{}:
				// without subkeys can't select list member to modify
				// so key:value spec is it ...
				if len(subkeys) == 0 {
					(m.(map[string]interface{}))[keys0] = value
					(*cnt)++
					break
				}
				nv := make([]interface{}, 0)
				var valmodified bool
				for _, v := range endVal.([]interface{}) {
					// check entry subkeys
					if ok := hasSubKeys(v, subkeys); ok {
						// replace v with value
						nv = append(nv, value)
						valmodified = true
						(*cnt)++
						continue
					}
					nv = append(nv, v)
				}
				if valmodified {
					(m.(map[string]interface{}))[keys0] = interface{}(nv)
				}
			default: // anything else is a strict replacement
				if len(subkeys) == 0 {
					(m.(map[string]interface{}))[keys0] = value
					(*cnt)++
				}
			}
			return
		}

		// so value is for an element of endVal
		// if endVal is a map then 'key' must be there w/ subkeys
		// if endVal is a list then 'key' must be in a list member w/ subkeys
		switch endVal.(type) {
		case map[string]interface{}:
			if ok := hasSubKeys(endVal, subkeys); !ok {
				return
			}
			if _, ok := (endVal.(map[string]interface{}))[key]; ok {
				(endVal.(map[string]interface{}))[key] = value
				(*cnt)++
			}
		case []interface{}: // keys0 points to a list, check subkeys
			for _, v := range endVal.([]interface{}) {
				// got to be a map so we can replace value for 'key'
				vv, vok := v.(map[string]interface{})
				if !vok {
					continue
				}
				if _, ok := vv[key]; !ok {
					continue
				}
				if !hasSubKeys(vv, subkeys) {
					continue
				}
				vv[key] = value
				(*cnt)++
			}
		}
	case []interface{}: // key may be in a list member
		// don't need to handle keys0 == "*"; we're looking at everything, anyway.
		for _, v := range m.([]interface{}) {
			// only map values - we're looking for 'key'
			mm, ok := v.(map[string]interface{})
			if !ok {
				continue
			}
			if _, ok := mm[key]; !ok {
				continue
			}
			if !hasSubKeys(mm, subkeys) {
				continue
			}
			mm[key] = value
			(*cnt)++
		}
	}

	// return
}

// Copyright 2012-2014 Charles Banning. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file

//	keyvalues.go: Extract values from an arbitrary XML doc. Tag path can include wildcard characters.

package mxj

import (
	"fmt"
	"strconv"
	"strings"
)

// ----------------------------- get everything FOR a single key -------------------------

const (
	minArraySize = 32
)

var defaultArraySize int = minArraySize

// Adjust the buffers for expected number of values to return from ValuesForKey() and ValuesForPath().
// This can have the effect of significantly reducing memory allocation-copy functions for large data sets.
// Returns the initial buffer size.
func SetArraySize(size int) int {
	if size > minArraySize {
		defaultArraySize = size
	} else {
		defaultArraySize = minArraySize
	}
	return defaultArraySize
}

// Return all values in Map, 'mv', associated with a 'key'. If len(returned_values) == 0, then no match.
// On error, the returned array is 'nil'. NOTE: 'key' can be wildcard, "*".
//   'subkeys' (optional) are "key:val[:type]" strings representing attributes or elements in a list.
//             - By default 'val' is of type string. "key:val:bool" and "key:val:float" to coerce them.
//             - For attributes prefix the label with a hyphen, '-', e.g., "-seq:3".
//             - If the 'key' refers to a list, then "key:value" could select a list member of the list.
//             - The subkey can be wildcarded - "key:*" - to require that it's there with some value.
//             - If a subkey is preceeded with the '!' character, the key:value[:type] entry is treated as an
//               exclusion critera - e.g., "!author:William T. Gaddis".
func (mv Map) ValuesForKey(key string, subkeys ...string) ([]interface{}, error) {
	m := map[string]interface{}(mv)
	var subKeyMap map[string]interface{}
	if len(subkeys) > 0 {
		var err error
		subKeyMap, err = getSubKeyMap(subkeys...)
		if err != nil {
			return nil, err
		}
	}

	ret := make([]interface{}, 0, defaultArraySize)
	var cnt int
	hasKey(m, key, &ret, &cnt, subKeyMap)
	return ret[:cnt], nil
	// ret := make([]interface{}, 0)
	// hasKey(m, key, &ret, subKeyMap)
	// return ret, nil
}

// hasKey - if the map 'key' exists append it to array
//          if it doesn't do nothing except scan array and map values
func hasKey(iv interface{}, key string, ret *[]interface{}, cnt *int, subkeys map[string]interface{}) {
	// func hasKey(iv interface{}, key string, ret *[]interface{}, subkeys map[string]interface{}) {
	switch iv.(type) {
	case map[string]interface{}:
		vv := iv.(map[string]interface{})
		// see if the current value is of interest
		if v, ok := vv[key]; ok {
			switch v.(type) {
			case map[string]interface{}:
				if hasSubKeys(v, subkeys) {
					*ret = append(*ret, v)
					*cnt++
				}
			case []interface{}:
				for _, av := range v.([]interface{}) {
					if hasSubKeys(av, subkeys) {
						*ret = append(*ret, av)
						*cnt++
					}
				}
			default:
				if len(subkeys) == 0 {
					*ret = append(*ret, v)
					*cnt++
				}
			}
		}

		// wildcard case
		if key == "*" {
			for _, v := range vv {
				switch v.(type) {
				case map[string]interface{}:
					if hasSubKeys(v, subkeys) {
						*ret = append(*ret, v)
						*cnt++
					}
				case []interface{}:
					for _, av := range v.([]interface{}) {
						if hasSubKeys(av, subkeys) {
							*ret = append(*ret, av)
							*cnt++
						}
					}
				default:
					if len(subkeys) == 0 {
						*ret = append(*ret, v)
						*cnt++
					}
				}
			}
		}

		// scan the rest
		for _, v := range vv {
			hasKey(v, key, ret, cnt, subkeys)
			// hasKey(v, key, ret, subkeys)
		}
	case []interface{}:
		for _, v := range iv.([]interface{}) {
			hasKey(v, key, ret, cnt, subkeys)
			// hasKey(v, key, ret, subkeys)
		}
	}
}

// -----------------------  get everything for a node in the Map ---------------------------

// Allow indexed arrays in "path" specification. (Request from Abhijit Kadam - abhijitk100@gmail.com.)
// 2014.04.28 - implementation note.
// Implemented as a wrapper of (old)ValuesForPath() because we need look-ahead logic to handle expansion
// of wildcards and unindexed arrays.  Embedding such logic into valuesForKeyPath() would have made the
// code much more complicated; this wrapper is straightforward, easy to debug, and doesn't add significant overhead.

// Retrieve all values for a path from the Map.  If len(returned_values) == 0, then no match.
// On error, the returned array is 'nil'.
//   'path' is a dot-separated path of key values.
//          - If a node in the path is '*', then everything beyond is walked.
//          - 'path' can contain indexed array references, such as, "*.data[1]" and "msgs[2].data[0].field" -
//            even "*[2].*[0].field".
//   'subkeys' (optional) are "key:val[:type]" strings representing attributes or elements in a list.
//             - By default 'val' is of type string. "key:val:bool" and "key:val:float" to coerce them.
//             - For attributes prefix the label with a hyphen, '-', e.g., "-seq:3".
//             - If the 'path' refers to a list, then "tag:value" would return member of the list.
//             - The subkey can be wildcarded - "key:*" - to require that it's there with some value.
//             - If a subkey is preceeded with the '!' character, the key:value[:type] entry is treated as an
//               exclusion critera - e.g., "!author:William T. Gaddis".
func (mv Map) ValuesForPath(path string, subkeys ...string) ([]interface{}, error) {
	// If there are no array indexes in path, use legacy ValuesForPath() logic.
	if strings.Index(path, "[") < 0 {
		return mv.oldValuesForPath(path, subkeys...)
	}

	var subKeyMap map[string]interface{}
	if len(subkeys) > 0 {
		var err error
		subKeyMap, err = getSubKeyMap(subkeys...)
		if err != nil {
			return nil, err
		}
	}

	keys, kerr := parsePath(path)
	if kerr != nil {
		return nil, kerr
	}

	vals, verr := valuesForArray(keys, mv)
	if verr != nil {
		return nil, verr // Vals may be nil, but return empty array.
	}

	// Need to handle subkeys ... only return members of vals that satisfy conditions.
	retvals := make([]interface{}, 0)
	for _, v := range vals {
		if hasSubKeys(v, subKeyMap) {
			retvals = append(retvals, v)
		}
	}
	return retvals, nil
}

func valuesForArray(keys []*key, m Map) ([]interface{}, error) {
	var tmppath string
	var haveFirst bool
	var vals []interface{}
	var verr error

	lastkey := len(keys) - 1
	for i := 0; i <= lastkey; i++ {
		if !haveFirst {
			tmppath = keys[i].name
			haveFirst = true
		} else {
			tmppath += "." + keys[i].name
		}

		// Look-ahead: explode wildcards and unindexed arrays.
		// Need to handle un-indexed list recursively:
		// e.g., path is "stuff.data[0]" rather than "stuff[0].data[0]".
		// Need to treat it as "stuff[0].data[0]", "stuff[1].data[0]", ...
		if !keys[i].isArray && i < lastkey && keys[i+1].isArray {
			// Can't pass subkeys because we may not be at literal end of path.
			vv, vverr := m.oldValuesForPath(tmppath)
			if vverr != nil {
				return nil, vverr
			}
			for _, v := range vv {
				// See if we can walk the value.
				am, ok := v.(map[string]interface{})
				if !ok {
					continue
				}
				// Work the backend.
				nvals, nvalserr := valuesForArray(keys[i+1:], Map(am))
				if nvalserr != nil {
					return nil, nvalserr
				}
				vals = append(vals, nvals...)
			}
			break // have recursed the whole path - return
		}

		if keys[i].isArray || i == lastkey {
			// Don't pass subkeys because may not be at literal end of path.
			vals, verr = m.oldValuesForPath(tmppath)
		} else {
			continue
		}
		if verr != nil {
			return nil, verr
		}

		if i == lastkey && !keys[i].isArray {
			break
		}

		// Now we're looking at an array - supposedly.
		// Is index in range of vals?
		if len(vals) <= keys[i].position {
			vals = nil
			break
		}

		// Return the array member of interest, if at end of path.
		if i == lastkey {
			vals = vals[keys[i].position:(keys[i].position + 1)]
			break
		}

		// Extract the array member of interest.
		am := vals[keys[i].position:(keys[i].position + 1)]

		// must be a map[string]interface{} value so we can keep walking the path
		amm, ok := am[0].(map[string]interface{})
		if !ok {
			vals = nil
			break
		}

		m = Map(amm)
		haveFirst = false
	}

	return vals, nil
}

type key struct {
	name     string
	isArray  bool
	position int
}

func parsePath(s string) ([]*key, error) {
	keys := strings.Split(s, ".")

	ret := make([]*key, 0)

	for i := 0; i < len(keys); i++ {
		if keys[i] == "" {
			continue
		}

		newkey := new(key)
		if strings.Index(keys[i], "[") < 0 {
			newkey.name = keys[i]
			ret = append(ret, newkey)
			continue
		}

		p := strings.Split(keys[i], "[")
		newkey.name = p[0]
		p = strings.Split(p[1], "]")
		if p[0] == "" { // no right bracket
			return nil, fmt.Errorf("no right bracket on key index: %s", keys[i])
		}
		// convert p[0] to a int value
		pos, nerr := strconv.ParseInt(p[0], 10, 32)
		if nerr != nil {
			return nil, fmt.Errorf("cannot convert index to int value: %s", p[0])
		}
		newkey.position = int(pos)
		newkey.isArray = true
		ret = append(ret, newkey)
	}

	return ret, nil
}

// legacy ValuesForPath() - now wrapped to handle special case of indexed arrays in 'path'.
func (mv Map) oldValuesForPath(path string, subkeys ...string) ([]interface{}, error) {
	m := map[string]interface{}(mv)
	var subKeyMap map[string]interface{}
	if len(subkeys) > 0 {
		var err error
		subKeyMap, err = getSubKeyMap(subkeys...)
		if err != nil {
			return nil, err
		}
	}

	keys := strings.Split(path, ".")
	if keys[len(keys)-1] == "" {
		keys = keys[:len(keys)-1]
	}
	// ivals := make([]interface{}, 0)
	// valuesForKeyPath(&ivals, m, keys, subKeyMap)
	// return ivals, nil
	ivals := make([]interface{}, 0, defaultArraySize)
	var cnt int
	valuesForKeyPath(&ivals, &cnt, m, keys, subKeyMap)
	return ivals[:cnt], nil
}

func valuesForKeyPath(ret *[]interface{}, cnt *int, m interface{}, keys []string, subkeys map[string]interface{}) {
	lenKeys := len(keys)

	// load 'm' values into 'ret'
	// expand any lists
	if lenKeys == 0 {
		switch m.(type) {
		case map[string]interface{}:
			if subkeys != nil {
				if ok := hasSubKeys(m, subkeys); !ok {
					return
				}
			}
			*ret = append(*ret, m)
			*cnt++
		case []interface{}:
			for i, v := range m.([]interface{}) {
				if subkeys != nil {
					if ok := hasSubKeys(v, subkeys); !ok {
						continue // only load list members with subkeys
					}
				}
				*ret = append(*ret, (m.([]interface{}))[i])
				*cnt++
			}
		default:
			if subkeys != nil {
				return // must be map[string]interface{} if there are subkeys
			}
			*ret = append(*ret, m)
			*cnt++
		}
		return
	}

	// key of interest
	key := keys[0]
	switch key {
	case "*": // wildcard - scan all values
		switch m.(type) {
		case map[string]interface{}:
			for _, v := range m.(map[string]interface{}) {
				// valuesForKeyPath(ret, v, keys[1:], subkeys)
				valuesForKeyPath(ret, cnt, v, keys[1:], subkeys)
			}
		case []interface{}:
			for _, v := range m.([]interface{}) {
				switch v.(type) {
				// flatten out a list of maps - keys are processed
				case map[string]interface{}:
					for _, vv := range v.(map[string]interface{}) {
						// valuesForKeyPath(ret, vv, keys[1:], subkeys)
						valuesForKeyPath(ret, cnt, vv, keys[1:], subkeys)
					}
				default:
					// valuesForKeyPath(ret, v, keys[1:], subkeys)
					valuesForKeyPath(ret, cnt, v, keys[1:], subkeys)
				}
			}
		}
	default: // key - must be map[string]interface{}
		switch m.(type) {
		case map[string]interface{}:
			if v, ok := m.(map[string]interface{})[key]; ok {
				// valuesForKeyPath(ret, v, keys[1:], subkeys)
				valuesForKeyPath(ret, cnt, v, keys[1:], subkeys)
			}
		case []interface{}: // may be buried in list
			for _, v := range m.([]interface{}) {
				switch v.(type) {
				case map[string]interface{}:
					if vv, ok := v.(map[string]interface{})[key]; ok {
						// valuesForKeyPath(ret, vv, keys[1:], subkeys)
						valuesForKeyPath(ret, cnt, vv, keys[1:], subkeys)
					}
				}
			}
		}
	}
}

// hasSubKeys() - interface{} equality works for string, float64, bool
// 'v' must be a map[string]interface{} value to have subkeys
// 'a' can have k:v pairs with v.(string) == "*", which is treated like a wildcard.
func hasSubKeys(v interface{}, subkeys map[string]interface{}) bool {
	if len(subkeys) == 0 {
		return true
	}

	switch v.(type) {
	case map[string]interface{}:
		// do all subKey name:value pairs match?
		mv := v.(map[string]interface{})
		for skey, sval := range subkeys {
			isNotKey := false
			if skey[:1] == "!" { // a NOT-key
				skey = skey[1:]
				isNotKey = true
			}
			vv, ok := mv[skey]
			if !ok { // key doesn't exist
				if isNotKey { // key not there, but that's what we want
					if kv, ok := sval.(string); ok && kv == "*" {
						continue
					}
				}
				return false
			}
			// wildcard check
			if kv, ok := sval.(string); ok && kv == "*" {
				if isNotKey { // key is there, and we don't want it
					return false
				}
				continue
			}
			switch sval.(type) {
			case string:
				if s, ok := vv.(string); ok && s == sval.(string) {
					if isNotKey {
						return false
					}
					continue
				}
			case bool:
				if b, ok := vv.(bool); ok && b == sval.(bool) {
					if isNotKey {
						return false
					}
					continue
				}
			case float64:
				if f, ok := vv.(float64); ok && f == sval.(float64) {
					if isNotKey {
						return false
					}
					continue
				}
			}
			// key there but didn't match subkey value
			if isNotKey { // that's what we want
				continue
			}
			return false
		}
		// all subkeys matched
		return true
	}

	// not a map[string]interface{} value, can't have subkeys
	return false
}

// Generate map of key:value entries as map[string]string.
//	'kv' arguments are "name:value" pairs: attribute keys are designated with prepended hyphen, '-'.
//	If len(kv) == 0, the return is (nil, nil).
func getSubKeyMap(kv ...string) (map[string]interface{}, error) {
	if len(kv) == 0 {
		return nil, nil
	}
	m := make(map[string]interface{}, 0)
	for _, v := range kv {
		vv := strings.Split(v, ":")
		switch len(vv) {
		case 2:
			m[vv[0]] = interface{}(vv[1])
		case 3:
			switch vv[3] {
			case "string", "char", "text":
				m[vv[0]] = interface{}(vv[1])
			case "bool", "boolean":
				// ParseBool treats "1"==true & "0"==false
				b, err := strconv.ParseBool(vv[1])
				if err != nil {
					return nil, fmt.Errorf("can't convert subkey value to bool: %s", vv[1])
				}
				m[vv[0]] = interface{}(b)
			case "float", "float64", "num", "number", "numeric":
				f, err := strconv.ParseFloat(vv[1], 64)
				if err != nil {
					return nil, fmt.Errorf("can't convert subkey value to float: %s", vv[1])
				}
				m[vv[0]] = interface{}(f)
			default:
				return nil, fmt.Errorf("unknown subkey conversion spec: %s", v)
			}
		default:
			return nil, fmt.Errorf("unknown subkey spec: %s", v)
		}
	}
	return m, nil
}

// -------------------------------  END of valuesFor ... ----------------------------

// ----------------------- locate where a key value is in the tree -------------------

//----------------------------- find all paths to a key --------------------------------

// Get all paths through Map, 'mv', (in dot-notation) that terminate with the specified key.
// Results can be used with ValuesForPath.
func (mv Map) PathsForKey(key string) []string {
	m := map[string]interface{}(mv)
	breadbasket := make(map[string]bool, 0)
	breadcrumbs := ""

	hasKeyPath(breadcrumbs, m, key, breadbasket)
	if len(breadbasket) == 0 {
		return nil
	}

	// unpack map keys to return
	res := make([]string, len(breadbasket))
	var i int
	for k, _ := range breadbasket {
		res[i] = k
		i++
	}

	return res
}

// Extract the shortest path from all possible paths - from PathsForKey() - in Map, 'mv'..
// Paths are strings using dot-notation.
func (mv Map) PathForKeyShortest(key string) string {
	paths := mv.PathsForKey(key)

	lp := len(paths)
	if lp == 0 {
		return ""
	}
	if lp == 1 {
		return paths[0]
	}

	shortest := paths[0]
	shortestLen := len(strings.Split(shortest, "."))

	for i := 1; i < len(paths); i++ {
		vlen := len(strings.Split(paths[i], "."))
		if vlen < shortestLen {
			shortest = paths[i]
			shortestLen = vlen
		}
	}

	return shortest
}

// hasKeyPath - if the map 'key' exists append it to KeyPath.path and increment KeyPath.depth
// This is really just a breadcrumber that saves all trails that hit the prescribed 'key'.
func hasKeyPath(crumbs string, iv interface{}, key string, basket map[string]bool) {
	switch iv.(type) {
	case map[string]interface{}:
		vv := iv.(map[string]interface{})
		if _, ok := vv[key]; ok {
			if crumbs == "" {
				crumbs = key
			} else {
				crumbs += "." + key
			}
			// *basket = append(*basket, crumb)
			basket[crumbs] = true
		}
		// walk on down the path, key could occur again at deeper node
		for k, v := range vv {
			// create a new breadcrumb, intialized with the one we have
			var nbc string
			if crumbs == "" {
				nbc = k
			} else {
				nbc = crumbs + "." + k
			}
			hasKeyPath(nbc, v, key, basket)
		}
	case []interface{}:
		// crumb-trail doesn't change, pass it on
		for _, v := range iv.([]interface{}) {
			hasKeyPath(crumbs, v, key, basket)
		}
	}
}

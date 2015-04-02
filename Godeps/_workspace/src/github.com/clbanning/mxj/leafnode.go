package mxj

// leafnode.go - return leaf nodes with paths and values for the Map
// inspired by: https://groups.google.com/forum/#!topic/golang-nuts/3JhuVKRuBbw

import (
	"strconv"
)

const (
	NoAttributes = true // suppress LeafNode values that are attributes
)

// LeafNode - a terminal path value in a Map.
// For XML Map values it represents an attribute or simple element value  - of type
// string unless Map was created using Cast flag. For JSON Map values it represents
// a string, numeric, boolean, or null value.
type LeafNode struct {
	Path  string      // a dot-notation representation of the path with array subscripting
	Value interface{} // the value at the path termination
}

// LeafNodes - returns an array of all LeafNode values for the Map.
// The option no_attr argument suppresses attribute values (keys with prepended hyphen, '-')
// as well as the "#text" key for the associated simple element value.
func (mv Map) LeafNodes(no_attr ...bool) []LeafNode {
	var a bool
	if len(no_attr) == 1 {
		a = no_attr[0]
	}

	l := make([]LeafNode, 0)
	getLeafNodes("", "", map[string]interface{}(mv), &l, a)
	return l
}

func getLeafNodes(path, node string, mv interface{}, l *[]LeafNode, noattr bool) {
	// if stripping attributes, then also strip "#text" key
	if !noattr || node != "#text" {
		if path != "" && node[:1] != "[" {
			path += "."
		}
		path += node
	}
	switch mv.(type) {
	case map[string]interface{}:
		for k, v := range mv.(map[string]interface{}) {
			if noattr && k[:1] == "-" {
				continue
			}
			getLeafNodes(path, k, v, l, noattr)
		}
	case []interface{}:
		for i, v := range mv.([]interface{}) {
			getLeafNodes(path, "["+strconv.Itoa(i)+"]", v, l, noattr)
		}
	default:
		// can't walk any further, so create leaf
		n := LeafNode{path, mv}
		*l = append(*l, n)
	}
}

// LeafPaths - all paths that terminate in LeafNode values.
func (mv Map) LeafPaths(no_attr ...bool) []string {
	ln := mv.LeafNodes()
	ss := make([]string, len(ln))
	for i := 0; i < len(ln); i++ {
		ss[i] = ln[i].Path
	}
	return ss
}

// LeafValues - all terminal values in the Map.
func (mv Map) LeafValues(no_attr ...bool) []interface{} {
	ln := mv.LeafNodes()
	vv := make([]interface{}, len(ln))
	for i := 0; i < len(ln); i++ {
		vv[i] = ln[i].Value
	}
	return vv
}

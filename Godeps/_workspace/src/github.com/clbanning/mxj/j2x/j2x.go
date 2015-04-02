// Copyright 2012-2014 Charles Banning. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file

// j2x.go - For (mostly) backwards compatibility with legacy j2x package.
// Wrappers for end-to-end JSON to XML transformation and value manipulation.
package j2x

import (
	. "github.com/clbanning/mxj"
	"io"
)

// FromJson() --> map[string]interface{}
func JsonToMap(jsonVal []byte) (map[string]interface{}, error) {
	return NewMapJson(jsonVal)
}

// interface{} --> ToJson (w/o safe encoding, default) {
func MapToJson(m map[string]interface{}, safeEncoding ...bool) ([]byte, error) {
	return Map(m).Json()
}

// FromJson() --> ToXml().
func JsonToXml(jsonVal []byte) ([]byte, error) {
	m, err := NewMapJson(jsonVal)
	if err != nil {
		return nil, err
	}
	return m.Xml()
}

// FromJson() --> ToXmlWriter().
func JsonToXmlWriter(jsonVal []byte, xmlWriter io.Writer) ([]byte, error) {
	m, err := NewMapJson(jsonVal)
	if err != nil {
		return nil, err
	}
	return m.XmlWriterRaw(xmlWriter)
}

// FromJsonReader() --> ToXml().
func JsonReaderToXml(jsonReader io.Reader) ([]byte, []byte, error) {
	m, jraw, err := NewMapJsonReaderRaw(jsonReader)
	if err != nil {
		return jraw, nil, err
	}
	x, xerr := m.Xml()
	return jraw, x, xerr
}

// FromJsonReader() --> ToXmlWriter().  Handy for transforming bulk message sets.
func JsonReaderToXmlWriter(jsonReader io.Reader, xmlWriter io.Writer) ([]byte, []byte, error) {
	m, jraw, err := NewMapJsonReaderRaw(jsonReader)
	if err != nil {
		return jraw, nil, err
	}
	xraw, xerr := m.XmlWriterRaw(xmlWriter)
	return jraw, xraw, xerr
}

// JSON wrappers for Map methods implementing key path and value functions.

// Wrap PathsForKey for JSON.
func JsonPathsForKey(jsonVal []byte, key string) ([]string, error) {
	m, err := NewMapJson(jsonVal)
	if err != nil {
		return nil, err
	}
	paths := m.PathsForKey(key)
	return paths, nil
}

// Wrap PathForKeyShortest for JSON.
func JsonPathForKeyShortest(jsonVal []byte, key string) (string, error) {
	m, err := NewMapJson(jsonVal)
	if err != nil {
		return "", err
	}
	path := m.PathForKeyShortest(key)
	return path, nil
}

// Wrap ValuesForKey for JSON.
func JsonValuesForKey(jsonVal []byte, key string, subkeys ...string) ([]interface{}, error) {
	m, err := NewMapJson(jsonVal)
	if err != nil {
		return nil, err
	}
	return m.ValuesForKey(key, subkeys...)
}

// Wrap ValuesForKeyPath for JSON.
func JsonValuesForKeyPath(jsonVal []byte, path string, subkeys ...string) ([]interface{}, error) {
	m, err := NewMapJson(jsonVal)
	if err != nil {
		return nil, err
	}
	return m.ValuesForPath(path, subkeys...)
}

// Wrap UpdateValuesForPath for JSON
//	'jsonVal' is XML value
//	'newKeyValue' is the value to replace an existing value at the end of 'path'
//	'path' is the dot-notation path with the key whose value is to be replaced at the end
//	       (can include wildcard character, '*')
//	'subkeys' are key:value pairs of key:values that must match for the key
func JsonUpdateValsForPath(jsonVal []byte, newKeyValue interface{}, path string, subkeys ...string) ([]byte, error) {
	m, err := NewMapJson(jsonVal)
	if err != nil {
		return nil, err
	}
	_, err = m.UpdateValuesForPath(newKeyValue, path, subkeys...)
	if err != nil {
		return nil, err
	}
	return m.Json()
}

// Wrap NewMap for JSON and return as JSON
// 'jsonVal' is an JSON value
// 'keypairs' are "oldKey:newKey" values that conform to 'keypairs' in (Map)NewMap.
func JsonNewJson(jsonVal []byte, keypairs ...string) ([]byte, error) {
	m, err := NewMapJson(jsonVal)
	if err != nil {
		return nil, err
	}
	n, err := m.NewMap(keypairs...)
	if err != nil {
		return nil, err
	}
	return n.Json()
}

// Wrap NewMap for JSON and return as XML
// 'jsonVal' is an JSON value
// 'keypairs' are "oldKey:newKey" values that conform to 'keypairs' in (Map)NewMap.
func JsonNewXml(jsonVal []byte, keypairs ...string) ([]byte, error) {
	m, err := NewMapJson(jsonVal)
	if err != nil {
		return nil, err
	}
	n, err := m.NewMap(keypairs...)
	if err != nil {
		return nil, err
	}
	return n.Xml()
}

// Wrap LeafNodes for JSON.
// 'jsonVal' is an JSON value
func JsonLeafNodes(jsonVal []byte) ([]LeafNode, error) {
	m, err := NewMapJson(jsonVal)
	if err != nil {
		return nil, err
	}
	return m.LeafNodes(), nil
}

// Wrap LeafValues for JSON.
// 'jsonVal' is an JSON value
func JsonLeafValues(jsonVal []byte) ([]interface{}, error) {
	m, err := NewMapJson(jsonVal)
	if err != nil {
		return nil, err
	}
	return m.LeafValues(), nil
}

// Wrap LeafPath for JSON.
// 'xmlVal' is an JSON value
func JsonLeafPath(jsonVal []byte) ([]string, error) {
	m, err := NewMapJson(jsonVal)
	if err != nil {
		return nil, err
	}
	return m.LeafPaths(), nil
}

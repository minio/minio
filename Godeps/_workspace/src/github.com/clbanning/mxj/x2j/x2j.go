// Copyright 2012-2014 Charles Banning. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file

// x2j - For (mostly) backwards compatibility with legacy x2j package.
// Wrappers for end-to-end XML to JSON transformation and value manipulation.
package x2j

import (
	. "github.com/clbanning/mxj"
	"io"
)

// FromXml() --> map[string]interface{}
func XmlToMap(xmlVal []byte) (map[string]interface{}, error) {
	m, err := NewMapXml(xmlVal)
	if err != nil {
		return nil, err
	}
	return map[string]interface{}(m), nil
}

// map[string]interface{} --> ToXml()
func MapToXml(m map[string]interface{}) ([]byte, error) {
	return Map(m).Xml()
}

// FromXml() --> ToJson().
func XmlToJson(xmlVal []byte, safeEncoding ...bool) ([]byte, error) {
	m, err := NewMapXml(xmlVal)
	if err != nil {
		return nil, err
	}
	return m.Json(safeEncoding...)
}

// FromXml() --> ToJsonWriterRaw().
func XmlToJsonWriter(xmlVal []byte, jsonWriter io.Writer, safeEncoding ...bool) ([]byte, error) {
	m, err := NewMapXml(xmlVal)
	if err != nil {
		return nil, err
	}
	return m.JsonWriterRaw(jsonWriter, safeEncoding...)
}

// FromXmlReaderRaw() --> ToJson().
func XmlReaderToJson(xmlReader io.Reader, safeEncoding ...bool) ([]byte, []byte, error) {
	m, xraw, err := NewMapXmlReaderRaw(xmlReader)
	if err != nil {
		return xraw, nil, err
	}
	j, jerr := m.Json(safeEncoding...)
	return xraw, j, jerr
}

// FromXmlReader() --> ToJsonWriter().  Handy for bulk transformation of documents.
func XmlReaderToJsonWriter(xmlReader io.Reader, jsonWriter io.Writer, safeEncoding ...bool) ([]byte, []byte, error) {
	m, xraw, err := NewMapXmlReaderRaw(xmlReader)
	if err != nil {
		return xraw, nil, err
	}
	jraw, jerr := m.JsonWriterRaw(jsonWriter, safeEncoding...)
	return xraw, jraw, jerr
}

// XML wrappers for Map methods implementing tag path and value functions.

// Wrap PathsForKey for XML.
func XmlPathsForTag(xmlVal []byte, tag string) ([]string, error) {
	m, err := NewMapXml(xmlVal)
	if err != nil {
		return nil, err
	}
	paths := m.PathsForKey(tag)
	return paths, nil
}

// Wrap PathForKeyShortest for XML.
func XmlPathForTagShortest(xmlVal []byte, tag string) (string, error) {
	m, err := NewMapXml(xmlVal)
	if err != nil {
		return "", err
	}
	path := m.PathForKeyShortest(tag)
	return path, nil
}

// Wrap ValuesForKey for XML.
// 'attrs' are key:value pairs for attributes, where key is attribute label prepended with a hypen, '-'.
func XmlValuesForTag(xmlVal []byte, tag string, attrs ...string) ([]interface{}, error) {
	m, err := NewMapXml(xmlVal)
	if err != nil {
		return nil, err
	}
	return m.ValuesForKey(tag, attrs...)
}

// Wrap ValuesForPath for XML.
// 'attrs' are key:value pairs for attributes, where key is attribute label prepended with a hypen, '-'.
func XmlValuesForPath(xmlVal []byte, path string, attrs ...string) ([]interface{}, error) {
	m, err := NewMapXml(xmlVal)
	if err != nil {
		return nil, err
	}
	return m.ValuesForPath(path, attrs...)
}

// Wrap UpdateValuesForPath for XML
//	'xmlVal' is XML value
//	'newTagValue' is the value to replace an existing value at the end of 'path'
//	'path' is the dot-notation path with the tag whose value is to be replaced at the end
//	       (can include wildcard character, '*')
//	'subkeys' are key:value pairs of tag:values that must match for the tag
func XmlUpdateValsForPath(xmlVal []byte, newTagValue interface{}, path string, subkeys ...string) ([]byte, error) {
	m, err := NewMapXml(xmlVal)
	if err != nil {
		return nil, err
	}
	_, err = m.UpdateValuesForPath(newTagValue, path, subkeys...)
	if err != nil {
		return nil, err
	}
	return m.Xml()
}

// Wrap NewMap for XML and return as XML
// 'xmlVal' is an XML value
// 'tagpairs' are "oldTag:newTag" values that conform to 'keypairs' in (Map)NewMap.
func XmlNewXml(xmlVal []byte, tagpairs ...string) ([]byte, error) {
	m, err := NewMapXml(xmlVal)
	if err != nil {
		return nil, err
	}
	n, err := m.NewMap(tagpairs...)
	if err != nil {
		return nil, err
	}
	return n.Xml()
}

// Wrap NewMap for XML and return as JSON
// 'xmlVal' is an XML value
// 'tagpairs' are "oldTag:newTag" values that conform to 'keypairs' in (Map)NewMap.
func XmlNewJson(xmlVal []byte, tagpairs ...string) ([]byte, error) {
	m, err := NewMapXml(xmlVal)
	if err != nil {
		return nil, err
	}
	n, err := m.NewMap(tagpairs...)
	if err != nil {
		return nil, err
	}
	return n.Json()
}

// Wrap LeafNodes for XML.
// 'xmlVal' is an XML value
func XmlLeafNodes(xmlVal []byte) ([]LeafNode, error) {
	m, err := NewMapXml(xmlVal)
	if err != nil {
		return nil, err
	}
	return m.LeafNodes(), nil
}

// Wrap LeafValues for XML.
// 'xmlVal' is an XML value
func XmlLeafValues(xmlVal []byte) ([]interface{}, error) {
	m, err := NewMapXml(xmlVal)
	if err != nil {
		return nil, err
	}
	return m.LeafValues(), nil
}

// Wrap LeafPath for XML.
// 'xmlVal' is an XML value
func XmlLeafPath(xmlVal []byte) ([]string, error) {
	m, err := NewMapXml(xmlVal)
	if err != nil {
		return nil, err
	}
	return m.LeafPaths(), nil
}

package mxj

import (
	"encoding/xml"
	"reflect"
)

const (
	DefaultElementTag = "element"
)

// Encode arbitrary value as XML. 
// 
// Note: unmarshaling the resultant
// XML may not return the original value, since tag labels may have been injected
// to create the XML representation of the value.
/*
 Encode an arbitrary JSON object.
	package main
	
	import (
		"encoding/json"
		"fmt"
		"github/clbanning/mxj"
	)
	
	func main() {
		jsondata := []byte(`[
			{ "somekey":"somevalue" },
			"string",
			3.14159265,
			true
		]`)
		var i interface{}
		err := json.Unmarshal(jsondata, &i)
		if err != nil {
			// do something
		}
		x, err := anyxml.XmlIndent(i, "", "  ", "mydoc")
		if err != nil {
			// do something else
		}
		fmt.Println(string(x))
	}
	
	output:
		<mydoc>
		  <somekey>somevalue</somekey>
		  <element>string</element>
		  <element>3.14159265</element>
		  <element>true</element>
		</mydoc>
*/
// Alternative values for DefaultRootTag and DefaultElementTag can be set as:
// AnyXmlIndent( v, myRootTag, myElementTag).
func AnyXml(v interface{}, tags ...string) ([]byte, error) {
	if reflect.TypeOf(v).Kind() == reflect.Struct {
		return xml.Marshal(v)
	}

	var err error
	s := new(string)
	p := new(pretty)

	var rt, et string
	if len(tags) == 1 || len(tags) == 2 {
		rt = tags[0]
	} else {
		rt = DefaultRootTag
	}
	if len(tags) == 2 {
		et = tags[1]
	} else {
		et = DefaultElementTag
	}

	var ss string
	var b []byte
	switch v.(type) {
	case []interface{}:
		ss = "<" + rt + ">"
		for _, vv := range v.([]interface{}) {
			switch vv.(type) {
			case map[string]interface{}:
				m := vv.(map[string]interface{})
				if len(m) == 1 {
					for tag, val := range m {
						err = mapToXmlIndent(false, s, tag, val, p)
					}
				} else {
					err = mapToXmlIndent(false, s, et, vv, p)
				}
			default:
				err = mapToXmlIndent(false, s, et, vv, p)
			}
			if err != nil {
				break
			}
		}
		ss += *s + "</" + rt + ">"
		b = []byte(ss)
	case map[string]interface{}:
		m := Map(v.(map[string]interface{}))
		b, err = m.Xml(rt)
	default:
		err = mapToXmlIndent(false, s, rt, v, p)
		b = []byte(*s)
	}

	return b, err
}


// Encode an arbitrary value as a pretty XML string.
// Alternative values for DefaultRootTag and DefaultElementTag can be set as:
// AnyXmlIndent( v, "", "  ", myRootTag, myElementTag).
func AnyXmlIndent(v interface{}, prefix, indent string, tags ...string) ([]byte, error) {
	if reflect.TypeOf(v).Kind() == reflect.Struct {
		return xml.MarshalIndent(v, prefix, indent)
	}

	var err error
	s := new(string)
	p := new(pretty)
	p.indent = indent
	p.padding = prefix

	var rt, et string
	if len(tags) == 1 || len(tags) == 2 {
		rt = tags[0]
	} else {
		rt = DefaultRootTag
	}
	if len(tags) == 2 {
		et = tags[1]
	} else {
		et = DefaultElementTag
	}

	var ss string
	var b []byte
	switch v.(type) {
	case []interface{}:
		ss = "<" + rt + ">\n"
		p.Indent()
		for _, vv := range v.([]interface{}) {
			switch vv.(type) {
			case map[string]interface{}:
				m := vv.(map[string]interface{})
				if len(m) == 1 {
					for tag, val := range m {
						err = mapToXmlIndent(true, s, tag, val, p)
					}
				} else {
					p.start = 1 // we 1 tag in
					err = mapToXmlIndent(true, s, et, vv, p)
					*s += "\n"
				}
			default:
				p.start = 0 // in case trailing p.start = 1
				err = mapToXmlIndent(true, s, et, vv, p)
			}
			if err != nil {
				break
			}
		}
		ss += *s + "</" + rt + ">"
		b = []byte(ss)
	case map[string]interface{}:
		m := Map(v.(map[string]interface{}))
		b, err = m.XmlIndent(prefix, indent, rt)
	default:
		err = mapToXmlIndent(true, s, rt, v, p)
		b = []byte(*s)
	}

	return b, err
}

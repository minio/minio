package mxj

import (
	"fmt"
	"testing"
)

func TestMxjHeader(t *testing.T) {
	fmt.Println("\n----------------  mxj_test.go ...\n")
}

func TestMap(t *testing.T) {
	m := New()

	m["key"] = interface{}("value")
	v := map[string]interface{}{"bool": true, "float": 3.14159, "string": "Now is the time"}
	vv := []interface{}{3.1415962535, false, "for all good men"}
	v["listkey"] = interface{}(vv)
	m["newkey"] = interface{}(v)

	fmt.Println("TestMap, m:", m)
	fmt.Println("TestMap, StringIndent:", m.StringIndent())

	o := interface{}(m.Old())
	switch o.(type) {
	case map[string]interface{}:
		// do nothing
	default:
		t.Fatal("invalid type for m.Old()")
	}

	m, _ = NewMapXml([]byte(`<doc><tag><sub_tag1>Hello</sub_tag1><sub_tag2>World</sub_tag2></tag></doc>`))
	fmt.Println("TestMap, m_fromXML:", m)
	fmt.Println("TestMap, StringIndent:", m.StringIndent())

	mm, _ := m.Copy()
	fmt.Println("TestMap, m.Copy():", mm)
}

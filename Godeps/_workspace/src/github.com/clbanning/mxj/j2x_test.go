package mxj

import (
	"fmt"
	"testing"
)

var jjdata = []byte(`{ "key1":"string", "key2":34, "key3":true, "key4":"unsafe: <>&", "key5":null }`)

func TestJ2XHeader(t *testing.T) {
	fmt.Println("\n---------------- j2x_test .go ...\n")
}

func TestJ2X(t *testing.T) {

	m, err := NewMapJson(jjdata)
	if err != nil {
		t.Fatal("NewMapJson, err:", err)
	}

	x, err := m.Xml()
	if err != nil {
		t.Fatal("m.Xml(), err:", err)
	}

	fmt.Println("j2x, jdata:", string(jjdata))
	fmt.Println("j2x, m    :", m)
	fmt.Println("j2x, xml  :", string(x))
}

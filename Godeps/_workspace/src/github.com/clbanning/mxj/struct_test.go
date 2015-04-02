package mxj

import (
	"fmt"
	"testing"
)

func TestStructHeader(t *testing.T) {
	fmt.Println("\n----------------  struct_test.go ...\n")
}

func TestNewMapStruct(t *testing.T) {
	type str struct {
		IntVal   int     `json:"int"`
		StrVal   string  `json:"str"`
		FloatVal float64 `json:"float"`
		BoolVal  bool    `json:"bool"`
		private  string
	}
	s := str{IntVal: 4, StrVal: "now's the time", FloatVal: 3.14159, BoolVal: true, private: "It's my party"}

	m, merr := NewMapStruct(s)
	if merr != nil {
		t.Fatal("merr:", merr.Error())
	}

	fmt.Printf("NewMapStruct, s: %#v\n", s)
	fmt.Printf("NewMapStruct, m: %#v\n", m)

	m, merr = NewMapStruct(s)
	if merr != nil {
		t.Fatal("merr:", merr.Error())
	}

	fmt.Printf("NewMapStruct, s: %#v\n", s)
	fmt.Printf("NewMapStruct, m: %#v\n", m)
}

func TestNewMapStructError(t *testing.T) {
	var s string
	_, merr := NewMapStruct(s)
	if merr == nil {
		t.Fatal("NewMapStructError, merr is nil")
	}

	fmt.Println("NewMapStructError, merr:", merr.Error())
}

func TestStruct(t *testing.T) {
	type str struct {
		IntVal   int     `json:"int"`
		StrVal   string  `json:"str"`
		FloatVal float64 `json:"float"`
		BoolVal  bool    `json:"bool"`
		private  string
	}
	var s str
	m := Map{"int": 4, "str": "now's the time", "float": 3.14159, "bool": true, "private": "Somewhere over the rainbow"}

	mverr := m.Struct(&s)
	if mverr != nil {
		t.Fatal("mverr:", mverr.Error())
	}

	fmt.Printf("Struct, m: %#v\n", m)
	fmt.Printf("Struct, s: %#v\n", s)
}

func TestStructError(t *testing.T) {
	type str struct {
		IntVal   int     `json:"int"`
		StrVal   string  `json:"str"`
		FloatVal float64 `json:"float"`
		BoolVal  bool    `json:"bool"`
	}
	var s str
	mv := Map{"int": 4, "str": "now's the time", "float": 3.14159, "bool": true}

	mverr := mv.Struct(s)
	if mverr == nil {
		t.Fatal("StructError, no error returned")
	}
	fmt.Println("StructError, mverr:", mverr.Error())
}

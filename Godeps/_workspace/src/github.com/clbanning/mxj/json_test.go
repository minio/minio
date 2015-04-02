package mxj

import (
	"bytes"
	"fmt"
	"io"
	"testing"
)

var jdata = []byte(`{ "key1":"string", "key2":34, "key3":true, "key4":"unsafe: <>&", "key5":null }`)
var jdata2 = []byte(`{ "key1":"string", "key2":34, "key3":true, "key4":"unsafe: <>&" },
	{ "key":"value in new JSON string" }`)

func TestJsonHeader(t *testing.T) {
	fmt.Println("\n----------------  json_test.go ...\n")
}

func TestNewMapJson(t *testing.T) {

	m, merr := NewMapJson(jdata)
	if merr != nil {
		t.Fatal("NewMapJson, merr:", merr.Error())
	}

	fmt.Println("NewMapJson, jdata:", string(jdata))
	fmt.Printf("NewMapJson, m    : %#v\n", m)
}

func TestNewMapJsonNumber(t *testing.T) {

	JsonUseNumber = true

	m, merr := NewMapJson(jdata)
	if merr != nil {
		t.Fatal("NewMapJson, merr:", merr.Error())
	}

	fmt.Println("NewMapJson, jdata:", string(jdata))
	fmt.Printf("NewMapJson, m    : %#v\n", m)

	JsonUseNumber = false
}

func TestNewMapJsonError(t *testing.T) {

	m, merr := NewMapJson(jdata[:len(jdata)-2])
	if merr == nil {
		t.Fatal("NewMapJsonError, m:", m)
	}

	fmt.Println("NewMapJsonError, jdata :", string(jdata[:len(jdata)-2]))
	fmt.Println("NewMapJsonError, merror:", merr.Error())

	newData := []byte(`{ "this":"is", "in":error }`)
	m, merr = NewMapJson(newData)
	if merr == nil {
		t.Fatal("NewMapJsonError, m:", m)
	}

	fmt.Println("NewMapJsonError, newData :", string(newData))
	fmt.Println("NewMapJsonError, merror  :", merr.Error())
}

func TestNewMapJsonReader(t *testing.T) {

	rdr := bytes.NewBuffer(jdata2)

	for {
		m, jb, merr := NewMapJsonReaderRaw(rdr)
		if merr != nil && merr != io.EOF {
			t.Fatal("NewMapJsonReader, merr:", merr.Error())
		}
		if merr == io.EOF {
			break
		}

		fmt.Println("NewMapJsonReader, jb:", string(jb))
		fmt.Printf("NewMapJsonReader, m : %#v\n", m)
	}
}

func TestNewMapJsonReaderNumber(t *testing.T) {

	JsonUseNumber = true

	rdr := bytes.NewBuffer(jdata2)

	for {
		m, jb, merr := NewMapJsonReaderRaw(rdr)
		if merr != nil && merr != io.EOF {
			t.Fatal("NewMapJsonReader, merr:", merr.Error())
		}
		if merr == io.EOF {
			break
		}

		fmt.Println("NewMapJsonReader, jb:", string(jb))
		fmt.Printf("NewMapJsonReader, m : %#v\n", m)
	}

	JsonUseNumber = false
}

func TestJson(t *testing.T) {

	m, _ := NewMapJson(jdata)

	j, jerr := m.Json()
	if jerr != nil {
		t.Fatal("Json, jerr:", jerr.Error())
	}

	fmt.Println("Json, jdata:", string(jdata))
	fmt.Println("Json, j    :", string(j))

	j, _ = m.Json(true)
	fmt.Println("Json, j safe:", string(j))
}

func TestJsonWriter(t *testing.T) {
	mv := Map(map[string]interface{}{"this": "is a", "float": 3.14159, "and": "a", "bool": true})

	w := new(bytes.Buffer)
	raw, err := mv.JsonWriterRaw(w)
	if err != nil {
		t.Fatal("err:", err.Error())
	}

	b := make([]byte, w.Len())
	_, err = w.Read(b)
	if err != nil {
		t.Fatal("err:", err.Error())
	}

	fmt.Println("JsonWriter, raw:", string(raw))
	fmt.Println("JsonWriter, b  :", string(b))
}

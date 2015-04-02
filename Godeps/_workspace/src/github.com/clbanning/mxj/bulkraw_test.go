// bulk_test.go - uses Handler and Writer functions to process some streams as a demo.

package mxj

import (
	"bytes"
	"fmt"
	"testing"
)

func TestBulkRawHeader(t *testing.T) {
	fmt.Println("\n----------------  bulkraw_test.go ...\n")
}

// use data from bulk_test.go

var jsonWriterRaw = new(bytes.Buffer)
var xmlWriterRaw = new(bytes.Buffer)

var jsonErrLogRaw = new(bytes.Buffer)
var xmlErrLogRaw = new(bytes.Buffer)

func TestXmlReaderRaw(t *testing.T) {
	// create Reader for xmldata
	xmlReader := bytes.NewReader(xmldata)

	// read XML from Reader and pass Map value with the raw XML to handler
	err := HandleXmlReaderRaw(xmlReader, bxmaphandlerRaw, bxerrhandlerRaw)
	if err != nil {
		t.Fatal("err:", err.Error())
	}

	// get the JSON
	j := make([]byte, jsonWriterRaw.Len())
	_, _ = jsonWriterRaw.Read(j)

	// get the errors
	e := make([]byte, xmlErrLogRaw.Len())
	_, _ = xmlErrLogRaw.Read(e)

	// print the input
	fmt.Println("XmlReaderRaw, xmldata:\n", string(xmldata))
	// print the result
	fmt.Println("XmlReaderRaw, result :\n", string(j))
	// print the errors
	fmt.Println("XmlReaderRaw, errors :\n", string(e))
}

func bxmaphandlerRaw(m Map, raw []byte) bool {
	j, err := m.JsonIndent("", "  ", true)
	if err != nil {
		return false
	}

	_, _ = jsonWriterRaw.Write(j)
	// put in a NL to pretty up printing the Writer
	_, _ = jsonWriterRaw.Write([]byte("\n"))
	return true
}

func bxerrhandlerRaw(err error, raw []byte) bool {
	// write errors to file
	_, _ = xmlErrLogRaw.Write([]byte(err.Error()))
	_, _ = xmlErrLogRaw.Write([]byte("\n")) // pretty up
	_, _ = xmlErrLogRaw.Write(raw)
	_, _ = xmlErrLogRaw.Write([]byte("\n")) // pretty up
	return true
}

func TestJsonReaderRaw(t *testing.T) {
	jsonReader := bytes.NewReader(jsondata)

	// read all the JSON
	err := HandleJsonReaderRaw(jsonReader, bjmaphandlerRaw, bjerrhandlerRaw)
	if err != nil {
		t.Fatal("err:", err.Error())
	}

	// get the XML
	x := make([]byte, xmlWriterRaw.Len())
	_, _ = xmlWriterRaw.Read(x)

	// get the errors
	e := make([]byte, jsonErrLogRaw.Len())
	_, _ = jsonErrLogRaw.Read(e)

	// print the input
	fmt.Println("JsonReaderRaw, jsondata:\n", string(jsondata))
	// print the result
	fmt.Println("JsonReaderRaw, result  :\n", string(x))
	// print the errors
	fmt.Println("JsonReaderRaw, errors :\n", string(e))
}

func bjmaphandlerRaw(m Map, raw []byte) bool {
	x, err := m.XmlIndent("  ", "  ")
	if err != nil {
		return false
	}
	_, _ = xmlWriterRaw.Write(x)
	// put in a NL to pretty up printing the Writer
	_, _ = xmlWriterRaw.Write([]byte("\n"))
	return true
}

func bjerrhandlerRaw(err error, raw []byte) bool {
	// write errors to file
	_, _ = jsonErrLogRaw.Write([]byte(err.Error()))
	_, _ = jsonErrLogRaw.Write([]byte("\n")) // pretty up, Error() from json.Unmarshal !NL
	_, _ = jsonErrLogRaw.Write(raw)
	_, _ = jsonErrLogRaw.Write([]byte("\n")) // pretty up
	return true
}

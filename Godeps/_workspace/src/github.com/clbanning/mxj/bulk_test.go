// bulk_test.go - uses Handler and Writer functions to process some streams as a demo.

package mxj

import (
	"bytes"
	"fmt"
	"testing"
)

func TestBulkHeader(t *testing.T) {
	fmt.Println("\n----------------  bulk_test.go ...\n")
}

var jsonWriter = new(bytes.Buffer)
var xmlWriter = new(bytes.Buffer)

var jsonErrLog = new(bytes.Buffer)
var xmlErrLog = new(bytes.Buffer)

func TestXmlReader(t *testing.T) {
	// create Reader for xmldata
	xmlReader := bytes.NewReader(xmldata)

	// read XML from Readerand pass Map value with the raw XML to handler
	err := HandleXmlReader(xmlReader, bxmaphandler, bxerrhandler)
	if err != nil {
		t.Fatal("err:", err.Error())
	}

	// get the JSON
	j := make([]byte, jsonWriter.Len())
	_, _ = jsonWriter.Read(j)

	// get the errors
	e := make([]byte, xmlErrLog.Len())
	_, _ = xmlErrLog.Read(e)

	// print the input
	fmt.Println("XmlReader, xmldata:\n", string(xmldata))
	// print the result
	fmt.Println("XmlReader, result :\n", string(j))
	// print the errors
	fmt.Println("XmlReader, errors :\n", string(e))
}

func bxmaphandler(m Map) bool {
	j, err := m.JsonIndent("", "  ", true)
	if err != nil {
		return false
	}

	_, _ = jsonWriter.Write(j)
	// put in a NL to pretty up printing the Writer
	_, _ = jsonWriter.Write([]byte("\n"))
	return true
}

func bxerrhandler(err error) bool {
	// write errors to file
	_, _ = xmlErrLog.Write([]byte(err.Error()))
	_, _ = xmlErrLog.Write([]byte("\n")) // pretty up
	return true
}

func TestJsonReader(t *testing.T) {
	jsonReader := bytes.NewReader(jsondata)

	// read all the JSON
	err := HandleJsonReader(jsonReader, bjmaphandler, bjerrhandler)
	if err != nil {
		t.Fatal("err:", err.Error())
	}

	// get the XML
	x := make([]byte, xmlWriter.Len())
	_, _ = xmlWriter.Read(x)

	// get the errors
	e := make([]byte, jsonErrLog.Len())
	_, _ = jsonErrLog.Read(e)

	// print the input
	fmt.Println("JsonReader, jsondata:\n", string(jsondata))
	// print the result
	fmt.Println("JsonReader, result  :\n", string(x))
	// print the errors
	fmt.Println("JsonReader, errors :\n", string(e))
}

func bjmaphandler(m Map) bool {
	x, err := m.XmlIndent("  ", "  ")
	if err != nil {
		return false
	}
	_, _ = xmlWriter.Write(x)
	// put in a NL to pretty up printing the Writer
	_, _ = xmlWriter.Write([]byte("\n"))
	return true
}

func bjerrhandler(err error) bool {
	// write errors to file
	_, _ = jsonErrLog.Write([]byte(err.Error()))
	_, _ = jsonErrLog.Write([]byte("\n")) // pretty up
	return true
}

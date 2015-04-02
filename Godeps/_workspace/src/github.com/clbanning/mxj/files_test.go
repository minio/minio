package mxj

import (
	"fmt"
	"testing"
)

func TestFilesHeader(t *testing.T) {
	fmt.Println("\n----------------  files_test.go ...\n")
}

func TestNewJsonFile(t *testing.T) {
	fmt.Println("NewMapsFromJsonFile()")
	am, err := NewMapsFromJsonFile("files_test.json")
	if err != nil {
		t.Fatal(err.Error())
	}
	for _, v := range am {
		fmt.Printf("%v\n", v)
	}

	am, err = NewMapsFromJsonFile("nil")
	if err == nil {
		t.Fatal("no error returned for read of nil file")
	}
	fmt.Println("caught error: ", err.Error())

	am, err = NewMapsFromJsonFile("files_test.badjson")
	if err == nil {
		t.Fatal("no error returned for read of badjson file")
	}
	fmt.Println("caught error: ", err.Error())
}

func TestNewJsonFileRaw(t *testing.T) {
	fmt.Println("NewMapsFromJsonFileRaw()")
	mr, err := NewMapsFromJsonFileRaw("files_test.json")
	if err != nil {
		t.Fatal(err.Error())
	}
	for _, v := range mr {
		fmt.Printf("%v\n", v)
	}

	mr, err = NewMapsFromJsonFileRaw("nil")
	if err == nil {
		t.Fatal("no error returned for read of nil file")
	}
	fmt.Println("caught error: ", err.Error())

	mr, err = NewMapsFromJsonFileRaw("files_test.badjson")
	if err == nil {
		t.Fatal("no error returned for read of badjson file")
	}
	fmt.Println("caught error: ", err.Error())
}

func TestNewXmFile(t *testing.T) {
	fmt.Println("NewMapsFromXmlFile()")
	am, err := NewMapsFromXmlFile("files_test.xml")
	if err != nil {
		t.Fatal(err.Error())
	}
	for _, v := range am {
		fmt.Printf("%v\n", v)
	}

	am, err = NewMapsFromXmlFile("nil")
	if err == nil {
		t.Fatal("no error returned for read of nil file")
	}
	fmt.Println("caught error: ", err.Error())

	am, err = NewMapsFromXmlFile("files_test.badxml")
	if err == nil {
		t.Fatal("no error returned for read of badjson file")
	}
	fmt.Println("caught error: ", err.Error())
}

func TestNewXmFileRaw(t *testing.T) {
	fmt.Println("NewMapsFromXmlFileRaw()")
	mr, err := NewMapsFromXmlFileRaw("files_test.xml")
	if err != nil {
		t.Fatal(err.Error())
	}
	for _, v := range mr {
		fmt.Printf("%v\n", v)
	}

	mr, err = NewMapsFromXmlFileRaw("nil")
	if err == nil {
		t.Fatal("no error returned for read of nil file")
	}
	fmt.Println("caught error: ", err.Error())

	mr, err = NewMapsFromXmlFileRaw("files_test.badxml")
	if err == nil {
		t.Fatal("no error returned for read of badjson file")
	}
	fmt.Println("caught error: ", err.Error())
}

func TestMaps(t *testing.T) {
	fmt.Println("TestMaps()")
	mvs := NewMaps()
	for i := 0; i < 2; i++ {
		m, _ := NewMapJson([]byte(`{ "this":"is", "a":"test" }`))
		mvs = append(mvs, m)
	}
	fmt.Println("mvs:", mvs)

	s, _ := mvs.JsonString()
	fmt.Println("JsonString():", s)

	s, _ = mvs.JsonStringIndent("", "  ")
	fmt.Println("JsonStringIndent():", s)

	s, _ = mvs.XmlString()
	fmt.Println("XmlString():", s)

	s, _ = mvs.XmlStringIndent("", "  ")
	fmt.Println("XmlStringIndent():", s)
}

func TestJsonFile(t *testing.T) {
	am, err := NewMapsFromJsonFile("files_test.json")
	if err != nil {
		t.Fatal(err.Error())
	}
	for _, v := range am {
		fmt.Printf("%v\n", v)
	}

	err = am.JsonFile("files_test_dup.json")
	if err != nil {
		t.Fatal(err.Error())
	}
	fmt.Println("files_test_dup.json written")

	err = am.JsonFileIndent("files_test_indent.json", "", "  ")
	if err != nil {
		t.Fatal(err.Error())
	}
	fmt.Println("files_test_indent.json written")
}

func TestXmlFile(t *testing.T) {
	am, err := NewMapsFromXmlFile("files_test.xml")
	if err != nil {
		t.Fatal(err.Error())
	}
	for _, v := range am {
		fmt.Printf("%v\n", v)
	}

	err = am.XmlFile("files_test_dup.xml")
	if err != nil {
		t.Fatal(err.Error())
	}
	fmt.Println("files_test_dup.xml written")

	err = am.XmlFileIndent("files_test_indent.xml", "", "  ")
	if err != nil {
		t.Fatal(err.Error())
	}
	fmt.Println("files_test_indent.xml written")
}

package mxj

import (
	"encoding/json"
	"fmt"
	"testing"
)

func TestAnyXmlHeader(t *testing.T) {
	fmt.Println("\n----------------  anyxml_test.go ...\n")
}

var anydata = []byte(`[
    {
        "somekey": "somevalue"
    },
    {
        "somekey": "somevalue"
    },
    {
        "somekey": "somevalue",
        "someotherkey": "someothervalue"
    },
    "string",
    3.14159265,
    true
]`)

type MyStruct struct {
	Somekey string  `xml:"somekey"`
	B       float32 `xml:"floatval"`
}

func TestAnyXml(t *testing.T) {
	var i interface{}
	err := json.Unmarshal(anydata, &i)
	if err != nil {
		t.Fatal(err)
	}
	x, err := AnyXml(i)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println("[]->x:", string(x))

	a := []interface{}{"try", "this", 3.14159265, true}
	x, err = AnyXml(a)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println("a->x:", string(x))

	x, err = AnyXml(a, "myRootTag", "myElementTag")
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println("a->x:", string(x))

	x, err = AnyXml(3.14159625)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println("f->x:", string(x))

	s := MyStruct{"somevalue", 3.14159625}
	x, err = AnyXml(s)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println("s->x:", string(x))
}

func TestAnyXmlIndent(t *testing.T) {
	var i interface{}
	err := json.Unmarshal(anydata, &i)
	if err != nil {
		t.Fatal(err)
	}
	x, err := AnyXmlIndent(i, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println("[]->x:\n", string(x))

	a := []interface{}{"try", "this", 3.14159265, true}
	x, err = AnyXmlIndent(a, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println("a->x:\n", string(x))

	x, err = AnyXmlIndent(3.14159625, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println("f->x:\n", string(x))

	x, err = AnyXmlIndent(3.14159625, "", "  ", "myRootTag", "myElementTag")
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println("f->x:\n", string(x))

	s := MyStruct{"somevalue", 3.14159625}
	x, err = AnyXmlIndent(s, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println("s->x:\n", string(x))
}

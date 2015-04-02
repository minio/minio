package mxj

import (
	"bytes"
	"fmt"
	"io"
	"testing"
)

func TestNewMapHeader(t *testing.T) {
	fmt.Println("\n----------------  newmap_test.go ...\n")
}

func TestNewMap(t *testing.T) {
	j := []byte(`{ "A":"this", "B":"is", "C":"a", "D":"test" }`)
	fmt.Println("j:", string(j))

	m, _ := NewMapJson(j)
	fmt.Printf("m: %#v\n", m)

	fmt.Println("\n", `eval - m.NewMap("A:AA", "B:BB", "C:cc", "D:help")`)
	n, err := m.NewMap("A:AA", "B:BB", "C:cc", "D:help")
	if err != nil {
		fmt.Println("err:", err.Error())
	}
	j, _ = n.Json()
	fmt.Println("n.Json():", string(j))
	x, _ := n.Xml()
	fmt.Println("n.Xml():\n", string(x))
	x, _ = n.XmlIndent("", "  ")
	fmt.Println("n.XmlIndent():\n", string(x))

	fmt.Println("\n", `eval - m.NewMap("A:AA.A", "B:AA.B", "C:AA.B.cc", "D:hello.help")`)
	n, err = m.NewMap("A:AA.A", "B:AA.B", "C:AA.B.cc", "D:hello.help")
	if err != nil {
		fmt.Println("err:", err.Error())
	}
	j, _ = n.Json()
	fmt.Println("n.Json():", string(j))
	x, _ = n.Xml()
	fmt.Println("n.Xml():\n", string(x))
	x, _ = n.XmlIndent("", "  ")
	fmt.Println("n.XmlIndent():\n", string(x))

	var keypairs = []string{"A:xml.AA", "B:xml.AA.hello.again", "C:xml.AA", "D:xml.AA.hello.help"}
	fmt.Println("\n", `eval - m.NewMap keypairs:`, keypairs)
	n, err = m.NewMap(keypairs...)
	if err != nil {
		fmt.Println("err:", err.Error())
	}
	j, _ = n.Json()
	fmt.Println("n.Json():", string(j))
	x, _ = n.Xml()
	fmt.Println("n.Xml():\n", string(x))
	x, _ = n.XmlIndent("", "  ")
	fmt.Println("n.XmlIndent():\n", string(x))
}

// Need to normalize from an XML stream the values for "netid" and "idnet".
// Solution: make everything "netid"
// Demo how to re-label a key using mv.NewMap()

var msg1 = []byte(`
<?xml version="1.0" encoding="UTF-8"?>
<data>
    <netid>
        <disable>no</disable>
        <text1>default:text</text1>
        <word1>default:word</word1>
    </netid>
</data>
`)

var msg2 = []byte(`
<?xml version="1.0" encoding="UTF-8"?>
<data>
    <idnet>
        <disable>yes</disable>
        <text1>default:text</text1>
        <word1>default:word</word1>
    </idnet>
</data>
`)

func TestNetId(t *testing.T) {
	// let's create a message stream
	buf := new(bytes.Buffer)
	// load a couple of messages into it
	_, _ = buf.Write(msg1)
	_, _ = buf.Write(msg2)

	n := 0
	for {
		n++
		// read the stream as Map values - quit on io.EOF
		m, raw, merr := NewMapXmlReaderRaw(buf)
		if merr != nil && merr != io.EOF {
			// handle error - for demo we just print it and continue
			fmt.Printf("msg: %d - merr: %s\n", n, merr.Error())
			continue
		} else if merr == io.EOF {
			break
		}

		// the first keypair retains values if data correct
		// the second keypair relabels "idnet" to "netid"
		n, _ := m.NewMap("data.netid", "data.idnet:data.netid")
		x, _ := n.XmlIndent("", "  ")

		fmt.Println("original value:", string(raw))
		fmt.Println("new value:")
		fmt.Println(string(x))
	}
}

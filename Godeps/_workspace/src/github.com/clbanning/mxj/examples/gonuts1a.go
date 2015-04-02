// https://groups.google.com/forum/#!searchin/golang-nuts/idnet$20netid/golang-nuts/guM3ZHHqSF0/K1pBpMqQSSwJ
// http://play.golang.org/p/BFFDxphKYK

package main

import (
	"bytes"
	"fmt"
	"github.com/clbanning/mxj"
	"io"
)

// Demo how to re-label a key using mv.NewMap().
// Need to normalize from an XML stream the tags "netid" and "idnet".
// Solution: make everything "netid".

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

func main() {
	// let's create a message stream
	buf := new(bytes.Buffer)
	// load a couple of messages into it
	_, _ = buf.Write(msg1)
	_, _ = buf.Write(msg2)

	n := 0
	for {
		n++
		// read the stream as Map values - quit on io.EOF
		m, raw, merr := mxj.NewMapXmlReaderRaw(buf)
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

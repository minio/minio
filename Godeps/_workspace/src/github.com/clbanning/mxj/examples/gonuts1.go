// https://groups.google.com/forum/#!searchin/golang-nuts/idnet$20netid/golang-nuts/guM3ZHHqSF0/K1pBpMqQSSwJ
// http://play.golang.org/p/BFFDxphKYK

package main

import (
	"bytes"
	"fmt"
	"io"
	"github.com/clbanning/mxj"
)

// Demo how to compensate for irregular tag labels in data.
// Need to extract from an XML stream the values for "netid" and "idnet".
// Solution: use a wildcard path "data.*" to anonymize the "netid" and "idnet" tags.

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
		fmt.Println("\nMessage to parse:", string(*raw))
		fmt.Println("Map value for XML message:", m.StringIndent())

		// get the values for "netid" or "idnet" key using path == "data.*"
		values, _ := m.ValuesForPath("data.*")
		fmt.Println("\nmsg:", n, "> path == data.* - got array of values, len:", len(values))
		for i, val := range values {
			fmt.Println("ValuesForPath result array member -", i, ":", val)
			fmt.Println("              k:v pairs for array member:", i)
			for key, val := range val.(map[string]interface{}) {
				// You'd probably want to process the value, as appropriate.
				// Here we just print it out.
				fmt.Println("\t\t", key, ":", val)
			}
		}

		// This shows what happens if you wildcard the value keys for "idnet" and "netid"
		values, _ = m.ValuesForPath("data.*.*")
		fmt.Println("\npath == data.*.* - got an array of values, len(v):", len(values))
		fmt.Println("(Note: values returned by ValuesForPath are at maximum depth of the tree. So just have values.)")
		for i, val := range values {
			fmt.Println("ValuesForPath array member -", i, ":", val)
		}
	}
}

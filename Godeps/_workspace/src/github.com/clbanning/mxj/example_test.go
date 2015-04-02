// note - "// Output:" is  a key for "go test" to match function ouput with the lines that follow.
//        It is also use by "godoc" to build the Output block of the function / method documentation.
//        To skip processing Example* functions, use: go test -run "Test*"
//	       or make sure example function output matches // Output: documentation EXACTLY.

package mxj_test

import (
	"bytes"
	"fmt"
	"github.com/clbanning/mxj"
	"io"
)

func ExampleHandleXmlReader() {
	/*
		Bulk processing XML to JSON seems to be a common requirement.
		See: bulk_test.go for working example.
		     Run "go test" in package directory then scroll back to find output.

		The logic is as follows.

			// need somewhere to write the JSON.
			var jsonWriter io.Writer

			// probably want to log any errors in reading the XML stream
			var xmlErrLogger io.Writer

			// func to handle Map value from XML Reader
			func maphandler(m mxj.Map) bool {
				// marshal Map as JSON
				jsonVal, err := m.Json()
				if err != nil {
					// log error
					return false // stops further processing of XML Reader
				}

				// write JSON somewhere
				_, err = jsonWriter.Write(jsonVal)
				if err != nil {
					// log error
					return false // stops further processing of XML Reader
				}

				// continue - get next XML from Reader
				return true
			}

			// func to handle error from unmarshaling XML Reader
			func errhandler(errVal error) bool {
				// log error somewhere
				_, err := xmlErrLogger.Write([]byte(errVal.Error()))
				if err != nil {
					// log error
					return false // stops further processing of XML Reader
				}

				// continue processing
				return true
			}

			// func that starts bulk processing of the XML
				...
				// set up io.Reader for XML data - perhaps an os.File
				...
				err := mxj.HandleXmlReader(xmlReader, maphandler, errhandler)
				if err != nil {
					// handle error
				}
				...
	*/
}

func ExampleHandleXmlReaderRaw() {
	/*
		See: bulkraw_test.go for working example.
		Run "go test" in package directory then scroll back to find output.

		Basic logic for bulk XML to JSON processing is in HandleXmlReader example;
		the only major difference is in handler function signatures so they are passed
		the raw XML.  (Read documentation on NewXmlReader regarding performance.)
	*/
}

func ExampleHandleJsonReader() {
	/*
		See: bulk_test.go for working example.
		Run "go test" in package directory then scroll back to find output.

		Basic logic for bulk JSON to XML processing is similar to that for
		bulk XML to JSON processing as outlined in the HandleXmlReader example.
		The test case is also a good example.
	*/
}

func ExampleHandleJsonReaderRaw() {
	/*
		See: bulkraw_test.go for working example.
		Run "go test" in package directory then scroll back to find output.

		Basic logic for bulk JSON to XML processing is similar to that for
		bulk XML to JSON processing as outlined in the HandleXmlReader example.
		The test case is also a good example.
	*/
}

/*
func ExampleNewMapXmlReaderRaw() {
	// in an http.Handler

	mapVal, raw, err := mxj.NewMapXmlReader(req.Body)
	if err != nil {
		// handle error
	}
	logger.Print(string(*raw))
	// do something with mapVal

}
*/

func ExampleNewMapStruct() {
	type str struct {
		IntVal   int     `json:"int"`
		StrVal   string  `json:"str"`
		FloatVal float64 `json:"float"`
		BoolVal  bool    `json:"bool"`
		private  string
	}
	strVal := str{IntVal: 4, StrVal: "now's the time", FloatVal: 3.14159, BoolVal: true, private: "Skies are blue"}

	mapVal, merr := mxj.NewMapStruct(strVal)
	if merr != nil {
		// handle error
	}

	fmt.Printf("strVal: %#v\n", strVal)
	fmt.Printf("mapVal: %#v\n", mapVal)
	// Note: example output is conformed to pass "go test".  "mxj_test" is example_test.go package name.

	// Output:
	// strVal: mxj_test.str{IntVal:4, StrVal:"now's the time", FloatVal:3.14159, BoolVal:true, private:"Skies are blue"}
	// mapVal: mxj.Map{"int":4, "str":"now's the time", "float":3.14159, "bool":true}
}

func ExampleMap_Struct() {
	type str struct {
		IntVal   int     `json:"int"`
		StrVal   string  `json:"str"`
		FloatVal float64 `json:"float"`
		BoolVal  bool    `json:"bool"`
		private  string
	}

	mapVal := mxj.Map{"int": 4, "str": "now's the time", "float": 3.14159, "bool": true, "private": "Somewhere over the rainbow"}

	var strVal str
	mverr := mapVal.Struct(&strVal)
	if mverr != nil {
		// handle error
	}

	fmt.Printf("mapVal: %#v\n", mapVal)
	fmt.Printf("strVal: %#v\n", strVal)
	// Note: example output is conformed to pass "go test".  "mxj_test" is example_test.go package name.

	// Output:
	// mapVal: mxj.Map{"int":4, "str":"now's the time", "float":3.14159, "bool":true, "private":"Somewhere over the rainbow"}
	// strVal: mxj_test.str{IntVal:4, StrVal:"now's the time", FloatVal:3.14159, BoolVal:true, private:""}
}

func ExampleMap_ValuesForKeyPath() {
	// a snippet from examples/gonuts1.go
	// How to compensate for irregular tag labels in data.
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

	// let's create a message stream
	buf := new(bytes.Buffer)
	// load a couple of messages into it
	_, _ = buf.Write(msg1)
	_, _ = buf.Write(msg2)

	n := 0
	for {
		n++
		// Read the stream as Map values - quit on io.EOF.
		// Get the raw XML as well as the Map value.
		m, merr := mxj.NewMapXmlReader(buf)
		if merr != nil && merr != io.EOF {
			// handle error - for demo we just print it and continue
			fmt.Printf("msg: %d - merr: %s\n", n, merr.Error())
			continue
		} else if merr == io.EOF {
			break
		}

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
	}
	// Output:
	// msg: 1 > path == data.* - got array of values, len: 1
	// ValuesForPath result array member - 0 : map[disable:no text1:default:text word1:default:word]
	//               k:v pairs for array member: 0
	// 		 disable : no
	// 		 text1 : default:text
	// 		 word1 : default:word
	//
	// msg: 2 > path == data.* - got array of values, len: 1
	// ValuesForPath result array member - 0 : map[disable:yes text1:default:text word1:default:word]
	//               k:v pairs for array member: 0
	// 		 disable : yes
	// 		 text1 : default:text
	// 		 word1 : default:word
}

func ExampleMap_UpdateValuesForPath() {
	/*

	   var biblioDoc = []byte(`
	   <biblio>
	   	<author>
	   		<name>William Gaddis</name>
	   		<books>
	   			<book>
	   				<title>The Recognitions</title>
	   				<date>1955</date>
	   				<review>A novel that changed the face of American literature.</review>
	   			</book>
	   			<book>
	   				<title>JR</title>
	   				<date>1975</date>
	   				<review>Winner of National Book Award for Fiction.</review>
	   			</book>
	   		</books>
	   	</author>
	   </biblio>`)

	   	...
	   	m, merr := mxj.NewMapXml(biblioDoc)
	   	if merr != nil {
	   		// handle error
	   	}

	   	// change 'review' for a book
	   	count, err := m.UpdateValuesForPath("review:National Book Award winner." "*.*.*.*", "title:JR")
	   	if err != nil {
	   		// handle error
	   	}
	   	...

	   	// change 'date' value from string type to float64 type
	   	// Note: the following is equivalent to m, merr := NewMapXml(biblioDoc, mxj.Cast).
	   	path := m.PathForKeyShortest("date")
	   	v, err := m.ValuesForPath(path)
	   	if err != nil {
	   		// handle error
	   	}
	   	var total int
	   	for _, vv := range v {
	   		oldVal := "date:" + vv.(string)
	   		newVal := "date:" + vv.(string) + ":num"
	   		n, err := m.UpdateValuesForPath(newVal, path, oldVal)
	   		if err != nil {
	   			// handle error
	   		}
	   		total += n
	   	}
	   	...
	*/
}

func ExampleMap_Copy() {
	// Hand-crafted Map values that include structures do NOT Copy() as expected,
	// since to simulate a deep copy the original Map value is JSON encoded then decoded.

	type str struct {
		IntVal   int     `json:"int"`
		StrVal   string  `json:"str"`
		FloatVal float64 `json:"float"`
		BoolVal  bool    `json:"bool"`
		private  string
	}
	s := str{IntVal: 4, StrVal: "now's the time", FloatVal: 3.14159, BoolVal: true, private: "Skies are blue"}
	m := make(map[string]interface{}, 0)
	m["struct"] = interface{}(s)
	m["struct_ptr"] = interface{}(&s)
	m["misc"] = interface{}(`Now is the time`)

	mv := mxj.Map(m)
	cp, _ := mv.Copy()

	fmt.Printf("mv:%s\n", mv.StringIndent(2))
	fmt.Printf("cp:%s\n", cp.StringIndent(2))

	// Output:
	// mv:
	//     struct :[unknown] mxj_test.str{IntVal:4, StrVal:"now's the time", FloatVal:3.14159, BoolVal:true, private:"Skies are blue"}
	//     struct_ptr :[unknown] &mxj_test.str{IntVal:4, StrVal:"now's the time", FloatVal:3.14159, BoolVal:true, private:"Skies are blue"}
	//     misc :[string] Now is the time
	// cp:
	//     misc :[string] Now is the time
	//     struct :
	//       int :[float64] 4.00e+00
	//       str :[string] now's the time
	//       float :[float64] 3.14e+00
	//       bool :[bool] true
	//     struct_ptr :
	//       int :[float64] 4.00e+00
	//       str :[string] now's the time
	//       float :[float64] 3.14e+00
	//       bool :[bool] true
}

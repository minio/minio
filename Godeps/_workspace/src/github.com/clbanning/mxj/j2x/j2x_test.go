// thanks to Chris Malek (chris.r.malek@gmail.com) for suggestion to handle JSON list docs.

package j2x

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"testing"
)

func TestJsonToXml_1(t *testing.T) {
	// mimic a io.Reader
	// Body := bytes.NewReader([]byte(`{"some-null-value":"", "a-non-null-value":"bar"}`))
	Body := bytes.NewReader([]byte(`[{"some-null-value":"", "a-non-null-value":"bar"}]`))

	//body, err := ioutil.ReadAll(req.Body)
	body, err := ioutil.ReadAll(Body)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(string(body))
	//if err != nil {
	//	t.Fatal(err)
	//}

	var xmloutput []byte
	//xmloutput, err = j2x.JsonToXml(body)
	xmloutput, err = JsonToXml(body)

	//log.Println(string(xmloutput))

	if err != nil {
		t.Fatal(err)
		// log.Println(err)
		// http.Error(rw, "Could not convert to xml", 400)
	}
	fmt.Println("xmloutput:", string(xmloutput))
}

func TestJsonToXml_2(t *testing.T) {
	// mimic a io.Reader
	Body := bytes.NewReader([]byte(`{"somekey":[{"value":"1st"},{"value":"2nd"}]}`))

	//body, err := ioutil.ReadAll(req.Body)
	body, err := ioutil.ReadAll(Body)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(string(body))
	//if err != nil {
	//	t.Fatal(err)
	//}

	var xmloutput []byte
	//xmloutput, err = j2x.JsonToXml(body)
	xmloutput, err = JsonToXml(body)

	//log.Println(string(xmloutput))

	if err != nil {
		t.Fatal(err)
		// log.Println(err)
		// http.Error(rw, "Could not convert to xml", 400)
	}
	fmt.Println("xmloutput:", string(xmloutput))
}

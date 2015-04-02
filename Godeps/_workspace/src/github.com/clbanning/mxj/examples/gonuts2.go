// https://groups.google.com/forum/#!topic/golang-nuts/V83jUKluLnM
// http://play.golang.org/p/alWGk4MDBc

// Here messsages come in one of three forms:
//		<GetClaimStatusCodesResult>... list of ClaimStatusCodeRecord ...</GetClaimStatusCodeResult>
//		<GetClaimStatusCodesResult>... one instance of ClaimStatusCodeRecord ...</GetClaimStatusCodeResult>
//		<GetClaimStatusCodesResult>... empty element ...</GetClaimStatusCodeResult>
//		ValueForPath

package main

import (
	"fmt"
	"github.com/clbanning/mxj"
)

var xmlmsg1 = []byte(`<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">
<s:Body>
<GetClaimStatusCodesResponse xmlns="http://tempuri.org/">
<GetClaimStatusCodesResult xmlns:a="http://schemas.datacontract.org/2004/07/MRA.Claim.WebService.Domain" xmlns:i="http://www.w3.org/2001/XMLSchema-instance">
<a:ClaimStatusCodeRecord>
<a:Active>true</a:Active>
<a:Code>A</a:Code>
<a:Description>Initial Claim Review/Screening</a:Description>
</a:ClaimStatusCodeRecord>
<a:ClaimStatusCodeRecord>
<a:Active>true</a:Active>
<a:Code>B</a:Code>
<a:Description>Initial Contact Made w/ Provider</a:Description>
</a:ClaimStatusCodeRecord>
</GetClaimStatusCodesResult>
</GetClaimStatusCodesResponse>
</s:Body>
</s:Envelope>
`)

var xmlmsg2 = []byte(`<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">
<s:Body>
<GetClaimStatusCodesResponse xmlns="http://tempuri.org/">
<GetClaimStatusCodesResult xmlns:a="http://schemas.datacontract.org/2004/07/MRA.Claim.WebService.Domain" xmlns:i="http://www.w3.org/2001/XMLSchema-instance">
<a:ClaimStatusCodeRecord>
<a:Active>true</a:Active>
<a:Code>A</a:Code>
<a:Description>Initial Claim Review/Screening</a:Description>
</a:ClaimStatusCodeRecord>
</GetClaimStatusCodesResult>
</GetClaimStatusCodesResponse>
</s:Body>
</s:Envelope>
`)

var xmlmsg3 = []byte(`<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">
<s:Body>
<GetClaimStatusCodesResponse xmlns="http://tempuri.org/">
<GetClaimStatusCodesResult xmlns:a="http://schemas.datacontract.org/2004/07/MRA.Claim.WebService.Domain" xmlns:i="http://www.w3.org/2001/XMLSchema-instance">
</GetClaimStatusCodesResult>
</GetClaimStatusCodesResponse>
</s:Body>
</s:Envelope>
`)

func main() {
	xmldata := [][]byte{xmlmsg1, xmlmsg2, xmlmsg3}
	fullPath(xmldata)
	partPath1(xmlmsg1)
	partPath2(xmlmsg1)
	partPath3(xmlmsg1)
	partPath4(xmlmsg1)
	partPath5(xmlmsg1)
	partPath6(xmlmsg1)
}

func fullPath(xmldata [][]byte) {
	for i, msg := range xmldata {
		fmt.Println("\ndoc:", i)
		fmt.Println(string(msg))
		// decode the XML
		m, _ := mxj.NewMapXml(msg)

		// get the value for the key path of interest
		path := "Envelope.Body.GetClaimStatusCodesResponse.GetClaimStatusCodesResult.ClaimStatusCodeRecord"
		values, err := m.ValueForPath(path)
		if err != nil {
			fmt.Println("err:", err.Error())
			return
		}
		if values == nil {
			fmt.Println("path:", path)
			fmt.Println("No ClaimStatusCodesResult code records.")
			continue
		}
		fmt.Println("\nPath:", path)
		fmt.Println("Number of code records:", len(values))
		fmt.Println("values:", values, "\n")
		for _, v := range values {
			switch v.(type) {
			case map[string]interface{}:
				fmt.Println("map[string]interface{}:", v.(map[string]interface{}))
			case []map[string]interface{}:
				fmt.Println("[]map[string]interface{}:", v.([]map[string]interface{}))
			case []interface{}:
				fmt.Println("[]interface{}:", v.([]interface{}))
			case interface{}:
				fmt.Println("interface{}:", v.(interface{}))
			}
		}
	}
}

func partPath1(msg []byte) {
	fmt.Println("\nmsg:",string(msg))
	m, _ := mxj.NewMapXml(msg)
	fmt.Println("m:",m.StringIndent())
	path := "Envelope.Body.*.*.ClaimStatusCodeRecord"
	values, err := m.ValueForPath(path)
	if err != nil {
		fmt.Println("err:", err.Error())
		return
	}
	if values == nil {
		fmt.Println("path:", path)
		fmt.Println("No ClaimStatusCodesResult code records.")
		return
	}
	fmt.Println("\nPath:", path)
	fmt.Println("Number of code records:", len(values))
	for n, v := range values {
		fmt.Printf("\t#%d: %v\n", n, v)
	}
}

func partPath2(msg []byte) {
	fmt.Println("\nmsg:",string(msg))
	m, _ := mxj.NewMapXml(msg)
	fmt.Println("m:",m.StringIndent())
	path := "Envelope.Body.*.*.*"
	values, err := m.ValueForPath(path)
	if err != nil {
		fmt.Println("err:", err.Error())
		return
	}
	if values == nil {
		fmt.Println("path:", path)
		fmt.Println("No ClaimStatusCodesResult code records.")
		return
	}
	fmt.Println("\nPath:", path)
	fmt.Println("Number of code records:", len(values))
	for n, v := range values {
		fmt.Printf("\t#%d: %v\n", n, v)
	}
}

func partPath3(msg []byte) {
	fmt.Println("\nmsg:",string(msg))
	m, _ := mxj.NewMapXml(msg)
	fmt.Println("m:",m.StringIndent())
	path := "*.*.*.*.*"
	values, err := m.ValueForPath(path)
	if err != nil {
		fmt.Println("err:", err.Error())
		return
	}
	if values == nil {
		fmt.Println("path:", path)
		fmt.Println("No ClaimStatusCodesResult code records.")
		return
	}
	fmt.Println("\nPath:", path)
	fmt.Println("Number of code records:", len(values))
	for n, v := range values {
		fmt.Printf("\t#%d: %v\n", n, v)
	}
}

func partPath4(msg []byte) {
	fmt.Println("\nmsg:",string(msg))
	m, _ := mxj.NewMapXml(msg)
	fmt.Println("m:",m.StringIndent())
	path := "*.*.*.*.*.Description"
	values, err := m.ValueForPath(path)
	if err != nil {
		fmt.Println("err:", err.Error())
		return
	}
	if values == nil {
		fmt.Println("path:", path)
		fmt.Println("No ClaimStatusCodesResult code records.")
		return
	}
	fmt.Println("\nPath:", path)
	fmt.Println("Number of code records:", len(values))
	for n, v := range values {
		fmt.Printf("\t#%d: %v\n", n, v)
	}
}

func partPath5(msg []byte) {
	fmt.Println("\nmsg:",string(msg))
	m, _ := mxj.NewMapXml(msg)
	fmt.Println("m:",m.StringIndent())
	path := "*.*.*.*.*.*"
	values, err := m.ValueForPath(path)
	if err != nil {
		fmt.Println("err:", err.Error())
		return
	}
	if values == nil {
		fmt.Println("path:", path)
		fmt.Println("No ClaimStatusCodesResult code records.")
		return
	}
	fmt.Println("\nPath:", path)
	fmt.Println("Number of code records:", len(values))
	for n, v := range values {
		fmt.Printf("\t#%d: %v\n", n, v)
	}
}

func partPath6(msg []byte) {
	fmt.Println("\nmsg:",string(msg))
	m, _ := mxj.NewMapXml(msg)
	fmt.Println("m:",m.StringIndent())
	path := "*.*.*.*.*.*.*"
	values, err := m.ValueForPath(path)
	if err != nil {
		fmt.Println("err:", err.Error())
		return
	}
	if values == nil {
		fmt.Println("path:", path)
		fmt.Println("No ClaimStatusCodesResult code records.")
		return
	}
	fmt.Println("\nPath:", path)
	fmt.Println("Number of code records:", len(values))
	for n, v := range values {
		fmt.Printf("\t#%d: %v\n", n, v)
	}
}

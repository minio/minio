package mxj

import (
	"fmt"
	"testing"
)

func TestLNHeader(t *testing.T) {
	fmt.Println("\n----------------  leafnode_test.go ...")
}

func TestLeafNodes(t *testing.T) {
	json1 := []byte(`{
		"friends": [
			{
				"skills": [
					44, 12
				]
			}
		]
	}`)

	json2 := []byte(`{
		"friends":
			{
				"skills": [
					44, 12
				]
			}

	}`)

	m, _ := NewMapJson(json1)
	ln := m.LeafNodes()
	fmt.Println("\njson1-LeafNodes:")
	for _, v := range ln {
		fmt.Printf("%#v\n", v)
	}
	p := m.LeafPaths()
	fmt.Println("\njson1-LeafPaths:")
	for _, v := range p {
		fmt.Printf("%#v\n", v)
	}

	m, _ = NewMapJson(json2)
	ln = m.LeafNodes()
	fmt.Println("\njson2-LeafNodes:")
	for _, v := range ln {
		fmt.Printf("%#v\n", v)
	}
	v := m.LeafValues()
	fmt.Println("\njson1-LeafValues:")
	for _, v := range v {
		fmt.Printf("%#v\n", v)
	}

	json3 := []byte(`{ "a":"list", "of":["data", "of", 3, "types", true]}`)
	m, _ = NewMapJson(json3)
	ln = m.LeafNodes()
	fmt.Println("\njson3-LeafNodes:")
	for _, v := range ln {
		fmt.Printf("%#v\n", v)
	}
	v = m.LeafValues()
	fmt.Println("\njson3-LeafValues:")
	for _, v := range v {
		fmt.Printf("%#v\n", v)
	}
	p = m.LeafPaths()
	fmt.Println("\njson3-LeafPaths:")
	for _, v := range p {
		fmt.Printf("%#v\n", v)
	}

	xmldata2 := []byte(`
		<doc>
			<item num="2" color="blue">Item 2 is blue</item>
			<item num="3" color="green">
				<arm side="left" length="3.5"/>
				<arm side="right" length="3.6"/>
			</item>
		</doc>`)
	m, err := NewMapXml(xmldata2)
	if err != nil {
		t.Fatal(err.Error())
	}
	fmt.Println("\nxml2data2-LeafValues:")
	ln = m.LeafNodes()
	for _, v := range ln {
		fmt.Printf("%#v\n", v)
	}
	fmt.Println("\nxml2data2-LeafValues(NoAttributes):")
	ln = m.LeafNodes(NoAttributes)
	for _, v := range ln {
		fmt.Printf("%#v\n", v)
	}
}

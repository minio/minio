// gonuts5.go - from https://groups.google.com/forum/#!topic/golang-nuts/MWoYY19of3o
// problem is to extract entries from <lst name="list3-1-1-1"></lst> by "int name="

package main

import (
	"fmt"
	"github.com/clbanning/mxj"
)

var xmlData = []byte(`<?xml version="1.0" encoding="UTF-8"?>
<response>
	<lst name="list1">
	</lst>
	<lst name="list2">
	</lst>
	<lst name="list3">
		<int name="docId">1</int>
		<lst name="list3-1">
			<lst name="list3-1-1">
				<lst name="list3-1-1-1">
					<int name="field1">1</int>
					<int name="field2">2</int>
					<int name="field3">3</int>
					<int name="field4">4</int>
					<int name="field5">5</int>
				</lst>
			</lst>
			<lst name="list3-1-2">
				<lst name="list3-1-2-1">
					<int name="field1">1</int>
					<int name="field2">2</int>
					<int name="field3">3</int>
					<int name="field4">4</int>
					<int name="field5">5</int>
				</lst>
			</lst>
		</lst>
	</lst>
</response>`)

func main() {
	// parse XML into a Map
	m, merr := mxj.NewMapXml(xmlData)
	if merr != nil {
		fmt.Println("merr:", merr.Error())
		return
	}

	// extract the 'list3-1-1-1' node - there'll be just 1?
	// NOTE: attribute keys are prepended with '-'
	lstVal, lerr := m.ValuesForPath("*.*.*.*.*", "-name:list3-1-1-1")
	if lerr != nil {
		fmt.Println("ierr:", lerr.Error())
		return
	}

	// assuming just one value returned - create a new Map
	mv := mxj.Map(lstVal[0].(map[string]interface{}))

	// extract the 'int' values by 'name' attribute: "-name"
	// interate over list of 'name' values of interest
	var names = []string{"field1", "field2", "field3", "field4", "field5"}
	for _, n := range names {
		vals, verr := mv.ValuesForKey("int", "-name:"+n)
		if verr != nil {
			fmt.Println("verr:", verr.Error(), len(vals))
			return
		}

		// values for simple elements have key '#text'
		// NOTE: there can be only one value for key '#text'
		fmt.Println(n, ":", vals[0].(map[string]interface{})["#text"])
	}
}

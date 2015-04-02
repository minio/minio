// https://groups.google.com/forum/#!topic/golang-nuts/cok6xasvI3w
// retrieve 'src' values from 'image' tags

package main

import (
	"fmt"
	"github.com/clbanning/mxj"
)

var xmldata = []byte(`
<doc>
	<image src="something.png"></image>
	<something>
		<image src="something else.jpg"></image>
		<title>something else</title>
	</something>
	<more_stuff>
		<some_images>
			<image src="first.gif"></image>
			<image src="second.gif"></image>
		</some_images>
	</more_stuff>
</doc>`)

func main() {
	fmt.Println("xmldata:", string(xmldata))

	// get all image tag values - []interface{}
	m, merr := mxj.NewMapXml(xmldata)
	if merr != nil {
		fmt.Println("merr:", merr.Error())
		return
	}

	// grab all values for attribute "src"
	// Note: attributes are prepended with a hyphen, '-'.
	sources, err := m.ValuesForKey("-src")
	if err != nil {
		fmt.Println("err:", err.Error())
		return
	}

	for _, src := range sources {
		fmt.Println(src)
	}
}

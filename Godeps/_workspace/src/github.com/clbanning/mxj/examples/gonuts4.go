// https://groups.google.com/forum/#!topic/golang-nuts/-N9Toa6qlu8
// shows that you can extract attribute values directly from tag/key path.
// NOTE: attribute values are encoded by prepending a hyphen, '-'.

package main

import (
	"fmt"
	"github.com/clbanning/mxj"
)

var xmldata = []byte(`
	<doc>
		<some_tag>
			<geoInfo>
				<city name="SEATTLE"/>
				<state name="WA"/>
				<country name="USA"/>
			</geoInfo>
			<geoInfo>
				<city name="VANCOUVER"/>
				<state name="BC"/>
				<country name="CA"/>
			</geoInfo>
			<geoInfo>
				<city name="LONDON"/>
				<country name="UK"/>
			</geoInfo>
		</some_tag>
	</doc>`)

func main() {
	fmt.Println("xmldata:", string(xmldata))

	m, merr := mxj.NewMapXml(xmldata)
	if merr != nil {
		fmt.Println("merr:", merr)
		return
	}

	// Attributes are keys with prepended hyphen, '-'.
	values, err := m.ValuesForPath("doc.some_tag.geoInfo.country.-name")
	if err != nil {
		fmt.Println("err:", err.Error())
	}

	for _, v := range values {
		fmt.Println("v:", v)
	}
}

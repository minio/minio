// Note: this illustrates ValuesForKey() and ValuesForPath() methods

package main

import (
	"fmt"
	"github.com/clbanning/mxj"
	"log"
)

var xmldata = []byte(`
   <books>
      <book seq="1">
         <author>William H. Gaddis</author>
         <title>The Recognitions</title>
         <review>One of the great seminal American novels of the 20th century.</review>
      </book>
      <book seq="2">
         <author>Austin Tappan Wright</author>
         <title>Islandia</title>
         <review>An example of earlier 20th century American utopian fiction.</review>
      </book>
      <book seq="3">
         <author>John Hawkes</author>
         <title>The Beetle Leg</title>
         <review>A lyrical novel about the construction of Ft. Peck Dam in Montana.</review>
      </book>
      <book seq="4">
         <author>T.E. Porter</author>
         <title>King's Day</title>
         <review>A magical novella.</review>
      </book>
   </books>
`)

func main() {
	fmt.Println(string(xmldata))

	m, err := mxj.NewMapXml(xmldata)
	if err != nil {
		log.Fatal("err:", err.Error())
	}

	v, _ := m.ValuesForKey("books")
	fmt.Println("path: books; len(v):", len(v))
	fmt.Printf("\t%+v\n", v)

	v, _ = m.ValuesForPath("books.book")
	fmt.Println("path: books.book; len(v):", len(v))
	for _, vv := range v {
		fmt.Printf("\t%+v\n", vv)
	}

	v, _ = m.ValuesForPath("books.*")
	fmt.Println("path: books.*; len(v):", len(v))
	for _, vv := range v {
		fmt.Printf("\t%+v\n", vv)
	}

	v, _ = m.ValuesForPath("books.*.title")
	fmt.Println("path: books.*.title len(v):", len(v))
	for _, vv := range v {
		fmt.Printf("\t%+v\n", vv)
	}

	v, _ = m.ValuesForPath("books.*.*")
	fmt.Println("path: books.*.*; len(v):", len(v))
	for _, vv := range v {
		fmt.Printf("\t%+v\n", vv)
	}
}

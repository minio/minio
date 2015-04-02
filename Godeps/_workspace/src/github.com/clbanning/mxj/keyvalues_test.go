// keyvalues_test.go - test keyvalues.go methods

package mxj

import (
	// "bytes"
	"fmt"
	// "io"
	"testing"
)

func TestKVHeader(t *testing.T) {
	fmt.Println("\n----------------  keyvalues_test.go ...\n")
}

var doc1 = []byte(`
<doc> 
   <books>
      <book seq="1">
         <author>William T. Gaddis</author>
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
         <author>
            <first_name>T.E.</first_name>
            <last_name>Porter</last_name>
         </author>
         <title>King's Day</title>
         <review>A magical novella.</review>
      </book>
   </books>
</doc>
`)

var doc2 = []byte(`
<doc>
   <books>
      <book seq="1">
         <author>William T. Gaddis</author>
         <title>The Recognitions</title>
         <review>One of the great seminal American novels of the 20th century.</review>
      </book>
   </books>
	<book>Something else.</book>
</doc>
`)

// the basic demo/test case - a small bibliography with mixed element types
func TestPathsForKey(t *testing.T) {
	fmt.Println("PathsForKey, doc1 ...")
	m, merr := NewMapXml(doc1)
	if merr != nil {
		t.Fatal("merr:", merr.Error())
	}
	fmt.Println("PathsForKey, doc1#author")
	ss := m.PathsForKey("author")
	fmt.Println("... ss:", ss)

	fmt.Println("PathsForKey, doc1#books")
	ss = m.PathsForKey("books")
	fmt.Println("... ss:", ss)

	fmt.Println("PathsForKey, doc2 ...")
	m, merr = NewMapXml(doc2)
	if merr != nil {
		t.Fatal("merr:", merr.Error())
	}
	fmt.Println("PathForKey, doc2#book")
	ss = m.PathsForKey("book")
	fmt.Println("... ss:", ss)

	fmt.Println("PathForKeyShortest, doc2#book")
	s := m.PathForKeyShortest("book")
	fmt.Println("... s :", s)
}

func TestValuesForKey(t *testing.T) {
	fmt.Println("ValuesForKey ...")
	m, merr := NewMapXml(doc1)
	if merr != nil {
		t.Fatal("merr:", merr.Error())
	}
	fmt.Println("ValuesForKey, doc1#author")
	ss, sserr := m.ValuesForKey("author")
	if sserr != nil {
		t.Fatal("sserr:", sserr.Error())
	}
	for _, v := range ss {
		fmt.Println("... ss.v:", v)
	}

	fmt.Println("ValuesForKey, doc1#book")
	ss, sserr = m.ValuesForKey("book")
	if sserr != nil {
		t.Fatal("sserr:", sserr.Error())
	}
	for _, v := range ss {
		fmt.Println("... ss.v:", v)
	}

	fmt.Println("ValuesForKey, doc1#book,-seq:3")
	ss, sserr = m.ValuesForKey("book", "-seq:3")
	if sserr != nil {
		t.Fatal("sserr:", sserr.Error())
	}
	for _, v := range ss {
		fmt.Println("... ss.v:", v)
	}

	fmt.Println("ValuesForKey, doc1#book, author:William T. Gaddis")
	ss, sserr = m.ValuesForKey("book", "author:William T. Gaddis")
	if sserr != nil {
		t.Fatal("sserr:", sserr.Error())
	}
	for _, v := range ss {
		fmt.Println("... ss.v:", v)
	}

	fmt.Println("ValuesForKey, doc1#author, -seq:1")
	ss, sserr = m.ValuesForKey("author", "-seq:1")
	if sserr != nil {
		t.Fatal("sserr:", sserr.Error())
	}
	for _, v := range ss {	// should be len(ss) == 0
		fmt.Println("... ss.v:", v)
	}
}

func TestValuesForPath(t *testing.T) {
	fmt.Println("ValuesForPath ...")
	m, merr := NewMapXml(doc1)
	if merr != nil {
		t.Fatal("merr:", merr.Error())
	}
	fmt.Println("ValuesForPath, doc.books.book.author")
	ss, sserr := m.ValuesForPath("doc.books.book.author")
	if sserr != nil {
		t.Fatal("sserr:", sserr.Error())
	}
	for _, v := range ss {
		fmt.Println("... ss.v:", v)
	}

	fmt.Println("ValuesForPath, doc.books.book")
	ss, sserr = m.ValuesForPath("doc.books.book")
	if sserr != nil {
		t.Fatal("sserr:", sserr.Error())
	}
	for _, v := range ss {
		fmt.Println("... ss.v:", v)
	}

	fmt.Println("ValuesForPath, doc.books.book -seq=3")
	ss, sserr = m.ValuesForPath("doc.books.book", "-seq:3")
	if sserr != nil {
		t.Fatal("sserr:", sserr.Error())
	}
	for _, v := range ss {
		fmt.Println("... ss.v:", v)
	}

	fmt.Println("ValuesForPath, doc.books.* -seq=3")
	ss, sserr = m.ValuesForPath("doc.books.*", "-seq:3")
	if sserr != nil {
		t.Fatal("sserr:", sserr.Error())
	}
	for _, v := range ss {
		fmt.Println("... ss.v:", v)
	}

	fmt.Println("ValuesForPath, doc.*.* -seq=3")
	ss, sserr = m.ValuesForPath("doc.*.*", "-seq:3")
	if sserr != nil {
		t.Fatal("sserr:", sserr.Error())
	}
	for _, v := range ss {
		fmt.Println("... ss.v:", v)
	}
}

func TestValuesForNotKey( t *testing.T) {
	fmt.Println("ValuesForNotKey ...")
	m, merr := NewMapXml(doc1)
	if merr != nil {
		t.Fatal("merr:", merr.Error())
	}
	fmt.Println("ValuesForPath, doc.books.book !author:William T. Gaddis")
	ss, sserr := m.ValuesForPath("doc.books.book", "!author:William T. Gaddis")
	if sserr != nil {
		t.Fatal("sserr:", sserr.Error())
	}
	for _, v := range ss {
		fmt.Println("... ss.v:", v)
	}

	fmt.Println("ValuesForPath, doc.books.book !author:*")
	ss, sserr = m.ValuesForPath("doc.books.book", "!author:*")
	if sserr != nil {
		t.Fatal("sserr:", sserr.Error())
	}
	for _, v := range ss {	// expect len(ss) == 0
		fmt.Println("... ss.v:", v)
	}

	fmt.Println("ValuesForPath, doc.books.book !unknown:*")
	ss, sserr = m.ValuesForPath("doc.books.book", "!unknown:*")
	if sserr != nil {
		t.Fatal("sserr:", sserr.Error())
	}
	for _, v := range ss {
		fmt.Println("... ss.v:", v)
	}
}

func TestIAHeader(t *testing.T) {
	fmt.Println("\n----------------  indexedarray_test.go ...\n")
}

var ak_data = []byte(`{ "section1":{"data" : [{"F1" : "F1 data","F2" : "F2 data"},{"F1" : "demo 123","F2" : "abc xyz"}]}}`)
var j_data = []byte(`{ "stuff":[ { "data":[ { "F":1 }, { "F":2 }, { "F":3 } ] }, { "data":[ 4, 5, 6 ] } ] }`)
var x_data = []byte(`
<doc>
	<stuff>
		<data seq="1.1">
			<F>1</F>
		</data>
		<data seq="1.2">
			<F>2</F>
		</data>
		<data seq="1.3">
			<F>3</F>
		</data>
	</stuff>
	<stuff>
		<data seq="2.1">
			<F>4</F>
		</data>
		<data seq="2.2">
			<F>5</F>
		</data>
		<data seq="2.3">
			<F>6</F>
		</data>
	</stuff>
</doc>`)

func TestValuesForIndexedArray(t *testing.T) {
	j_main(t)
	x_main(t)
	ak_main(t)
}

func ak_main(t *testing.T) {
	fmt.Println("\nak_data:", string(ak_data))
	m, merr := NewMapJson(ak_data)
	if merr != nil {
		t.Fatal("merr:", merr.Error())
		return
	}
	fmt.Println("m:", m)

	v, verr := m.ValuesForPath("section1.data[0].F1")
	if verr != nil {
		t.Fatal("verr:", verr.Error())
	}
	fmt.Println("section1.data[0].F1:", v)
}

func j_main(t *testing.T) {
	fmt.Println("j_data:", string(j_data))
	m, merr := NewMapJson(j_data)
	if merr != nil {
		t.Fatal("merr:", merr.Error())
		return
	}
	fmt.Println("m:", m)

	v, verr := m.ValuesForPath("stuff[0]")
	if verr != nil {
		t.Fatal("verr:", verr.Error())
	}
	fmt.Println("stuff[0]:", v)

	v, verr = m.ValuesForPath("stuff.data")
	if verr != nil {
		t.Fatal("verr:", verr.Error())
	}
	fmt.Println("stuff.data:", v)

	v, verr = m.ValuesForPath("stuff[0].data")
	if verr != nil {
		t.Fatal("verr:", verr.Error())
	}
	fmt.Println("stuff[0].data:", v)

	v, verr = m.ValuesForPath("stuff.data[0]")
	if verr != nil {
		t.Fatal("verr:", verr.Error())
	}
	fmt.Println("stuff.data[0]:", v)

	v, verr = m.ValuesForPath("stuff.*[2]")
	if verr != nil {
		t.Fatal("verr:", verr.Error())
	}
	fmt.Println("stuff.*[2]:", v)

	v, verr = m.ValuesForPath("stuff.data.F")
	if verr != nil {
		t.Fatal("verr:", verr.Error())
	}
	fmt.Println("stuff.data.F:", v)

	v, verr = m.ValuesForPath("*.*.F")
	if verr != nil {
		t.Fatal("verr:", verr.Error())
	}
	fmt.Println("*.*.F:", v)

	v, verr = m.ValuesForPath("stuff.data[0].F")
	if verr != nil {
		t.Fatal("verr:", verr.Error())
	}
	fmt.Println("stuff.data[0].F:", v)

	v, verr = m.ValuesForPath("stuff.data[1].F")
	if verr != nil {
		t.Fatal("verr:", verr.Error())
	}
	fmt.Println("stuff.data[1].F:", v)

	v, verr = m.ValuesForPath("stuff[0].data[2]")
	if verr != nil {
		t.Fatal("verr:", verr.Error())
	}
	fmt.Println("stuff[0].data[2]:", v)

	v, verr = m.ValuesForPath("stuff[1].data[1]")
	if verr != nil {
		t.Fatal("verr:", verr.Error())
	}
	fmt.Println("stuff[1].data[1]:", v)

	v, verr = m.ValuesForPath("stuff[1].data[1].F")
	if verr != nil {
		t.Fatal("verr:", verr.Error())
	}
	fmt.Println("stuff[1].data[1].F", v)

	v, verr = m.ValuesForPath("stuff[1].data.F")
	if verr != nil {
		t.Fatal("verr:", verr.Error())
	}
	fmt.Println("stuff[1].data.F:", v)
}

func x_main(t *testing.T) {
	fmt.Println("\nx_data:", string(x_data))
	m, merr := NewMapXml(x_data)
	if merr != nil {
		t.Fatal("merr:", merr.Error())
		return
	}
	fmt.Println("m:", m)

	v, verr := m.ValuesForPath("doc.stuff[0]")
	if verr != nil {
		t.Fatal("verr:", verr.Error())
	}
	fmt.Println("doc.stuff[0]:", v)

	v, verr = m.ValuesForPath("doc.stuff.data[0]")
	if verr != nil {
		t.Fatal("verr:", verr.Error())
	}
	fmt.Println("doc.stuff.data[0]:", v)

	v, verr = m.ValuesForPath("doc.stuff.data[0]", "-seq:2.1")
	if verr != nil {
		t.Fatal("verr:", verr.Error())
	}
	fmt.Println("doc.stuff.data[0] -seq:2.1:", v)

	v, verr = m.ValuesForPath("doc.stuff.data[0].F")
	if verr != nil {
		t.Fatal("verr:", verr.Error())
	}
	fmt.Println("doc.stuff.data[0].F:", v)

	v, verr = m.ValuesForPath("doc.stuff[0].data[2]")
	if verr != nil {
		t.Fatal("verr:", verr.Error())
	}
	fmt.Println("doc.stuff[0].data[2]:", v)

	v, verr = m.ValuesForPath("doc.stuff[1].data[1].F")
	if verr != nil {
		t.Fatal("verr:", verr.Error())
	}
	fmt.Println("doc.stuff[1].data[1].F:", v)
}

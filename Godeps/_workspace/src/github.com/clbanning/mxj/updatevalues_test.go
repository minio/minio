// modifyvalues_test.go - test keyvalues.go methods

package mxj

import (
	"fmt"
	"testing"
)

func TestUVHeader(t *testing.T) {
	fmt.Println("\n----------------  updatevalues_test.go ...\n")
}


func TestUpdateValuesForPath_Author(t *testing.T) {
	m, merr := NewMapXml(doc1)
	if merr != nil {
		t.Fatal("merr:", merr.Error())
	}
	fmt.Println("m:", m)

	ss, _ := m.ValuesForPath("doc.books.book.author")
	for _, v := range ss {
		fmt.Println("v:", v)
	}
	fmt.Println("m.UpdateValuesForPath(\"author:NoName\", \"doc.books.book.author\")")
	n, err := m.UpdateValuesForPath("author:NoName", "doc.books.book.author")
	if err != nil {
		t.Fatal("err:", err.Error())
	}
	fmt.Println(n, "updates")
	ss, _ = m.ValuesForPath("doc.books.book.author")
	for _, v := range ss {
		fmt.Println("v:", v)
	}

	fmt.Println("m.UpdateValuesForPath(\"author:William Gadddis\", \"doc.books.book.author\", \"title:The Recognitions\")")
	n, err = m.UpdateValuesForPath("author:William Gadddis", "doc.books.book.author", "title:The Recognitions")
	o, _ := m.UpdateValuesForPath("author:Austin Tappen Wright", "doc.books.book", "title:Islandia")
	p, _ := m.UpdateValuesForPath("author:John Hawkes", "doc.books.book", "title:The Beetle Leg")
	q, _ := m.UpdateValuesForPath("author:T. E. Porter", "doc.books.book", "title:King's Day")
	if err != nil {
		t.Fatal("err:", err.Error())
	}
	fmt.Println(n+o+p+q, "updates")
	ss, _ = m.ValuesForPath("doc.books.book.author")
	for _, v := range ss {
		fmt.Println("v:", v)
	}

	fmt.Println("m.UpdateValuesForPath(\"author:William T. Gaddis\", \"doc.books.book.*\", \"title:The Recognitions\")")
	n, _ = m.UpdateValuesForPath("author:William T. Gaddis", "doc.books.book.*", "title:The Recognitions")
	fmt.Println(n, "updates")
	ss, _ = m.ValuesForPath("doc.books.book.author")
	for _, v := range ss {
		fmt.Println("v:", v)
	}

	fmt.Println("m.UpdateValuesForPath(\"title:The Cannibal\", \"doc.books.book.title\", \"author:John Hawkes\")")
	n, _ = m.UpdateValuesForPath("title:The Cannibal", "doc.books.book.title", "author:John Hawkes")
	o, _ = m.UpdateValuesForPath("review:A novel on his experiences in WWII.", "doc.books.book.review", "title:The Cannibal")
	fmt.Println(n+o, "updates")
	ss, _ = m.ValuesForPath("doc.books.book")
	for _, v := range ss {
		fmt.Println("v:", v)
	}

	fmt.Println("m.UpdateValuesForPath(\"books:\", \"doc.books\")")
	n, _ = m.UpdateValuesForPath("books:", "doc.books")
	fmt.Println(n, "updates")
	fmt.Println("m:", m)

	fmt.Println("m.UpdateValuesForPath(mm, \"*\")")
	mm, _  := NewMapXml(doc1)
	n, err = m.UpdateValuesForPath(mm, "*")
	if err != nil {
		t.Fatal("err:", err.Error())
	}
	fmt.Println(n, "updates")
	fmt.Println("m:", m)

	// ---------------------- newDoc
	var newDoc = []byte(`<tag color="green" shape="square">simple element</tag>`)
	m, merr = NewMapXml(newDoc)
	if merr != nil {
		t.Fatal("merr:",merr.Error())
	}
	fmt.Println("\nnewDoc:", string(newDoc))
	fmt.Println("m:", m)
	fmt.Println("m.UpdateValuesForPath(\"#text:maybe not so simple element\", \"tag\")")
	n, _ = m.UpdateValuesForPath("#text:maybe not so simple element", "tag")
	fmt.Println("n:", n, "m:", m)
	fmt.Println("m.UpdateValuesForPath(\"#text:simple element again\", \"*\")")
	n, _ = m.UpdateValuesForPath("#text:simple element again", "*")
	fmt.Println("n:", n, "m:", m)

/*
	fmt.Println("UpdateValuesForPath, doc.books.book, title:The Recognitions : NoBook")
	n, err = m.UpdateValuesForPath("NoBook", "doc.books.book", "title:The Recognitions")
	if err != nil {
		t.Fatal("err:", err.Error())
	}
	fmt.Println(n, "updates")
	ss, _ = m.ValuesForPath("doc.books.book")
	for _, v := range ss {
		fmt.Println("v:", v)
	}

	fmt.Println("UpdateValuesForPath, doc.books.book.title -seq=3: The Blood Oranges")
	n, err = m.UpdateValuesForPath("The Blood Oranges", "doc.books.book.title", "-seq:3")
	if err != nil {
		t.Fatal("err:", err.Error())
	}
	fmt.Println(n, "updates")
	ss, _ = m.ValuesForPath("doc.books.book.title")
	for _, v := range ss {
		fmt.Println("v:", v)
	}
*/
}

var authorDoc = []byte(`
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
	<author>
		<name>John Hawkes</name>
		<books>
			<book>
				<title>The Cannibal</title>
				<date>1949</date>
				<review>A novel on his experiences in WWII.</review>
			</book>
			<book>
				<title>The Beetle Leg</title>
				<date>1951</date>
				<review>A lyrical novel about the construction of Ft. Peck Dam in Montana.</review>
			</book>
			<book>
				<title>The Blood Oranges</title>
				<date>1970</date>
				<review>Where everyone wants to vacation.</review>
			</book>
		</books>
	</author>
</biblio>`)

func TestAuthorDoc(t *testing.T) {
	m, merr := NewMapXml(authorDoc)
	if merr != nil	{
		t.Fatal("merr:", merr.Error())
	}
	fmt.Println(m.StringIndent())

	fmt.Println("m.UpdateValuesForPath(\"review:National Book Award winner.\", \"*.*.*.*\", \"title:JR\")")
	n, _ := m.UpdateValuesForPath("review:National Book Award winner.", "*.*.*.*", "title:JR")
	fmt.Println(n, "updates")
	ss, _ := m.ValuesForPath("biblio.author", "name:William Gaddis")
	for _, v := range ss {
		fmt.Println("v:", v)
	}

	fmt.Println("m.UpdateValuesForPath(newVal, path, oldVal)")
	path := m.PathForKeyShortest("date")
	v,_ := m.ValuesForPath(path)
	var counter int
	for _, vv := range v {
		oldVal := "date:" + vv.(string)
		newVal := "date:" + vv.(string) + ":num"
		n, _ = m.UpdateValuesForPath(newVal, path, oldVal)
		counter += n
	}
	fmt.Println(counter, "updates")
	fmt.Println(m.StringIndent())
}



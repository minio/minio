// seqnum.go

package mxj

import (
	"fmt"
	"testing"
)

var seqdata1 = []byte(`
	<Obj c="la" x="dee" h="da">
		<IntObj id="3"/>
		<IntObj1 id="1"/>
		<IntObj id="2"/>
	</Obj>`)

var seqdata2 = []byte(`
	<Obj c="la" x="dee" h="da">
		<IntObj id="3"/>
		<NewObj>
			<id>1</id>
			<StringObj>hello</StringObj>
			<BoolObj>true</BoolObj>
		</NewObj>
		<IntObj id="2"/>
	</Obj>`)

func TestSeqNumHeader(t *testing.T) {
	fmt.Println("\n----------------  seqnum_test.go ...\n")
}

func TestSeqNum(t *testing.T) {
	IncludeTagSeqNum(true)

	m, err := NewMapXml(seqdata1, Cast)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("m1: %#v\n", m)
	j, _ := m.JsonIndent("", "  ")
	fmt.Println(string(j))

	m, err = NewMapXml(seqdata2, Cast)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("m2: %#v\n", m)
	j, _ = m.JsonIndent("", "  ")
	fmt.Println(string(j))

	IncludeTagSeqNum(false)
}

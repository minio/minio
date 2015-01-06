// Copyright (c) 2014 The go-patricia AUTHORS
//
// Use of this source code is governed by The MIT License
// that can be found in the LICENSE file.

package patricia

import (
	"bytes"
	"crypto/rand"
	"encoding/gob"
	"log"
	"reflect"
	"testing"
)

// Tests -----------------------------------------------------------------------

func TestTrie_GetNonexistentPrefix(t *testing.T) {
	trie := NewTrie()

	data := []testData{
		{"aba", 0, success},
	}

	for _, v := range data {
		t.Logf("INSERT prefix=%v, item=%v, success=%v", v.key, v.value, v.retVal)
		if ok := trie.Insert(Prefix(v.key), v.value); ok != v.retVal {
			t.Errorf("Unexpected return value, expected=%v, got=%v", v.retVal, ok)
		}
	}

	t.Logf("GET prefix=baa, expect item=nil")
	if item := trie.Get(Prefix("baa")); item != nil {
		t.Errorf("Unexpected return value, expected=<nil>, got=%v", item)
	}
}

func TestTrie_RandomKitchenSink(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	const count, size = 750000, 16
	b := make([]byte, count+size+1)
	if _, err := rand.Read(b); err != nil {
		t.Fatal("error generating random bytes", err)
	}
	m := make(map[string]string)
	for i := 0; i < count; i++ {
		m[string(b[i:i+size])] = string(b[i+1 : i+size+1])
	}
	trie := NewTrie()
	getAndDelete := func(k, v string) {
		i := trie.Get(Prefix(k))
		if i == nil {
			t.Fatalf("item not found, prefix=%v", []byte(k))
		} else if s, ok := i.(string); !ok {
			t.Fatalf("unexpected item type, expecting=%v, got=%v", reflect.TypeOf(k), reflect.TypeOf(i))
		} else if s != v {
			t.Fatalf("unexpected item, expecting=%v, got=%v", []byte(k), []byte(s))
		} else if !trie.Delete(Prefix(k)) {
			t.Fatalf("delete failed, prefix=%v", []byte(k))
		} else if i = trie.Get(Prefix(k)); i != nil {
			t.Fatalf("unexpected item, expecting=<nil>, got=%v", i)
		} else if trie.Delete(Prefix(k)) {
			t.Fatalf("extra delete succeeded, prefix=%v", []byte(k))
		}
	}
	for k, v := range m {
		if !trie.Insert(Prefix(k), v) {
			t.Fatalf("insert failed, prefix=%v", []byte(k))
		}
		if byte(k[size/2]) < 128 {
			getAndDelete(k, v)
			delete(m, k)
		}
	}
	for k, v := range m {
		getAndDelete(k, v)
	}
}

func TestTrieSerializes(t *testing.T) {
	gob.Register(&SparseChildList{})
	gob.Register(&DenseChildList{})
	trie := NewTrie()
	trie.Insert([]byte("hello1"), "world1")
	trie.Insert([]byte("hello2"), "world2")
	var bytesBuffer bytes.Buffer
	encoder := gob.NewEncoder(&bytesBuffer)
	if err := encoder.Encode(trie); err != nil {
		t.Fatalf("Serialization failed:", err)
	}
	encodedReader := bytes.NewReader(bytesBuffer.Bytes())
	decoder := gob.NewDecoder(encodedReader)
	trie2 := NewTrie()
	decoder.Decode(&trie2)
	log.Println(trie2)
	if value := trie2.Get([]byte("hello1")); value == nil {
		t.Fatalf("hello1 not serialized")
	}
	if value := trie2.Get([]byte("hello2")); value == nil {
		t.Fatalf("hello1 not serialized")
	}
}

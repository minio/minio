package db

import (
	"io/ioutil"
	"os"
	"strings"
	"testing"
)

func TestIdxCRUD(t *testing.T) {
	os.RemoveAll(TEST_DATA_DIR)
	defer os.RemoveAll(TEST_DATA_DIR)
	if err := os.MkdirAll(TEST_DATA_DIR, 0700); err != nil {
		t.Fatal(err)
	}
	if err := ioutil.WriteFile(TEST_DATA_DIR+"/number_of_partitions", []byte("2"), 0600); err != nil {
		t.Fatal(err)
	}
	db, err := OpenDB(TEST_DATA_DIR)
	if err != nil {
		t.Fatal(err)
	}
	if err = db.Create("col"); err != nil {
		t.Fatal(err)
	}
	col := db.Use("col")
	if len(col.AllIndexes()) != 0 {
		t.Fatal(col.AllIndexes())
	}
	// Create index & verify
	if err = col.Index([]string{"a", "b"}); err != nil {
		t.Fatal(err)
	}
	if col.Index([]string{"a", "b"}) == nil {
		t.Fatal(col.indexPaths, "Did not error")
	}
	if len(col.AllIndexes()) != 1 || col.AllIndexes()[0][0] != "a" || col.AllIndexes()[0][1] != "b" {
		t.Fatal(col.AllIndexes())
	}
	if err = col.Index([]string{"c"}); err != nil {
		t.Fatal(err)
	}
	allIndexes := col.AllIndexes()
	idx0 := strings.Join(allIndexes[0], ",")
	idx1 := strings.Join(allIndexes[1], ",")
	if !(idx0 == "a,b" && idx1 == "c" || idx0 == "c" && idx1 == "a,b") {
		t.Fatal(allIndexes)
	}
	// Unindex & verify
	if col.Unindex([]string{"%&^*"}) == nil {
		t.Fatal("Did not error")
	}
	if err = col.Unindex([]string{"c"}); err != nil {
		t.Fatal(err)
	}
	if len(col.AllIndexes()) != 1 || col.AllIndexes()[0][0] != "a" || col.AllIndexes()[0][1] != "b" {
		t.Fatal(col.AllIndexes())
	}
	if err = col.Unindex([]string{"a", "b"}); err != nil {
		t.Fatal(err)
	}
	if len(col.AllIndexes()) != 0 {
		t.Fatal(col.AllIndexes())
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

package db

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/HouzuoGuo/tiedot/dberr"
)

func StrHashTest(t *testing.T) {
	strings := []string{"", " ", "abc", "123"}
	hashes := []int{0, 32, 417419622498, 210861491250}
	for i := range strings {
		if StrHash(strings[i]) != hashes[i] {
			t.Fatalf("Hash of %s equals to %d, it should equal to %d", strings[i], StrHash(strings[i]), hashes[i])
		}
	}
}

func GetInTest(t *testing.T) {
	var obj interface{}
	// Get inside a JSON object
	json.Unmarshal([]byte(`{"a": {"b": {"c": 1}}}`), &obj)
	if val, ok := GetIn(obj, []string{"a", "b", "c"})[0].(float64); !ok || val != 1 {
		t.Fatal()
	}
	// Get inside a JSON array
	json.Unmarshal([]byte(`{"a": {"b": {"c": [1, 2, 3]}}}`), &obj)
	if val, ok := GetIn(obj, []string{"a", "b", "c"})[0].(float64); !ok || val != 1 {
		t.Fatal()
	}
	if val, ok := GetIn(obj, []string{"a", "b", "c"})[1].(float64); !ok || val != 2 {
		t.Fatal()
	}
	if val, ok := GetIn(obj, []string{"a", "b", "c"})[2].(float64); !ok || val != 3 {
		t.Fatal()
	}
	// Get inside JSON objects contained in JSON array
	json.Unmarshal([]byte(`{"a": [{"b": {"c": [1]}}, {"b": {"c": [2, 3]}}]}`), &obj)
	if val, ok := GetIn(obj, []string{"a", "b", "c"})[0].(float64); !ok || val != 1 {
		t.Fatal()
	}
	if val, ok := GetIn(obj, []string{"a", "b", "c"})[1].(float64); !ok || val != 2 {
		t.Fatal()
	}
	if val, ok := GetIn(obj, []string{"a", "b", "c"})[2].(float64); !ok || val != 3 {
		t.Fatal()
	}
	// Get inside a JSON array and fetch attributes from array elements, which are JSON objects
	json.Unmarshal([]byte(`{"a": [{"b": {"c": [4]}}, {"b": {"c": [5, 6]}}], "d": [0, 9]}`), &obj)
	if val, ok := GetIn(obj, []string{"a", "b", "c"})[0].(float64); !ok || val != 4 {
		t.Fatal()
	}
	if val, ok := GetIn(obj, []string{"a", "b", "c"})[1].(float64); !ok || val != 5 {
		t.Fatal()
	}
	if val, ok := GetIn(obj, []string{"a", "b", "c"})[2].(float64); !ok || val != 6 {
		t.Fatal()
	}
	if len(GetIn(obj, []string{"a", "b", "c"})) != 3 {
		t.Fatal()
	}
	if val, ok := GetIn(obj, []string{"d"})[0].(float64); !ok || val != 0 {
		t.Fatal()
	}
	if val, ok := GetIn(obj, []string{"d"})[1].(float64); !ok || val != 9 {
		t.Fatal()
	}
	if len(GetIn(obj, []string{"d"})) != 2 {
		t.Fatal()
	}
	// Another example
	json.Unmarshal([]byte(`{"a": {"b": [{"c": 2}]}, "d": 0}`), &obj)
	if val, ok := GetIn(obj, []string{"a", "b", "c"})[0].(float64); !ok || val != 2 {
		t.Fatal()
	}
	if len(GetIn(obj, []string{"a", "b", "c"})) != 1 {
		t.Fatal()
	}
}

func idxHas(col *Col, path []string, idxVal interface{}, docID int) error {
	idxName := strings.Join(path, INDEX_PATH_SEP)
	hashKey := StrHash(fmt.Sprint(idxVal))
	vals := col.hts[hashKey%col.db.numParts][idxName].Get(hashKey, 0)
	if len(vals) != 1 || vals[0] != docID {
		return fmt.Errorf("Looking for %v (%v) docID %v in %v partition %d, but got result %v", idxVal, hashKey, docID, path, hashKey%col.db.numParts, vals)
	}
	return nil
}

func idxHasNot(col *Col, path []string, idxVal, docID int) error {
	idxName := strings.Join(path, INDEX_PATH_SEP)
	hashKey := StrHash(fmt.Sprint(idxVal))
	vals := col.hts[hashKey%col.db.numParts][idxName].Get(hashKey, 0)
	for _, v := range vals {
		if v == docID {
			return fmt.Errorf("Looking for %v %v %v in %v (should not return any), but got result %v", idxVal, hashKey, docID, path, vals)
		}
	}
	return nil
}

func TestDocCrudAndIdx(t *testing.T) {
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
	// Prepare collection and index
	if err = db.Create("col"); err != nil {
		t.Fatal(err)
	}
	col := db.Use("col")
	if err = col.Index([]string{"a", "b"}); err != nil {
		t.Fatal(err)
	}
	numDocs := 2011
	docIDs := make([]int, numDocs)
	// Insert documents
	for i := 0; i < numDocs; i++ {
		if docIDs[i], err = col.Insert(map[string]interface{}{"a": map[string]interface{}{"b": i}}); err != nil {
			t.Fatal(err)
		}
	}
	// Read documents and verify index
	if _, err = col.Read(123456); dberr.Type(err) != dberr.ErrorNoDoc {
		t.Fatal("Did not error")
	}
	for i, docID := range docIDs {
		if doc, err := col.Read(docID); err != nil || doc["a"].(map[string]interface{})["b"].(float64) != float64(i) {
			t.Fatal(docID, doc)
		}
		if err = idxHas(col, []string{"a", "b"}, i, docID); err != nil {
			t.Fatal(err)
		}
	}
	// Update document
	if err = col.Update(654321, map[string]interface{}{}); dberr.Type(err) != dberr.ErrorNoDoc {
		t.Fatal("Did not error")
	}
	for i, docID := range docIDs {
		// i -> i * 2
		if err = col.Update(docID, map[string]interface{}{"a": map[string]interface{}{"b": i * 2}}); err != nil {
			t.Fatal(err)
		}
	}
	// After update - verify documents and index
	for i, docID := range docIDs {
		if doc, err := col.Read(docID); err != nil || doc["a"].(map[string]interface{})["b"].(float64) != float64(i*2) {
			t.Fatal(docID, doc)
		}
		if i == 0 {
			if err = idxHas(col, []string{"a", "b"}, 0, docID); err != nil {
				t.Fatal(err)
			}
		} else {
			if err = idxHasNot(col, []string{"a", "b"}, i, docID); err != nil {
				t.Fatal(err)
			}
			if err = idxHas(col, []string{"a", "b"}, i*2, docID); err != nil {
				t.Fatal(err)
			}
		}
	}
	// Delete half of those documents
	if err = col.Delete(654321); dberr.Type(err) != dberr.ErrorNoDoc {
		t.Fatal("Did not error")
	}
	for i := 0; i < numDocs/2+1; i++ {
		if err := col.Delete(docIDs[i]); err != nil {
			t.Fatal(err)
		}
		if err := col.Delete(docIDs[i]); dberr.Type(err) != dberr.ErrorNoDoc {
			t.Fatal("Did not error")
		}
	}
	// After delete - verify
	for i, docID := range docIDs {
		if i < numDocs/2+1 {
			// After delete - verify deleted documents and index
			if _, err := col.Read(docID); dberr.Type(err) != dberr.ErrorNoDoc {
				t.Fatal("Did not delete", i, docID)
			}
			if err = idxHasNot(col, []string{"a", "b"}, i*2, docID); err != nil {
				t.Fatal(err)
			}
		} else {
			// After delete - verify unaffected documents and index
			if doc, err := col.Read(docID); err != nil || doc["a"].(map[string]interface{})["b"].(float64) != float64(i*2) {
				t.Fatal(docID, doc)
			}
			if err = idxHas(col, []string{"a", "b"}, i*2, docID); err != nil {
				t.Fatal(err)
			}
		}
	}
	// Recreate index and verify
	if err = col.Unindex([]string{"a", "b"}); err != nil {
		t.Fatal(err)
	}
	if err = col.Index([]string{"a", "b"}); err != nil {
		t.Fatal(err)
	}
	for i := numDocs/2 + 1; i < numDocs; i++ {
		if doc, err := col.Read(docIDs[i]); err != nil || doc["a"].(map[string]interface{})["b"].(float64) != float64(i*2) {
			t.Fatal(doc, err)
		}
		if err = idxHas(col, []string{"a", "b"}, i*2, docIDs[i]); err != nil {
			t.Fatal(err)
		}
	}

	// Verify that there are approximately 1000 documents
	t.Log("ApproxDocCount", col.ApproxDocCount())
	if col.ApproxDocCount() < 600 || col.ApproxDocCount() > 1400 {
		t.Fatal("Approximate is way off", col.ApproxDocCount())
	}

	// Scrub and verify
	if err = db.Scrub("col"); err != nil {
		t.Fatal(err)
	}
	col = db.Use("col")
	for i := numDocs/2 + 1; i < numDocs; i++ {
		if doc, err := col.Read(docIDs[i]); err != nil || doc["a"].(map[string]interface{})["b"].(float64) != float64(i*2) {
			t.Fatal(doc, err)
		}
		if err = idxHas(col, []string{"a", "b"}, i*2, docIDs[i]); err != nil {
			t.Fatal(err)
		}
	}

	// Iterate over all documents 10 times
	start := time.Now().UnixNano()
	for i := 0; i < 10; i++ {
		col.ForEachDoc(func(_ int, _ []byte) bool {
			return true
		})
	}
	timediff := time.Now().UnixNano() - start
	t.Log("It took", timediff/1000000, "milliseconds")

	// Verify again that there are approximately 1000 documents
	t.Log("ApproxDocCount", col.ApproxDocCount())
	if col.ApproxDocCount() < 600 || col.ApproxDocCount() > 1400 {
		t.Fatal("Approximate is way off", col.ApproxDocCount())
	}

	// Read back all documents page by pabe
	totalPage := col.ApproxDocCount() / 100
	collectedIDs := make(map[int]struct{})
	for page := 0; page < totalPage; page++ {
		col.ForEachDocInPage(page, totalPage, func(id int, _ []byte) bool {
			collectedIDs[id] = struct{}{}
			return true
		})
		t.Log("Went through page ", page, " got ", len(collectedIDs), " documents so far")
	}
	if len(collectedIDs) != numDocs/2 {
		t.Fatal("Wrong number of docs", len(collectedIDs))
	}

	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
}

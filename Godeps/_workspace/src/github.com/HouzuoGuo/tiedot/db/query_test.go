package db

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/HouzuoGuo/tiedot/dberr"
)

func ensureMapHasKeys(m map[int]struct{}, keys ...int) bool {
	if len(m) != len(keys) {
		return false
	}
	for _, v := range keys {
		if _, ok := m[v]; !ok {
			return false
		}
	}
	return true
}

func runQuery(query string, col *Col) (map[int]struct{}, error) {
	result := make(map[int]struct{})
	var jq interface{}
	if err := json.Unmarshal([]byte(query), &jq); err != nil {
		fmt.Println(err)
	}
	return result, EvalQuery(jq, col, &result)
}

func TestQuery(t *testing.T) {
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
	defer db.Close()
	// Prepare collection and index
	if err = db.Create("col"); err != nil {
		t.Fatal(err)
	}
	col := db.Use("col")
	docs := []string{
		`{"a": {"b": [1]}, "c": 1, "d": 1, "f": 1, "g": 1, "special": {"thing": null}, "h": 1}`,
		`{"a": {"b": 1}, "c": [1], "d": 2, "f": 2, "g": 2}`,
		`{"a": [{"b": [2]}], "c": 2, "d": 1, "f": 3, "g": 3, "h": 3}`,
		`{"a": {"b": 3}, "c": [3], "d": 2, "f": 4, "g": 4}`,
		`{"a": {"b": [4]}, "c": 4, "d": 1, "f": 5, "g": 5}`,
		`{"a": [{"b": 5}, {"b": 6}], "c": 4, "d": 1, "f": 5, "g": 5, "h": 2}`,
		`{"a": [{"b": "val1"}, {"b": "val2"}]}`,
		`{"a": [{"b": "val3"}, {"b": ["val4", "val5"]}]}`}
	ids := make([]int, len(docs))
	for i, doc := range docs {
		var jsonDoc map[string]interface{}
		if err := json.Unmarshal([]byte(doc), &jsonDoc); err != nil {
			panic(err)
		}
		if ids[i], err = col.Insert(jsonDoc); err != nil {
			t.Fatal(err)
			return
		}
	}
	q, err := runQuery(`["all"]`, col)
	if err != nil {
		t.Fatal(err)
	}
	col.Index([]string{"a", "b"})
	col.Index([]string{"f"})
	col.Index([]string{"h"})
	col.Index([]string{"special"})
	col.Index([]string{"e"})
	// expand numbers
	q, err = runQuery(`["1", "2", ["3", "4"], "5"]`, col)
	if err != nil {
		t.Fatal(err)
	}
	if !ensureMapHasKeys(q, 1, 2, 3, 4, 5) {
		t.Fatal(q)
	}
	// hash scan
	q, err = runQuery(`{"eq": 1, "in": ["a", "b"]}`, col)
	if err != nil {
		t.Fatal(err)
	}
	if !ensureMapHasKeys(q, ids[0], ids[1]) {
		t.Fatal(q)
	}
	q, err = runQuery(`{"eq": 5, "in": ["a", "b"]}`, col)
	if err != nil {
		t.Fatal(err)
	}
	if !ensureMapHasKeys(q, ids[5]) {
		t.Fatal(q)
	}
	q, err = runQuery(`{"eq": 6, "in": ["a", "b"]}`, col)
	if err != nil {
		t.Fatal(err)
	}
	if !ensureMapHasKeys(q, ids[5]) {
		t.Fatal(q)
	}
	q, err = runQuery(`{"eq": 1, "limit": 1, "in": ["a", "b"]}`, col)
	if err != nil {
		fmt.Println(err)
	}
	if !ensureMapHasKeys(q, ids[1]) && !ensureMapHasKeys(q, ids[0]) {
		t.Fatal(q, ids[1], ids[0])
	}
	// collection scan
	q, err = runQuery(`{"eq": 1, "in": ["c"]}`, col)
	if dberr.Type(err) != dberr.ErrorNeedIndex {
		t.Fatal("Collection scan should not happen")
	}
	// lookup on "special" (null)
	q, err = runQuery(`{"eq": {"thing": null},  "in": ["special"]}`, col)
	if err != nil {
		t.Fatal(err)
	}
	if !ensureMapHasKeys(q, ids[0]) {
		t.Fatal(q)
	}
	// lookup in list
	q, err = runQuery(`{"eq": "val1",  "in": ["a", "b"]}`, col)
	if err != nil {
		t.Fatal(err)
	}
	if !ensureMapHasKeys(q, ids[6]) {
		t.Fatal(q)
	}
	q, err = runQuery(`{"eq": "val5",  "in": ["a", "b"]}`, col)
	if err != nil {
		t.Fatal(err)
	}
	if !ensureMapHasKeys(q, ids[7]) {
		t.Fatal(q)
	}
	// "e" should not exist
	q, err = runQuery(`{"has": ["e"]}`, col)
	if err != nil {
		t.Fatal(err)
	}
	if !ensureMapHasKeys(q) {
		t.Fatal(q)
	}
	// existence test, hash scan, with limit
	q, err = runQuery(`{"has": ["h"], "limit": 2}`, col)
	if err != nil {
		t.Fatal(err)
	}
	if !ensureMapHasKeys(q, ids[0], ids[2]) && !ensureMapHasKeys(q, ids[2], ids[5]) && !ensureMapHasKeys(q, ids[5], ids[0]) {
		t.Fatal(q, ids[0], ids[1], ids[2])
	}
	// existence test with incorrect input
	q, err = runQuery(`{"has": ["c"], "limit": "a"}`, col)
	if dberr.Type(err) != dberr.ErrorExpectingInt {
		t.Fatal(err)
	}
	// existence test, collection scan & PK
	q, err = runQuery(`{"has": ["c"], "limit": 2}`, col)
	if dberr.Type(err) != dberr.ErrorNeedIndex {
		t.Fatal("Existence test should return error")
	}
	q, err = runQuery(`{"has": ["@id"], "limit": 2}`, col)
	if dberr.Type(err) != dberr.ErrorNeedIndex {
		t.Fatal("Existence test should return error")
	}
	// int range scan with incorrect input
	q, err = runQuery(`{"int-from": "a", "int-to": 4, "in": ["f"], "limit": 1}`, col)
	if dberr.Type(err) != dberr.ErrorExpectingInt {
		t.Fatal(err)
	}
	q, err = runQuery(`{"int-from": 1, "int-to": "a", "in": ["f"], "limit": 1}`, col)
	if dberr.Type(err) != dberr.ErrorExpectingInt {
		t.Fatal(err)
	}
	q, err = runQuery(`{"int-from": 1, "int-to": 2, "in": ["f"], "limit": "a"}`, col)
	if dberr.Type(err) != dberr.ErrorExpectingInt {
		t.Fatal(err)
	}
	// int range scan
	q, err = runQuery(`{"int-from": 2, "int-to": 4, "in": ["f"]}`, col)
	if err != nil {
		t.Fatal(err)
	}
	if !ensureMapHasKeys(q, ids[1], ids[2], ids[3]) {
		t.Fatal(q)
	}
	q, err = runQuery(`{"int-from": 2, "int-to": 4, "in": ["f"], "limit": 2}`, col)
	if err != nil {
		t.Fatal(err)
	}
	if !ensureMapHasKeys(q, ids[1], ids[2]) {
		t.Fatal(q, ids[1], ids[2])
	}
	// int hash scan using reversed range and limit
	q, err = runQuery(`{"int-from": 10, "int-to": 0, "in": ["f"]}`, col)
	if err != nil {
		t.Fatal(err)
	}
	if !ensureMapHasKeys(q, ids[5], ids[4], ids[3], ids[2], ids[1], ids[0]) {
		t.Fatal(q)
	}
	q, err = runQuery(`{"int-from": 10, "int-to": 0, "in": ["f"], "limit": 2}`, col)
	if err != nil {
		t.Fatal(err)
	}
	if !ensureMapHasKeys(q, ids[5], ids[4]) {
		t.Fatal(q)
	}
	// all documents
	q, err = runQuery(`"all"`, col)
	if err != nil {
		t.Fatal(err)
	}
	if !ensureMapHasKeys(q, ids[0], ids[1], ids[2], ids[3], ids[4], ids[5], ids[6], ids[7]) {
		t.Fatal(q)
	}
	// union
	col.Index([]string{"c"})
	q, err = runQuery(`[{"eq": 4, "limit": 1, "in": ["a", "b"]}, {"eq": 1, "limit": 1, "in": ["c"]}]`, col)
	if err != nil {
		t.Fatal(err)
	}
	if !ensureMapHasKeys(q, ids[0], ids[4]) && !ensureMapHasKeys(q, ids[1], ids[4]) {
		t.Fatal(q)
	}
	// intersection
	col.Index([]string{"d"})
	q, err = runQuery(`{"n": [{"eq": 2, "in": ["d"]}, "all"]}`, col)
	if err != nil {
		t.Fatal(err)
	}
	if !ensureMapHasKeys(q, ids[1], ids[3]) {
		t.Fatal(q)
	}
	// intersection with incorrect input
	q, err = runQuery(`{"c": null}`, col)
	if dberr.Type(err) != dberr.ErrorExpectingSubQuery {
		t.Fatal(err)
	}
	// complement
	q, err = runQuery(`{"c": [{"eq": 4,  "in": ["c"]}, {"eq": 2, "in": ["d"]}, "all"]}`, col)
	if err != nil {
		t.Fatal(err)
	}
	if !ensureMapHasKeys(q, ids[0], ids[2], ids[6], ids[7]) {
		t.Fatal(q)
	}
	// complement with incorrect input
	q, err = runQuery(`{"c": null}`, col)
	if dberr.Type(err) != dberr.ErrorExpectingSubQuery {
		t.Fatal(err)
	}
	// union of intersection
	q, err = runQuery(`[{"n": [{"eq": 3, "in": ["c"]}]}, {"n": [{"eq": 2, "in": ["c"]}]}]`, col)
	if !ensureMapHasKeys(q, ids[2], ids[3]) {
		t.Fatal(q)
	}
	// union of complement
	q, err = runQuery(`[{"c": [{"eq": 3, "in": ["c"]}]}, {"c": [{"eq": 2, "in": ["c"]}]}]`, col)
	if !ensureMapHasKeys(q, ids[2], ids[3]) {
		t.Fatal(q)
	}
	// union of complement of intersection
	q, err = runQuery(`[{"c": [{"n": [{"eq": 1, "in": ["d"]},{"eq": 1, "in": ["c"]}]},{"eq": 1, "in": ["d"]}]},{"eq": 2, "in": ["c"]}]`, col)
	if !ensureMapHasKeys(q, ids[2], ids[4], ids[5]) {
		t.Fatal(q)
	}
}

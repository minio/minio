/*
 * MinIO Cloud Storage, (C) 2019 MinIO, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package sql

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/alecthomas/participle"
	"github.com/bcicen/jstream"
)

func getJSONStructs(b []byte) ([]interface{}, error) {
	dec := jstream.NewDecoder(bytes.NewBuffer(b), 0).ObjectAsKVS()
	var result []interface{}
	for parsedVal := range dec.Stream() {
		result = append(result, parsedVal.Value)
	}
	if err := dec.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

func TestJsonpathEval(t *testing.T) {
	f, err := os.Open(filepath.Join("jsondata", "books.json"))
	if err != nil {
		t.Fatal(err)
	}

	b, err := ioutil.ReadAll(f)
	if err != nil {
		t.Fatal(err)
	}

	p := participle.MustBuild(
		&JSONPath{},
		participle.Lexer(sqlLexer),
		participle.CaseInsensitive("Keyword"),
	)
	cases := []struct {
		str string
		res []interface{}
	}{
		{"s.title", []interface{}{"Murder on the Orient Express", "The Robots of Dawn", "Pigs Have Wings"}},
		{"s.authorInfo.yearRange", []interface{}{[]interface{}{1890.0, 1976.0}, []interface{}{1920.0, 1992.0}, []interface{}{1881.0, 1975.0}}},
		{"s.authorInfo.name", []interface{}{"Agatha Christie", "Isaac Asimov", "P. G. Wodehouse"}},
		{"s.authorInfo.yearRange[0]", []interface{}{1890.0, 1920.0, 1881.0}},
		{"s.publicationHistory[0].pages", []interface{}{256.0, 336.0, nil}},
	}
	for i, tc := range cases {
		jp := JSONPath{}
		err := p.ParseString(tc.str, &jp)
		// fmt.Println(jp)
		if err != nil {
			t.Fatalf("parse failed!: %d %v %s", i, err, tc)
		}

		// Read only the first json object from the file
		recs, err := getJSONStructs(b)
		if err != nil || len(recs) != 3 {
			t.Fatalf("%v or length was not 3", err)
		}

		for j, rec := range recs {
			// fmt.Println(rec)
			r, err := jsonpathEval(jp.PathExpr, rec)
			if err != nil {
				t.Errorf("Error: %d %d %v", i, j, err)
			}
			if !reflect.DeepEqual(r, tc.res[j]) {
				fmt.Printf("%#v (%v) != %v (%v)\n", r, reflect.TypeOf(r), tc.res[j], reflect.TypeOf(tc.res[j]))
				t.Errorf("case: %d %d failed", i, j)
			}
		}
	}
}

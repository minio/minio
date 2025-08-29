// Copyright (c) 2015-2021 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package sql

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/alecthomas/participle"
	"github.com/minio/minio/internal/s3select/jstream"
)

func getJSONStructs(b []byte) ([]any, error) {
	dec := jstream.NewDecoder(bytes.NewBuffer(b), 0).ObjectAsKVS().MaxDepth(100)
	var result []any
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

	b, err := io.ReadAll(f)
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
		res []any
	}{
		{"s.title", []any{"Murder on the Orient Express", "The Robots of Dawn", "Pigs Have Wings"}},
		{"s.authorInfo.yearRange", []any{[]any{1890.0, 1976.0}, []any{1920.0, 1992.0}, []any{1881.0, 1975.0}}},
		{"s.authorInfo.name", []any{"Agatha Christie", "Isaac Asimov", "P. G. Wodehouse"}},
		{"s.authorInfo.yearRange[0]", []any{1890.0, 1920.0, 1881.0}},
		{"s.publicationHistory[0].pages", []any{256.0, 336.0, Missing{}}},
	}
	for i, tc := range cases {
		t.Run(tc.str, func(t *testing.T) {
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
				r, _, err := jsonpathEval(jp.PathExpr, rec)
				if err != nil {
					t.Errorf("Error: %d %d %v", i, j, err)
				}
				if !reflect.DeepEqual(r, tc.res[j]) {
					fmt.Printf("%#v (%v) != %v (%v)\n", r, reflect.TypeOf(r), tc.res[j], reflect.TypeOf(tc.res[j]))
					t.Errorf("case: %d %d failed", i, j)
				}
			}
		})
	}
}

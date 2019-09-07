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

package csv

import (
	"bytes"
	"io"
	"io/ioutil"
	"path/filepath"
	"strings"
	"testing"

	"github.com/minio/minio/pkg/s3select/sql"
)

func TestRead(t *testing.T) {
	cases := []struct {
		content         string
		recordDelimiter string
		fieldDelimiter  string
	}{
		{"1,2,3\na,b,c\n", "\n", ","},
		{"1,2,3\ta,b,c\t", "\t", ","},
		{"1,2,3\r\na,b,c\r\n", "\r\n", ","},
	}

	for i, c := range cases {
		var err error
		var record sql.Record
		var result bytes.Buffer

		r, _ := NewReader(ioutil.NopCloser(strings.NewReader(c.content)), &ReaderArgs{
			FileHeaderInfo:             none,
			RecordDelimiter:            c.recordDelimiter,
			FieldDelimiter:             c.fieldDelimiter,
			QuoteCharacter:             defaultQuoteCharacter,
			QuoteEscapeCharacter:       defaultQuoteEscapeCharacter,
			CommentCharacter:           defaultCommentCharacter,
			AllowQuotedRecordDelimiter: true,
			unmarshaled:                true,
		})

		for {
			record, err = r.Read(record)
			if err != nil {
				break
			}
			record.WriteCSV(&result, []rune(c.fieldDelimiter)[0])
			result.Truncate(result.Len() - 1)
			result.WriteString(c.recordDelimiter)
		}
		r.Close()
		if err != io.EOF {
			t.Fatalf("Case %d failed with %s", i, err)
		}

		if result.String() != c.content {
			t.Errorf("Case %d failed: expected %v result %v", i, c.content, result.String())
		}
	}
}

func BenchmarkReaderBasic(b *testing.B) {
	const dataDir = "testdata"
	args := ReaderArgs{
		FileHeaderInfo:             use,
		RecordDelimiter:            "\n",
		FieldDelimiter:             ",",
		QuoteCharacter:             defaultQuoteCharacter,
		QuoteEscapeCharacter:       defaultQuoteEscapeCharacter,
		CommentCharacter:           defaultCommentCharacter,
		AllowQuotedRecordDelimiter: true,
		unmarshaled:                true,
	}
	f, err := ioutil.ReadFile(filepath.Join(dataDir, "nyc-taxi-data-100k.csv"))
	if err != nil {
		b.Fatal(err)
	}
	r, err := NewReader(ioutil.NopCloser(bytes.NewBuffer(f)), &args)
	if err != nil {
		b.Fatalf("Reading init failed with %s", err)
	}
	defer r.Close()
	b.ReportAllocs()
	b.ResetTimer()
	var record sql.Record
	for i := 0; i < b.N; i++ {
		r, err = NewReader(ioutil.NopCloser(bytes.NewBuffer(f)), &args)
		if err != nil {
			b.Fatalf("Reading init failed with %s", err)
		}

		for err == nil {
			record, err = r.Read(record)
			if err != nil && err != io.EOF {
				b.Fatalf("Reading failed with %s", err)
			}
		}
		r.Close()
	}
}

func BenchmarkReaderReplace(b *testing.B) {
	const dataDir = "testdata"
	args := ReaderArgs{
		FileHeaderInfo:             use,
		RecordDelimiter:            "^",
		FieldDelimiter:             ",",
		QuoteCharacter:             defaultQuoteCharacter,
		QuoteEscapeCharacter:       defaultQuoteEscapeCharacter,
		CommentCharacter:           defaultCommentCharacter,
		AllowQuotedRecordDelimiter: true,
		unmarshaled:                true,
	}
	f, err := ioutil.ReadFile(filepath.Join(dataDir, "nyc-taxi-data-100k-single-delim.csv"))
	if err != nil {
		b.Fatal(err)
	}
	r, err := NewReader(ioutil.NopCloser(bytes.NewBuffer(f)), &args)
	if err != nil {
		b.Fatalf("Reading init failed with %s", err)
	}
	defer r.Close()
	b.ReportAllocs()
	b.ResetTimer()
	var record sql.Record
	for i := 0; i < b.N; i++ {
		r, err = NewReader(ioutil.NopCloser(bytes.NewBuffer(f)), &args)
		if err != nil {
			b.Fatalf("Reading init failed with %s", err)
		}

		for err == nil {
			record, err = r.Read(record)
			if err != nil && err != io.EOF {
				b.Fatalf("Reading failed with %s", err)
			}
		}
		r.Close()
	}
}

func BenchmarkReaderReplaceTwo(b *testing.B) {
	const dataDir = "testdata"
	args := ReaderArgs{
		FileHeaderInfo:             use,
		RecordDelimiter:            "^Y",
		FieldDelimiter:             ",",
		QuoteCharacter:             defaultQuoteCharacter,
		QuoteEscapeCharacter:       defaultQuoteEscapeCharacter,
		CommentCharacter:           defaultCommentCharacter,
		AllowQuotedRecordDelimiter: true,
		unmarshaled:                true,
	}
	f, err := ioutil.ReadFile(filepath.Join(dataDir, "nyc-taxi-data-100k-multi-delim.csv"))
	if err != nil {
		b.Fatal(err)
	}
	r, err := NewReader(ioutil.NopCloser(bytes.NewBuffer(f)), &args)
	if err != nil {
		b.Fatalf("Reading init failed with %s", err)
	}
	defer r.Close()
	b.ReportAllocs()
	b.ResetTimer()
	var record sql.Record
	for i := 0; i < b.N; i++ {
		r, err = NewReader(ioutil.NopCloser(bytes.NewBuffer(f)), &args)
		if err != nil {
			b.Fatalf("Reading init failed with %s", err)
		}

		for err == nil {
			record, err = r.Read(record)
			if err != nil && err != io.EOF {
				b.Fatalf("Reading failed with %s", err)
			}
		}
		r.Close()
	}
}

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

package s3select

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/klauspost/cpuid/v2"
	"github.com/minio/minio-go/v7"
	"github.com/minio/simdjson-go"
)

func newStringRSC(s string) io.ReadSeekCloser {
	return newBytesRSC([]byte(s))
}

func newBytesRSC(b []byte) io.ReadSeekCloser {
	r := bytes.NewReader(b)
	segmentReader := func(offset int64) (io.ReadCloser, error) {
		_, err := r.Seek(offset, io.SeekStart)
		if err != nil {
			return nil, err
		}
		return io.NopCloser(r), nil
	}
	return NewObjectReadSeekCloser(segmentReader, int64(len(b)))
}

type testResponseWriter struct {
	statusCode int
	response   []byte
}

func (w *testResponseWriter) Header() http.Header {
	return nil
}

func (w *testResponseWriter) Write(p []byte) (int, error) {
	w.response = append(w.response, p...)
	return len(p), nil
}

func (w *testResponseWriter) WriteHeader(statusCode int) {
	w.statusCode = statusCode
}

func (w *testResponseWriter) Flush() {
}

func TestJSONQueries(t *testing.T) {
	input := `{"id": 0,"title": "Test Record","desc": "Some text","synonyms": ["foo", "bar", "whatever"]}
	{"id": 1,"title": "Second Record","desc": "another text","synonyms": ["some", "synonym", "value"]}
	{"id": 2,"title": "Second Record","desc": "another text","numbers": [2, 3.0, 4]}
	{"id": 3,"title": "Second Record","desc": "another text","nested": [[2, 3.0, 4], [7, 8.5, 9]]}`

	testTable := []struct {
		name       string
		query      string
		requestXML []byte // override request XML
		wantResult string
		withJSON   string // Override JSON input
	}{
		{
			name:       "select-in-array-full",
			query:      `SELECT * from s3object s WHERE 'bar' IN s.synonyms[*]`,
			wantResult: `{"id":0,"title":"Test Record","desc":"Some text","synonyms":["foo","bar","whatever"]}`,
		},
		{
			name:  "simple-in-array",
			query: `SELECT * from s3object s WHERE s.id IN (1,3)`,
			wantResult: `{"id":1,"title":"Second Record","desc":"another text","synonyms":["some","synonym","value"]}
{"id":3,"title":"Second Record","desc":"another text","nested":[[2,3,4],[7,8.5,9]]}`,
		},
		{
			name:       "select-in-array-single",
			query:      `SELECT synonyms from s3object s WHERE 'bar' IN s.synonyms[*] `,
			wantResult: `{"synonyms":["foo","bar","whatever"]}`,
		},
		{
			name:       "donatello-1",
			query:      `SELECT * from s3object s WHERE 'bar' in s.synonyms`,
			wantResult: `{"id":0,"title":"Test Record","desc":"Some text","synonyms":["foo","bar","whatever"]}`,
		},
		{
			name:       "donatello-2",
			query:      `SELECT * from s3object s WHERE 'bar' in s.synonyms[*]`,
			wantResult: `{"id":0,"title":"Test Record","desc":"Some text","synonyms":["foo","bar","whatever"]}`,
		},
		{
			name:       "in-expression-precedence-1",
			query:      "select * from s3object s where 'bar' in s.synonyms and s.id = 0",
			wantResult: `{"id":0,"title":"Test Record","desc":"Some text","synonyms":["foo","bar","whatever"]}`,
		},
		{
			name:       "in-expression-precedence-2",
			query:      "select * from s3object s where 'some' in ('somex', s.synonyms[0]) and s.id = 1",
			wantResult: `{"id":1,"title":"Second Record","desc":"another text","synonyms":["some","synonym","value"]}`,
		},
		{
			name:       "in-expression-with-aggregation",
			query:      "select SUM(s.id) from s3object s Where 2 in s.numbers[*] or 'some' in s.synonyms[*]",
			wantResult: `{"_1":3}`,
		},
		{
			name:  "bignum-1",
			query: `SELECT id from s3object s WHERE s.id <= 9223372036854775807`,
			wantResult: `{"id":0}
{"id":1}
{"id":2}
{"id":3}`,
		},
		{
			name:  "bignum-2",
			query: `SELECT id from s3object s WHERE s.id >= -9223372036854775808`,
			wantResult: `{"id":0}
{"id":1}
{"id":2}
{"id":3}`,
		},
		{
			name:       "donatello-3",
			query:      `SELECT * from s3object s WHERE 'value' IN s.synonyms[*]`,
			wantResult: `{"id":1,"title":"Second Record","desc":"another text","synonyms":["some","synonym","value"]}`,
		},
		{
			name:       "select-in-number",
			query:      `SELECT * from s3object s WHERE 4 in s.numbers[*]`,
			wantResult: `{"id":2,"title":"Second Record","desc":"another text","numbers":[2,3,4]}`,
		},
		{
			name:       "select-in-number-float",
			query:      `SELECT * from s3object s WHERE 3 in s.numbers[*]`,
			wantResult: `{"id":2,"title":"Second Record","desc":"another text","numbers":[2,3,4]}`,
		},
		{
			name:       "select-in-number-float-in-sql",
			query:      `SELECT * from s3object s WHERE 3.0 in s.numbers[*]`,
			wantResult: `{"id":2,"title":"Second Record","desc":"another text","numbers":[2,3,4]}`,
		},
		{
			name:       "select-in-list-match",
			query:      `SELECT * from s3object s WHERE (2,3,4) IN s.nested[*]`,
			wantResult: `{"id":3,"title":"Second Record","desc":"another text","nested":[[2,3,4],[7,8.5,9]]}`,
		},
		{
			name:       "select-in-nested-float",
			query:      `SELECT s.nested from s3object s WHERE 8.5 IN s.nested[*][*]`,
			wantResult: `{"nested":[[2,3,4],[7,8.5,9]]}`,
		},
		{
			name:       "select-in-combine-and",
			query:      `SELECT s.nested from s3object s WHERE (8.5 IN s.nested[*][*]) AND (s.id > 0)`,
			wantResult: `{"nested":[[2,3,4],[7,8.5,9]]}`,
		},
		{
			name:       "select-in-combine-and-no",
			query:      `SELECT s.nested from s3object s WHERE (8.5 IN s.nested[*][*]) AND (s.id = 0)`,
			wantResult: ``,
		},
		{
			name:       "select-in-nested-float-no-flat",
			query:      `SELECT s.nested from s3object s WHERE 8.5 IN s.nested[*]`,
			wantResult: ``,
		},
		{
			name:       "select-empty-field-result",
			query:      `SELECT * from s3object s WHERE s.nested[0][0] = 2`,
			wantResult: `{"id":3,"title":"Second Record","desc":"another text","nested":[[2,3,4],[7,8.5,9]]}`,
		},
		{
			name:       "select-arrays-specific",
			query:      `SELECT * from s3object s WHERE s.nested[1][0] = 7`,
			wantResult: `{"id":3,"title":"Second Record","desc":"another text","nested":[[2,3,4],[7,8.5,9]]}`,
		},
		{
			name:       "wrong-index-no-result",
			query:      `SELECT * from s3object s WHERE s.nested[0][0] = 7`,
			wantResult: ``,
		},
		{
			name:  "not-equal-result",
			query: `SELECT * from s3object s WHERE s.nested[1][0] != 7`,
			wantResult: `{"id":0,"title":"Test Record","desc":"Some text","synonyms":["foo","bar","whatever"]}
{"id":1,"title":"Second Record","desc":"another text","synonyms":["some","synonym","value"]}
{"id":2,"title":"Second Record","desc":"another text","numbers":[2,3,4]}`,
		},
		{
			name:       "indexed-list-match",
			query:      `SELECT * from s3object s WHERE (7,8.5,9) IN s.nested[1]`,
			wantResult: ``,
		},
		{
			name:       "indexed-list-match-equals",
			query:      `SELECT * from s3object s WHERE (7,8.5,9) = s.nested[1]`,
			wantResult: `{"id":3,"title":"Second Record","desc":"another text","nested":[[2,3,4],[7,8.5,9]]}`,
		},
		{
			name:       "indexed-list-match-equals-s-star",
			query:      `SELECT s.* from s3object s WHERE (7,8.5,9) = s.nested[1]`,
			wantResult: `{"id":3,"title":"Second Record","desc":"another text","nested":[[2,3,4],[7,8.5,9]]}`,
		},
		{
			name:       "indexed-list-match-equals-s-index",
			query:      `SELECT s.nested[1], s.nested[0] from s3object s WHERE (7,8.5,9) = s.nested[1]`,
			wantResult: `{"_1":[7,8.5,9],"_2":[2,3,4]}`,
		},
		{
			name:  "indexed-list-match-not-equals",
			query: `SELECT * from s3object s WHERE (7,8.5,9) != s.nested[1]`,
			wantResult: `{"id":0,"title":"Test Record","desc":"Some text","synonyms":["foo","bar","whatever"]}
{"id":1,"title":"Second Record","desc":"another text","synonyms":["some","synonym","value"]}
{"id":2,"title":"Second Record","desc":"another text","numbers":[2,3,4]}`,
		},
		{
			name:       "indexed-list-square-bracket",
			query:      `SELECT * from s3object s WHERE [7,8.5,9] = s.nested[1]`,
			wantResult: `{"id":3,"title":"Second Record","desc":"another text","nested":[[2,3,4],[7,8.5,9]]}`,
		},
		{
			name:       "indexed-list-square-bracket",
			query:      `SELECT * from s3object s WHERE [7,8.5,9] IN s.nested`,
			wantResult: `{"id":3,"title":"Second Record","desc":"another text","nested":[[2,3,4],[7,8.5,9]]}`,
		},
		{
			name:  "indexed-list-square-bracket",
			query: `SELECT * from s3object s WHERE id IN [3,2]`,
			wantResult: `{"id":2,"title":"Second Record","desc":"another text","numbers":[2,3,4]}
{"id":3,"title":"Second Record","desc":"another text","nested":[[2,3,4],[7,8.5,9]]}`,
		},
		{
			name:       "index-wildcard-in",
			query:      `SELECT * from s3object s WHERE (8.5) IN s.nested[1][*]`,
			wantResult: `{"id":3,"title":"Second Record","desc":"another text","nested":[[2,3,4],[7,8.5,9]]}`,
		},
		{
			name:       "index-wildcard-in",
			query:      `SELECT * from s3object s WHERE (8.0+0.5) IN s.nested[1][*]`,
			wantResult: `{"id":3,"title":"Second Record","desc":"another text","nested":[[2,3,4],[7,8.5,9]]}`,
		},
		{
			name:       "compare-mixed",
			query:      `SELECT id from s3object s WHERE value = true`,
			wantResult: `{"id":1}`,
			withJSON: `{"id":0, "value": false}
{"id":1, "value": true}
{"id":2, "value": 42}
{"id":3, "value": "true"}
`,
		},
		{
			name:       "compare-mixed-not",
			query:      `SELECT COUNT(id) as n from s3object s WHERE value != true`,
			wantResult: `{"n":3}`,
			withJSON: `{"id":0, "value": false}
{"id":1, "value": true}
{"id":2, "value": 42}
{"id":3, "value": "true"}
`,
		},
		{
			name:       "index-wildcard-in",
			query:      `SELECT * from s3object s WHERE title = 'Test Record'`,
			wantResult: `{"id":0,"title":"Test Record","desc":"Some text","synonyms":["foo","bar","whatever"]}`,
		},
		{
			name: "select-output-field-as-csv",
			requestXML: []byte(`<?xml version="1.0" encoding="UTF-8"?>
<SelectObjectContentRequest>
    <Expression>SELECT s.synonyms from s3object s WHERE 'whatever' IN s.synonyms</Expression>
    <ExpressionType>SQL</ExpressionType>
    <InputSerialization>
        <CompressionType>NONE</CompressionType>
        <JSON>
            <Type>DOCUMENT</Type>
        </JSON>
    </InputSerialization>
    <OutputSerialization>
        <CSV>
	   <QuoteCharacter>"</QuoteCharacter>
        </CSV>
    </OutputSerialization>
    <RequestProgress>
        <Enabled>FALSE</Enabled>
    </RequestProgress>
</SelectObjectContentRequest>`),
			wantResult: `"[""foo"",""bar"",""whatever""]"`,
		},
		{
			name:  "document",
			query: "",
			requestXML: []byte(`
<?xml version="1.0" encoding="UTF-8"?>
<SelectObjectContentRequest>
    <Expression>select * from s3object[*].elements[*] s where s.element_type = '__elem__merfu'</Expression>
    <ExpressionType>SQL</ExpressionType>
    <InputSerialization>
        <CompressionType>NONE</CompressionType>
        <JSON>
            <Type>DOCUMENT</Type>
        </JSON>
    </InputSerialization>
    <OutputSerialization>
        <JSON>
        </JSON>
    </OutputSerialization>
    <RequestProgress>
        <Enabled>FALSE</Enabled>
    </RequestProgress>
</SelectObjectContentRequest>`),
			withJSON: `
{
  "name": "small_pdf1.pdf",
  "lume_id": "9507193e-572d-4f95-bcf1-e9226d96be65",
  "elements": [
    {
      "element_type": "__elem__image",
      "element_id": "859d09c4-7cf1-4a37-9674-3a7de8b56abc",
      "attributes": {
        "__attr__image_dpi": 300,
        "__attr__image_size": [
          2550,
          3299
        ],
        "__attr__image_index": 1,
        "__attr__image_format": "JPEG",
        "__attr__file_extension": "jpg",
        "__attr__data": null
      }
    },
    {
      "element_type": "__elem__merfu",
      "element_id": "d868aefe-ef9a-4be2-b9b2-c9fd89cc43eb",
      "attributes": {
        "__attr__image_dpi": 300,
        "__attr__image_size": [
          2550,
          3299
        ],
        "__attr__image_index": 2,
        "__attr__image_format": "JPEG",
        "__attr__file_extension": "jpg",
        "__attr__data": null
      }
    }
  ],
  "data": "asdascasdc1234e123erdasdas"
}`,
			wantResult: `{"element_type":"__elem__merfu","element_id":"d868aefe-ef9a-4be2-b9b2-c9fd89cc43eb","attributes":{"__attr__image_dpi":300,"__attr__image_size":[2550,3299],"__attr__image_index":2,"__attr__image_format":"JPEG","__attr__file_extension":"jpg","__attr__data":null}}`,
		},
		{
			name:       "date_diff_month",
			query:      `SELECT date_diff(MONTH, '2019-10-20T', '2020-01-20T') FROM S3Object LIMIT 1`,
			wantResult: `{"_1":3}`,
		},
		{
			name:       "date_diff_month_neg",
			query:      `SELECT date_diff(MONTH, '2020-01-20T', '2019-10-20T') FROM S3Object LIMIT 1`,
			wantResult: `{"_1":-3}`,
		},
		// Examples from https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-glacier-select-sql-reference-date.html#s3-glacier-select-sql-reference-date-diff
		{
			name:       "date_diff_year",
			query:      `SELECT date_diff(year, '2010-01-01T', '2011-01-01T') FROM S3Object LIMIT 1`,
			wantResult: `{"_1":1}`,
		},
		{
			name:       "date_diff_year",
			query:      `SELECT date_diff(month, '2010-01-01T', '2010-05T') FROM S3Object LIMIT 1`,
			wantResult: `{"_1":4}`,
		},
		{
			name:       "date_diff_month_oney",
			query:      `SELECT date_diff(month, '2010T', '2011T') FROM S3Object LIMIT 1`,
			wantResult: `{"_1":12}`,
		},
		{
			name:       "date_diff_month_neg",
			query:      `SELECT date_diff(month, '2011T', '2010T') FROM S3Object LIMIT 1`,
			wantResult: `{"_1":-12}`,
		},
		{
			name:       "date_diff_days",
			query:      `SELECT date_diff(day, '2010-01-01T23:00:00Z', '2010-01-02T01:00:00Z') FROM S3Object LIMIT 1`,
			wantResult: `{"_1":0}`,
		},
		{
			name:       "date_diff_days_one",
			query:      `SELECT date_diff(day, '2010-01-01T23:00:00Z', '2010-01-02T23:00:00Z') FROM S3Object LIMIT 1`,
			wantResult: `{"_1":1}`,
		},
		{
			name:       "cast_from_int_to_float",
			query:      `SELECT cast(1 as float) FROM S3Object LIMIT 1`,
			wantResult: `{"_1":1}`,
		},
		{
			name:       "cast_from_float_to_float",
			query:      `SELECT cast(1.0 as float) FROM S3Object LIMIT 1`,
			wantResult: `{"_1":1}`,
		},
		{
			name:       "arithmetic_integer_operand",
			query:      `SELECT 1 / 2 FROM S3Object LIMIT 1`,
			wantResult: `{"_1":0}`,
		},
		{
			name:       "arithmetic_float_operand",
			query:      `SELECT 1.0 / 2.0 * .3 FROM S3Object LIMIT 1`,
			wantResult: `{"_1":0.15}`,
		},
		{
			name:       "arithmetic_integer_float_operand",
			query:      `SELECT 3.0 / 2, 5 / 2.0 FROM S3Object LIMIT 1`,
			wantResult: `{"_1":1.5,"_2":2.5}`,
		},
		{
			name:  "limit-1",
			query: `SELECT * FROM S3Object[*].elements[*] LIMIT 1`,
			requestXML: []byte(`
<?xml version="1.0" encoding="UTF-8"?>
<SelectObjectContentRequest>
    <Expression>select * from s3object[*].elements[*] s where s.element_type = '__elem__merfu'</Expression>
    <ExpressionType>SQL</ExpressionType>
    <InputSerialization>
        <CompressionType>NONE</CompressionType>
        <JSON>
            <Type>DOCUMENT</Type>
        </JSON>
    </InputSerialization>
    <OutputSerialization>
        <JSON>
        </JSON>
    </OutputSerialization>
    <RequestProgress>
        <Enabled>FALSE</Enabled>
    </RequestProgress>
</SelectObjectContentRequest>`),
			wantResult: `{"element_type":"__elem__merfu","element_id":"d868aefe-ef9a-4be2-b9b2-c9fd89cc43eb","attributes":{"__attr__image_dpi":300,"__attr__image_size":[2550,3299],"__attr__image_index":2,"__attr__image_format":"JPEG","__attr__file_extension":"jpg","__attr__data":null}}`,
			withJSON: `
{
  "name": "small_pdf1.pdf",
  "lume_id": "9507193e-572d-4f95-bcf1-e9226d96be65",
  "elements": [
    {
      "element_type": "__elem__image",
      "element_id": "859d09c4-7cf1-4a37-9674-3a7de8b56abc",
      "attributes": {
        "__attr__image_dpi": 300,
        "__attr__image_size": [
          2550,
          3299
        ],
        "__attr__image_index": 1,
        "__attr__image_format": "JPEG",
        "__attr__file_extension": "jpg",
        "__attr__data": null
      }
    },
    {
      "element_type": "__elem__merfu",
      "element_id": "d868aefe-ef9a-4be2-b9b2-c9fd89cc43eb",
      "attributes": {
        "__attr__image_dpi": 300,
        "__attr__image_size": [
          2550,
          3299
        ],
        "__attr__image_index": 2,
        "__attr__image_format": "JPEG",
        "__attr__file_extension": "jpg",
        "__attr__data": null
      }
    }
  ],
  "data": "asdascasdc1234e123erdasdas"
}`,
		},
		{
			name:       "limit-2",
			query:      `select * from s3object[*].person[*] limit 1`,
			wantResult: `{"Id":1,"Name":"Anshu","Address":"Templestowe","Car":"Jeep"}`,
			withJSON:   `{ "person": [ { "Id": 1, "Name": "Anshu", "Address": "Templestowe", "Car": "Jeep" }, { "Id": 2, "Name": "Ben Mostafa", "Address": "Las Vegas", "Car": "Mustang" }, { "Id": 3, "Name": "Rohan Wood", "Address": "Wooddon", "Car": "VW" } ] }`,
		},
		{
			name:       "lower-case-is",
			query:      `select * from s3object[*] as s where s.request.header['User-Agent'] is not null`,
			wantResult: `{"request":{"uri":"/1","header":{"User-Agent":"test"}}}`,
			withJSON: `{"request":{"uri":"/1","header":{"User-Agent":"test"}}}
{"request":{"uri":"/2","header":{}}}`,
		},
		{
			name:       "is-not-missing",
			query:      `select * from s3object[*] as s where s.request.header['User-Agent'] is not missing`,
			wantResult: `{"request":{"uri":"/1","header":{"User-Agent":"test"}}}`,
			withJSON: `{"request":{"uri":"/1","header":{"User-Agent":"test"}}}
{"request":{"uri":"/2","header":{}}}`,
		},
		{
			name:  "is-not-missing-2",
			query: `select * from s3object[*] as s where s.request.header is not missing`,
			wantResult: `{"request":{"uri":"/1","header":{"User-Agent":"test"}}}
{"request":{"uri":"/2","header":{}}}`,
			withJSON: `{"request":{"uri":"/1","header":{"User-Agent":"test"}}}
{"request":{"uri":"/2","header":{}}}`,
		},
		{
			name:  "is-not-missing-null",
			query: `select * from s3object[*] as s where s.request.header is not missing`,
			wantResult: `{"request":{"uri":"/1","header":{"User-Agent":"test"}}}
{"request":{"uri":"/2","header":null}}`,
			withJSON: `{"request":{"uri":"/1","header":{"User-Agent":"test"}}}
{"request":{"uri":"/2","header":null}}`,
		},
		{
			name:       "is-not-missing",
			query:      `select * from s3object[*] as s where s.request.header['User-Agent'] is not missing`,
			wantResult: `{"request":{"uri":"/1","header":{"User-Agent":"test"}}}`,
			withJSON: `{"request":{"uri":"/1","header":{"User-Agent":"test"}}}
{"request":{"uri":"/2","header":{}}}`,
		},
		{
			name:  "is-not-missing-2",
			query: `select * from s3object[*] as s where s.request.header is not missing`,
			wantResult: `{"request":{"uri":"/1","header":{"User-Agent":"test"}}}
{"request":{"uri":"/2","header":{}}}`,
			withJSON: `{"request":{"uri":"/1","header":{"User-Agent":"test"}}}
{"request":{"uri":"/2","header":{}}}`,
		},
		{
			name:  "is-not-missing-null",
			query: `select * from s3object[*] as s where s.request.header is not missing`,
			wantResult: `{"request":{"uri":"/1","header":{"User-Agent":"test"}}}
{"request":{"uri":"/2","header":null}}`,
			withJSON: `{"request":{"uri":"/1","header":{"User-Agent":"test"}}}
{"request":{"uri":"/2","header":null}}`,
		},

		{
			name:       "is-missing",
			query:      `select * from s3object[*] as s where s.request.header['User-Agent'] is missing`,
			wantResult: `{"request":{"uri":"/2","header":{}}}`,
			withJSON: `{"request":{"uri":"/1","header":{"User-Agent":"test"}}}
{"request":{"uri":"/2","header":{}}}`,
		},
		{
			name:       "is-missing-2",
			query:      `select * from s3object[*] as s where s.request.header is missing`,
			wantResult: ``,
			withJSON: `{"request":{"uri":"/1","header":{"User-Agent":"test"}}}
{"request":{"uri":"/2","header":{}}}`,
		},
		{
			name:       "is-missing",
			query:      `select * from s3object[*] as s where s.request.header['User-Agent'] = missing`,
			wantResult: `{"request":{"uri":"/2","header":{}}}`,
			withJSON: `{"request":{"uri":"/1","header":{"User-Agent":"test"}}}
{"request":{"uri":"/2","header":{}}}`,
		},
		{
			name:       "is-missing-null",
			query:      `select * from s3object[*] as s where s.request.header is missing`,
			wantResult: ``,
			withJSON: `{"request":{"uri":"/1","header":{"User-Agent":"test"}}}
{"request":{"uri":"/2","header":null}}`,
		},
		{
			name:  "is-missing-select",
			query: `select s.request.header['User-Agent'] as h from s3object[*] as s`,
			wantResult: `{"h":"test"}
{}`,
			withJSON: `{"request":{"uri":"/1","header":{"User-Agent":"test"}}}
{"request":{"uri":"/2","header":{}}}`,
		},
	}

	defRequest := `<?xml version="1.0" encoding="UTF-8"?>
<SelectObjectContentRequest>
    <Expression>%s</Expression>
    <ExpressionType>SQL</ExpressionType>
    <InputSerialization>
        <CompressionType>NONE</CompressionType>
        <JSON>
            <Type>LINES</Type>
        </JSON>
    </InputSerialization>
    <OutputSerialization>
        <JSON>
        </JSON>
    </OutputSerialization>
    <RequestProgress>
        <Enabled>FALSE</Enabled>
    </RequestProgress>
</SelectObjectContentRequest>`

	for _, testCase := range testTable {
		t.Run(testCase.name, func(t *testing.T) {
			// Hack cpuid to the CPU doesn't appear to support AVX2.
			// Restore whatever happens.
			if cpuid.CPU.Supports(cpuid.AVX2) {
				cpuid.CPU.Disable(cpuid.AVX2)
				defer cpuid.CPU.Enable(cpuid.AVX2)
			}
			if simdjson.SupportedCPU() {
				t.Fatal("setup error: expected cpu to be unsupported")
			}
			testReq := testCase.requestXML
			if len(testReq) == 0 {
				var escaped bytes.Buffer
				xml.EscapeText(&escaped, []byte(testCase.query))
				testReq = fmt.Appendf(nil, defRequest, escaped.String())
			}
			s3Select, err := NewS3Select(bytes.NewReader(testReq))
			if err != nil {
				t.Fatal(err)
			}

			in := input
			if len(testCase.withJSON) > 0 {
				in = testCase.withJSON
			}
			if err = s3Select.Open(newStringRSC(in)); err != nil {
				t.Fatal(err)
			}

			w := &testResponseWriter{}
			s3Select.Evaluate(w)
			s3Select.Close()
			resp := http.Response{
				StatusCode:    http.StatusOK,
				Body:          io.NopCloser(bytes.NewReader(w.response)),
				ContentLength: int64(len(w.response)),
			}
			res, err := minio.NewSelectResults(&resp, "testbucket")
			if err != nil {
				t.Error(err)
				return
			}
			got, err := io.ReadAll(res)
			if err != nil {
				t.Error(err)
				return
			}
			gotS := strings.TrimSpace(string(got))
			if !reflect.DeepEqual(gotS, testCase.wantResult) {
				t.Errorf("received response does not match with expected reply. Query: %s\ngot: %s\nwant:%s", testCase.query, gotS, testCase.wantResult)
			}
		})
		t.Run("simd-"+testCase.name, func(t *testing.T) {
			if !simdjson.SupportedCPU() {
				t.Skip("No CPU support")
			}
			testReq := testCase.requestXML
			if len(testReq) == 0 {
				var escaped bytes.Buffer
				xml.EscapeText(&escaped, []byte(testCase.query))
				testReq = fmt.Appendf(nil, defRequest, escaped.String())
			}
			s3Select, err := NewS3Select(bytes.NewReader(testReq))
			if err != nil {
				t.Fatal(err)
			}

			in := input
			if len(testCase.withJSON) > 0 {
				in = testCase.withJSON
			}
			if err = s3Select.Open(newStringRSC(in)); err != nil {
				t.Fatal(err)
			}

			w := &testResponseWriter{}
			s3Select.Evaluate(w)
			s3Select.Close()
			resp := http.Response{
				StatusCode:    http.StatusOK,
				Body:          io.NopCloser(bytes.NewReader(w.response)),
				ContentLength: int64(len(w.response)),
			}
			res, err := minio.NewSelectResults(&resp, "testbucket")
			if err != nil {
				t.Error(err)
				return
			}
			got, err := io.ReadAll(res)
			if err != nil {
				t.Error(err)
				return
			}
			gotS := strings.TrimSpace(string(got))
			if !reflect.DeepEqual(gotS, testCase.wantResult) {
				t.Errorf("received response does not match with expected reply. Query: %s\ngot: %s\nwant:%s", testCase.query, gotS, testCase.wantResult)
			}
		})
	}
}

func TestCSVQueries(t *testing.T) {
	input := `index,ID,CaseNumber,Date,Day,Month,Year,Block,IUCR,PrimaryType,Description,LocationDescription,Arrest,Domestic,Beat,District,Ward,CommunityArea,FBI Code,XCoordinate,YCoordinate,UpdatedOn,Latitude,Longitude,Location
2700763,7732229,,2010-05-26 00:00:00,26,May,2010,113XX S HALSTED ST,1150,,CREDIT CARD FRAUD,,False,False,2233,22.0,34.0,,11,,,,41.688043288,-87.6422444,"(41.688043288, -87.6422444)"`

	testTable := []struct {
		name       string
		query      string
		requestXML []byte
		wantResult string
	}{
		{
			name:       "select-in-text-simple",
			query:      `SELECT index FROM s3Object s WHERE "Month"='May'`,
			wantResult: `2700763`,
		},
	}

	defRequest := `<?xml version="1.0" encoding="UTF-8"?>
<SelectObjectContentRequest>
    <Expression>%s</Expression>
    <ExpressionType>SQL</ExpressionType>
    <InputSerialization>
        <CompressionType>NONE</CompressionType>
        <CSV>
        	<FieldDelimiter>,</FieldDelimiter>
        	<FileHeaderInfo>USE</FileHeaderInfo>
        	<QuoteCharacter>"</QuoteCharacter>
        	<QuoteEscapeCharacter>"</QuoteEscapeCharacter>
        	<RecordDelimiter>\n</RecordDelimiter>
        </CSV>
    </InputSerialization>
    <OutputSerialization>
        <CSV>
        </CSV>
    </OutputSerialization>
    <RequestProgress>
        <Enabled>FALSE</Enabled>
    </RequestProgress>
</SelectObjectContentRequest>`

	for _, testCase := range testTable {
		t.Run(testCase.name, func(t *testing.T) {
			testReq := testCase.requestXML
			if len(testReq) == 0 {
				testReq = fmt.Appendf(nil, defRequest, testCase.query)
			}
			s3Select, err := NewS3Select(bytes.NewReader(testReq))
			if err != nil {
				t.Fatal(err)
			}

			if err = s3Select.Open(newStringRSC(input)); err != nil {
				t.Fatal(err)
			}

			w := &testResponseWriter{}
			s3Select.Evaluate(w)
			s3Select.Close()
			resp := http.Response{
				StatusCode:    http.StatusOK,
				Body:          io.NopCloser(bytes.NewReader(w.response)),
				ContentLength: int64(len(w.response)),
			}
			res, err := minio.NewSelectResults(&resp, "testbucket")
			if err != nil {
				t.Error(err)
				return
			}
			got, err := io.ReadAll(res)
			if err != nil {
				t.Error(err)
				return
			}
			gotS := strings.TrimSpace(string(got))
			if !reflect.DeepEqual(gotS, testCase.wantResult) {
				t.Errorf("received response does not match with expected reply. Query: %s\ngot: %s\nwant:%s", testCase.query, gotS, testCase.wantResult)
			}
		})
	}
}

func TestCSVQueries2(t *testing.T) {
	testInput := []byte(`id,time,num,num2,text
1,2010-01-01T,7867786,4565.908123,"a text, with comma"
2,2017-01-02T03:04Z,-5, 0.765111,
`)
	testTable := []struct {
		name       string
		query      string
		input      []byte
		requestXML []byte // override request XML
		wantResult string
	}{
		{
			name:       "select-all",
			input:      testInput,
			query:      `SELECT * from s3object AS s WHERE id = '1'`,
			wantResult: `{"id":"1","time":"2010-01-01T","num":"7867786","num2":"4565.908123","text":"a text, with comma"}`,
		},
		{
			name:       "select-all-2",
			input:      testInput,
			query:      `SELECT * from s3object s WHERE id = 2`,
			wantResult: `{"id":"2","time":"2017-01-02T03:04Z","num":"-5","num2":" 0.765111","text":""}`,
		},
		{
			name:       "select-text-convert",
			input:      testInput,
			query:      `SELECT CAST(text AS STRING) AS text from s3object s WHERE id = 1`,
			wantResult: `{"text":"a text, with comma"}`,
		},
		{
			name:       "select-text-direct",
			input:      testInput,
			query:      `SELECT text from s3object s WHERE id = 1`,
			wantResult: `{"text":"a text, with comma"}`,
		},
		{
			name:       "select-time-direct",
			input:      testInput,
			query:      `SELECT time from s3object s WHERE id = 2`,
			wantResult: `{"time":"2017-01-02T03:04Z"}`,
		},
		{
			name:       "select-int-direct",
			input:      testInput,
			query:      `SELECT num from s3object s WHERE id = 2`,
			wantResult: `{"num":"-5"}`,
		},
		{
			name:       "select-float-direct",
			input:      testInput,
			query:      `SELECT num2 from s3object s WHERE id = 2`,
			wantResult: `{"num2":" 0.765111"}`,
		},
		{
			name:       "select-in-array",
			input:      testInput,
			query:      `select id from S3Object s WHERE id in [1,3]`,
			wantResult: `{"id":"1"}`,
		},
		{
			name:       "select-in-array-matchnone",
			input:      testInput,
			query:      `select id from S3Object s WHERE s.id in [4,3]`,
			wantResult: ``,
		},
		{
			name:       "select-float-by-val",
			input:      testInput,
			query:      `SELECT num2 from s3object s WHERE num2 = 0.765111`,
			wantResult: `{"num2":" 0.765111"}`,
		},
		{
			name:       "select-non_exiting_values",
			input:      testInput,
			query:      `SELECT _1 as first, s._100 from s3object s LIMIT 1`,
			wantResult: `{"first":"1","_100":null}`,
		},
		{
			name:       "select-is_null_noresults",
			input:      testInput,
			query:      `select _2 from S3object where _2 IS NULL`,
			wantResult: ``,
		},
		{
			name:  "select-is_null_results",
			input: testInput,
			query: `select _2 from S3object WHERE _100 IS NULL`,
			wantResult: `{"_2":"2010-01-01T"}
{"_2":"2017-01-02T03:04Z"}`,
		},
		{
			name:  "select-is_not_null_results",
			input: testInput,
			query: `select _2 from S3object where _2 IS NOT NULL`,
			wantResult: `{"_2":"2010-01-01T"}
{"_2":"2017-01-02T03:04Z"}`,
		},
		{
			name:       "select-is_not_null_noresults",
			input:      testInput,
			query:      `select _2 from S3object WHERE _100 IS NOT NULL`,
			wantResult: ``,
		},
		{
			name: "select-is_not_string",
			input: []byte(`c1,c2,c3
1,2,3
1,,3`),
			query:      `select * from S3object where _2 IS NOT ''`,
			wantResult: `{"c1":"1","c2":"2","c3":"3"}`,
		},
		{
			name: "select-is_not_string",
			input: []byte(`c1,c2,c3
1,2,3
1,,3`),
			query:      `select * from S3object where _2 != '' AND _2 > 1`,
			wantResult: `{"c1":"1","c2":"2","c3":"3"}`,
		},
	}

	defRequest := `<?xml version="1.0" encoding="UTF-8"?>
<SelectObjectContentRequest>
    <Expression>%s</Expression>
    <ExpressionType>SQL</ExpressionType>
    <InputSerialization>
        <CompressionType>NONE</CompressionType>
        <CSV>
            <FileHeaderInfo>USE</FileHeaderInfo>
	    <QuoteCharacter>"</QuoteCharacter>
        </CSV>
    </InputSerialization>
    <OutputSerialization>
        <JSON>
        </JSON>
    </OutputSerialization>
    <RequestProgress>
        <Enabled>FALSE</Enabled>
    </RequestProgress>
</SelectObjectContentRequest>`

	for _, testCase := range testTable {
		t.Run(testCase.name, func(t *testing.T) {
			testReq := testCase.requestXML
			if len(testReq) == 0 {
				testReq = fmt.Appendf(nil, defRequest, testCase.query)
			}
			s3Select, err := NewS3Select(bytes.NewReader(testReq))
			if err != nil {
				t.Fatal(err)
			}

			if err = s3Select.Open(newBytesRSC(testCase.input)); err != nil {
				t.Fatal(err)
			}

			w := &testResponseWriter{}
			s3Select.Evaluate(w)
			s3Select.Close()
			resp := http.Response{
				StatusCode:    http.StatusOK,
				Body:          io.NopCloser(bytes.NewReader(w.response)),
				ContentLength: int64(len(w.response)),
			}
			res, err := minio.NewSelectResults(&resp, "testbucket")
			if err != nil {
				t.Error(err)
				return
			}
			got, err := io.ReadAll(res)
			if err != nil {
				t.Error(err)
				return
			}
			gotS := strings.TrimSpace(string(got))
			if !reflect.DeepEqual(gotS, testCase.wantResult) {
				t.Errorf("received response does not match with expected reply. Query: %s\ngot: %s\nwant:%s", testCase.query, gotS, testCase.wantResult)
			}
		})
	}
}

func TestCSVQueries3(t *testing.T) {
	input := `na.me,qty,CAST
apple,1,true
mango,3,false
`
	testTable := []struct {
		name       string
		query      string
		requestXML []byte // override request XML
		wantResult string
	}{
		{
			name:  "Select a column containing dot",
			query: `select "na.me" from S3Object s`,
			wantResult: `apple
mango`,
		},
		{
			name:       "Select column containing dot with table name prefix",
			query:      `select count(S3Object."na.me") from S3Object`,
			wantResult: `2`,
		},
		{
			name:  "Select column containing dot with table alias prefix",
			query: `select s."na.me" from S3Object as s`,
			wantResult: `apple
mango`,
		},
		{
			name:  "Select column simplest",
			query: `select qty from S3Object`,
			wantResult: `1
3`,
		},
		{
			name:  "Select column with table name prefix",
			query: `select S3Object.qty from S3Object`,
			wantResult: `1
3`,
		},
		{
			name:  "Select column without table alias",
			query: `select qty from S3Object s`,
			wantResult: `1
3`,
		},
		{
			name:  "Select column with table alias",
			query: `select s.qty from S3Object s`,
			wantResult: `1
3`,
		},
		{
			name:  "Select reserved word column",
			query: `select "CAST"  from s3object`,
			wantResult: `true
false`,
		},
		{
			name:  "Select reserved word column with table alias",
			query: `select S3Object."CAST" from s3object`,
			wantResult: `true
false`,
		},
		{
			name:  "Select reserved word column with unused table alias",
			query: `select "CAST"  from s3object s`,
			wantResult: `true
false`,
		},
		{
			name:  "Select reserved word column with table alias",
			query: `select s."CAST"  from s3object s`,
			wantResult: `true
false`,
		},
		{
			name:  "Select reserved word column with table alias",
			query: `select NOT CAST(s."CAST" AS Bool)  from s3object s`,
			wantResult: `false
true`,
		},
	}

	defRequest := `<?xml version="1.0" encoding="UTF-8"?>
<SelectObjectContentRequest>
    <Expression>%s</Expression>
    <ExpressionType>SQL</ExpressionType>
    <InputSerialization>
        <CompressionType>NONE</CompressionType>
        <CSV>
            <FileHeaderInfo>USE</FileHeaderInfo>
	    <QuoteCharacter>"</QuoteCharacter>
        </CSV>
    </InputSerialization>
    <OutputSerialization>
        <CSV/>
    </OutputSerialization>
    <RequestProgress>
        <Enabled>FALSE</Enabled>
    </RequestProgress>
</SelectObjectContentRequest>`

	for _, testCase := range testTable {
		t.Run(testCase.name, func(t *testing.T) {
			testReq := testCase.requestXML
			if len(testReq) == 0 {
				testReq = fmt.Appendf(nil, defRequest, testCase.query)
			}
			s3Select, err := NewS3Select(bytes.NewReader(testReq))
			if err != nil {
				t.Fatal(err)
			}

			if err = s3Select.Open(newStringRSC(input)); err != nil {
				t.Fatal(err)
			}

			w := &testResponseWriter{}
			s3Select.Evaluate(w)
			s3Select.Close()
			resp := http.Response{
				StatusCode:    http.StatusOK,
				Body:          io.NopCloser(bytes.NewReader(w.response)),
				ContentLength: int64(len(w.response)),
			}
			res, err := minio.NewSelectResults(&resp, "testbucket")
			if err != nil {
				t.Error(err)
				return
			}
			got, err := io.ReadAll(res)
			if err != nil {
				t.Error(err)
				return
			}
			gotS := strings.TrimSpace(string(got))
			if gotS != testCase.wantResult {
				t.Errorf("received response does not match with expected reply.\nQuery: %s\n=====\ngot: %s\n=====\nwant: %s\n=====\n", testCase.query, gotS, testCase.wantResult)
			}
		})
	}
}

func TestCSVInput(t *testing.T) {
	testTable := []struct {
		requestXML     []byte
		expectedResult []byte
	}{
		{
			[]byte(`
<?xml version="1.0" encoding="UTF-8"?>
<SelectObjectContentRequest>
    <Expression>SELECT one, two, three from S3Object</Expression>
    <ExpressionType>SQL</ExpressionType>
    <InputSerialization>
        <CompressionType>NONE</CompressionType>
        <CSV>
            <FileHeaderInfo>USE</FileHeaderInfo>
        </CSV>
    </InputSerialization>
    <OutputSerialization>
        <CSV>
        </CSV>
    </OutputSerialization>
    <RequestProgress>
        <Enabled>FALSE</Enabled>
    </RequestProgress>
</SelectObjectContentRequest>
`), []byte{
				0, 0, 0, 137, 0, 0, 0, 85, 194, 213, 168, 241, 13, 58, 109, 101, 115, 115, 97, 103, 101, 45, 116, 121, 112, 101, 7, 0, 5, 101, 118, 101, 110, 116, 13, 58, 99, 111, 110, 116, 101, 110, 116, 45, 116, 121, 112, 101, 7, 0, 24, 97, 112, 112, 108, 105, 99, 97, 116, 105, 111, 110, 47, 111, 99, 116, 101, 116, 45, 115, 116, 114, 101, 97, 109, 11, 58, 101, 118, 101, 110, 116, 45, 116, 121, 112, 101, 7, 0, 7, 82, 101, 99, 111, 114, 100, 115, 45, 49, 44, 102, 111, 111, 44, 116, 114, 117, 101, 10, 44, 98, 97, 114, 44, 102, 97, 108, 115, 101, 10, 50, 46, 53, 44, 98, 97, 122, 44, 116, 114, 117, 101, 10, 75, 182, 193, 80, 0, 0, 0, 235, 0, 0, 0, 67, 213, 243, 57, 141, 13, 58, 109, 101, 115, 115, 97, 103, 101, 45, 116, 121, 112, 101, 7, 0, 5, 101, 118, 101, 110, 116, 13, 58, 99, 111, 110, 116, 101, 110, 116, 45, 116, 121, 112, 101, 7, 0, 8, 116, 101, 120, 116, 47, 120, 109, 108, 11, 58, 101, 118, 101, 110, 116, 45, 116, 121, 112, 101, 7, 0, 5, 83, 116, 97, 116, 115, 60, 63, 120, 109, 108, 32, 118, 101, 114, 115, 105, 111, 110, 61, 34, 49, 46, 48, 34, 32, 101, 110, 99, 111, 100, 105, 110, 103, 61, 34, 85, 84, 70, 45, 56, 34, 63, 62, 60, 83, 116, 97, 116, 115, 62, 60, 66, 121, 116, 101, 115, 83, 99, 97, 110, 110, 101, 100, 62, 53, 48, 60, 47, 66, 121, 116, 101, 115, 83, 99, 97, 110, 110, 101, 100, 62, 60, 66, 121, 116, 101, 115, 80, 114, 111, 99, 101, 115, 115, 101, 100, 62, 53, 48, 60, 47, 66, 121, 116, 101, 115, 80, 114, 111, 99, 101, 115, 115, 101, 100, 62, 60, 66, 121, 116, 101, 115, 82, 101, 116, 117, 114, 110, 101, 100, 62, 51, 54, 60, 47, 66, 121, 116, 101, 115, 82, 101, 116, 117, 114, 110, 101, 100, 62, 60, 47, 83, 116, 97, 116, 115, 62, 253, 105, 8, 216, 0, 0, 0, 56, 0, 0, 0, 40, 193, 198, 132, 212, 13, 58, 109, 101, 115, 115, 97, 103, 101, 45, 116, 121, 112, 101, 7, 0, 5, 101, 118, 101, 110, 116, 11, 58, 101, 118, 101, 110, 116, 45, 116, 121, 112, 101, 7, 0, 3, 69, 110, 100, 207, 151, 211, 146,
			},
		},
		{
			[]byte(`
<?xml version="1.0" encoding="UTF-8"?>
<SelectObjectContentRequest>
    <Expression>SELECT COUNT(*) AS total_record_count from S3Object</Expression>
    <ExpressionType>SQL</ExpressionType>
    <InputSerialization>
        <CompressionType>NONE</CompressionType>
        <CSV>
            <FileHeaderInfo>USE</FileHeaderInfo>
        </CSV>
    </InputSerialization>
    <OutputSerialization>
        <JSON>
        </JSON>
    </OutputSerialization>
    <RequestProgress>
        <Enabled>FALSE</Enabled>
    </RequestProgress>
</SelectObjectContentRequest>
`), []byte{
				0, 0, 0, 126, 0, 0, 0, 85, 56, 193, 36, 188, 13, 58, 109, 101, 115, 115, 97, 103, 101, 45, 116, 121, 112, 101, 7, 0, 5, 101, 118, 101, 110, 116, 13, 58, 99, 111, 110, 116, 101, 110, 116, 45, 116, 121, 112, 101, 7, 0, 24, 97, 112, 112, 108, 105, 99, 97, 116, 105, 111, 110, 47, 111, 99, 116, 101, 116, 45, 115, 116, 114, 101, 97, 109, 11, 58, 101, 118, 101, 110, 116, 45, 116, 121, 112, 101, 7, 0, 7, 82, 101, 99, 111, 114, 100, 115, 123, 34, 116, 111, 116, 97, 108, 95, 114, 101, 99, 111, 114, 100, 95, 99, 111, 117, 110, 116, 34, 58, 51, 125, 10, 196, 183, 134, 242, 0, 0, 0, 235, 0, 0, 0, 67, 213, 243, 57, 141, 13, 58, 109, 101, 115, 115, 97, 103, 101, 45, 116, 121, 112, 101, 7, 0, 5, 101, 118, 101, 110, 116, 13, 58, 99, 111, 110, 116, 101, 110, 116, 45, 116, 121, 112, 101, 7, 0, 8, 116, 101, 120, 116, 47, 120, 109, 108, 11, 58, 101, 118, 101, 110, 116, 45, 116, 121, 112, 101, 7, 0, 5, 83, 116, 97, 116, 115, 60, 63, 120, 109, 108, 32, 118, 101, 114, 115, 105, 111, 110, 61, 34, 49, 46, 48, 34, 32, 101, 110, 99, 111, 100, 105, 110, 103, 61, 34, 85, 84, 70, 45, 56, 34, 63, 62, 60, 83, 116, 97, 116, 115, 62, 60, 66, 121, 116, 101, 115, 83, 99, 97, 110, 110, 101, 100, 62, 53, 48, 60, 47, 66, 121, 116, 101, 115, 83, 99, 97, 110, 110, 101, 100, 62, 60, 66, 121, 116, 101, 115, 80, 114, 111, 99, 101, 115, 115, 101, 100, 62, 53, 48, 60, 47, 66, 121, 116, 101, 115, 80, 114, 111, 99, 101, 115, 115, 101, 100, 62, 60, 66, 121, 116, 101, 115, 82, 101, 116, 117, 114, 110, 101, 100, 62, 50, 53, 60, 47, 66, 121, 116, 101, 115, 82, 101, 116, 117, 114, 110, 101, 100, 62, 60, 47, 83, 116, 97, 116, 115, 62, 47, 153, 24, 28, 0, 0, 0, 56, 0, 0, 0, 40, 193, 198, 132, 212, 13, 58, 109, 101, 115, 115, 97, 103, 101, 45, 116, 121, 112, 101, 7, 0, 5, 101, 118, 101, 110, 116, 11, 58, 101, 118, 101, 110, 116, 45, 116, 121, 112, 101, 7, 0, 3, 69, 110, 100, 207, 151, 211, 146,
			},
		},
		{
			[]byte(`
<?xml version="1.0" encoding="UTF-8"?>
<SelectObjectContentRequest>
    <Expression>SELECT * from S3Object</Expression>
    <ExpressionType>SQL</ExpressionType>
    <InputSerialization>
        <CompressionType>NONE</CompressionType>
        <CSV>
            <FileHeaderInfo>USE</FileHeaderInfo>
        </CSV>
    </InputSerialization>
    <OutputSerialization>
        <JSON>
        </JSON>
    </OutputSerialization>
    <RequestProgress>
        <Enabled>FALSE</Enabled>
    </RequestProgress>
</SelectObjectContentRequest>
`), []byte{0x0, 0x0, 0x0, 0xdd, 0x0, 0x0, 0x0, 0x55, 0xf, 0x46, 0xc1, 0xfa, 0xd, 0x3a, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x2d, 0x74, 0x79, 0x70, 0x65, 0x7, 0x0, 0x5, 0x65, 0x76, 0x65, 0x6e, 0x74, 0xd, 0x3a, 0x63, 0x6f, 0x6e, 0x74, 0x65, 0x6e, 0x74, 0x2d, 0x74, 0x79, 0x70, 0x65, 0x7, 0x0, 0x18, 0x61, 0x70, 0x70, 0x6c, 0x69, 0x63, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x2f, 0x6f, 0x63, 0x74, 0x65, 0x74, 0x2d, 0x73, 0x74, 0x72, 0x65, 0x61, 0x6d, 0xb, 0x3a, 0x65, 0x76, 0x65, 0x6e, 0x74, 0x2d, 0x74, 0x79, 0x70, 0x65, 0x7, 0x0, 0x7, 0x52, 0x65, 0x63, 0x6f, 0x72, 0x64, 0x73, 0x7b, 0x22, 0x6f, 0x6e, 0x65, 0x22, 0x3a, 0x22, 0x2d, 0x31, 0x22, 0x2c, 0x22, 0x74, 0x77, 0x6f, 0x22, 0x3a, 0x22, 0x66, 0x6f, 0x6f, 0x22, 0x2c, 0x22, 0x74, 0x68, 0x72, 0x65, 0x65, 0x22, 0x3a, 0x22, 0x74, 0x72, 0x75, 0x65, 0x22, 0x7d, 0xa, 0x7b, 0x22, 0x6f, 0x6e, 0x65, 0x22, 0x3a, 0x22, 0x22, 0x2c, 0x22, 0x74, 0x77, 0x6f, 0x22, 0x3a, 0x22, 0x62, 0x61, 0x72, 0x22, 0x2c, 0x22, 0x74, 0x68, 0x72, 0x65, 0x65, 0x22, 0x3a, 0x22, 0x66, 0x61, 0x6c, 0x73, 0x65, 0x22, 0x7d, 0xa, 0x7b, 0x22, 0x6f, 0x6e, 0x65, 0x22, 0x3a, 0x22, 0x32, 0x2e, 0x35, 0x22, 0x2c, 0x22, 0x74, 0x77, 0x6f, 0x22, 0x3a, 0x22, 0x62, 0x61, 0x7a, 0x22, 0x2c, 0x22, 0x74, 0x68, 0x72, 0x65, 0x65, 0x22, 0x3a, 0x22, 0x74, 0x72, 0x75, 0x65, 0x22, 0x7d, 0xa, 0x7e, 0xb5, 0x99, 0xfb, 0x0, 0x0, 0x0, 0xec, 0x0, 0x0, 0x0, 0x43, 0x67, 0xd3, 0xe5, 0x9d, 0xd, 0x3a, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x2d, 0x74, 0x79, 0x70, 0x65, 0x7, 0x0, 0x5, 0x65, 0x76, 0x65, 0x6e, 0x74, 0xd, 0x3a, 0x63, 0x6f, 0x6e, 0x74, 0x65, 0x6e, 0x74, 0x2d, 0x74, 0x79, 0x70, 0x65, 0x7, 0x0, 0x8, 0x74, 0x65, 0x78, 0x74, 0x2f, 0x78, 0x6d, 0x6c, 0xb, 0x3a, 0x65, 0x76, 0x65, 0x6e, 0x74, 0x2d, 0x74, 0x79, 0x70, 0x65, 0x7, 0x0, 0x5, 0x53, 0x74, 0x61, 0x74, 0x73, 0x3c, 0x3f, 0x78, 0x6d, 0x6c, 0x20, 0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x3d, 0x22, 0x31, 0x2e, 0x30, 0x22, 0x20, 0x65, 0x6e, 0x63, 0x6f, 0x64, 0x69, 0x6e, 0x67, 0x3d, 0x22, 0x55, 0x54, 0x46, 0x2d, 0x38, 0x22, 0x3f, 0x3e, 0x3c, 0x53, 0x74, 0x61, 0x74, 0x73, 0x3e, 0x3c, 0x42, 0x79, 0x74, 0x65, 0x73, 0x53, 0x63, 0x61, 0x6e, 0x6e, 0x65, 0x64, 0x3e, 0x35, 0x30, 0x3c, 0x2f, 0x42, 0x79, 0x74, 0x65, 0x73, 0x53, 0x63, 0x61, 0x6e, 0x6e, 0x65, 0x64, 0x3e, 0x3c, 0x42, 0x79, 0x74, 0x65, 0x73, 0x50, 0x72, 0x6f, 0x63, 0x65, 0x73, 0x73, 0x65, 0x64, 0x3e, 0x35, 0x30, 0x3c, 0x2f, 0x42, 0x79, 0x74, 0x65, 0x73, 0x50, 0x72, 0x6f, 0x63, 0x65, 0x73, 0x73, 0x65, 0x64, 0x3e, 0x3c, 0x42, 0x79, 0x74, 0x65, 0x73, 0x52, 0x65, 0x74, 0x75, 0x72, 0x6e, 0x65, 0x64, 0x3e, 0x31, 0x32, 0x30, 0x3c, 0x2f, 0x42, 0x79, 0x74, 0x65, 0x73, 0x52, 0x65, 0x74, 0x75, 0x72, 0x6e, 0x65, 0x64, 0x3e, 0x3c, 0x2f, 0x53, 0x74, 0x61, 0x74, 0x73, 0x3e, 0x5a, 0xe5, 0xd, 0x84, 0x0, 0x0, 0x0, 0x38, 0x0, 0x0, 0x0, 0x28, 0xc1, 0xc6, 0x84, 0xd4, 0xd, 0x3a, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x2d, 0x74, 0x79, 0x70, 0x65, 0x7, 0x0, 0x5, 0x65, 0x76, 0x65, 0x6e, 0x74, 0xb, 0x3a, 0x65, 0x76, 0x65, 0x6e, 0x74, 0x2d, 0x74, 0x79, 0x70, 0x65, 0x7, 0x0, 0x3, 0x45, 0x6e, 0x64, 0xcf, 0x97, 0xd3, 0x92},
		},
		{
			[]byte(`
<?xml version="1.0" encoding="UTF-8"?>
<SelectObjectContentRequest>
    <Expression>SELECT one from S3Object limit 1</Expression>
    <ExpressionType>SQL</ExpressionType>
    <InputSerialization>
        <CompressionType>NONE</CompressionType>
        <CSV>
            <FileHeaderInfo>USE</FileHeaderInfo>
        </CSV>
    </InputSerialization>
    <OutputSerialization>
        <CSV>
        </CSV>
    </OutputSerialization>
    <RequestProgress>
        <Enabled>FALSE</Enabled>
    </RequestProgress>
</SelectObjectContentRequest>
`), []byte{
				0x0, 0x0, 0x0, 0x68, 0x0, 0x0, 0x0, 0x55, 0xd7, 0x61, 0x46, 0x9e, 0xd, 0x3a, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x2d, 0x74, 0x79, 0x70, 0x65, 0x7, 0x0, 0x5, 0x65, 0x76, 0x65, 0x6e, 0x74, 0xd, 0x3a, 0x63, 0x6f, 0x6e, 0x74, 0x65, 0x6e, 0x74, 0x2d, 0x74, 0x79, 0x70, 0x65, 0x7, 0x0, 0x18, 0x61, 0x70, 0x70, 0x6c, 0x69, 0x63, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x2f, 0x6f, 0x63, 0x74, 0x65, 0x74, 0x2d, 0x73, 0x74, 0x72, 0x65, 0x61, 0x6d, 0xb, 0x3a, 0x65, 0x76, 0x65, 0x6e, 0x74, 0x2d, 0x74, 0x79, 0x70, 0x65, 0x7, 0x0, 0x7, 0x52, 0x65, 0x63, 0x6f, 0x72, 0x64, 0x73, 0x2d, 0x31, 0xa, 0x17, 0xfb, 0x1, 0x90, 0x0, 0x0, 0x0, 0xea, 0x0, 0x0, 0x0, 0x43, 0xe8, 0x93, 0x10, 0x3d, 0xd, 0x3a, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x2d, 0x74, 0x79, 0x70, 0x65, 0x7, 0x0, 0x5, 0x65, 0x76, 0x65, 0x6e, 0x74, 0xd, 0x3a, 0x63, 0x6f, 0x6e, 0x74, 0x65, 0x6e, 0x74, 0x2d, 0x74, 0x79, 0x70, 0x65, 0x7, 0x0, 0x8, 0x74, 0x65, 0x78, 0x74, 0x2f, 0x78, 0x6d, 0x6c, 0xb, 0x3a, 0x65, 0x76, 0x65, 0x6e, 0x74, 0x2d, 0x74, 0x79, 0x70, 0x65, 0x7, 0x0, 0x5, 0x53, 0x74, 0x61, 0x74, 0x73, 0x3c, 0x3f, 0x78, 0x6d, 0x6c, 0x20, 0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x3d, 0x22, 0x31, 0x2e, 0x30, 0x22, 0x20, 0x65, 0x6e, 0x63, 0x6f, 0x64, 0x69, 0x6e, 0x67, 0x3d, 0x22, 0x55, 0x54, 0x46, 0x2d, 0x38, 0x22, 0x3f, 0x3e, 0x3c, 0x53, 0x74, 0x61, 0x74, 0x73, 0x3e, 0x3c, 0x42, 0x79, 0x74, 0x65, 0x73, 0x53, 0x63, 0x61, 0x6e, 0x6e, 0x65, 0x64, 0x3e, 0x35, 0x30, 0x3c, 0x2f, 0x42, 0x79, 0x74, 0x65, 0x73, 0x53, 0x63, 0x61, 0x6e, 0x6e, 0x65, 0x64, 0x3e, 0x3c, 0x42, 0x79, 0x74, 0x65, 0x73, 0x50, 0x72, 0x6f, 0x63, 0x65, 0x73, 0x73, 0x65, 0x64, 0x3e, 0x35, 0x30, 0x3c, 0x2f, 0x42, 0x79, 0x74, 0x65, 0x73, 0x50, 0x72, 0x6f, 0x63, 0x65, 0x73, 0x73, 0x65, 0x64, 0x3e, 0x3c, 0x42, 0x79, 0x74, 0x65, 0x73, 0x52, 0x65, 0x74, 0x75, 0x72, 0x6e, 0x65, 0x64, 0x3e, 0x33, 0x3c, 0x2f, 0x42, 0x79, 0x74, 0x65, 0x73, 0x52, 0x65, 0x74, 0x75, 0x72, 0x6e, 0x65, 0x64, 0x3e, 0x3c, 0x2f, 0x53, 0x74, 0x61, 0x74, 0x73, 0x3e, 0x15, 0x72, 0x19, 0x94, 0x0, 0x0, 0x0, 0x38, 0x0, 0x0, 0x0, 0x28, 0xc1, 0xc6, 0x84, 0xd4, 0xd, 0x3a, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x2d, 0x74, 0x79, 0x70, 0x65, 0x7, 0x0, 0x5, 0x65, 0x76, 0x65, 0x6e, 0x74, 0xb, 0x3a, 0x65, 0x76, 0x65, 0x6e, 0x74, 0x2d, 0x74, 0x79, 0x70, 0x65, 0x7, 0x0, 0x3, 0x45, 0x6e, 0x64, 0xcf, 0x97, 0xd3, 0x92,
			},
		},
	}

	csvData := []byte(`one,two,three
-1,foo,true
,bar,false
2.5,baz,true
`)

	for i, testCase := range testTable {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			s3Select, err := NewS3Select(bytes.NewReader(testCase.requestXML))
			if err != nil {
				t.Fatal(err)
			}

			if err = s3Select.Open(newBytesRSC(csvData)); err != nil {
				t.Fatal(err)
			}

			w := &testResponseWriter{}
			s3Select.Evaluate(w)
			s3Select.Close()

			if !reflect.DeepEqual(w.response, testCase.expectedResult) {
				resp := http.Response{
					StatusCode:    http.StatusOK,
					Body:          io.NopCloser(bytes.NewReader(w.response)),
					ContentLength: int64(len(w.response)),
				}
				res, err := minio.NewSelectResults(&resp, "testbucket")
				if err != nil {
					t.Error(err)
					return
				}
				got, err := io.ReadAll(res)
				if err != nil {
					t.Error(err)
					return
				}

				t.Errorf("received response does not match with expected reply\ngot: %#v\nwant:%#v\ndecoded:%s", w.response, testCase.expectedResult, string(got))
			}
		})
	}
}

func TestJSONInput(t *testing.T) {
	testTable := []struct {
		requestXML     []byte
		expectedResult []byte
	}{
		{
			[]byte(`
<?xml version="1.0" encoding="UTF-8"?>
<SelectObjectContentRequest>
    <Expression>SELECT one, two, three from S3Object</Expression>
    <ExpressionType>SQL</ExpressionType>
    <InputSerialization>
        <CompressionType>NONE</CompressionType>
        <JSON>
            <Type>DOCUMENT</Type>
        </JSON>
    </InputSerialization>
    <OutputSerialization>
        <CSV>
        </CSV>
    </OutputSerialization>
    <RequestProgress>
        <Enabled>FALSE</Enabled>
    </RequestProgress>
</SelectObjectContentRequest>
`), []byte{
				0, 0, 0, 137, 0, 0, 0, 85, 194, 213, 168, 241, 13, 58, 109, 101, 115, 115, 97, 103, 101, 45, 116, 121, 112, 101, 7, 0, 5, 101, 118, 101, 110, 116, 13, 58, 99, 111, 110, 116, 101, 110, 116, 45, 116, 121, 112, 101, 7, 0, 24, 97, 112, 112, 108, 105, 99, 97, 116, 105, 111, 110, 47, 111, 99, 116, 101, 116, 45, 115, 116, 114, 101, 97, 109, 11, 58, 101, 118, 101, 110, 116, 45, 116, 121, 112, 101, 7, 0, 7, 82, 101, 99, 111, 114, 100, 115, 45, 49, 44, 102, 111, 111, 44, 116, 114, 117, 101, 10, 44, 98, 97, 114, 44, 102, 97, 108, 115, 101, 10, 50, 46, 53, 44, 98, 97, 122, 44, 116, 114, 117, 101, 10, 75, 182, 193, 80, 0, 0, 0, 237, 0, 0, 0, 67, 90, 179, 204, 45, 13, 58, 109, 101, 115, 115, 97, 103, 101, 45, 116, 121, 112, 101, 7, 0, 5, 101, 118, 101, 110, 116, 13, 58, 99, 111, 110, 116, 101, 110, 116, 45, 116, 121, 112, 101, 7, 0, 8, 116, 101, 120, 116, 47, 120, 109, 108, 11, 58, 101, 118, 101, 110, 116, 45, 116, 121, 112, 101, 7, 0, 5, 83, 116, 97, 116, 115, 60, 63, 120, 109, 108, 32, 118, 101, 114, 115, 105, 111, 110, 61, 34, 49, 46, 48, 34, 32, 101, 110, 99, 111, 100, 105, 110, 103, 61, 34, 85, 84, 70, 45, 56, 34, 63, 62, 60, 83, 116, 97, 116, 115, 62, 60, 66, 121, 116, 101, 115, 83, 99, 97, 110, 110, 101, 100, 62, 49, 49, 50, 60, 47, 66, 121, 116, 101, 115, 83, 99, 97, 110, 110, 101, 100, 62, 60, 66, 121, 116, 101, 115, 80, 114, 111, 99, 101, 115, 115, 101, 100, 62, 49, 49, 50, 60, 47, 66, 121, 116, 101, 115, 80, 114, 111, 99, 101, 115, 115, 101, 100, 62, 60, 66, 121, 116, 101, 115, 82, 101, 116, 117, 114, 110, 101, 100, 62, 51, 54, 60, 47, 66, 121, 116, 101, 115, 82, 101, 116, 117, 114, 110, 101, 100, 62, 60, 47, 83, 116, 97, 116, 115, 62, 181, 40, 50, 250, 0, 0, 0, 56, 0, 0, 0, 40, 193, 198, 132, 212, 13, 58, 109, 101, 115, 115, 97, 103, 101, 45, 116, 121, 112, 101, 7, 0, 5, 101, 118, 101, 110, 116, 11, 58, 101, 118, 101, 110, 116, 45, 116, 121, 112, 101, 7, 0, 3, 69, 110, 100, 207, 151, 211, 146,
			},
		},
		{
			[]byte(`
<?xml version="1.0" encoding="UTF-8"?>
<SelectObjectContentRequest>
    <Expression>SELECT COUNT(*) AS total_record_count from S3Object</Expression>
    <ExpressionType>SQL</ExpressionType>
    <InputSerialization>
        <CompressionType>NONE</CompressionType>
        <JSON>
            <Type>DOCUMENT</Type>
        </JSON>
    </InputSerialization>
    <OutputSerialization>
        <CSV>
        </CSV>
    </OutputSerialization>
    <RequestProgress>
        <Enabled>FALSE</Enabled>
    </RequestProgress>
</SelectObjectContentRequest>
`), []byte{
				0, 0, 0, 103, 0, 0, 0, 85, 85, 49, 209, 79, 13, 58, 109, 101, 115, 115, 97, 103, 101, 45, 116, 121, 112, 101, 7, 0, 5, 101, 118, 101, 110, 116, 13, 58, 99, 111, 110, 116, 101, 110, 116, 45, 116, 121, 112, 101, 7, 0, 24, 97, 112, 112, 108, 105, 99, 97, 116, 105, 111, 110, 47, 111, 99, 116, 101, 116, 45, 115, 116, 114, 101, 97, 109, 11, 58, 101, 118, 101, 110, 116, 45, 116, 121, 112, 101, 7, 0, 7, 82, 101, 99, 111, 114, 100, 115, 51, 10, 175, 58, 213, 152, 0, 0, 0, 236, 0, 0, 0, 67, 103, 211, 229, 157, 13, 58, 109, 101, 115, 115, 97, 103, 101, 45, 116, 121, 112, 101, 7, 0, 5, 101, 118, 101, 110, 116, 13, 58, 99, 111, 110, 116, 101, 110, 116, 45, 116, 121, 112, 101, 7, 0, 8, 116, 101, 120, 116, 47, 120, 109, 108, 11, 58, 101, 118, 101, 110, 116, 45, 116, 121, 112, 101, 7, 0, 5, 83, 116, 97, 116, 115, 60, 63, 120, 109, 108, 32, 118, 101, 114, 115, 105, 111, 110, 61, 34, 49, 46, 48, 34, 32, 101, 110, 99, 111, 100, 105, 110, 103, 61, 34, 85, 84, 70, 45, 56, 34, 63, 62, 60, 83, 116, 97, 116, 115, 62, 60, 66, 121, 116, 101, 115, 83, 99, 97, 110, 110, 101, 100, 62, 49, 49, 50, 60, 47, 66, 121, 116, 101, 115, 83, 99, 97, 110, 110, 101, 100, 62, 60, 66, 121, 116, 101, 115, 80, 114, 111, 99, 101, 115, 115, 101, 100, 62, 49, 49, 50, 60, 47, 66, 121, 116, 101, 115, 80, 114, 111, 99, 101, 115, 115, 101, 100, 62, 60, 66, 121, 116, 101, 115, 82, 101, 116, 117, 114, 110, 101, 100, 62, 50, 60, 47, 66, 121, 116, 101, 115, 82, 101, 116, 117, 114, 110, 101, 100, 62, 60, 47, 83, 116, 97, 116, 115, 62, 52, 192, 77, 114, 0, 0, 0, 56, 0, 0, 0, 40, 193, 198, 132, 212, 13, 58, 109, 101, 115, 115, 97, 103, 101, 45, 116, 121, 112, 101, 7, 0, 5, 101, 118, 101, 110, 116, 11, 58, 101, 118, 101, 110, 116, 45, 116, 121, 112, 101, 7, 0, 3, 69, 110, 100, 207, 151, 211, 146,
			},
		},
		{
			[]byte(`
<?xml version="1.0" encoding="UTF-8"?>
<SelectObjectContentRequest>
    <Expression>SELECT * from S3Object</Expression>
    <ExpressionType>SQL</ExpressionType>
    <InputSerialization>
        <CompressionType>NONE</CompressionType>
        <JSON>
            <Type>DOCUMENT</Type>
        </JSON>
    </InputSerialization>
    <OutputSerialization>
        <CSV>
        </CSV>
    </OutputSerialization>
    <RequestProgress>
        <Enabled>FALSE</Enabled>
    </RequestProgress>
</SelectObjectContentRequest>
`), []byte{0x0, 0x0, 0x0, 0x89, 0x0, 0x0, 0x0, 0x55, 0xc2, 0xd5, 0xa8, 0xf1, 0xd, 0x3a, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x2d, 0x74, 0x79, 0x70, 0x65, 0x7, 0x0, 0x5, 0x65, 0x76, 0x65, 0x6e, 0x74, 0xd, 0x3a, 0x63, 0x6f, 0x6e, 0x74, 0x65, 0x6e, 0x74, 0x2d, 0x74, 0x79, 0x70, 0x65, 0x7, 0x0, 0x18, 0x61, 0x70, 0x70, 0x6c, 0x69, 0x63, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x2f, 0x6f, 0x63, 0x74, 0x65, 0x74, 0x2d, 0x73, 0x74, 0x72, 0x65, 0x61, 0x6d, 0xb, 0x3a, 0x65, 0x76, 0x65, 0x6e, 0x74, 0x2d, 0x74, 0x79, 0x70, 0x65, 0x7, 0x0, 0x7, 0x52, 0x65, 0x63, 0x6f, 0x72, 0x64, 0x73, 0x74, 0x72, 0x75, 0x65, 0x2c, 0x66, 0x6f, 0x6f, 0x2c, 0x2d, 0x31, 0xa, 0x66, 0x61, 0x6c, 0x73, 0x65, 0x2c, 0x62, 0x61, 0x72, 0x2c, 0xa, 0x74, 0x72, 0x75, 0x65, 0x2c, 0x62, 0x61, 0x7a, 0x2c, 0x32, 0x2e, 0x35, 0xa, 0xef, 0x22, 0x13, 0xa3, 0x0, 0x0, 0x0, 0xed, 0x0, 0x0, 0x0, 0x43, 0x5a, 0xb3, 0xcc, 0x2d, 0xd, 0x3a, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x2d, 0x74, 0x79, 0x70, 0x65, 0x7, 0x0, 0x5, 0x65, 0x76, 0x65, 0x6e, 0x74, 0xd, 0x3a, 0x63, 0x6f, 0x6e, 0x74, 0x65, 0x6e, 0x74, 0x2d, 0x74, 0x79, 0x70, 0x65, 0x7, 0x0, 0x8, 0x74, 0x65, 0x78, 0x74, 0x2f, 0x78, 0x6d, 0x6c, 0xb, 0x3a, 0x65, 0x76, 0x65, 0x6e, 0x74, 0x2d, 0x74, 0x79, 0x70, 0x65, 0x7, 0x0, 0x5, 0x53, 0x74, 0x61, 0x74, 0x73, 0x3c, 0x3f, 0x78, 0x6d, 0x6c, 0x20, 0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x3d, 0x22, 0x31, 0x2e, 0x30, 0x22, 0x20, 0x65, 0x6e, 0x63, 0x6f, 0x64, 0x69, 0x6e, 0x67, 0x3d, 0x22, 0x55, 0x54, 0x46, 0x2d, 0x38, 0x22, 0x3f, 0x3e, 0x3c, 0x53, 0x74, 0x61, 0x74, 0x73, 0x3e, 0x3c, 0x42, 0x79, 0x74, 0x65, 0x73, 0x53, 0x63, 0x61, 0x6e, 0x6e, 0x65, 0x64, 0x3e, 0x31, 0x31, 0x32, 0x3c, 0x2f, 0x42, 0x79, 0x74, 0x65, 0x73, 0x53, 0x63, 0x61, 0x6e, 0x6e, 0x65, 0x64, 0x3e, 0x3c, 0x42, 0x79, 0x74, 0x65, 0x73, 0x50, 0x72, 0x6f, 0x63, 0x65, 0x73, 0x73, 0x65, 0x64, 0x3e, 0x31, 0x31, 0x32, 0x3c, 0x2f, 0x42, 0x79, 0x74, 0x65, 0x73, 0x50, 0x72, 0x6f, 0x63, 0x65, 0x73, 0x73, 0x65, 0x64, 0x3e, 0x3c, 0x42, 0x79, 0x74, 0x65, 0x73, 0x52, 0x65, 0x74, 0x75, 0x72, 0x6e, 0x65, 0x64, 0x3e, 0x33, 0x36, 0x3c, 0x2f, 0x42, 0x79, 0x74, 0x65, 0x73, 0x52, 0x65, 0x74, 0x75, 0x72, 0x6e, 0x65, 0x64, 0x3e, 0x3c, 0x2f, 0x53, 0x74, 0x61, 0x74, 0x73, 0x3e, 0xb5, 0x28, 0x32, 0xfa, 0x0, 0x0, 0x0, 0x38, 0x0, 0x0, 0x0, 0x28, 0xc1, 0xc6, 0x84, 0xd4, 0xd, 0x3a, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x2d, 0x74, 0x79, 0x70, 0x65, 0x7, 0x0, 0x5, 0x65, 0x76, 0x65, 0x6e, 0x74, 0xb, 0x3a, 0x65, 0x76, 0x65, 0x6e, 0x74, 0x2d, 0x74, 0x79, 0x70, 0x65, 0x7, 0x0, 0x3, 0x45, 0x6e, 0x64, 0xcf, 0x97, 0xd3, 0x92},
		},
	}

	jsonData := []byte(`{"three":true,"two":"foo","one":-1}
{"three":false,"two":"bar","one":null}
{"three":true,"two":"baz","one":2.5}
`)

	for i, testCase := range testTable {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			s3Select, err := NewS3Select(bytes.NewReader(testCase.requestXML))
			if err != nil {
				t.Fatal(err)
			}

			if err = s3Select.Open(newBytesRSC(jsonData)); err != nil {
				t.Fatal(err)
			}

			w := &testResponseWriter{}
			s3Select.Evaluate(w)
			s3Select.Close()

			if !reflect.DeepEqual(w.response, testCase.expectedResult) {
				resp := http.Response{
					StatusCode:    http.StatusOK,
					Body:          io.NopCloser(bytes.NewReader(w.response)),
					ContentLength: int64(len(w.response)),
				}
				res, err := minio.NewSelectResults(&resp, "testbucket")
				if err != nil {
					t.Error(err)
					return
				}
				got, err := io.ReadAll(res)
				if err != nil {
					t.Error(err)
					return
				}

				t.Errorf("received response does not match with expected reply\ngot: %#v\nwant:%#v\ndecoded:%s", w.response, testCase.expectedResult, string(got))
			}
		})
	}
}

func TestCSVRanges(t *testing.T) {
	testInput := []byte(`id,time,num,num2,text
1,2010-01-01T,7867786,4565.908123,"a text, with comma"
2,2017-01-02T03:04Z,-5, 0.765111,
`)
	testTable := []struct {
		name       string
		query      string
		input      []byte
		requestXML []byte // override request XML
		wantResult string
		wantErr    bool
	}{
		{
			name:  "select-all",
			input: testInput,
			query: ``,
			// Since we are doing offset, no headers are used.
			wantResult: `{"_1":"2","_2":"2017-01-02T03:04Z","_3":"-5","_4":" 0.765111","_5":""}`,
			requestXML: []byte(`<?xml version="1.0" encoding="UTF-8"?>
<SelectObjectContentRequest>
    <Expression>SELECT * from s3object AS s</Expression>
    <ExpressionType>SQL</ExpressionType>
    <InputSerialization>
        <CompressionType>NONE</CompressionType>
        <CSV>
        <FileHeaderInfo>NONE</FileHeaderInfo>
	    <QuoteCharacter>"</QuoteCharacter>
        </CSV>
    </InputSerialization>
    <OutputSerialization>
        <JSON>
        </JSON>
    </OutputSerialization>
    <RequestProgress>
        <Enabled>FALSE</Enabled>
    </RequestProgress>
	<ScanRange><Start>76</Start><End>109</End></ScanRange>
</SelectObjectContentRequest>`),
		},
		{
			name:  "select-remain",
			input: testInput,
			// Since we are doing offset, no headers are used.
			wantResult: `{"_1":"2","_2":"2017-01-02T03:04Z","_3":"-5","_4":" 0.765111","_5":""}`,
			requestXML: []byte(`<?xml version="1.0" encoding="UTF-8"?>
<SelectObjectContentRequest>
    <Expression>SELECT * from s3object AS s</Expression>
    <ExpressionType>SQL</ExpressionType>
    <InputSerialization>
        <CompressionType>NONE</CompressionType>
        <CSV>
        <FileHeaderInfo>NONE</FileHeaderInfo>
	    <QuoteCharacter>"</QuoteCharacter>
        </CSV>
    </InputSerialization>
    <OutputSerialization>
        <JSON>
        </JSON>
    </OutputSerialization>
    <RequestProgress>
        <Enabled>FALSE</Enabled>
    </RequestProgress>
	<ScanRange><Start>76</Start></ScanRange>
</SelectObjectContentRequest>`),
		},
		{
			name:  "select-end-bytes",
			input: testInput,
			query: ``,
			// Since we are doing offset, no headers are used.
			wantResult: `{"_1":"2","_2":"2017-01-02T03:04Z","_3":"-5","_4":" 0.765111","_5":""}`,
			requestXML: []byte(`<?xml version="1.0" encoding="UTF-8"?>
<SelectObjectContentRequest>
    <Expression>SELECT * from s3object AS s</Expression>
    <ExpressionType>SQL</ExpressionType>
    <InputSerialization>
        <CompressionType>NONE</CompressionType>
        <CSV>
        <FileHeaderInfo>NONE</FileHeaderInfo>
	    <QuoteCharacter>"</QuoteCharacter>
        </CSV>
    </InputSerialization>
    <OutputSerialization>
        <JSON>
        </JSON>
    </OutputSerialization>
    <RequestProgress>
        <Enabled>FALSE</Enabled>
    </RequestProgress>
	<ScanRange><End>35</End></ScanRange>
</SelectObjectContentRequest>`),
		},
		{
			name:  "select-middle",
			input: testInput,
			// Since we are doing offset, no headers are used.
			wantResult: `{"_1":"a text, with comma"}`,
			requestXML: []byte(`<?xml version="1.0" encoding="UTF-8"?>
<SelectObjectContentRequest>
    <Expression>SELECT * from s3object AS s</Expression>
    <ExpressionType>SQL</ExpressionType>
    <InputSerialization>
        <CompressionType>NONE</CompressionType>
        <CSV>
        <FileHeaderInfo>NONE</FileHeaderInfo>
	    <QuoteCharacter>"</QuoteCharacter>
        </CSV>
    </InputSerialization>
    <OutputSerialization>
        <JSON>
        </JSON>
    </OutputSerialization>
    <RequestProgress>
        <Enabled>FALSE</Enabled>
    </RequestProgress>
	<ScanRange><Start>56</Start><End>76</End></ScanRange>
</SelectObjectContentRequest>`),
		},
		{
			name:  "error-end-before-start",
			input: testInput,
			// Since we are doing offset, no headers are used.
			wantResult: ``,
			wantErr:    true,
			requestXML: []byte(`<?xml version="1.0" encoding="UTF-8"?>
<SelectObjectContentRequest>
    <Expression>SELECT * from s3object AS s</Expression>
    <ExpressionType>SQL</ExpressionType>
    <InputSerialization>
        <CompressionType>NONE</CompressionType>
        <CSV>
        <FileHeaderInfo>NONE</FileHeaderInfo>
	    <QuoteCharacter>"</QuoteCharacter>
        </CSV>
    </InputSerialization>
    <OutputSerialization>
        <JSON>
        </JSON>
    </OutputSerialization>
    <RequestProgress>
        <Enabled>FALSE</Enabled>
    </RequestProgress>
	<ScanRange><Start>56</Start><End>26</End></ScanRange>
</SelectObjectContentRequest>`),
		},
		{
			name:  "error-empty",
			input: testInput,
			// Since we are doing offset, no headers are used.
			wantResult: ``,
			wantErr:    true,
			requestXML: []byte(`<?xml version="1.0" encoding="UTF-8"?>
<SelectObjectContentRequest>
    <Expression>SELECT * from s3object AS s</Expression>
    <ExpressionType>SQL</ExpressionType>
    <InputSerialization>
        <CompressionType>NONE</CompressionType>
        <CSV>
        <FileHeaderInfo>NONE</FileHeaderInfo>
	    <QuoteCharacter>"</QuoteCharacter>
        </CSV>
    </InputSerialization>
    <OutputSerialization>
        <JSON>
        </JSON>
    </OutputSerialization>
    <RequestProgress>
        <Enabled>FALSE</Enabled>
    </RequestProgress>
	<ScanRange></ScanRange>
</SelectObjectContentRequest>`),
		},
		{
			name: "var-field-count",
			input: []byte(`id,time,num,num2,text
1,2010-01-01T,7867786,4565.908123
2,2017-01-02T03:04Z,-5, 0.765111,Some some
`),
			// Since we are doing offset, no headers are used.
			wantResult: `{"id":"1","time":"2010-01-01T","num":"7867786","num2":"4565.908123"}
{"id":"2","time":"2017-01-02T03:04Z","num":"-5","num2":" 0.765111","text":"Some some"}`,
			wantErr: false,
			requestXML: []byte(`<?xml version="1.0" encoding="UTF-8"?>
<SelectObjectContentRequest>
    <Expression>SELECT * from s3object</Expression>
    <ExpressionType>SQL</ExpressionType>
    <InputSerialization>
        <CompressionType>NONE</CompressionType>
        <CSV>
        <FileHeaderInfo>USE</FileHeaderInfo>
	    <QuoteCharacter>"</QuoteCharacter>
        </CSV>
    </InputSerialization>
    <OutputSerialization>
        <JSON>
        </JSON>
    </OutputSerialization>
    <RequestProgress>
        <Enabled>FALSE</Enabled>
    </RequestProgress>
</SelectObjectContentRequest>`),
		},
		{
			name:  "error-after-eof",
			input: testInput,
			// Since we are doing offset, no headers are used.
			wantResult: ``,
			wantErr:    true,
			requestXML: []byte(`<?xml version="1.0" encoding="UTF-8"?>
<SelectObjectContentRequest>
    <Expression>SELECT * from s3object AS s</Expression>
    <ExpressionType>SQL</ExpressionType>
    <InputSerialization>
        <CompressionType>NONE</CompressionType>
        <CSV>
        <FileHeaderInfo>NONE</FileHeaderInfo>
	    <QuoteCharacter>"</QuoteCharacter>
        </CSV>
    </InputSerialization>
    <OutputSerialization>
        <JSON>
        </JSON>
    </OutputSerialization>
    <RequestProgress>
        <Enabled>FALSE</Enabled>
    </RequestProgress>
	<ScanRange><Start>2600000</Start></ScanRange>
</SelectObjectContentRequest>`),
		},
		{
			name:  "error-after-eof",
			input: testInput,
			// Since we are doing offset, no headers are used.
			wantResult: ``,
			wantErr:    true,
			requestXML: []byte(`<?xml version="1.0" encoding="UTF-8"?>
<SelectObjectContentRequest>
    <Expression>SELECT * from s3object AS s</Expression>
    <ExpressionType>SQL</ExpressionType>
    <InputSerialization>
        <CompressionType>NONE</CompressionType>
        <CSV>
        <FileHeaderInfo>NONE</FileHeaderInfo>
	    <QuoteCharacter>"</QuoteCharacter>
        </CSV>
    </InputSerialization>
    <OutputSerialization>
        <JSON>
        </JSON>
    </OutputSerialization>
    <RequestProgress>
        <Enabled>FALSE</Enabled>
    </RequestProgress>
	<ScanRange><Start>2600000</Start><End>2600001</End></ScanRange>
</SelectObjectContentRequest>`),
		},
	}

	for _, testCase := range testTable {
		t.Run(testCase.name, func(t *testing.T) {
			testReq := testCase.requestXML
			s3Select, err := NewS3Select(bytes.NewReader(testReq))
			if err != nil {
				if !testCase.wantErr {
					t.Fatal(err)
				}
				t.Logf("got expected error: %v", err)
				return
			}

			if err = s3Select.Open(newBytesRSC(testCase.input)); err != nil {
				if !testCase.wantErr {
					t.Fatal(err)
				}
				t.Logf("got expected error: %v", err)
				return
			} else if testCase.wantErr {
				t.Error("did not get expected error")
				return
			}

			w := &testResponseWriter{}
			s3Select.Evaluate(w)
			s3Select.Close()
			resp := http.Response{
				StatusCode:    http.StatusOK,
				Body:          io.NopCloser(bytes.NewReader(w.response)),
				ContentLength: int64(len(w.response)),
			}
			res, err := minio.NewSelectResults(&resp, "testbucket")
			if err != nil {
				t.Error(err)
				return
			}
			got, err := io.ReadAll(res)
			if err != nil {
				t.Error(err)
				return
			}
			gotS := strings.TrimSpace(string(got))
			if !reflect.DeepEqual(gotS, testCase.wantResult) {
				t.Errorf("received response does not match with expected reply. Query: %s\ngot: %s\nwant:%s", testCase.query, gotS, testCase.wantResult)
			}
		})
	}
}

func TestParquetInput(t *testing.T) {
	saved := parquetSupport
	defer func() {
		parquetSupport = saved
	}()
	parquetSupport = true

	testTable := []struct {
		requestXML     []byte
		expectedResult []byte
	}{
		{
			[]byte(`
<?xml version="1.0" encoding="UTF-8"?>
<SelectObjectContentRequest>
    <Expression>SELECT one, two, three from S3Object</Expression>
    <ExpressionType>SQL</ExpressionType>
    <InputSerialization>
        <CompressionType>NONE</CompressionType>
        <Parquet>
        </Parquet>
    </InputSerialization>
    <OutputSerialization>
        <CSV>
        </CSV>
    </OutputSerialization>
    <RequestProgress>
        <Enabled>FALSE</Enabled>
    </RequestProgress>
</SelectObjectContentRequest>
`), []byte{
				0, 0, 0, 137, 0, 0, 0, 85, 194, 213, 168, 241, 13, 58, 109, 101, 115, 115, 97, 103, 101, 45, 116, 121, 112, 101, 7, 0, 5, 101, 118, 101, 110, 116, 13, 58, 99, 111, 110, 116, 101, 110, 116, 45, 116, 121, 112, 101, 7, 0, 24, 97, 112, 112, 108, 105, 99, 97, 116, 105, 111, 110, 47, 111, 99, 116, 101, 116, 45, 115, 116, 114, 101, 97, 109, 11, 58, 101, 118, 101, 110, 116, 45, 116, 121, 112, 101, 7, 0, 7, 82, 101, 99, 111, 114, 100, 115, 45, 49, 44, 102, 111, 111, 44, 116, 114, 117, 101, 10, 44, 98, 97, 114, 44, 102, 97, 108, 115, 101, 10, 50, 46, 53, 44, 98, 97, 122, 44, 116, 114, 117, 101, 10, 75, 182, 193, 80, 0, 0, 0, 235, 0, 0, 0, 67, 213, 243, 57, 141, 13, 58, 109, 101, 115, 115, 97, 103, 101, 45, 116, 121, 112, 101, 7, 0, 5, 101, 118, 101, 110, 116, 13, 58, 99, 111, 110, 116, 101, 110, 116, 45, 116, 121, 112, 101, 7, 0, 8, 116, 101, 120, 116, 47, 120, 109, 108, 11, 58, 101, 118, 101, 110, 116, 45, 116, 121, 112, 101, 7, 0, 5, 83, 116, 97, 116, 115, 60, 63, 120, 109, 108, 32, 118, 101, 114, 115, 105, 111, 110, 61, 34, 49, 46, 48, 34, 32, 101, 110, 99, 111, 100, 105, 110, 103, 61, 34, 85, 84, 70, 45, 56, 34, 63, 62, 60, 83, 116, 97, 116, 115, 62, 60, 66, 121, 116, 101, 115, 83, 99, 97, 110, 110, 101, 100, 62, 45, 49, 60, 47, 66, 121, 116, 101, 115, 83, 99, 97, 110, 110, 101, 100, 62, 60, 66, 121, 116, 101, 115, 80, 114, 111, 99, 101, 115, 115, 101, 100, 62, 45, 49, 60, 47, 66, 121, 116, 101, 115, 80, 114, 111, 99, 101, 115, 115, 101, 100, 62, 60, 66, 121, 116, 101, 115, 82, 101, 116, 117, 114, 110, 101, 100, 62, 51, 54, 60, 47, 66, 121, 116, 101, 115, 82, 101, 116, 117, 114, 110, 101, 100, 62, 60, 47, 83, 116, 97, 116, 115, 62, 128, 96, 253, 66, 0, 0, 0, 56, 0, 0, 0, 40, 193, 198, 132, 212, 13, 58, 109, 101, 115, 115, 97, 103, 101, 45, 116, 121, 112, 101, 7, 0, 5, 101, 118, 101, 110, 116, 11, 58, 101, 118, 101, 110, 116, 45, 116, 121, 112, 101, 7, 0, 3, 69, 110, 100, 207, 151, 211, 146,
			},
		},
		{
			[]byte(`
<?xml version="1.0" encoding="UTF-8"?>
<SelectObjectContentRequest>
    <Expression>SELECT COUNT(*) AS total_record_count from S3Object</Expression>
    <ExpressionType>SQL</ExpressionType>
    <InputSerialization>
        <CompressionType>NONE</CompressionType>
        <Parquet>
        </Parquet>
    </InputSerialization>
    <OutputSerialization>
        <CSV>
        </CSV>
    </OutputSerialization>
    <RequestProgress>
        <Enabled>FALSE</Enabled>
    </RequestProgress>
</SelectObjectContentRequest>
`), []byte{
				0, 0, 0, 103, 0, 0, 0, 85, 85, 49, 209, 79, 13, 58, 109, 101, 115, 115, 97, 103, 101, 45, 116, 121, 112, 101, 7, 0, 5, 101, 118, 101, 110, 116, 13, 58, 99, 111, 110, 116, 101, 110, 116, 45, 116, 121, 112, 101, 7, 0, 24, 97, 112, 112, 108, 105, 99, 97, 116, 105, 111, 110, 47, 111, 99, 116, 101, 116, 45, 115, 116, 114, 101, 97, 109, 11, 58, 101, 118, 101, 110, 116, 45, 116, 121, 112, 101, 7, 0, 7, 82, 101, 99, 111, 114, 100, 115, 51, 10, 175, 58, 213, 152, 0, 0, 0, 234, 0, 0, 0, 67, 232, 147, 16, 61, 13, 58, 109, 101, 115, 115, 97, 103, 101, 45, 116, 121, 112, 101, 7, 0, 5, 101, 118, 101, 110, 116, 13, 58, 99, 111, 110, 116, 101, 110, 116, 45, 116, 121, 112, 101, 7, 0, 8, 116, 101, 120, 116, 47, 120, 109, 108, 11, 58, 101, 118, 101, 110, 116, 45, 116, 121, 112, 101, 7, 0, 5, 83, 116, 97, 116, 115, 60, 63, 120, 109, 108, 32, 118, 101, 114, 115, 105, 111, 110, 61, 34, 49, 46, 48, 34, 32, 101, 110, 99, 111, 100, 105, 110, 103, 61, 34, 85, 84, 70, 45, 56, 34, 63, 62, 60, 83, 116, 97, 116, 115, 62, 60, 66, 121, 116, 101, 115, 83, 99, 97, 110, 110, 101, 100, 62, 45, 49, 60, 47, 66, 121, 116, 101, 115, 83, 99, 97, 110, 110, 101, 100, 62, 60, 66, 121, 116, 101, 115, 80, 114, 111, 99, 101, 115, 115, 101, 100, 62, 45, 49, 60, 47, 66, 121, 116, 101, 115, 80, 114, 111, 99, 101, 115, 115, 101, 100, 62, 60, 66, 121, 116, 101, 115, 82, 101, 116, 117, 114, 110, 101, 100, 62, 50, 60, 47, 66, 121, 116, 101, 115, 82, 101, 116, 117, 114, 110, 101, 100, 62, 60, 47, 83, 116, 97, 116, 115, 62, 190, 146, 162, 21, 0, 0, 0, 56, 0, 0, 0, 40, 193, 198, 132, 212, 13, 58, 109, 101, 115, 115, 97, 103, 101, 45, 116, 121, 112, 101, 7, 0, 5, 101, 118, 101, 110, 116, 11, 58, 101, 118, 101, 110, 116, 45, 116, 121, 112, 101, 7, 0, 3, 69, 110, 100, 207, 151, 211, 146,
			},
		},
	}

	for i, testCase := range testTable {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			testdataFile := "testdata/testdata.parquet"
			file, err := os.Open(testdataFile)
			if err != nil {
				t.Fatal(err)
			}

			s3Select, err := NewS3Select(bytes.NewReader(testCase.requestXML))
			if err != nil {
				t.Fatal(err)
			}

			if err = s3Select.Open(file); err != nil {
				t.Fatal(err)
			}

			fmt.Printf("R: \nE: %s\n" /* string(w.response), */, string(testCase.expectedResult))

			w := &testResponseWriter{}
			s3Select.Evaluate(w)
			s3Select.Close()

			if !reflect.DeepEqual(w.response, testCase.expectedResult) {
				resp := http.Response{
					StatusCode:    http.StatusOK,
					Body:          io.NopCloser(bytes.NewReader(w.response)),
					ContentLength: int64(len(w.response)),
				}
				res, err := minio.NewSelectResults(&resp, "testbucket")
				if err != nil {
					t.Error(err)
					return
				}
				got, err := io.ReadAll(res)
				if err != nil {
					t.Error(err)
					return
				}

				t.Errorf("received response does not match with expected reply\ngot: %#v\nwant:%#v\ndecoded:%s", w.response, testCase.expectedResult, string(got))
			}
		})
	}
}

func TestParquetInputSchema(t *testing.T) {
	saved := parquetSupport
	defer func() {
		parquetSupport = saved
	}()
	parquetSupport = true

	testTable := []struct {
		requestXML []byte
		wantResult string
	}{
		{
			requestXML: []byte(`
<?xml version="1.0" encoding="UTF-8"?>
<SelectObjectContentRequest>
    <Expression>SELECT * FROM S3Object LIMIT 5</Expression>
    <ExpressionType>SQL</ExpressionType>
    <InputSerialization>
        <CompressionType>NONE</CompressionType>
        <Parquet>
        </Parquet>
    </InputSerialization>
    <OutputSerialization>
        <JSON>
        </JSON>
    </OutputSerialization>
    <RequestProgress>
        <Enabled>FALSE</Enabled>
    </RequestProgress>
</SelectObjectContentRequest>
`), wantResult: `{"shipdate":"1996-03-13T"}
{"shipdate":"1996-04-12T"}
{"shipdate":"1996-01-29T"}
{"shipdate":"1996-04-21T"}
{"shipdate":"1996-03-30T"}`,
		},
		{
			requestXML: []byte(`
<?xml version="1.0" encoding="UTF-8"?>
<SelectObjectContentRequest>
    <Expression>SELECT DATE_ADD(day, 2, shipdate) as shipdate FROM S3Object LIMIT 5</Expression>
    <ExpressionType>SQL</ExpressionType>
    <InputSerialization>
        <CompressionType>NONE</CompressionType>
        <Parquet>
        </Parquet>
    </InputSerialization>
    <OutputSerialization>
        <JSON>
        </JSON>
    </OutputSerialization>
    <RequestProgress>
        <Enabled>FALSE</Enabled>
    </RequestProgress>
</SelectObjectContentRequest>
`), wantResult: `{"shipdate":"1996-03-15T"}
{"shipdate":"1996-04-14T"}
{"shipdate":"1996-01-31T"}
{"shipdate":"1996-04-23T"}
{"shipdate":"1996-04T"}`,
		},
	}

	for i, testCase := range testTable {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			testdataFile := "testdata/lineitem_shipdate.parquet"
			file, err := os.Open(testdataFile)
			if err != nil {
				t.Fatal(err)
			}

			s3Select, err := NewS3Select(bytes.NewReader(testCase.requestXML))
			if err != nil {
				t.Fatal(err)
			}

			if err = s3Select.Open(file); err != nil {
				t.Fatal(err)
			}

			w := &testResponseWriter{}
			s3Select.Evaluate(w)
			s3Select.Close()
			resp := http.Response{
				StatusCode:    http.StatusOK,
				Body:          io.NopCloser(bytes.NewReader(w.response)),
				ContentLength: int64(len(w.response)),
			}
			res, err := minio.NewSelectResults(&resp, "testbucket")
			if err != nil {
				t.Error(err)
				return
			}
			got, err := io.ReadAll(res)
			if err != nil {
				t.Error(err)
				return
			}
			gotS := strings.TrimSpace(string(got))
			if !reflect.DeepEqual(gotS, testCase.wantResult) {
				t.Errorf("received response does not match with expected reply. Query: %s\ngot: %s\nwant:%s", testCase.requestXML, gotS, testCase.wantResult)
			}
		})
	}
}

func TestParquetInputSchemaCSV(t *testing.T) {
	saved := parquetSupport
	defer func() {
		parquetSupport = saved
	}()
	parquetSupport = true

	testTable := []struct {
		requestXML []byte
		wantResult string
	}{
		{
			requestXML: []byte(`
<?xml version="1.0" encoding="UTF-8"?>
<SelectObjectContentRequest>
    <Expression>SELECT * FROM S3Object LIMIT 5</Expression>
    <ExpressionType>SQL</ExpressionType>
    <InputSerialization>
        <CompressionType>NONE</CompressionType>
        <Parquet>
        </Parquet>
    </InputSerialization>
    <OutputSerialization>
        <CSV/>
    </OutputSerialization>
    <RequestProgress>
        <Enabled>FALSE</Enabled>
    </RequestProgress>
</SelectObjectContentRequest>
`), wantResult: `1996-03-13T
1996-04-12T
1996-01-29T
1996-04-21T
1996-03-30T`,
		},
		{
			requestXML: []byte(`
<?xml version="1.0" encoding="UTF-8"?>
<SelectObjectContentRequest>
    <Expression>SELECT DATE_ADD(day, 2, shipdate) as shipdate FROM S3Object LIMIT 5</Expression>
    <ExpressionType>SQL</ExpressionType>
    <InputSerialization>
        <CompressionType>NONE</CompressionType>
        <Parquet>
        </Parquet>
    </InputSerialization>
    <OutputSerialization>
        <CSV/>
    </OutputSerialization>
    <RequestProgress>
        <Enabled>FALSE</Enabled>
    </RequestProgress>
</SelectObjectContentRequest>
`), wantResult: `1996-03-15T
1996-04-14T
1996-01-31T
1996-04-23T
1996-04T`,
		},
	}

	for i, testCase := range testTable {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			testdataFile := "testdata/lineitem_shipdate.parquet"
			file, err := os.Open(testdataFile)
			if err != nil {
				t.Fatal(err)
			}

			s3Select, err := NewS3Select(bytes.NewReader(testCase.requestXML))
			if err != nil {
				t.Fatal(err)
			}

			if err = s3Select.Open(file); err != nil {
				t.Fatal(err)
			}

			w := &testResponseWriter{}
			s3Select.Evaluate(w)
			s3Select.Close()
			resp := http.Response{
				StatusCode:    http.StatusOK,
				Body:          io.NopCloser(bytes.NewReader(w.response)),
				ContentLength: int64(len(w.response)),
			}
			res, err := minio.NewSelectResults(&resp, "testbucket")
			if err != nil {
				t.Error(err)
				return
			}
			got, err := io.ReadAll(res)
			if err != nil {
				t.Error(err)
				return
			}
			gotS := strings.TrimSpace(string(got))
			if !reflect.DeepEqual(gotS, testCase.wantResult) {
				t.Errorf("received response does not match with expected reply. Query: %s\ngot: %s\nwant:%s", testCase.requestXML, gotS, testCase.wantResult)
			}
		})
	}
}

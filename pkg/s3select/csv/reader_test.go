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
	"io"
	"io/ioutil"
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

		result := ""
		for {
			record, err = r.Read()
			if err != nil {
				break
			}
			s, _ := record.MarshalCSV([]rune(c.fieldDelimiter)[0])
			result += string(s) + c.recordDelimiter
		}
		r.Close()
		if err != io.EOF {
			t.Fatalf("Case %d failed with %s", i, err)
		}

		if result != c.content {
			t.Errorf("Case %d failed: expected %v result %v", i, c.content, result)
		}
	}
}

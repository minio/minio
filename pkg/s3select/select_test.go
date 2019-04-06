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

package s3select

import (
	"bytes"
	"go/build"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path"
	"reflect"
	"testing"
)

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

func TestCSVInput(t *testing.T) {
	var requestXML = []byte(`
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
`)

	var csvData = []byte(`one,two,three
-1,foo,true
,bar,false
2.5,baz,true
`)

	var expectedResult = []byte{
		0, 0, 0, 137, 0, 0, 0, 85, 194, 213, 168, 241, 13, 58, 109, 101, 115, 115, 97, 103, 101, 45, 116, 121, 112, 101, 7, 0, 5, 101, 118, 101, 110, 116, 13, 58, 99, 111, 110, 116, 101, 110, 116, 45, 116, 121, 112, 101, 7, 0, 24, 97, 112, 112, 108, 105, 99, 97, 116, 105, 111, 110, 47, 111, 99, 116, 101, 116, 45, 115, 116, 114, 101, 97, 109, 11, 58, 101, 118, 101, 110, 116, 45, 116, 121, 112, 101, 7, 0, 7, 82, 101, 99, 111, 114, 100, 115, 45, 49, 44, 102, 111, 111, 44, 116, 114, 117, 101, 10, 44, 98, 97, 114, 44, 102, 97, 108, 115, 101, 10, 50, 46, 53, 44, 98, 97, 122, 44, 116, 114, 117, 101, 10, 75, 182, 193, 80, 0, 0, 0, 235, 0, 0, 0, 67, 213, 243, 57, 141, 13, 58, 109, 101, 115, 115, 97, 103, 101, 45, 116, 121, 112, 101, 7, 0, 5, 101, 118, 101, 110, 116, 13, 58, 99, 111, 110, 116, 101, 110, 116, 45, 116, 121, 112, 101, 7, 0, 8, 116, 101, 120, 116, 47, 120, 109, 108, 11, 58, 101, 118, 101, 110, 116, 45, 116, 121, 112, 101, 7, 0, 5, 83, 116, 97, 116, 115, 60, 63, 120, 109, 108, 32, 118, 101, 114, 115, 105, 111, 110, 61, 34, 49, 46, 48, 34, 32, 101, 110, 99, 111, 100, 105, 110, 103, 61, 34, 85, 84, 70, 45, 56, 34, 63, 62, 60, 83, 116, 97, 116, 115, 62, 60, 66, 121, 116, 101, 115, 83, 99, 97, 110, 110, 101, 100, 62, 53, 48, 60, 47, 66, 121, 116, 101, 115, 83, 99, 97, 110, 110, 101, 100, 62, 60, 66, 121, 116, 101, 115, 80, 114, 111, 99, 101, 115, 115, 101, 100, 62, 53, 48, 60, 47, 66, 121, 116, 101, 115, 80, 114, 111, 99, 101, 115, 115, 101, 100, 62, 60, 66, 121, 116, 101, 115, 82, 101, 116, 117, 114, 110, 101, 100, 62, 51, 54, 60, 47, 66, 121, 116, 101, 115, 82, 101, 116, 117, 114, 110, 101, 100, 62, 60, 47, 83, 116, 97, 116, 115, 62, 253, 105, 8, 216, 0, 0, 0, 56, 0, 0, 0, 40, 193, 198, 132, 212, 13, 58, 109, 101, 115, 115, 97, 103, 101, 45, 116, 121, 112, 101, 7, 0, 5, 101, 118, 101, 110, 116, 11, 58, 101, 118, 101, 110, 116, 45, 116, 121, 112, 101, 7, 0, 3, 69, 110, 100, 207, 151, 211, 146,
	}

	s3Select, err := NewS3Select(bytes.NewReader(requestXML))
	if err != nil {
		t.Fatal(err)
	}

	if err = s3Select.Open(func(offset, length int64) (io.ReadCloser, error) {
		return ioutil.NopCloser(bytes.NewReader(csvData)), nil
	}); err != nil {
		t.Fatal(err)
	}

	w := &testResponseWriter{}
	s3Select.Evaluate(w)
	s3Select.Close()

	if !reflect.DeepEqual(w.response, expectedResult) {
		t.Fatalf("received response does not match with expected reply")
	}
}

func TestJSONInput(t *testing.T) {
	var requestXML = []byte(`
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
`)

	var jsonData = []byte(`{"three":true,"two":"foo","one":-1}
{"three":false,"two":"bar","one":null}
{"three":true,"two":"baz","one":2.5}
`)

	var expectedResult = []byte{
		0, 0, 0, 137, 0, 0, 0, 85, 194, 213, 168, 241, 13, 58, 109, 101, 115, 115, 97, 103, 101, 45, 116, 121, 112, 101, 7, 0, 5, 101, 118, 101, 110, 116, 13, 58, 99, 111, 110, 116, 101, 110, 116, 45, 116, 121, 112, 101, 7, 0, 24, 97, 112, 112, 108, 105, 99, 97, 116, 105, 111, 110, 47, 111, 99, 116, 101, 116, 45, 115, 116, 114, 101, 97, 109, 11, 58, 101, 118, 101, 110, 116, 45, 116, 121, 112, 101, 7, 0, 7, 82, 101, 99, 111, 114, 100, 115, 45, 49, 44, 102, 111, 111, 44, 116, 114, 117, 101, 10, 44, 98, 97, 114, 44, 102, 97, 108, 115, 101, 10, 50, 46, 53, 44, 98, 97, 122, 44, 116, 114, 117, 101, 10, 75, 182, 193, 80, 0, 0, 0, 237, 0, 0, 0, 67, 90, 179, 204, 45, 13, 58, 109, 101, 115, 115, 97, 103, 101, 45, 116, 121, 112, 101, 7, 0, 5, 101, 118, 101, 110, 116, 13, 58, 99, 111, 110, 116, 101, 110, 116, 45, 116, 121, 112, 101, 7, 0, 8, 116, 101, 120, 116, 47, 120, 109, 108, 11, 58, 101, 118, 101, 110, 116, 45, 116, 121, 112, 101, 7, 0, 5, 83, 116, 97, 116, 115, 60, 63, 120, 109, 108, 32, 118, 101, 114, 115, 105, 111, 110, 61, 34, 49, 46, 48, 34, 32, 101, 110, 99, 111, 100, 105, 110, 103, 61, 34, 85, 84, 70, 45, 56, 34, 63, 62, 60, 83, 116, 97, 116, 115, 62, 60, 66, 121, 116, 101, 115, 83, 99, 97, 110, 110, 101, 100, 62, 49, 49, 50, 60, 47, 66, 121, 116, 101, 115, 83, 99, 97, 110, 110, 101, 100, 62, 60, 66, 121, 116, 101, 115, 80, 114, 111, 99, 101, 115, 115, 101, 100, 62, 49, 49, 50, 60, 47, 66, 121, 116, 101, 115, 80, 114, 111, 99, 101, 115, 115, 101, 100, 62, 60, 66, 121, 116, 101, 115, 82, 101, 116, 117, 114, 110, 101, 100, 62, 51, 54, 60, 47, 66, 121, 116, 101, 115, 82, 101, 116, 117, 114, 110, 101, 100, 62, 60, 47, 83, 116, 97, 116, 115, 62, 181, 40, 50, 250, 0, 0, 0, 56, 0, 0, 0, 40, 193, 198, 132, 212, 13, 58, 109, 101, 115, 115, 97, 103, 101, 45, 116, 121, 112, 101, 7, 0, 5, 101, 118, 101, 110, 116, 11, 58, 101, 118, 101, 110, 116, 45, 116, 121, 112, 101, 7, 0, 3, 69, 110, 100, 207, 151, 211, 146,
	}

	s3Select, err := NewS3Select(bytes.NewReader(requestXML))
	if err != nil {
		t.Fatal(err)
	}

	if err = s3Select.Open(func(offset, length int64) (io.ReadCloser, error) {
		return ioutil.NopCloser(bytes.NewReader(jsonData)), nil
	}); err != nil {
		t.Fatal(err)
	}

	w := &testResponseWriter{}
	s3Select.Evaluate(w)
	s3Select.Close()

	if !reflect.DeepEqual(w.response, expectedResult) {
		t.Fatalf("received response does not match with expected reply")
	}
}

func TestParquetInput(t *testing.T) {
	var requestXML = []byte(`
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
`)

	getReader := func(offset int64, length int64) (io.ReadCloser, error) {
		testdataFile := path.Join(build.Default.GOPATH, "src/github.com/minio/minio/pkg/s3select/testdata.parquet")
		file, err := os.Open(testdataFile)
		if err != nil {
			return nil, err
		}

		fi, err := file.Stat()
		if err != nil {
			return nil, err
		}

		if offset < 0 {
			offset = fi.Size() + offset
		}

		if _, err = file.Seek(offset, os.SEEK_SET); err != nil {
			return nil, err
		}

		return file, nil
	}

	var expectedResult = []byte{
		0, 0, 0, 137, 0, 0, 0, 85, 194, 213, 168, 241, 13, 58, 109, 101, 115, 115, 97, 103, 101, 45, 116, 121, 112, 101, 7, 0, 5, 101, 118, 101, 110, 116, 13, 58, 99, 111, 110, 116, 101, 110, 116, 45, 116, 121, 112, 101, 7, 0, 24, 97, 112, 112, 108, 105, 99, 97, 116, 105, 111, 110, 47, 111, 99, 116, 101, 116, 45, 115, 116, 114, 101, 97, 109, 11, 58, 101, 118, 101, 110, 116, 45, 116, 121, 112, 101, 7, 0, 7, 82, 101, 99, 111, 114, 100, 115, 45, 49, 44, 102, 111, 111, 44, 116, 114, 117, 101, 10, 44, 98, 97, 114, 44, 102, 97, 108, 115, 101, 10, 50, 46, 53, 44, 98, 97, 122, 44, 116, 114, 117, 101, 10, 75, 182, 193, 80, 0, 0, 0, 235, 0, 0, 0, 67, 213, 243, 57, 141, 13, 58, 109, 101, 115, 115, 97, 103, 101, 45, 116, 121, 112, 101, 7, 0, 5, 101, 118, 101, 110, 116, 13, 58, 99, 111, 110, 116, 101, 110, 116, 45, 116, 121, 112, 101, 7, 0, 8, 116, 101, 120, 116, 47, 120, 109, 108, 11, 58, 101, 118, 101, 110, 116, 45, 116, 121, 112, 101, 7, 0, 5, 83, 116, 97, 116, 115, 60, 63, 120, 109, 108, 32, 118, 101, 114, 115, 105, 111, 110, 61, 34, 49, 46, 48, 34, 32, 101, 110, 99, 111, 100, 105, 110, 103, 61, 34, 85, 84, 70, 45, 56, 34, 63, 62, 60, 83, 116, 97, 116, 115, 62, 60, 66, 121, 116, 101, 115, 83, 99, 97, 110, 110, 101, 100, 62, 45, 49, 60, 47, 66, 121, 116, 101, 115, 83, 99, 97, 110, 110, 101, 100, 62, 60, 66, 121, 116, 101, 115, 80, 114, 111, 99, 101, 115, 115, 101, 100, 62, 45, 49, 60, 47, 66, 121, 116, 101, 115, 80, 114, 111, 99, 101, 115, 115, 101, 100, 62, 60, 66, 121, 116, 101, 115, 82, 101, 116, 117, 114, 110, 101, 100, 62, 51, 54, 60, 47, 66, 121, 116, 101, 115, 82, 101, 116, 117, 114, 110, 101, 100, 62, 60, 47, 83, 116, 97, 116, 115, 62, 128, 96, 253, 66, 0, 0, 0, 56, 0, 0, 0, 40, 193, 198, 132, 212, 13, 58, 109, 101, 115, 115, 97, 103, 101, 45, 116, 121, 112, 101, 7, 0, 5, 101, 118, 101, 110, 116, 11, 58, 101, 118, 101, 110, 116, 45, 116, 121, 112, 101, 7, 0, 3, 69, 110, 100, 207, 151, 211, 146,
	}

	s3Select, err := NewS3Select(bytes.NewReader(requestXML))
	if err != nil {
		t.Fatal(err)
	}

	if err = s3Select.Open(getReader); err != nil {
		t.Fatal(err)
	}

	w := &testResponseWriter{}
	s3Select.Evaluate(w)
	s3Select.Close()

	if !reflect.DeepEqual(w.response, expectedResult) {
		t.Fatalf("received response does not match with expected reply")
	}
}

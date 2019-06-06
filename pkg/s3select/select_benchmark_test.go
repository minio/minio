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
	"encoding/csv"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"strconv"
	"testing"
	"time"

	humanize "github.com/dustin/go-humanize"
)

var randSrc = rand.New(rand.NewSource(time.Now().UnixNano()))

const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

func newRandString(length int) string {
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[randSrc.Intn(len(charset))]
	}
	return string(b)
}

func genSampleCSVData(count int) []byte {
	buf := &bytes.Buffer{}
	csvWriter := csv.NewWriter(buf)
	csvWriter.Write([]string{"id", "name", "age", "city"})

	for i := 0; i < count; i++ {
		csvWriter.Write([]string{
			strconv.Itoa(i),
			newRandString(10),
			newRandString(5),
			newRandString(10),
		})
	}

	csvWriter.Flush()
	return buf.Bytes()
}

type nullResponseWriter struct {
}

func (w *nullResponseWriter) Header() http.Header {
	return nil
}

func (w *nullResponseWriter) Write(p []byte) (int, error) {
	return len(p), nil
}

func (w *nullResponseWriter) WriteHeader(statusCode int) {
}

func (w *nullResponseWriter) Flush() {
}

func benchmarkSelect(b *testing.B, count int, query string) {
	var requestXML = []byte(`
<?xml version="1.0" encoding="UTF-8"?>
<SelectObjectContentRequest>
    <Expression>` + query + `</Expression>
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

	s3Select, err := NewS3Select(bytes.NewReader(requestXML))
	if err != nil {
		b.Fatal(err)
	}

	csvData := genSampleCSVData(count)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		if err = s3Select.Open(func(offset, length int64) (io.ReadCloser, error) {
			return ioutil.NopCloser(bytes.NewReader(csvData)), nil
		}); err != nil {
			b.Fatal(err)
		}

		s3Select.Evaluate(&nullResponseWriter{})
		s3Select.Close()
	}
}

func benchmarkSelectAll(b *testing.B, count int) {
	benchmarkSelect(b, count, "select * from S3Object")
}

// BenchmarkSelectAll_100K - benchmark * function with 100k records.
func BenchmarkSelectAll_100K(b *testing.B) {
	benchmarkSelectAll(b, 100*humanize.KiByte)
}

// BenchmarkSelectAll_1M - benchmark * function with 1m records.
func BenchmarkSelectAll_1M(b *testing.B) {
	benchmarkSelectAll(b, 1*humanize.MiByte)
}

// BenchmarkSelectAll_2M - benchmark * function with 2m records.
func BenchmarkSelectAll_2M(b *testing.B) {
	benchmarkSelectAll(b, 2*humanize.MiByte)
}

// BenchmarkSelectAll_10M - benchmark * function with 10m records.
func BenchmarkSelectAll_10M(b *testing.B) {
	benchmarkSelectAll(b, 10*humanize.MiByte)
}

func benchmarkAggregateCount(b *testing.B, count int) {
	benchmarkSelect(b, count, "select count(*) from S3Object")
}

// BenchmarkAggregateCount_100K - benchmark count(*) function with 100k records.
func BenchmarkAggregateCount_100K(b *testing.B) {
	benchmarkAggregateCount(b, 100*humanize.KiByte)
}

// BenchmarkAggregateCount_1M - benchmark count(*) function with 1m records.
func BenchmarkAggregateCount_1M(b *testing.B) {
	benchmarkAggregateCount(b, 1*humanize.MiByte)
}

// BenchmarkAggregateCount_2M - benchmark count(*) function with 2m records.
func BenchmarkAggregateCount_2M(b *testing.B) {
	benchmarkAggregateCount(b, 2*humanize.MiByte)
}

// BenchmarkAggregateCount_10M - benchmark count(*) function with 10m records.
func BenchmarkAggregateCount_10M(b *testing.B) {
	benchmarkAggregateCount(b, 10*humanize.MiByte)
}

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
	"encoding/csv"
	"math/rand"
	"net/http"
	"strconv"
	"testing"
	"time"

	humanize "github.com/dustin/go-humanize"
)

const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

func newRandString(length int) string {
	randSrc := rand.New(rand.NewSource(time.Now().UnixNano()))

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

	for i := range count {
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

type nullResponseWriter struct{}

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
	requestXML := []byte(`
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

	csvData := genSampleCSVData(count)

	b.ResetTimer()
	b.ReportAllocs()
	b.SetBytes(int64(count))

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			s3Select, err := NewS3Select(bytes.NewReader(requestXML))
			if err != nil {
				b.Fatal(err)
			}

			if err = s3Select.Open(newBytesRSC(csvData)); err != nil {
				b.Fatal(err)
			}

			s3Select.Evaluate(&nullResponseWriter{})
			s3Select.Close()
		}
	})
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

func benchmarkSingleCol(b *testing.B, count int) {
	benchmarkSelect(b, count, "select id from S3Object")
}

// BenchmarkSingleRow_100K - benchmark SELECT column function with 100k records.
func BenchmarkSingleCol_100K(b *testing.B) {
	benchmarkSingleCol(b, 1e5)
}

// BenchmarkSelectAll_1M - benchmark * function with 1m records.
func BenchmarkSingleCol_1M(b *testing.B) {
	benchmarkSingleCol(b, 1e6)
}

// BenchmarkSelectAll_2M - benchmark * function with 2m records.
func BenchmarkSingleCol_2M(b *testing.B) {
	benchmarkSingleCol(b, 2e6)
}

// BenchmarkSelectAll_10M - benchmark * function with 10m records.
func BenchmarkSingleCol_10M(b *testing.B) {
	benchmarkSingleCol(b, 1e7)
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

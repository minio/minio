/*
 * Minio Cloud Storage, (C) 2018 Minio, Inc.
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
	"fmt"
	"math/rand"
	"strconv"
	"testing"
	"time"

	humanize "github.com/dustin/go-humanize"
	"github.com/tidwall/gjson"

	"github.com/minio/minio/pkg/s3select/format"
)

// This function returns the index of a string in a list
func stringIndex(a string, list []string) int {
	for i, v := range list {
		if v == a {
			return i
		}
	}
	return -1
}

// TestHelperFunctions is a unit test which tests some
// small helper string functions.
func TestHelperFunctions(t *testing.T) {
	tables := []struct {
		myReq    string
		myList   []string
		myIndex  int
		expected bool
	}{
		{"test1", []string{"test1", "test2", "test3", "test4", "test5"}, 0, true},
		{"random", []string{"test1", "test2", "test3", "test4", "test5"}, -1, false},
		{"test3", []string{"test1", "test2", "test3", "test4", "test5"}, 2, true},
	}
	for _, table := range tables {
		if format.StringInSlice(table.myReq, table.myList) != table.expected {
			t.Error()
		}
		if stringIndex(table.myReq, table.myList) != table.myIndex {
			t.Error()
		}
	}
}

// TestStateMachine is a unit test which ensures that the lowest level of the
// interpreter is converting properly.
func TestStateMachine(t *testing.T) {
	tables := []struct {
		operand  string
		operator string
		leftArg  string
		err      error
		expected bool
	}{
		{"", ">", "2012", nil, true},
		{"2005", ">", "2012", nil, true},
		{"2005", ">", "2012", nil, true},
		{"2012.0000", ">", "2014.000", nil, true},
		{"2012", "!=", "2014.000", nil, true},
		{"NA", ">", "2014.000", nil, true},
		{"2012", ">", "2014.000", nil, false},
		{"2012.0000", ">", "2014", nil, false},
		{"", "<", "2012", nil, false},
		{"2012.0000", "<", "2014.000", nil, false},
		{"2014", ">", "Random", nil, false},
		{"test3", ">", "aandom", nil, false},
		{"true", ">", "true", ErrUnsupportedSyntax, false},
	}
	for i, table := range tables {
		val, err := evaluateOperator(gjson.Parse(table.leftArg), table.operator, gjson.Parse(table.operand))
		if err != table.err {
			t.Errorf("Test %d: expected %v, got %v", i+1, table.err, err)
		}
		if val != table.expected {
			t.Errorf("Test %d: expected %t, got %t", i+1, table.expected, val)
		}
	}
}

// TestOperators is a unit test which ensures that the appropriate values are
// being returned from the operators functions.
func TestOperators(t *testing.T) {
	tables := []struct {
		operator string
		err      error
	}{
		{">", nil},
		{"%", ErrParseUnknownOperator},
	}
	for _, table := range tables {
		err := checkValidOperator(table.operator)
		if err != table.err {
			t.Error()
		}
	}
}

// Unit tests for the main function that performs aggreggation.
func TestAggregationFunc(t *testing.T) {
	columnsMap := make(map[string]int)
	columnsMap["Col1"] = 0
	columnsMap["Col2"] = 1
	tables := []struct {
		counter        int
		filtrCount     int
		myAggVals      []float64
		columnsMap     map[string]int
		storeReqCols   []string
		storeFunctions []string
		record         []byte
		err            error
		expectedVal    float64
	}{
		{10, 5, []float64{10, 11, 12, 13, 14}, columnsMap, []string{"Col1"}, []string{"count"}, []byte("{\"Col1\":\"1\",\"Col2\":\"2\"}"), nil, 11},
		{10, 5, []float64{10}, columnsMap, []string{"Col1"}, []string{"min"}, []byte("{\"Col1\":\"1\",\"Col2\":\"2\"}"), nil, 1},
		{10, 5, []float64{10}, columnsMap, []string{"Col1"}, []string{"max"}, []byte("{\"Col1\":\"1\",\"Col2\":\"2\"}"), nil, 10},
		{10, 5, []float64{10}, columnsMap, []string{"Col1"}, []string{"sum"}, []byte("{\"Col1\":\"1\",\"Col2\":\"2\"}"), nil, 11},
		{1, 1, []float64{10}, columnsMap, []string{"Col1"}, []string{"avg"}, []byte("{\"Col1\":\"1\",\"Col2\":\"2\"}"), nil, 5.500},
		{10, 5, []float64{0.0000}, columnsMap, []string{"Col1"}, []string{"random"}, []byte("{\"Col1\":\"1\",\"Col2\":\"2\"}"),
			ErrParseNonUnaryAgregateFunctionCall, 0},
		{0, 5, []float64{0}, columnsMap, []string{"0"}, []string{"count"}, []byte("{\"Col1\":\"1\",\"Col2\":\"2\"}"), nil, 1},
		{10, 5, []float64{10}, columnsMap, []string{"1"}, []string{"min"}, []byte("{\"_1\":\"1\",\"_2\":\"2\"}"), nil, 1},
	}

	for _, table := range tables {
		err := aggregationFns(table.counter, table.filtrCount, table.myAggVals, table.storeReqCols, table.storeFunctions, table.record)
		if table.err != err {
			t.Error()
		}
		if table.myAggVals[0] != table.expectedVal {
			t.Error()
		}

	}
}

// TestStringComparator is a unit test which ensures that the appropriate
// values are being compared for strings.
func TestStringComparator(t *testing.T) {
	tables := []struct {
		operand  string
		operator string
		myVal    string
		expected bool
		err      error
	}{
		{"random", ">", "myName", "random" > "myName", nil},
		{"12", "!=", "myName", "12" != "myName", nil},
		{"12", "=", "myName", "12" == "myName", nil},
		{"12", "<=", "myName", "12" <= "myName", nil},
		{"12", ">=", "myName", "12" >= "myName", nil},
		{"12", "<", "myName", "12" < "myName", nil},
		{"name", "like", "_x%", false, nil},
		{"12", "randomoperator", "myName", false, ErrUnsupportedSyntax},
	}
	for _, table := range tables {
		myVal, err := stringEval(table.operand, table.operator, table.myVal)
		if err != table.err {
			t.Error()
		}
		if myVal != table.expected {
			t.Error()
		}
	}
}

// TestFloatComparator is a unit test which ensures that the appropriate
// values are being compared for floats.
func TestFloatComparator(t *testing.T) {
	tables := []struct {
		operand  float64
		operator string
		myVal    float64
		expected bool
		err      error
	}{
		{12.000, ">", 13.000, 12.000 > 13.000, nil},
		{1000.000, "!=", 1000.000, 1000.000 != 1000.000, nil},
		{1000.000, "<", 1000.000, 1000.000 < 1000.000, nil},
		{1000.000, "<=", 1000.000, 1000.000 <= 1000.000, nil},
		{1000.000, ">=", 1000.000, 1000.000 >= 1000.000, nil},
		{1000.000, "=", 1000.000, 1000.000 == 1000.000, nil},
		{17.000, "randomoperator", 0.0, false, ErrUnsupportedSyntax},
	}
	for _, table := range tables {
		myVal, err := floatEval(table.operand, table.operator, table.myVal)
		if err != table.err {
			t.Error()
		}
		if myVal != table.expected {
			t.Error()
		}
	}
}

// TestIntComparator is a unit test which ensures that the appropriate values
// are being compared for ints.
func TestIntComparator(t *testing.T) {
	tables := []struct {
		operand  int64
		operator string
		myVal    int64
		expected bool
		err      error
	}{
		{12, ">", 13, 12.000 > 13.000, nil},
		{1000, "!=", 1000, 1000.000 != 1000.000, nil},
		{1000, "<", 1000, 1000.000 < 1000.000, nil},
		{1000, "<=", 1000, 1000.000 <= 1000.000, nil},
		{1000, ">=", 1000, 1000.000 >= 1000.000, nil},
		{1000, "=", 1000, 1000.000 >= 1000.000, nil},
		{17, "randomoperator", 0, false, ErrUnsupportedSyntax},
	}
	for _, table := range tables {
		myVal, err := intEval(table.operand, table.operator, table.myVal)
		if err != table.err {
			t.Error()
		}
		if myVal != table.expected {
			t.Error()
		}
	}
}

// TestSizeFunction is a function which provides unit testing for the function
// which calculates size.
func TestSizeFunction(t *testing.T) {
	tables := []struct {
		myRecord []string
		expected int64
	}{
		{[]string{"test1", "test2", "test3", "test4", "test5"}, 30},
	}
	for _, table := range tables {
		if format.ProcessSize(table.myRecord) != table.expected {
			t.Error()
		}

	}
}

func TestMatch(t *testing.T) {
	testCases := []struct {
		pattern string
		text    string
		matched bool
	}{
		// Test case - 1.
		// Test case so that the match occurs on the opening letter.
		{
			pattern: "a%",
			text:    "apple",
			matched: true,
		},
		// Test case - 2.
		// Test case so that the ending letter is true.
		{
			pattern: "%m",
			text:    "random",
			matched: true,
		},
		// Test case - 3.
		// Test case so that a character is at the appropriate position.
		{
			pattern: "_d%",
			text:    "adam",
			matched: true,
		},
		// Test case - 4.
		// Test case so that a character is at the appropriate position.
		{
			pattern: "_d%",
			text:    "apple",
			matched: false,
		},
		// Test case - 5.
		// Test case with checking that it is at least 3 in length
		{
			pattern: "a_%_%",
			text:    "ap",
			matched: false,
		},
		{
			pattern: "a_%_%",
			text:    "apple",
			matched: true,
		},
		{
			pattern: "%or%",
			text:    "orphan",
			matched: true,
		},
		{
			pattern: "%or%",
			text:    "dolphin",
			matched: false,
		},
		{
			pattern: "%or%",
			text:    "dorlphin",
			matched: true,
		},
		{
			pattern: "2__3",
			text:    "2003",
			matched: true,
		},
		{
			pattern: "_YYYY_",
			text:    "aYYYYa",
			matched: true,
		},
		{
			pattern: "C%",
			text:    "CA",
			matched: true,
		},
		{
			pattern: "C%",
			text:    "SC",
			matched: false,
		},
		{
			pattern: "%C",
			text:    "SC",
			matched: true,
		},
		{
			pattern: "%C",
			text:    "CA",
			matched: false,
		},
		{
			pattern: "%C",
			text:    "ACCC",
			matched: true,
		},
		{
			pattern: "C%",
			text:    "CCC",
			matched: true,
		},
		{
			pattern: "j%",
			text:    "mejri",
			matched: false,
		},
		{
			pattern: "a%o",
			text:    "ando",
			matched: true,
		},
		{
			pattern: "%j",
			text:    "mejri",
			matched: false,
		},
		{
			pattern: "%ja",
			text:    "mejrija",
			matched: true,
		},
		{
			pattern: "ja%",
			text:    "jamal",
			matched: true,
		},
		{
			pattern: "a%o",
			text:    "andp",
			matched: false,
		},
		{
			pattern: "_r%",
			text:    "arpa",
			matched: true,
		},
		{
			pattern: "_r%",
			text:    "apra",
			matched: false,
		},
		{
			pattern: "a_%_%",
			text:    "appple",
			matched: true,
		},
		{
			pattern: "l_b%",
			text:    "lebron",
			matched: true,
		},
		{
			pattern: "leb%",
			text:    "Dalembert",
			matched: false,
		},
		{
			pattern: "leb%",
			text:    "Landesberg",
			matched: false,
		},
		{
			pattern: "leb%",
			text:    "Mccalebb",
			matched: false,
		},
		{
			pattern: "%lebb",
			text:    "Mccalebb",
			matched: true,
		},
	}
	// Iterating over the test cases, call the function under test and asert the output.
	for i, testCase := range testCases {
		actualResult, err := likeConvert(testCase.pattern, testCase.text)
		if err != nil {
			t.Error()
		}
		if testCase.matched != actualResult {
			fmt.Println("Expected Pattern", testCase.pattern, "Expected Text", testCase.text)
			t.Errorf("Test %d: Expected the result to be `%v`, but instead found it to be `%v`", i+1, testCase.matched, actualResult)
		}
	}
}

// TestFuncProcessing is a unit test which ensures that the appropriate values are
// being returned from the Processing... functions.
func TestFuncProcessing(t *testing.T) {
	tables := []struct {
		myString    string
		coalList    []string
		myValString string
		myValCoal   string
		myValNull   string
		stringFunc  string
	}{
		{"lower", []string{"random", "hello", "random"}, "LOWER", "random", "", "UPPER"},
		{"LOWER", []string{"missing", "hello", "random"}, "lower", "hello", "null", "LOWER"},
	}
	for _, table := range tables {
		if table.coalList != nil {
			myVal := processCoalNoIndex(table.coalList)
			if myVal != table.myValCoal {
				t.Error()
			}
		}
		myVal := applyStrFunc(gjson.Result{
			Type: gjson.String,
			Str:  table.myString,
		}, table.stringFunc)
		if myVal != table.myValString {
			t.Error()
		}

	}
}

const charset = "abcdefghijklmnopqrstuvwxyz" + "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

var seededRand = rand.New(rand.NewSource(time.Now().UnixNano()))

func StringWithCharset(length int, charset string) string {
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[seededRand.Intn(len(charset))]
	}
	return string(b)
}

func String(length int) string {
	return StringWithCharset(length, charset)
}

func genCSV(b *bytes.Buffer, records int) error {
	b.Reset()
	w := csv.NewWriter(b)
	w.Write([]string{"id", "name", "age", "city"})

	for i := 0; i < records; i++ {
		w.Write([]string{
			strconv.Itoa(i),
			String(10),
			String(5),
			String(10),
		})
	}

	// Write any buffered data to the underlying writer (standard output).
	w.Flush()

	return w.Error()
}

func benchmarkSQLAll(b *testing.B, records int) {
	benchmarkSQL(b, records, "select * from S3Object")
}

func benchmarkSQLAggregate(b *testing.B, records int) {
	benchmarkSQL(b, records, "select count(*) from S3Object")
}

func benchmarkSQL(b *testing.B, records int, query string) {
	var (
		buf    bytes.Buffer
		output bytes.Buffer
	)
	genCSV(&buf, records)

	b.ResetTimer()
	b.ReportAllocs()

	sreq := ObjectSelectRequest{}
	sreq.Expression = query
	sreq.ExpressionType = QueryExpressionTypeSQL
	sreq.InputSerialization.CSV = &struct {
		FileHeaderInfo       CSVFileHeaderInfo
		RecordDelimiter      string
		FieldDelimiter       string
		QuoteCharacter       string
		QuoteEscapeCharacter string
		Comments             string
	}{}
	sreq.InputSerialization.CSV.FileHeaderInfo = CSVFileHeaderInfoUse
	sreq.InputSerialization.CSV.RecordDelimiter = "\n"
	sreq.InputSerialization.CSV.FieldDelimiter = ","

	sreq.OutputSerialization.CSV = &struct {
		QuoteFields          CSVQuoteFields
		RecordDelimiter      string
		FieldDelimiter       string
		QuoteCharacter       string
		QuoteEscapeCharacter string
	}{}
	sreq.OutputSerialization.CSV.RecordDelimiter = "\n"
	sreq.OutputSerialization.CSV.FieldDelimiter = ","

	s3s, err := New(&buf, int64(buf.Len()), sreq)
	if err != nil {
		b.Fatal(err)
	}

	for i := 0; i < b.N; i++ {
		output.Reset()
		if err = Execute(&output, s3s); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkSQLAggregate_100K - benchmark count(*) function with 100k records.
func BenchmarkSQLAggregate_100K(b *testing.B) {
	benchmarkSQLAggregate(b, humanize.KiByte*100)
}

// BenchmarkSQLAggregate_1M - benchmark count(*) function with 1m records.
func BenchmarkSQLAggregate_1M(b *testing.B) {
	benchmarkSQLAggregate(b, humanize.MiByte)
}

// BenchmarkSQLAggregate_2M - benchmark count(*) function with 2m records.
func BenchmarkSQLAggregate_2M(b *testing.B) {
	benchmarkSQLAggregate(b, 2*humanize.MiByte)
}

// BenchmarkSQLAggregate_10M - benchmark count(*) function with 10m records.
func BenchmarkSQLAggregate_10M(b *testing.B) {
	benchmarkSQLAggregate(b, 10*humanize.MiByte)
}

// BenchmarkSQLAll_100K - benchmark * function with 100k records.
func BenchmarkSQLAll_100K(b *testing.B) {
	benchmarkSQLAll(b, humanize.KiByte*100)
}

// BenchmarkSQLAll_1M - benchmark * function with 1m records.
func BenchmarkSQLAll_1M(b *testing.B) {
	benchmarkSQLAll(b, humanize.MiByte)
}

// BenchmarkSQLAll_2M - benchmark * function with 2m records.
func BenchmarkSQLAll_2M(b *testing.B) {
	benchmarkSQLAll(b, 2*humanize.MiByte)
}

// BenchmarkSQLAll_10M - benchmark * function with 10m records.
func BenchmarkSQLAll_10M(b *testing.B) {
	benchmarkSQLAll(b, 10*humanize.MiByte)
}

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
	"fmt"
	"reflect"
	"testing"

	"github.com/minio/minio/pkg/s3select/format"
)

// Unit Test for the checkForDuplicates function.
func TestCheckForDuplicates(t *testing.T) {
	tables := []struct {
		myReq     []string
		myHeaders map[string]int
		myDup     map[string]bool
		myLow     map[string]int
		myErr     error
	}{
		{[]string{"name", "id", "last_name", "last_name"}, make(map[string]int), make(map[string]bool), make(map[string]int), ErrAmbiguousFieldName},
		{[]string{"name", "id", "last_name", "another_name"}, make(map[string]int), make(map[string]bool), make(map[string]int), nil},
	}

	for _, table := range tables {
		err := checkForDuplicates(table.myReq, table.myHeaders, table.myDup, table.myLow)
		if err != table.myErr {
			t.Error()
		}
	}
}

// This function returns the index of a string in a list
func stringIndex(a string, list []string) int {
	for i, v := range list {
		if v == a {
			return i
		}
	}
	return -1
}

// TestMyHelperFunctions is a unit test which tests some small helper string
// functions.
func TestMyHelperFunctions(t *testing.T) {
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

// TestMyStateMachine is a unit test which ensures that the lowest level of the
// interpreter is converting properly.
func TestMyStateMachine(t *testing.T) {
	tables := []struct {
		operand  interface{}
		operator string
		leftArg  string
		err      error
		expected bool
	}{
		{"2005", ">", "2012", nil, true},
		{2005, ">", "2012", nil, true},
		{2012.0000, ">", "2014.000", nil, true},
		{"NA", ">", "2014.000", nil, false},
		{2014, ">", "Random", nil, false},
		{"test3", ">", "aandom", nil, false},
	}
	for _, table := range tables {
		val, err := evaluateOperator(table.leftArg, table.operator, table.operand)
		if err != table.err {
			t.Error()
		}
		if val != table.expected {
			t.Error()
		}
	}
}

// TestMyOperators is a unit test which ensures that the appropriate values are
// being returned from the operators functions.
func TestMyOperators(t *testing.T) {
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

// TestMyConversion ensures that the conversion of the value from the csv
// happens correctly.
func TestMyConversion(t *testing.T) {
	tables := []struct {
		myTblVal string
		expected reflect.Kind
	}{
		{"2014", reflect.Int},
		{"2014.000", reflect.Float64},
		{"String!!!", reflect.String},
	}
	for _, table := range tables {
		val := reflect.ValueOf(checkStringType(table.myTblVal)).Kind()
		if val != table.expected {
			t.Error()
		}
	}
}

// Unit tests for the main function that performs aggreggation.
func TestMyAggregationFunc(t *testing.T) {
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
		record         string
		err            error
		expectedVal    float64
	}{
		{10, 5, []float64{10, 11, 12, 13, 14}, columnsMap, []string{"Col1"}, []string{"count"}, "{\"Col1\":\"1\",\"Col2\":\"2\"}", nil, 11},
		{10, 5, []float64{10}, columnsMap, []string{"Col1"}, []string{"min"}, "{\"Col1\":\"1\",\"Col2\":\"2\"}", nil, 1},
		{10, 5, []float64{10}, columnsMap, []string{"Col1"}, []string{"max"}, "{\"Col1\":\"1\",\"Col2\":\"2\"}", nil, 10},
		{10, 5, []float64{10}, columnsMap, []string{"Col1"}, []string{"sum"}, "{\"Col1\":\"1\",\"Col2\":\"2\"}", nil, 11},
		{1, 1, []float64{10}, columnsMap, []string{"Col1"}, []string{"avg"}, "{\"Col1\":\"1\",\"Col2\":\"2\"}", nil, 5.500},
		{10, 5, []float64{0.0000}, columnsMap, []string{"Col1"}, []string{"random"}, "{\"Col1\":\"1\",\"Col2\":\"2\"}", ErrParseNonUnaryAgregateFunctionCall, 0},
		{0, 5, []float64{0}, columnsMap, []string{"0"}, []string{"count"}, "{\"Col1\":\"1\",\"Col2\":\"2\"}", nil, 1},
		{10, 5, []float64{10}, columnsMap, []string{"1"}, []string{"min"}, "{\"_1\":\"1\",\"_2\":\"2\"}", nil, 1},
	}

	for _, table := range tables {
		err := aggregationFunctions(table.counter, table.filtrCount, table.myAggVals, table.storeReqCols, table.storeFunctions, table.record)
		if table.err != err {
			t.Error()
		}
		if table.myAggVals[0] != table.expectedVal {
			t.Error()
		}

	}
}

// TestMyStringComparator is a unit test which ensures that the appropriate
// values are being compared for strings.
func TestMyStringComparator(t *testing.T) {
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

// TestMyFloatComparator is a unit test which ensures that the appropriate
// values are being compared for floats.
func TestMyFloatComparator(t *testing.T) {
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

// TestMyIntComparator is a unit test which ensures that the appropriate values
// are being compared for ints.
func TestMyIntComparator(t *testing.T) {
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

// TestMySizeFunction is a function which provides unit testing for the function
// which calculates size.
func TestMySizeFunction(t *testing.T) {
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

// TestMyFuncProcessing is a unit test which ensures that the appropriate values are
// being returned from the Processing... functions.
func TestMyFuncProcessing(t *testing.T) {
	tables := []struct {
		myString    string
		nullList    []string
		coalList    []string
		myValString string
		myValCoal   string
		myValNull   string
		stringFunc  string
	}{
		{"lower", []string{"yo", "yo"}, []string{"random", "hello", "random"}, "LOWER", "random", "", "UPPER"},
		{"LOWER", []string{"null", "random"}, []string{"missing", "hello", "random"}, "lower", "hello", "null", "LOWER"},
	}
	for _, table := range tables {
		if table.coalList != nil {
			myVal := processCoalNoIndex(table.coalList)
			if myVal != table.myValCoal {
				t.Error()
			}
		}
		if table.nullList != nil {
			myVal := processNullIf(table.nullList)
			if myVal != table.myValNull {
				t.Error()
			}
		}
		myVal := applyStrFunc(table.myString, table.stringFunc)
		if myVal != table.myValString {
			t.Error()
		}

	}
}

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
	"fmt"
	"reflect"
	"testing"
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

// Test for the function which processes columnnames to make sure that they are
// compatible with spaces.
func TestMyProcessing(t *testing.T) {
	options := &Options{
		HasHeader:            false,
		RecordDelimiter:      "\n",
		FieldDelimiter:       ",",
		Comments:             "",
		Name:                 "S3Object", // Default table name for all objects
		ReadFrom:             bytes.NewReader([]byte("Here , is,  a,  string + \n + random,random,stuff,stuff ")),
		Compressed:           "",
		Expression:           "",
		OutputFieldDelimiter: ",",
		StreamSize:           20,
	}
	s3s, err := NewInput(options)
	if err != nil {
		t.Error(err)
	}
	tables := []struct {
		myReq      []string
		myHeaders  map[string]int
		myDup      map[string]bool
		myLow      map[string]int
		myOpts     *Options
		input      *Input
		length     int
		testOutput string
		myErr      error
	}{
		{[]string{"name", "id", "last_name", "CAST"}, make(map[string]int), make(map[string]bool), make(map[string]int), options, s3s, 4, "CAST", nil},
		{[]string{"name", "id", "last_name", "another_name"}, make(map[string]int), make(map[string]bool), make(map[string]int), options, s3s, 4, "another_name", nil},
		{[]string{"name", "id", "last_name", "another_name"}, make(map[string]int), make(map[string]bool), make(map[string]int), options, s3s, 4, "another_name", nil},
		{[]string{"name", "id", "random_name", "fame_name", "another_col"}, make(map[string]int), make(map[string]bool), make(map[string]int), options, s3s, 5, "fame_name", nil},
	}
	for _, table := range tables {
		err = checkForDuplicates(table.myReq, table.myHeaders, table.myDup, table.myLow)
		if err != table.myErr {
			t.Error()
		}
		if len(table.myReq) != table.length {
			t.Errorf("UnexpectedError")
		}
		if table.myReq[3] != table.testOutput {
			t.Error()
		}
	}
}

// TestMyRowIndexResults is a unit test which makes sure that the rows that are
// being printed are appropriate to the query being requested.
func TestMyRowIndexResults(t *testing.T) {
	options := &Options{
		HasHeader:            false,
		RecordDelimiter:      "\n",
		FieldDelimiter:       ",",
		Comments:             "",
		Name:                 "S3Object", // Default table name for all objects
		ReadFrom:             bytes.NewReader([]byte("Here , is,  a,  string + \n + random,random,stuff,stuff ")),
		Compressed:           "",
		Expression:           "",
		OutputFieldDelimiter: ",",
		StreamSize:           20,
	}
	s3s, err := NewInput(options)
	if err != nil {
		t.Error(err)
	}
	tables := []struct {
		myReq     []string
		myHeaders map[string]int
		myDup     map[string]bool
		myLow     map[string]int
		myOpts    *Options
		input     *Input
		myRecord  []string
		myTarget  string
		myAsterix string
		columns   []string
		err       error
	}{
		{[]string{"1", "2"}, make(map[string]int), make(map[string]bool), make(map[string]int), options, s3s, []string{"target", "random", "hello", "stuff"}, "target,random", "target,random,hello,stuff", []string{"1", "2", "3", "4"}, nil},
		{[]string{"2", "3", "4"}, make(map[string]int), make(map[string]bool), make(map[string]int), options, s3s, []string{"random", "hullo", "thing", "stuff"}, "hullo,thing,stuff", "random,hullo,thing,stuff", []string{"1", "2", "3", "4"}, nil},
		{[]string{"3", "2"}, make(map[string]int), make(map[string]bool), make(map[string]int), options, s3s, []string{"random", "hullo", "thing", "stuff"}, "thing,hullo", "random,hullo,thing,stuff", []string{"1", "2", "3", "4"}, nil},
		{[]string{"11", "1"}, make(map[string]int), make(map[string]bool), make(map[string]int), options, s3s, []string{"random", "hullo", "thing", "stuff"}, "", "random,hullo,thing,stuff", []string{"1", "2", "3", "4"}, ErrInvalidColumnIndex},
	}
	for _, table := range tables {
		checkForDuplicates(table.columns, table.myHeaders, table.myDup, table.myLow)
		myRow, err := s3s.processColNameIndex(table.myRecord, table.myReq, table.columns)
		if err != table.err {
			t.Error()
		}
		if myRow != table.myTarget {
			t.Error()
		}
		myRow = table.input.printAsterix(table.myRecord)
		if myRow != table.myAsterix {
			t.Error()
		}
	}
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
		if stringInSlice(table.myReq, table.myList) != table.expected {
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

// Unit Tests for Parser.
func TestMyParser(t *testing.T) {
	tables := []struct {
		myQuery  string
		err      error
		reqCols  []string
		alias    string
		myLimit  int
		aggFuncs []string
		header   []string
	}{
		{"SELECT * FROM S3OBJECT", nil, []string{"*"}, "S3OBJECT", 0, make([]string, 1), []string{"name1", "name2", "name3", "name4"}},
		{"SELECT * FROM S3OBJECT AS A", nil, []string{"*"}, "A", 0, make([]string, 1), []string{"name1", "name2", "name3", "name4"}},
		{"SELECT col_name FROM S3OBJECT AS A", nil, []string{"col_name"}, "A", 0, make([]string, 1), []string{"col_name", "name2", "name3", "name4"}},
		{"SELECT col_name,col_other FROM S3OBJECT AS A LIMIT 5", nil, []string{"col_name", "col_other"}, "A", 5, make([]string, 2), []string{"col_name", "col_other", "name3", "name4"}},
		{"SELECT col_name,col_other FROM S3OBJECT AS A WHERE col_name = 'Name' LIMIT 5", nil, []string{"col_name", "col_other"}, "A", 5, make([]string, 2), []string{"col_name", "col_other", "name3", "name4"}},
		{"SELECT col_name,col_other FROM S3OBJECT AS A WHERE col_name = 'Name LIMIT 5", ErrLexerInvalidChar, nil, "", 0, nil, []string{"col_name", "col_other", "name3", "name4"}},
		{"SELECT count(*) FROM S3OBJECT AS A WHERE col_name = 'Name' LIMIT 5", nil, []string{"*"}, "A", 5, []string{"count"}, []string{"col_name", "col_other", "name3", "name4"}},
		{"SELECT sum(col_name),sum(col_other) FROM S3OBJECT AS A WHERE col_name = 'Name' LIMIT 5", nil, []string{"col_name", "col_other"}, "A", 5, []string{"sum", "sum"}, []string{"col_name", "col_other"}},
		{"SELECT A.col_name FROM S3OBJECT AS A", nil, []string{"col_name"}, "A", 0, make([]string, 1), []string{"col_name", "col_other", "name3", "name4"}},
		{"SELECT A._col_name FROM S3OBJECT AS A", nil, []string{"col_name"}, "A", 0, make([]string, 1), []string{"col_name", "col_other", "name3", "name4"}},
		{"SELECT A._col_name FROM S3OBJECT AS A WHERE randomname > 5", ErrMissingHeaders, nil, "", 0, nil, []string{"col_name", "col_other", "name3", "name4"}},
		{"SELECT A._col_name FROM S3OBJECT AS A WHERE A._11 > 5", ErrInvalidColumnIndex, nil, "", 0, nil, []string{"col_name", "col_other", "name3", "name4"}},
		{"SELECT COALESCE(col_name,col_other) FROM S3OBJECT AS A WHERE A._3 > 5", nil, []string{""}, "A", 0, []string{""}, []string{"col_name", "col_other", "name3", "name4"}},
		{"SELECT COALESCE(col_name,col_other),COALESCE(col_name,col_other) FROM S3OBJECT AS A WHERE A._3 > 5", nil, []string{"", ""}, "A", 0, []string{"", ""}, []string{"col_name", "col_other", "name3", "name4"}},
		{"SELECT COALESCE(col_name,col_other) ,col_name , COALESCE(col_name,col_other) FROM S3OBJECT AS A WHERE col_name > 5", nil, []string{"", "col_name", ""}, "A", 0, []string{"", "", ""}, []string{"col_name", "col_other", "name3", "name4"}},
		{"SELECT NULLIF(col_name,col_other) ,col_name , COALESCE(col_name,col_other) FROM S3OBJECT AS A WHERE col_name > 5", nil, []string{"", "col_name", ""}, "A", 0, []string{"", "", ""}, []string{"col_name", "col_other", "name3", "name4"}},
		{"SELECT NULLIF(col_name,col_other) FROM S3OBJECT AS A WHERE col_name > 5", nil, []string{""}, "A", 0, []string{""}, []string{"col_name", "col_other", "name3", "name4"}},
		{"SELECT NULLIF(randomname,col_other) FROM S3OBJECT AS A WHERE col_name > 5", ErrMissingHeaders, nil, "", 0, nil, []string{"col_name", "col_other", "name3", "name4"}},
		{"SELECT col_name FROM S3OBJECT AS A WHERE COALESCE(random,5) > 5", ErrMissingHeaders, nil, "", 0, nil, []string{"col_name", "col_other", "name3", "name4"}},
		{"SELECT col_name FROM S3OBJECT AS A WHERE NULLIF(random,5) > 5", ErrMissingHeaders, nil, "", 0, nil, []string{"col_name", "col_other", "name3", "name4"}},
		{"SELECT col_name FROM S3OBJECT AS A WHERE LOWER(col_name) BETWEEN 5 AND 7", nil, []string{"col_name"}, "A", 0, []string{""}, []string{"col_name", "col_other", "name3", "name4"}},
		{"SELECT UPPER(col_name) FROM S3OBJECT AS A WHERE LOWER(col_name) BETWEEN 5 AND 7", nil, []string{""}, "A", 0, []string{""}, []string{"col_name", "col_other", "name3", "name4"}},
		{"SELECT UPPER(*) FROM S3OBJECT AS A WHERE LOWER(col_name) BETWEEN 5 AND 7", ErrParseUnsupportedCallWithStar, nil, "", 0, nil, []string{"col_name", "col_other", "name3", "name4"}},
		{"SELECT NULLIF(col_name,col_name) FROM S3OBJECT AS A WHERE NULLIF(LOWER(col_name),col_name) BETWEEN 5 AND 7", nil, []string{""}, "A", 0, []string{""}, []string{"col_name", "col_other", "name3", "name4"}},
		{"SELECT COALESCE(col_name,col_name) FROM S3OBJECT AS A WHERE NULLIF(LOWER(col_name),col_name) BETWEEN 5 AND 7", nil, []string{""}, "A", 0, []string{""}, []string{"col_name", "col_other", "name3", "name4"}},
	}
	for _, table := range tables {
		options := &Options{
			HasHeader:            false,
			RecordDelimiter:      "\n",
			FieldDelimiter:       ",",
			Comments:             "",
			Name:                 "S3Object", // Default table name for all objects
			ReadFrom:             bytes.NewReader([]byte("name1,name2,name3,name4" + "\n" + "5,is,a,string" + "\n" + "random,random,stuff,stuff")),
			Compressed:           "",
			Expression:           "",
			OutputFieldDelimiter: ",",
			StreamSize:           20,
			HeaderOpt:            true,
		}
		s3s, err := NewInput(options)
		if err != nil {
			t.Error(err)
		}
		s3s.header = table.header
		reqCols, alias, myLimit, _, aggFunctionNames, _, err := s3s.ParseSelect(table.myQuery)
		if table.err != err {
			t.Error()
		}
		if !reflect.DeepEqual(reqCols, table.reqCols) {
			t.Error()
		}
		if alias != table.alias {
			t.Error()
		}
		if myLimit != int64(table.myLimit) {
			t.Error()
		}
		if !reflect.DeepEqual(table.aggFuncs, aggFunctionNames) {
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
		record         []string
		err            error
		expectedVal    float64
	}{
		{10, 5, []float64{10}, columnsMap, []string{"Col1"}, []string{"count"}, []string{"1", "2"}, nil, 11},
		{10, 5, []float64{10}, columnsMap, []string{"Col1"}, []string{"min"}, []string{"1", "2"}, nil, 1},
		{10, 5, []float64{10}, columnsMap, []string{"Col1"}, []string{"max"}, []string{"1", "2"}, nil, 10},
		{10, 5, []float64{10}, columnsMap, []string{"Col1"}, []string{"sum"}, []string{"1", "2"}, nil, 11},
		{1, 1, []float64{10}, columnsMap, []string{"Col1"}, []string{"avg"}, []string{"1", "2"}, nil, 5.500},
		{10, 5, []float64{0.000}, columnsMap, []string{"Col1"}, []string{"random"}, []string{"1", "2"}, ErrParseNonUnaryAgregateFunctionCall, 0},
		{0, 5, []float64{0}, columnsMap, []string{"0"}, []string{"count"}, []string{"1", "2"}, nil, 1},
		{10, 5, []float64{10}, columnsMap, []string{"1"}, []string{"min"}, []string{"1", "12"}, nil, 1},
	}
	for _, table := range tables {
		err := aggregationFunctions(table.counter, table.filtrCount, table.myAggVals, table.columnsMap, table.storeReqCols, table.storeFunctions, table.record)
		if table.err != err {
			t.Error()
		}
		if table.myAggVals[0] != table.expectedVal {
			t.Error()
		}

	}
}

// Unit Tests for the function which converts a float array to string.
func TestToStringAgg(t *testing.T) {
	options := &Options{
		HasHeader:            false,
		RecordDelimiter:      "\n",
		FieldDelimiter:       ",",
		Comments:             "",
		Name:                 "S3Object", // Default table name for all objects
		ReadFrom:             bytes.NewReader([]byte("Here , is,  a,  string + \n + random,random,stuff,stuff ")),
		Compressed:           "",
		Expression:           "",
		OutputFieldDelimiter: ",",
		StreamSize:           20,
		HeaderOpt:            true,
	}
	s3s, err := NewInput(options)
	if err != nil {
		t.Error(err)
	}
	tables := []struct {
		myAggVal []float64
		expected string
	}{
		{[]float64{10, 11, 12, 13, 14}, "10,11,12,13,14"},
		{[]float64{10, 11.3, 12, 13, 14}, "10,11.300000,12,13,14"},
		{[]float64{10.235, 11.3, 12, 13, 14}, "10.235000,11.300000,12,13,14"},
		{[]float64{10.235, 11.3, 12.123, 13.456, 14.789}, "10.235000,11.300000,12.123000,13.456000,14.789000"},
		{[]float64{10}, "10"},
	}
	for _, table := range tables {
		val := s3s.aggFuncToStr(table.myAggVal)
		if val != table.expected {
			t.Error()
		}
	}
}

// TestMyRowColLiteralResults is a unit test which makes sure that the rows that
// are being printed are appropriate to the query being requested.
func TestMyRowColLiteralResults(t *testing.T) {
	options := &Options{
		HasHeader:            false,
		RecordDelimiter:      "\n",
		FieldDelimiter:       ",",
		Comments:             "",
		Name:                 "S3Object", // Default table name for all objects
		ReadFrom:             bytes.NewReader([]byte("Here , is,  a,  string + \n + random,random,stuff,stuff ")),
		Compressed:           "",
		Expression:           "",
		OutputFieldDelimiter: ",",
		StreamSize:           20,
		HeaderOpt:            true,
	}
	s3s, err := NewInput(options)
	if err != nil {
		t.Error(err)
	}
	tables := []struct {
		myReq     []string
		myHeaders map[string]int
		myDup     map[string]bool
		myLow     map[string]int
		myOpts    *Options
		tempList  []string
		input     *Input
		myRecord  []string
		myTarget  string
		columns   []string
		err       error
	}{
		{[]string{"draft", "year"}, make(map[string]int), make(map[string]bool), make(map[string]int), options, []string{"draft", "year"}, s3s, []string{"target", "random", "hello", "stuff"}, "target,random", []string{"draft", "year", "random", "another"}, nil},
		{[]string{"year", "draft"}, make(map[string]int), make(map[string]bool), make(map[string]int), options, []string{"year", "draft"}, s3s, []string{"draft", "2012", "thing", "stuff"}, "2012,draft", []string{"draft", "year", "random", "another"}, nil},
		{[]string{"yearrandomstuff", "draft"}, make(map[string]int), make(map[string]bool), make(map[string]int), options, []string{"yearrandomstuff", "draft"}, s3s, []string{"draft", "2012", "thing", "stuff"}, "", []string{"draft", "year", "random", "another"}, ErrMissingHeaders},
		{[]string{"draft", "randomstuff"}, make(map[string]int), make(map[string]bool), make(map[string]int), options, []string{"yearrandomstuff", "draft"}, s3s, []string{"draft", "2012", "thing", "stuff"}, "", []string{"draft", "year", "random", "another"}, ErrMissingHeaders},
	}
	for _, table := range tables {
		checkForDuplicates(table.columns, table.myHeaders, table.myDup, table.myLow)
		myRow, err := table.input.processColNameLiteral(table.myRecord, table.myReq, table.tempList, table.myHeaders, nil)
		if err != table.err {
			t.Error()
		}
		if myRow != table.myTarget {
			t.Error()
		}
	}
}

// TestMyWhereEval is a function which provides unit tests for the function
// which evaluates the where clause.
func TestMyWhereEval(t *testing.T) {
	columnsMap := make(map[string]int)
	columnsMap["Col1"] = 0
	columnsMap["Col2"] = 1
	tables := []struct {
		myQuery  string
		record   []string
		err      error
		expected bool
		header   []string
	}{
		{"SELECT * FROM S3OBJECT", []string{"record_1,record_2,record_3,record_4"}, nil, true, []string{"Col1", "Col2"}},
		{"SELECT * FROM S3OBJECT WHERE Col1 < -1", []string{"0", "1"}, nil, false, []string{"Col1", "Col2"}},
		{"SELECT * FROM S3OBJECT WHERE Col1 < -1 OR Col2 > 15", []string{"151", "12"}, nil, false, []string{"Col1", "Col2"}},
		{"SELECT * FROM S3OBJECT WHERE Col1 > -1 AND Col2 > 15", []string{"151", "12"}, nil, false, []string{"Col1", "Col2"}},
		{"SELECT * FROM S3OBJECT WHERE Col1 > 1.00", []string{"151.0000", "12"}, nil, true, []string{"Col1", "Col2"}},
		{"SELECT * FROM S3OBJECT WHERE Col1 > 100", []string{"random", "12"}, nil, false, []string{"Col1", "Col2"}},
		{"SELECT * FROM S3OBJECT WHERE Col1 BETWEEN 100 AND 0", []string{"151", "12"}, nil, false, []string{"Col1", "Col2"}},
		{"SELECT * FROM S3OBJECT WHERE Col1 BETWEEN 100.0 AND 0.0", []string{"151", "12"}, nil, false, []string{"Col1", "Col2"}},
		{"SELECT * FROM S3OBJECT AS A WHERE A.1 BETWEEN 160 AND 150", []string{"151", "12"}, nil, true, []string{"Col1", "Col2"}},
		{"SELECT * FROM S3OBJECT AS A WHERE A._1 BETWEEN 160 AND 0", []string{"151", "12"}, nil, true, []string{"Col1", "Col2"}},
		{"SELECT * FROM S3OBJECT AS A WHERE A._1 BETWEEN 0 AND 160", []string{"151", "12"}, nil, true, []string{"Col1", "Col2"}},
		{"SELECT * FROM S3OBJECT A._1 LIKE 'r%'", []string{"record_1,record_2,record_3,record_4"}, nil, true, []string{"Col1", "Col2"}},
		{"SELECT s._2 FROM S3Object s WHERE s._2 = 'Steven'", []string{"record_1", "Steven", "Steven", "record_4"}, nil, true, []string{"Col1", "Col2"}},
		{"SELECT * FROM S3OBJECT AS A WHERE Col1 BETWEEN 0 AND 160", []string{"151", "12"}, nil, true, []string{"Col1", "Col2"}},
		{"SELECT * FROM S3OBJECT AS A WHERE Col1 BETWEEN 160 AND 0", []string{"151", "12"}, nil, true, []string{"Col1", "Col2"}},
		{"SELECT * FROM S3OBJECT AS A WHERE UPPER(Col1) BETWEEN 160 AND 0", []string{"151", "12"}, nil, true, []string{"Col1", "Col2"}},
		{"SELECT * FROM S3OBJECT AS A WHERE UPPER(Col1) = 'RANDOM'", []string{"random", "12"}, nil, true, []string{"Col1", "Col2"}},
		{"SELECT * FROM S3OBJECT AS A WHERE LOWER(UPPER(Col1) = 'random'", []string{"random", "12"}, nil, true, []string{"Col1", "Col2"}},
	}
	for _, table := range tables {
		options := &Options{
			HasHeader:            false,
			RecordDelimiter:      "\n",
			FieldDelimiter:       ",",
			Comments:             "",
			Name:                 "S3Object", // Default table name for all objects
			ReadFrom:             bytes.NewReader([]byte("name1,name2,name3,name4" + "\n" + "5,is,a,string" + "\n" + "random,random,stuff,stuff")),
			Compressed:           "",
			Expression:           "",
			OutputFieldDelimiter: ",",
			StreamSize:           20,
			HeaderOpt:            true,
		}
		s3s, err := NewInput(options)
		s3s.header = table.header

		if err != nil {
			t.Error(err)
		}
		_, alias, _, whereClause, _, _, _ := s3s.ParseSelect(table.myQuery)
		myVal, err := matchesMyWhereClause(table.record, columnsMap, alias, whereClause)
		if table.err != err {
			t.Error()
		}
		if myVal != table.expected {
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
		if processSize(table.myRecord) != table.expected {
			t.Error()
		}

	}
}

// TestInterpreter is a function which provides unit testing for the main
// interpreter function.
func TestInterpreter(t *testing.T) {
	tables := []struct {
		myQuery string
		myChan  chan *Row
		err     error
		header  []string
	}{
		{"Select random from S3OBJECT", make(chan *Row), ErrMissingHeaders, []string{"name1", "name2", "name3", "name4"}},
		{"Select * from S3OBJECT as A WHERE name2 > 5.00", make(chan *Row), nil, []string{"name1", "name2", "name3", "name4"}},
		{"Select * from S3OBJECT", make(chan *Row), nil, []string{"name1", "name2", "name3", "name4"}},
		{"Select A_1 from S3OBJECT as A", make(chan *Row), nil, []string{"1", "2", "3", "4"}},
		{"Select count(*) from S3OBJECT", make(chan *Row), nil, []string{"name1", "name2", "name3", "name4"}},
		{"Select * from S3OBJECT WHERE name1 > 5.00", make(chan *Row), nil, []string{"name1", "name2", "name3", "name4"}},
	}
	for _, table := range tables {
		options := &Options{
			HasHeader:            false,
			RecordDelimiter:      "\n",
			FieldDelimiter:       ",",
			Comments:             "",
			Name:                 "S3Object", // Default table name for all objects
			ReadFrom:             bytes.NewReader([]byte("name1,name2,name3,name4" + "\n" + "5,is,a,string" + "\n" + "random,random,stuff,stuff")),
			Compressed:           "",
			Expression:           "",
			OutputFieldDelimiter: ",",
			StreamSize:           20,
			HeaderOpt:            true,
		}
		s3s, err := NewInput(options)
		if err != nil {
			t.Error(err)
		}
		s3s.header = table.header
		reqCols, alias, myLimit, whereClause, aggFunctionNames, _, err := s3s.ParseSelect(table.myQuery)
		if err != table.err {
			t.Fatal()
		}
		if err == nil {
			go s3s.processSelectReq(reqCols, alias, whereClause, myLimit, aggFunctionNames, table.myChan, nil)
			select {
			case row, ok := <-table.myChan:
				if ok && len(row.record) > 0 {
				} else if ok && row.err != nil {
					if row.err != table.err {
						t.Error()
					}
					close(table.myChan)
				} else if !ok {
				}
			}
		}
	}
}

// TestMyXMLFunction is a function that provides unit testing for the XML
// creating function.
func TestMyXMLFunction(t *testing.T) {
	options := &Options{
		HasHeader:            false,
		RecordDelimiter:      "\n",
		FieldDelimiter:       ",",
		Comments:             "",
		Name:                 "S3Object", // Default table name for all objects
		ReadFrom:             bytes.NewReader([]byte("name1,name2,name3,name4" + "\n" + "5,is,a,string" + "\n" + "random,random,stuff,stuff")),
		Compressed:           "",
		Expression:           "",
		OutputFieldDelimiter: ",",
		StreamSize:           20,
		HeaderOpt:            true,
	}
	s3s, err := NewInput(options)
	if err != nil {
		t.Error(err)
	}
	tables := []struct {
		expectedStat     int
		expectedProgress int
	}{
		{150, 156},
	}
	for _, table := range tables {
		myVal, _ := s3s.createStatXML()
		myOtherVal, _ := s3s.createProgressXML()
		if len(myVal) != table.expectedStat {
			t.Error()
		}
		if len(myOtherVal) != table.expectedProgress {
			fmt.Println(len(myOtherVal))
			t.Error()
		}
	}
}

// TestMyProtocolFunction is a function which provides unit testing for several
// of the functions which write the binary protocol.
func TestMyProtocolFunction(t *testing.T) {
	options := &Options{
		HasHeader:            false,
		RecordDelimiter:      "\n",
		FieldDelimiter:       ",",
		Comments:             "",
		Name:                 "S3Object", // Default table name for all objects
		ReadFrom:             bytes.NewReader([]byte("name1,name2,name3,name4" + "\n" + "5,is,a,string" + "\n" + "random,random,stuff,stuff")),
		Compressed:           "",
		Expression:           "",
		OutputFieldDelimiter: ",",
		StreamSize:           20,
		HeaderOpt:            true,
	}
	s3s, err := NewInput(options)
	if err != nil {
		t.Error(err)
	}
	tables := []struct {
		payloadMsg     string
		expectedRecord int
		expectedEnd    int
	}{
		{"random payload", 115, 56},
	}
	for _, table := range tables {
		var currentMessage = &bytes.Buffer{}
		if len(s3s.writeRecordMessage(table.payloadMsg, currentMessage).Bytes()) != table.expectedRecord {
			t.Error()
		}
		currentMessage.Reset()
		if len(s3s.writeEndMessage(currentMessage).Bytes()) != table.expectedEnd {
			t.Error()
		}
		currentMessage.Reset()
		if len(s3s.writeContinuationMessage(currentMessage).Bytes()) != 57 {
			t.Error()
		}
		currentMessage.Reset()
	}
}

// TestMyInfoProtocolFunctions is a function which provides unit testing for the
// stat and progress messages of the protocols.
func TestMyInfoProtocolFunctions(t *testing.T) {
	options := &Options{
		HasHeader:            true,
		RecordDelimiter:      "\n",
		FieldDelimiter:       ",",
		Comments:             "",
		Name:                 "S3Object", // Default table name for all objects
		ReadFrom:             bytes.NewReader([]byte("name1,name2,name3,name4" + "\n" + "5,is,a,string" + "\n" + "random,random,stuff,stuff")),
		Compressed:           "",
		Expression:           "",
		OutputFieldDelimiter: ",",
		StreamSize:           20,
	}
	s3s, err := NewInput(options)
	if err != nil {
		t.Error(err)
	}
	myVal, _ := s3s.createStatXML()
	myOtherVal, _ := s3s.createProgressXML()

	tables := []struct {
		payloadStatMsg     string
		payloadProgressMsg string
		expectedStat       int
		expectedProgress   int
	}{
		{myVal, myOtherVal, 233, 243},
	}
	for _, table := range tables {
		var currBuf = &bytes.Buffer{}
		if len(s3s.writeStatMessage(table.payloadStatMsg, currBuf).Bytes()) != table.expectedStat {
			t.Error()
		}
		currBuf.Reset()
		if len(s3s.writeProgressMessage(table.payloadProgressMsg, currBuf).Bytes()) != table.expectedProgress {
			t.Error()
		}
	}
}

// TestMyErrorProtocolFunctions is a function which provides unit testing for
// the error message type of protocol.
func TestMyErrorProtocolFunctions(t *testing.T) {
	options := &Options{
		HasHeader:            false,
		RecordDelimiter:      "\n",
		FieldDelimiter:       ",",
		Comments:             "",
		Name:                 "S3Object", // Default table name for all objects
		ReadFrom:             bytes.NewReader([]byte("name1,name2,name3,name4" + "\n" + "5,is,a,string" + "\n" + "random,random,stuff,stuff")),
		Compressed:           "",
		Expression:           "",
		OutputFieldDelimiter: ",",
		StreamSize:           20,
		HeaderOpt:            true,
	}
	s3s, err := NewInput(options)
	if err != nil {
		t.Error(err)
	}
	tables := []struct {
		err           error
		expectedError int
	}{
		{ErrInvalidCast, 248},
		{ErrTruncatedInput, 200},
		{ErrUnsupportedSyntax, 114},
		{ErrCSVParsingError, 157},
	}
	for _, table := range tables {
		var currentMessage = &bytes.Buffer{}
		if len(s3s.writeErrorMessage(table.err, currentMessage).Bytes()) != table.expectedError {
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

// TestMyValids is a unit test which ensures that the appropriate values are
// being returned from the isValid... functions.
func TestMyValids(t *testing.T) {

	tables := []struct {
		myQuery    string
		indexList  []int
		myIndex    int
		myValIndex bool
		header     []string
		err        error
	}{
		{"SELECT UPPER(NULLIF(draft_year,random_name))", []int{3, 5, 6, 7, 8, 9}, 3, true, []string{"draft_year", "random_name"}, nil},
		{"SELECT UPPER(NULLIF(draft_year,xandom_name))", []int{3, 5, 6, 7, 8, 9}, 3, true, []string{"draft_year", "random_name"}, ErrMissingHeaders},
	}
	for _, table := range tables {
		options := &Options{
			HasHeader:            false,
			RecordDelimiter:      "\n",
			FieldDelimiter:       ",",
			Comments:             "",
			Name:                 "S3Object", // Default table name for all objects
			ReadFrom:             bytes.NewReader([]byte("name1,name2,name3,name4" + "\n" + "5,is,a,string" + "\n" + "random,random,stuff,stuff")),
			Compressed:           "",
			Expression:           "",
			OutputFieldDelimiter: ",",
			StreamSize:           20,
			HeaderOpt:            true,
		}
		s3s, err := NewInput(options)
		if err != nil {
			t.Error(err)
		}
		s3s.header = table.header
		_, _, _, _, _, _, err = s3s.ParseSelect(table.myQuery)
		if err != table.err {
			t.Fatal()
		}
		myVal := isValidFunc(table.indexList, table.myIndex)
		if myVal != table.myValIndex {
			t.Error()
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

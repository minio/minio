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

package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"os"
	"regexp"
	"strconv"
	"testing"
	"time"

	log "github.com/sirupsen/logrus"
)

var (
	bucket = flag.String("bucket", "",
		"bucket to test with if empty random bucket will be created")
	accessKey      = flag.String("access_key", "AKIDZfbOAo7cllgPvF9cXFrJD0a1ICvR98JM", "access key")
	secretKey      = flag.String("secret_key", "3yhFgx31rWzGKCuSq34yjLpBLRSHtIUU", "secret key")
	region         = flag.String("region", "us-east-1", "region")
	endpoint       = flag.String("endpoint", "http://127.0.0.1:9000", "endpoint")
	forcePathStyle = flag.Bool("force_path_style", false, "force path style")
)

type T struct{}

func (t *T) MatchString(pat, str string) (bool, error) {
	re, err := regexp.Compile(pat)
	if err != nil {
		return false, err
	}
	return re.MatchString(str), nil
}

func (t *T) StartCPUProfile(w io.Writer) error           { return errors.New("not support") }
func (t *T) StopCPUProfile()                             {}
func (t *T) WriteProfileTo(string, io.Writer, int) error { return errors.New("not support") }
func (t *T) ImportPath() string                          { return "" }
func (t *T) StartTestLog(io.Writer)                      {}
func (t *T) StopTestLog() error                          { return errors.New("not support") }

type Result struct {
	Payload    string // the payload we expect
	StatusCode string // the http status code we expect, for example 200 or 400
	ErrorCode  string // the error code we expect, for example EvaluatorNegativeLimit
}

type TestItem struct {
	Format     string // the format of the object
	Key        string // object key this test item will run on
	Content    string // object content this test will select upon
	Expression string // the sql expression to execute
	Expects    []Result
	Comment    string // comment for each test item

	Args map[string]string
}

func runTestItem(testItem *TestItem) Result {
	var result Result

	if testItem.Format == "JSON" {
		payload, statusCode, err := selectJsonObject(testItem.Key, testItem.Content, testItem.Expression, testItem.Args)
		result.Payload = payload
		result.StatusCode = strconv.Itoa(statusCode)
		if err != nil {
			result.ErrorCode = err.Error()
		}
	} else {
		payload, statusCode, err := selectCsvObject(testItem.Key, testItem.Content, testItem.Expression, testItem.Args)
		result.Payload = payload
		result.StatusCode = strconv.Itoa(statusCode)
		if err != nil {
			result.ErrorCode = err.Error()
		}
	}

	return result
}

func runTestCase(name string, f func() []TestItem) func(t *testing.T) {
	return func(t *testing.T) {
		testItems := f()

		failed := false
		for _, testItem := range testItems {
			startTime := time.Now()
			result := runTestItem(&testItem)
			if !checkTestItemResult(testItem.Expects, result) {
				//t.Logf("test item %v failed with result: %v", testItem, result)
				failureLog(name, map[string]interface{}{
					"format":     testItem.Format,
					"key":        testItem.Key,
					"content":    testItem.Content,
					"expression": testItem.Expression,
					"expects":    testItem.Expects,
					"args":       testItem.Args,
					"result":     result,
				}, startTime).Fatal()
				failed = true
			} else {
				successLogger(name, map[string]interface{}{
					"format":     testItem.Format,
					"key":        testItem.Key,
					"content":    testItem.Content,
					"expression": testItem.Expression,
					"expects":    testItem.Expects,
					"args":       testItem.Args,
					"result":     result,
				}, startTime).Info()
			}
		}

		if failed {
			t.Errorf("%s failed", name)
		}
	}
}

func checkTestItemResult(expects []Result, r Result) bool {
	for _, expect := range expects {
		if r.Payload == expect.Payload &&
			r.ErrorCode == expect.ErrorCode &&
			(expect.StatusCode == "" || r.StatusCode == expect.StatusCode) {
			return true
		}
	}

	return false
}

// s3select_testing is a small program for testing s3select basic functions
// and for testing compatibility between minio and aws s3.  It can run on
// minio or run on aws s3, so we can compare the results.
//
// Example:
//   run on aws s3:
//   s3select_testing -endpoint="" -bucket="YOUR BUCKET NAME" -region="YOUR BUCKET REGION" -access_key="YOUR ACCESS KEY" -secret_key="YOUR SECRET KEY"
//
//   only run TestJson_Normal on aws s3:
//   s3select_testing -endpoint="" -bucket="YOUR BUCKET NAME" -region="YOUR BUCKET REGION" -access_key="YOUR ACCESS KEY" -secret_key="YOUR SECRET KEY" -test.run="TestJson_Normal"
//
//   run on minio:
//   s3select_testing -endpoint=http://127.0.0.1:9000 -access_key="YOUR ACCESS KEY" -secret_key="YOUR SECRET KEY" -force_path_style
func main() {
	testing.Init()
	flag.Parse()

	if *bucket == "" {
		*bucket = randString(60, rand.NewSource(time.Now().UnixNano()), "s3select-testing-")
		err := createBucket(*bucket)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to create bucket: %v", err)
			os.Exit(125)
		}
		defer deleteBucket(*bucket)
	}

	// Output to stdout instead of the default stderr
	log.SetOutput(os.Stdout)
	// create custom formatter
	mintFormatter := mintJSONFormatter{}
	// set custom formatter
	log.SetFormatter(&mintFormatter)
	// log Info or above -- success cases are Info level, failures are Fatal level
	log.SetLevel(log.InfoLevel)

	// See: https://stackoverflow.com/questions/46205923/how-do-i-use-the-testing-package-in-go-playground
	testSuite := []testing.InternalTest{
		{
			Name: "TestCsv_AllowQuotedRecordDelimiter",
			F:    runTestCase("TestCsv_AllowQuotedRecordDelimiter", TestCsv_AllowQuotedRecordDelimiter),
		},
		{
			Name: "TestCsv_Unnormal",
			F:    runTestCase("TestCsv_Unnormal", TestCsv_Unnormal),
		},
		{
			Name: "TestCsv_EmptyRecord",
			F:    runTestCase("TestCsv_EmptyRecord", TestCsv_EmptyRecord),
		},
		{
			Name: "TestCsv_SelectColumn_ColumnHeaders",
			F:    runTestCase("TestCsv_SelectColumn_ColumnHeaders", TestCsv_SelectColumn_ColumnHeaders),
		},
		{
			Name: "TestCsv_SelectColumn_ColumnNumbers",
			F:    runTestCase("TestCsv_SelectColumn_ColumnNumbers", TestCsv_SelectColumn_ColumnNumbers),
		},
		{
			Name: "TestCsv_SelectColumn_Where",
			F:    runTestCase("TestCsv_SelectColumn_Where", TestCsv_SelectColumn_Where),
		},
		{
			Name: "TestCsv_SelectColumn_Limit",
			F:    runTestCase("TestCsv_SelectColumn_Limit", TestCsv_SelectColumn_Limit),
		},
		{
			Name: "TestCsv_Quotes",
			F:    runTestCase("TestCsv_Quotes", TestCsv_Quotes),
		},
		{
			Name: "TestCsv_Compression",
			F:    runTestCase("TestCsv_Compression", TestCsv_Compression),
		},
		{
			Name: "TestCsv_RecordDelimiter",
			F:    runTestCase("TestCsv_RecordDelimiter", TestCsv_RecordDelimiter),
		},
		{
			Name: "TestJson_Normal",
			F:    runTestCase("TestJson_Normal", TestJson_Normal),
		},
		{
			Name: "TestAccessLog_Normal",
			F:    runTestCase("TestAccessLog_Normal", TestAccessLog_Normal),
		},
		{
			Name: "TestSqlFunctions_Normal",
			F:    runTestCase("TestSqlFunctions_Normal", TestSqlFunctions_Normal),
		},
	}

	os.Exit(testing.MainStart(&T{}, testSuite, nil, nil).Run())
}

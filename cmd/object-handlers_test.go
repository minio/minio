/*
 * Minio Cloud Storage, (C) 2016 Minio, Inc.
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

package cmd

import (
	"bytes"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
)

// Wrapper for calling GetObject API handler tests for both XL multiple disks and FS single drive setup.
func TestAPIGetOjectHandler(t *testing.T) {
	ExecObjectLayerTest(t, testAPIGetOjectHandler)
}

func testAPIGetOjectHandler(obj ObjectLayer, instanceType string, t TestErrHandler) {

	// get random bucket name.
	bucketName := getRandomBucketName()
	objectName := "test-object"
	// Create bucket.
	err := obj.MakeBucket(bucketName)
	if err != nil {
		// failed to create newbucket, abort.
		t.Fatalf("%s : %s", instanceType, err)
	}
	// Register the API end points with XL/FS object layer.
	// Registering only the GetObject handler.
	apiRouter := initTestAPIEndPoints(obj, []string{"GetObject"})
	// initialize the server and obtain the credentials and root.
	// credentials are necessary to sign the HTTP request.
	rootPath, err := newTestConfig("us-east-1")
	if err != nil {
		t.Fatalf("Init Test config failed")
	}
	// remove the root folder after the test ends.
	defer removeAll(rootPath)

	credentials := serverConfig.GetCredential()

	// set of byte data for PutObject.
	// object has to be inserted before running tests for GetObject.
	// this is required even to assert the GetObject data,
	// since dataInserted === dataFetched back is a primary criteria for any object storage this assertion is critical.
	bytesData := []struct {
		byteData []byte
	}{
		{generateBytesData(6 * 1024 * 1024)},
	}
	// set of inputs for uploading the objects before tests for downloading is done.
	putObjectInputs := []struct {
		bucketName    string
		objectName    string
		contentLength int64
		textData      []byte
		metaData      map[string]string
	}{
		// case - 1.
		{bucketName, objectName, int64(len(bytesData[0].byteData)), bytesData[0].byteData, make(map[string]string)},
	}
	// iterate through the above set of inputs and upload the object.
	for i, input := range putObjectInputs {
		// uploading the object.
		_, err = obj.PutObject(input.bucketName, input.objectName, input.contentLength, bytes.NewBuffer(input.textData), input.metaData)
		// if object upload fails stop the test.
		if err != nil {
			t.Fatalf("Put Object case %d:  Error uploading object: <ERROR> %v", i+1, err)
		}
	}

	// test cases with inputs and expected result for GetObject.
	testCases := []struct {
		bucketName string
		objectName string
		byteRange  string // range of bytes to be fetched from GetObject.
		// expected output.
		expectedContent    []byte // expected response body.
		expectedRespStatus int    // expected response status body.
	}{
		// Test case - 1.
		// Fetching the entire object and validating its contents.
		{
			bucketName:         bucketName,
			objectName:         objectName,
			byteRange:          "",
			expectedContent:    bytesData[0].byteData,
			expectedRespStatus: http.StatusOK,
		},
		// Test case - 2.
		// Case with non-existent object name.
		{
			bucketName:         bucketName,
			objectName:         "abcd",
			byteRange:          "",
			expectedContent:    encodeResponse(getAPIErrorResponse(getAPIError(ErrNoSuchKey), getGetObjectURL("", bucketName, "abcd"))),
			expectedRespStatus: http.StatusNotFound,
		},
		// Test case - 3.
		// Requesting from range 10-100.
		{
			bucketName:         bucketName,
			objectName:         objectName,
			byteRange:          "bytes=10-100",
			expectedContent:    bytesData[0].byteData[10:101],
			expectedRespStatus: http.StatusPartialContent,
		},
		// Test case - 4.
		// Test case with invalid range.
		{
			bucketName:         bucketName,
			objectName:         objectName,
			byteRange:          "bytes=-0",
			expectedContent:    encodeResponse(getAPIErrorResponse(getAPIError(ErrInvalidRange), getGetObjectURL("", bucketName, objectName))),
			expectedRespStatus: http.StatusRequestedRangeNotSatisfiable,
		},
		// Test case - 5.
		// Test case with byte range exceeding the object size.
		// Expected to read till end of the object.
		{
			bucketName:         bucketName,
			objectName:         objectName,
			byteRange:          "bytes=10-1000000000000000",
			expectedContent:    bytesData[0].byteData[10:],
			expectedRespStatus: http.StatusPartialContent,
		},
	}
	// Iterating over the cases, fetching the object validating the response.
	for i, testCase := range testCases {
		// initialize HTTP NewRecorder, this records any mutations to response writer inside the handler.
		rec := httptest.NewRecorder()
		// construct HTTP request for Get Object end point.
		req, err := newTestSignedRequest("GET", getGetObjectURL("", testCase.bucketName, testCase.objectName),
			0, nil, credentials.AccessKeyID, credentials.SecretAccessKey)

		if err != nil {
			t.Fatalf("Test %d: Failed to create HTTP request for Get Object: <ERROR> %v", i+1, err)
		}
		if testCase.byteRange != "" {
			req.Header.Add("Range", testCase.byteRange)
		}
		// Since `apiRouter` satisfies `http.Handler` it has a ServeHTTP to execute the logic of the handler.
		// Call the ServeHTTP to execute the handler,`func (api objectAPIHandlers) GetObjectHandler`  handles the request.
		apiRouter.ServeHTTP(rec, req)
		// Assert the response code with the expected status.
		if rec.Code != testCase.expectedRespStatus {
			t.Fatalf("Case %d: Expected the response status to be `%d`, but instead found `%d`", i+1, testCase.expectedRespStatus, rec.Code)
		}
		// read the response body.
		actualContent, err := ioutil.ReadAll(rec.Body)
		if err != nil {
			t.Fatalf("Test %d: %s: Failed parsing response body: <ERROR> %v", i+1, instanceType, err)
		}
		// Verify whether the bucket obtained object is same as the one inserted.
		if !bytes.Equal(testCase.expectedContent, actualContent) {
			t.Errorf("Test %d: %s: Object content differs from expected value.: %s", i+1, instanceType, string(actualContent))
		}
	}
}

// Wrapper for calling Copy Object API handler tests for both XL multiple disks and single node setup.
func TestAPICopyObjectHandler(t *testing.T) {
	ExecObjectLayerTest(t, testAPICopyObjectHandler)
}

func testAPICopyObjectHandler(obj ObjectLayer, instanceType string, t TestErrHandler) {
	// get random bucket name.
	bucketName := getRandomBucketName()
	objectName := "test-object"
	// Create bucket.
	err := obj.MakeBucket(bucketName)
	if err != nil {
		// failed to create newbucket, abort.
		t.Fatalf("%s : %s", instanceType, err)
	}
	// Register the API end points with XL/FS object layer.
	// Registering only the Copy Object handler.
	apiRouter := initTestAPIEndPoints(obj, []string{"CopyObject"})
	// initialize the server and obtain the credentials and root.
	// credentials are necessary to sign the HTTP request.
	rootPath, err := newTestConfig("us-east-1")
	if err != nil {
		t.Fatalf("Init Test config failed")
	}
	// remove the root folder after the test ends.
	defer removeAll(rootPath)

	err = initEventNotifier(obj)
	if err != nil {
		t.Fatalf("Initializing event notifiers failed")
	}

	credentials := serverConfig.GetCredential()

	// set of byte data for PutObject.
	// object has to be inserted before running tests for Copy Object.
	// this is required even to assert the copied object,
	bytesData := []struct {
		byteData []byte
	}{
		{generateBytesData(6 * 1024 * 1024)},
	}

	buffers := []*bytes.Buffer{
		new(bytes.Buffer),
		new(bytes.Buffer),
	}

	// set of inputs for uploading the objects before tests for downloading is done.
	putObjectInputs := []struct {
		bucketName    string
		objectName    string
		contentLength int64
		textData      []byte
		metaData      map[string]string
	}{
		// case - 1.
		{bucketName, objectName, int64(len(bytesData[0].byteData)), bytesData[0].byteData, make(map[string]string)},
	}
	// iterate through the above set of inputs and upload the object.
	for i, input := range putObjectInputs {
		// uploading the object.
		_, err = obj.PutObject(input.bucketName, input.objectName, input.contentLength, bytes.NewBuffer(input.textData), input.metaData)
		// if object upload fails stop the test.
		if err != nil {
			t.Fatalf("Put Object case %d:  Error uploading object: <ERROR> %v", i+1, err)
		}
	}

	// test cases with inputs and expected result for Copy Object.
	testCases := []struct {
		bucketName       string
		newObjectName    string // name of the newly copied object.
		copySourceHeader string // data for "X-Amz-Copy-Source" header. Contains the object to be copied in the URL.
		// expected output.
		expectedRespStatus int
	}{
		// Test case - 1.
		{
			bucketName:         bucketName,
			newObjectName:      "newObject1",
			copySourceHeader:   url.QueryEscape("/" + bucketName + "/" + objectName),
			expectedRespStatus: http.StatusOK,
		},

		// Test case - 2.
		// Test case with invalid source object.
		{
			bucketName:         bucketName,
			newObjectName:      "newObject1",
			copySourceHeader:   url.QueryEscape("/"),
			expectedRespStatus: http.StatusBadRequest,
		},
		// Test case - 3.
		// Test case with new object name is same as object to be copied.
		{
			bucketName:         bucketName,
			newObjectName:      objectName,
			copySourceHeader:   url.QueryEscape("/" + bucketName + "/" + objectName),
			expectedRespStatus: http.StatusBadRequest,
		},
		// Test case - 4.
		// Test case with non-existent source file.
		// Case for the purpose of failing `api.ObjectAPI.GetObjectInfo`.
		// Expecting the response status code to http.StatusNotFound (404).
		{
			bucketName:         bucketName,
			newObjectName:      objectName,
			copySourceHeader:   url.QueryEscape("/" + bucketName + "/" + "non-existent-object"),
			expectedRespStatus: http.StatusNotFound,
		},
		// Test case - 5.
		// Test case with non-existent source file.
		// Case for the purpose of failing `api.ObjectAPI.PutObject`.
		// Expecting the response status code to http.StatusNotFound (404).
		{
			bucketName:         "non-existent-destination-bucket",
			newObjectName:      objectName,
			copySourceHeader:   url.QueryEscape("/" + bucketName + "/" + objectName),
			expectedRespStatus: http.StatusNotFound,
		},
	}

	for i, testCase := range testCases {
		// initialize HTTP NewRecorder, this records any mutations to response writer inside the handler.
		rec := httptest.NewRecorder()
		// construct HTTP request for copy object.
		req, err := newTestSignedRequest("PUT", getCopyObjectURL("", testCase.bucketName, testCase.newObjectName),
			0, nil, credentials.AccessKeyID, credentials.SecretAccessKey)

		if err != nil {
			t.Fatalf("Test %d: Failed to create HTTP request for copy Object: <ERROR> %v", i+1, err)
		}
		// "X-Amz-Copy-Source" header contains the information about the source bucket and the object to copied.
		if testCase.copySourceHeader != "" {
			req.Header.Set("X-Amz-Copy-Source", testCase.copySourceHeader)
		}
		// Since `apiRouter` satisfies `http.Handler` it has a ServeHTTP to execute the logic of the handler.
		// Call the ServeHTTP to execute the handler, `func (api objectAPIHandlers) CopyObjectHandler` handles the request.
		apiRouter.ServeHTTP(rec, req)
		// Assert the response code with the expected status.
		if rec.Code != testCase.expectedRespStatus {
			t.Fatalf("Test %d: %s:  Expected the response status to be `%d`, but instead found `%d`", i+1, instanceType, testCase.expectedRespStatus, rec.Code)
		}
		if rec.Code == http.StatusOK {
			// See if the new object is formed.
			// testing whether the copy was successful.
			err = obj.GetObject(testCase.bucketName, testCase.newObjectName, 0, int64(len(bytesData[0].byteData)), buffers[0])
			if err != nil {
				t.Fatalf("Test %d: %s: Failed to fetch the copied object: <ERROR> %s", i+1, instanceType, err)
			}
			if !bytes.Equal(bytesData[0].byteData, buffers[0].Bytes()) {
				t.Errorf("Test %d: %s: Data Mismatch: Data fetched back from the copied object doesn't match the original one.", i+1, instanceType)
			}
			buffers[0].Reset()
		}
	}
}

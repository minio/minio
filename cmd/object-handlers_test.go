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
	"crypto/md5"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"sync"
	"testing"
	"time"
)

// Type to capture different modifications to API request to simulate failure cases.
type Fault int

const (
	None Fault = iota
	MissingContentLength
	TooBigObject
	TooBigDecodedLength
	BadSignature
	BadMD5
	MissingUploadID
)

// Wrapper for calling GetObject API handler tests for both XL multiple disks and FS single drive setup.
func TestAPIGetObjectHandler(t *testing.T) {
	ExecObjectLayerAPITest(t, testAPIGetObjectHandler, []string{"GetObject"})
}

func testAPIGetObjectHandler(obj ObjectLayer, instanceType, bucketName string, apiRouter http.Handler,
	credentials credential, t *testing.T) {
	objectName := "test-object"
	// set of byte data for PutObject.
	// object has to be created before running tests for GetObject.
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
	sha256sum := ""
	// iterate through the above set of inputs and upload the object.
	for i, input := range putObjectInputs {
		// uploading the object.
		_, err := obj.PutObject(input.bucketName, input.objectName, input.contentLength, bytes.NewBuffer(input.textData), input.metaData, sha256sum)
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
		req, err := newTestSignedRequestV4("GET", getGetObjectURL("", testCase.bucketName, testCase.objectName),
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
		// Verify whether the bucket obtained object is same as the one created.
		if !bytes.Equal(testCase.expectedContent, actualContent) {
			t.Errorf("Test %d: %s: Object content differs from expected value.: %s", i+1, instanceType, string(actualContent))
		}
	}

	// Test for Anonymous/unsigned http request.
	anonReq, err := newTestRequest("GET", getGetObjectURL("", bucketName, objectName), 0, nil)

	if err != nil {
		t.Fatalf("Minio %s: Failed to create an anonymous request for %s/%s: <ERROR> %v",
			instanceType, bucketName, objectName, err)
	}

	// ExecObjectLayerAPIAnonTest - Calls the HTTP API handler using the anonymous request, validates the ErrAccessDeniedResponse,
	// sets the bucket policy using the policy statement generated from `getWriteOnlyObjectStatement` so that the
	// unsigned request goes through and its validated again.
	ExecObjectLayerAPIAnonTest(t, "TestAPIGetObjectHandler", bucketName, objectName, instanceType, apiRouter, anonReq, getReadOnlyObjectStatement)

	// HTTP request for testing when `objectLayer` is set to `nil`.
	// There is no need to use an existing bucket and valid input for creating the request
	// since the `objectLayer==nil`  check is performed before any other checks inside the handlers.
	// The only aim is to generate an HTTP request in a way that the relevant/registered end point is evoked/called.

	nilBucket := "dummy-bucket"
	nilObject := "dummy-object"
	nilReq, err := newTestSignedRequestV4("GET", getGetObjectURL("", nilBucket, nilObject),
		0, nil, "", "")

	if err != nil {
		t.Errorf("Minio %s: Failed to create HTTP request for testing the response when object Layer is set to `nil`.", instanceType)
	}
	// execute the object layer set to `nil` test.
	// `ExecObjectLayerAPINilTest` manages the operation.
	ExecObjectLayerAPINilTest(t, nilBucket, nilObject, instanceType, apiRouter, nilReq)
}

// Wrapper for calling PutObject API handler tests using streaming signature v4 for both XL multiple disks and FS single drive setup.
func TestAPIPutObjectStreamSigV4Handler(t *testing.T) {
	ExecObjectLayerAPITest(t, testAPIPutObjectStreamSigV4Handler, []string{"PutObject"})
}

func testAPIPutObjectStreamSigV4Handler(obj ObjectLayer, instanceType, bucketName string, apiRouter http.Handler,
	credentials credential, t *testing.T) {

	objectName := "test-object"
	bytesDataLen := 65 * 1024
	bytesData := bytes.Repeat([]byte{'a'}, bytesDataLen)
	oneKData := bytes.Repeat([]byte("a"), 1024)

	err := initEventNotifier(obj)
	if err != nil {
		t.Fatalf("[%s] - Failed to initialize event notifiers <ERROR> %v", instanceType, err)

	}
	type streamFault int
	const (
		None streamFault = iota
		malformedEncoding
		unexpectedEOF
		signatureMismatch
		chunkDateMismatch
	)

	// byte data for PutObject.
	// test cases with inputs and expected result for GetObject.
	testCases := []struct {
		bucketName string
		objectName string
		data       []byte
		dataLen    int
		chunkSize  int64
		// expected output.
		expectedContent    []byte // expected response body.
		expectedRespStatus int    // expected response status body.
		// Access keys
		accessKey        string
		secretKey        string
		shouldPass       bool
		removeAuthHeader bool
		fault            streamFault
	}{
		// Test case - 1.
		// Fetching the entire object and validating its contents.
		{
			bucketName:         bucketName,
			objectName:         objectName,
			data:               bytesData,
			dataLen:            len(bytesData),
			chunkSize:          64 * 1024, // 64k
			expectedContent:    []byte{},
			expectedRespStatus: http.StatusOK,
			accessKey:          credentials.AccessKeyID,
			secretKey:          credentials.SecretAccessKey,
			shouldPass:         true,
		},
		// Test case - 2
		// Small chunk size.
		{
			bucketName:         bucketName,
			objectName:         objectName,
			data:               bytesData,
			dataLen:            len(bytesData),
			chunkSize:          1 * 1024, // 1k
			expectedContent:    []byte{},
			expectedRespStatus: http.StatusOK,
			accessKey:          credentials.AccessKeyID,
			secretKey:          credentials.SecretAccessKey,
			shouldPass:         true,
		},
		// Test case - 3
		// Invalid access key id.
		{
			bucketName:         bucketName,
			objectName:         objectName,
			data:               bytesData,
			dataLen:            len(bytesData),
			chunkSize:          64 * 1024, // 64k
			expectedContent:    []byte{},
			expectedRespStatus: http.StatusForbidden,
			accessKey:          "",
			secretKey:          "",
			shouldPass:         false,
		},
		// Test case - 4
		// Wrong auth header returns as bad request.
		{
			bucketName:         bucketName,
			objectName:         objectName,
			data:               bytesData,
			dataLen:            len(bytesData),
			chunkSize:          64 * 1024, // 64k
			expectedContent:    []byte{},
			expectedRespStatus: http.StatusBadRequest,
			accessKey:          credentials.AccessKeyID,
			secretKey:          credentials.SecretAccessKey,
			shouldPass:         false,
			removeAuthHeader:   true,
		},
		// Test case - 5
		// Large chunk size.. also passes.
		{
			bucketName:         bucketName,
			objectName:         objectName,
			data:               bytesData,
			dataLen:            len(bytesData),
			chunkSize:          100 * 1024, // 100k
			expectedContent:    []byte{},
			expectedRespStatus: http.StatusOK,
			accessKey:          credentials.AccessKeyID,
			secretKey:          credentials.SecretAccessKey,
			shouldPass:         false,
		},
		// Test case - 6
		// Chunk with malformed encoding.
		{
			bucketName:         bucketName,
			objectName:         objectName,
			data:               oneKData,
			dataLen:            1024,
			chunkSize:          1024,
			expectedContent:    []byte{},
			expectedRespStatus: http.StatusInternalServerError,
			accessKey:          credentials.AccessKeyID,
			secretKey:          credentials.SecretAccessKey,
			shouldPass:         false,
			fault:              malformedEncoding,
		},
		// Test case - 7
		// Chunk with shorter than advertised chunk data.
		{
			bucketName:         bucketName,
			objectName:         objectName,
			data:               oneKData,
			dataLen:            1024,
			chunkSize:          1024,
			expectedContent:    []byte{},
			expectedRespStatus: http.StatusBadRequest,
			accessKey:          credentials.AccessKeyID,
			secretKey:          credentials.SecretAccessKey,
			shouldPass:         false,
			fault:              unexpectedEOF,
		},
		// Test case - 8
		// Chunk with first chunk data byte tampered.
		{
			bucketName:         bucketName,
			objectName:         objectName,
			data:               oneKData,
			dataLen:            1024,
			chunkSize:          1024,
			expectedContent:    []byte{},
			expectedRespStatus: http.StatusForbidden,
			accessKey:          credentials.AccessKeyID,
			secretKey:          credentials.SecretAccessKey,
			shouldPass:         false,
			fault:              signatureMismatch,
		},
		// Test case - 9
		// Different date (timestamps) used in seed signature calculation
		// and chunks signature calculation.
		{
			bucketName:         bucketName,
			objectName:         objectName,
			data:               oneKData,
			dataLen:            1024,
			chunkSize:          1024,
			expectedContent:    []byte{},
			expectedRespStatus: http.StatusForbidden,
			accessKey:          credentials.AccessKeyID,
			secretKey:          credentials.SecretAccessKey,
			shouldPass:         false,
			fault:              chunkDateMismatch,
		},
	}
	// Iterating over the cases, fetching the object validating the response.
	for i, testCase := range testCases {
		// initialize HTTP NewRecorder, this records any mutations to response writer inside the handler.
		rec := httptest.NewRecorder()
		// construct HTTP request for Put Object end point.
		var req *http.Request
		if testCase.fault == chunkDateMismatch {
			req, err = newTestStreamingSignedBadChunkDateRequest("PUT",
				getPutObjectURL("", testCase.bucketName, testCase.objectName),
				int64(testCase.dataLen), testCase.chunkSize, bytes.NewReader(testCase.data),
				testCase.accessKey, testCase.secretKey)

		} else {
			req, err = newTestStreamingSignedRequest("PUT",
				getPutObjectURL("", testCase.bucketName, testCase.objectName),
				int64(testCase.dataLen), testCase.chunkSize, bytes.NewReader(testCase.data),
				testCase.accessKey, testCase.secretKey)
		}
		if err != nil {
			t.Fatalf("Test %d: Failed to create HTTP request for Put Object: <ERROR> %v", i+1, err)
		}
		// Removes auth header if test case requires it.
		if testCase.removeAuthHeader {
			req.Header.Del("Authorization")
		}
		switch testCase.fault {
		case malformedEncoding:
			req, err = malformChunkSizeSigV4(req, testCase.chunkSize-1)
		case signatureMismatch:
			req, err = malformDataSigV4(req, 'z')
		case unexpectedEOF:
			req, err = truncateChunkByHalfSigv4(req)
		}
		// Since `apiRouter` satisfies `http.Handler` it has a ServeHTTP to execute the logic of the handler.
		// Call the ServeHTTP to execute the handler,`func (api objectAPIHandlers) GetObjectHandler`  handles the request.
		apiRouter.ServeHTTP(rec, req)
		// Assert the response code with the expected status.
		if rec.Code != testCase.expectedRespStatus {
			t.Errorf("Test %d %s: Expected the response status to be `%d`, but instead found `%d`",
				i+1, instanceType, testCase.expectedRespStatus, rec.Code)
		}
		// read the response body.
		actualContent, err := ioutil.ReadAll(rec.Body)
		if err != nil {
			t.Fatalf("Test %d: %s: Failed parsing response body: <ERROR> %v", i+1, instanceType, err)
		}
		if testCase.shouldPass {
			// Verify whether the bucket obtained object is same as the one created.
			if !bytes.Equal(testCase.expectedContent, actualContent) {
				t.Errorf("Test %d: %s: Object content differs from expected value.: %s", i+1, instanceType, string(actualContent))
				continue
			}

			buffer := new(bytes.Buffer)
			err = obj.GetObject(testCase.bucketName, testCase.objectName, 0, int64(bytesDataLen), buffer)
			if err != nil {
				t.Fatalf("Test %d: %s: Failed to fetch the copied object: <ERROR> %s", i+1, instanceType, err)
			}
			if !bytes.Equal(bytesData, buffer.Bytes()) {
				t.Errorf("Test %d: %s: Data Mismatch: Data fetched back from the uploaded object doesn't match the original one.", i+1, instanceType)
			}
			buffer.Reset()
		}
	}
}

// Wrapper for calling PutObject API handler tests for both XL multiple disks and FS single drive setup.
func TestAPIPutObjectHandler(t *testing.T) {
	ExecObjectLayerAPITest(t, testAPIPutObjectHandler, []string{"PutObject"})
}

func testAPIPutObjectHandler(obj ObjectLayer, instanceType, bucketName string, apiRouter http.Handler,
	credentials credential, t *testing.T) {

	// register event notifier.
	err := initEventNotifier(obj)

	if err != nil {
		t.Fatal("Notifier initialization failed.")
	}
	objectName := "test-object"
	// byte data for PutObject.
	bytesData := generateBytesData(6 * 1024 * 1024)

	// test cases with inputs and expected result for GetObject.
	testCases := []struct {
		bucketName string
		objectName string
		data       []byte
		dataLen    int
		// expected output.
		expectedContent    []byte // expected response body.
		expectedRespStatus int    // expected response status body.
	}{
		// Test case - 1.
		// Fetching the entire object and validating its contents.
		{
			bucketName:         bucketName,
			objectName:         objectName,
			data:               bytesData,
			dataLen:            len(bytesData),
			expectedContent:    []byte{},
			expectedRespStatus: http.StatusOK,
		},
	}
	// Iterating over the cases, fetching the object validating the response.
	for i, testCase := range testCases {
		var req *http.Request
		// initialize HTTP NewRecorder, this records any mutations to response writer inside the handler.
		rec := httptest.NewRecorder()
		// construct HTTP request for Get Object end point.
		req, err = newTestSignedRequestV4("PUT", getPutObjectURL("", testCase.bucketName, testCase.objectName),
			int64(testCase.dataLen), bytes.NewReader(testCase.data), credentials.AccessKeyID, credentials.SecretAccessKey)
		if err != nil {
			t.Fatalf("Test %d: Failed to create HTTP request for Put Object: <ERROR> %v", i+1, err)
		}
		// Since `apiRouter` satisfies `http.Handler` it has a ServeHTTP to execute the logic of the handler.
		// Call the ServeHTTP to execute the handler,`func (api objectAPIHandlers) GetObjectHandler`  handles the request.
		apiRouter.ServeHTTP(rec, req)
		// Assert the response code with the expected status.
		if rec.Code != testCase.expectedRespStatus {
			t.Fatalf("Case %d: Expected the response status to be `%d`, but instead found `%d`", i+1, testCase.expectedRespStatus, rec.Code)
		}
		// read the response body.
		var actualContent []byte
		actualContent, err = ioutil.ReadAll(rec.Body)
		if err != nil {
			t.Fatalf("Test %d: %s: Failed parsing response body: <ERROR> %v", i+1, instanceType, err)
		}
		// Verify whether the bucket obtained object is same as the one created.
		if !bytes.Equal(testCase.expectedContent, actualContent) {
			t.Errorf("Test %d: %s: Object content differs from expected value.: %s", i+1, instanceType, string(actualContent))
		}

		buffer := new(bytes.Buffer)
		err = obj.GetObject(testCase.bucketName, testCase.objectName, 0, int64(len(bytesData)), buffer)
		if err != nil {
			t.Fatalf("Test %d: %s: Failed to fetch the copied object: <ERROR> %s", i+1, instanceType, err)
		}
		if !bytes.Equal(bytesData, buffer.Bytes()) {
			t.Errorf("Test %d: %s: Data Mismatch: Data fetched back from the uploaded object doesn't match the original one.", i+1, instanceType)
		}
		buffer.Reset()
	}

	// Test for Anonymous/unsigned http request.
	anonReq, err := newTestRequest("PUT", getPutObjectURL("", bucketName, objectName),
		int64(len("hello")), bytes.NewReader([]byte("hello")))
	if err != nil {
		t.Fatalf("Minio %s: Failed to create an anonymous request for %s/%s: <ERROR> %v",
			instanceType, bucketName, objectName, err)
	}

	// ExecObjectLayerAPIAnonTest - Calls the HTTP API handler using the anonymous request, validates the ErrAccessDeniedResponse,
	// sets the bucket policy using the policy statement generated from `getWriteOnlyObjectStatement` so that the
	// unsigned request goes through and its validated again.
	ExecObjectLayerAPIAnonTest(t, "TestAPIPutObjectHandler", bucketName, objectName, instanceType, apiRouter, anonReq, getWriteOnlyObjectStatement)

	// HTTP request to test the case of `objectLayer` being set to `nil`.
	// There is no need to use an existing bucket or valid input for creating the request,
	// since the `objectLayer==nil`  check is performed before any other checks inside the handlers.
	// The only aim is to generate an HTTP request in a way that the relevant/registered end point is evoked/called.
	nilBucket := "dummy-bucket"
	nilObject := "dummy-object"

	nilReq, err := newTestSignedRequestV4("PUT", getPutObjectURL("", nilBucket, nilObject),
		0, nil, "", "")

	if err != nil {
		t.Errorf("Minio %s: Failed to create HTTP request for testing the response when object Layer is set to `nil`.", instanceType)
	}
	// execute the object layer set to `nil` test.
	// `ExecObjectLayerAPINilTest` manages the operation.
	ExecObjectLayerAPINilTest(t, nilBucket, nilObject, instanceType, apiRouter, nilReq)

}

// Wrapper for calling Copy Object API handler tests for both XL multiple disks and single node setup.
func TestAPICopyObjectHandler(t *testing.T) {
	ExecObjectLayerAPITest(t, testAPICopyObjectHandler, []string{"CopyObject"})
}

func testAPICopyObjectHandler(obj ObjectLayer, instanceType, bucketName string, apiRouter http.Handler,
	credentials credential, t *testing.T) {

	objectName := "test-object"
	// object used for anonymous HTTP request test.
	anonObject := "anon-object"
	// register event notifier.
	err := initEventNotifier(obj)
	if err != nil {
		t.Fatalf("Initializing event notifiers failed")
	}

	// set of byte data for PutObject.
	// object has to be created before running tests for Copy Object.
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

		// case - 2.
		// used for anonymous HTTP request test.
		{bucketName, anonObject, int64(len(bytesData[0].byteData)), bytesData[0].byteData, make(map[string]string)},
	}
	sha256sum := ""
	// iterate through the above set of inputs and upload the object.
	for i, input := range putObjectInputs {
		// uploading the object.
		_, err = obj.PutObject(input.bucketName, input.objectName, input.contentLength, bytes.NewBuffer(input.textData), input.metaData, sha256sum)
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
		var req *http.Request
		// initialize HTTP NewRecorder, this records any mutations to response writer inside the handler.
		rec := httptest.NewRecorder()
		// construct HTTP request for copy object.
		req, err = newTestSignedRequestV4("PUT", getCopyObjectURL("", testCase.bucketName, testCase.newObjectName),
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

	// Test for Anonymous/unsigned http request.
	newCopyAnonObject := "new-anon-obj"
	anonReq, err := newTestRequest("PUT", getCopyObjectURL("", bucketName, newCopyAnonObject), 0, nil)
	if err != nil {
		t.Fatalf("Minio %s: Failed to create an anonymous request for %s/%s: <ERROR> %v",
			instanceType, bucketName, "new-anon-obj", err)
	}

	// Below is how CopyObjectHandler is registered.
	// bucket.Methods("PUT").Path("/{object:.+}").HeadersRegexp("X-Amz-Copy-Source", ".*?(\\/|%2F).*?")
	// Its necessary to set the "X-Amz-Copy-Source" header for the request to be accepted by the handler.
	anonReq.Header.Set("X-Amz-Copy-Source", url.QueryEscape("/"+bucketName+"/"+anonObject))
	// ExecObjectLayerAPIAnonTest - Calls the HTTP API handler using the anonymous request, validates the ErrAccessDeniedResponse,
	// sets the bucket policy using the policy statement generated from `getWriteOnlyObjectStatement` so that the
	// unsigned request goes through and its validated again.
	ExecObjectLayerAPIAnonTest(t, "TestAPICopyObjectHandler", bucketName, newCopyAnonObject, instanceType, apiRouter, anonReq, getWriteOnlyObjectStatement)

	// HTTP request to test the case of `objectLayer` being set to `nil`.
	// There is no need to use an existing bucket or valid input for creating the request,
	// since the `objectLayer==nil`  check is performed before any other checks inside the handlers.
	// The only aim is to generate an HTTP request in a way that the relevant/registered end point is evoked/called.
	nilBucket := "dummy-bucket"
	nilObject := "dummy-object"

	nilReq, err := newTestSignedRequestV4("PUT", getCopyObjectURL("", nilBucket, nilObject),
		0, nil, "", "")

	// Below is how CopyObjectHandler is registered.
	// bucket.Methods("PUT").Path("/{object:.+}").HeadersRegexp("X-Amz-Copy-Source", ".*?(\\/|%2F).*?")
	// Its necessary to set the "X-Amz-Copy-Source" header for the request to be accepted by the handler.
	nilReq.Header.Set("X-Amz-Copy-Source", url.QueryEscape("/"+nilBucket+"/"+nilObject))
	if err != nil {
		t.Errorf("Minio %s: Failed to create HTTP request for testing the response when object Layer is set to `nil`.", instanceType)
	}

	// execute the object layer set to `nil` test.
	// `ExecObjectLayerAPINilTest` manages the operation.
	ExecObjectLayerAPINilTest(t, nilBucket, nilObject, instanceType, apiRouter, nilReq)

}

// Wrapper for calling NewMultipartUpload tests for both XL multiple disks and single node setup.
// First register the HTTP handler for NewMutlipartUpload, then a HTTP request for NewMultipart upload is made.
// The UploadID from the response body is parsed and its existence is asserted with an attempt to ListParts using it.
func TestAPINewMultipartHandler(t *testing.T) {
	ExecObjectLayerAPITest(t, testAPINewMultipartHandler, []string{"NewMultipart"})
}

func testAPINewMultipartHandler(obj ObjectLayer, instanceType, bucketName string, apiRouter http.Handler,
	credentials credential, t *testing.T) {

	objectName := "test-object-new-multipart"
	rec := httptest.NewRecorder()
	// construct HTTP request for copy object.
	req, err := newTestSignedRequestV4("POST", getNewMultipartURL("", bucketName, objectName),
		0, nil, credentials.AccessKeyID, credentials.SecretAccessKey)

	if err != nil {
		t.Fatalf("Failed to create HTTP request for copy Object: <ERROR> %v", err)
	}
	// Since `apiRouter` satisfies `http.Handler` it has a ServeHTTP to execute the logic of the handler.
	// Call the ServeHTTP to executes the registered handler.
	apiRouter.ServeHTTP(rec, req)
	// Assert the response code with the expected status.
	if rec.Code != http.StatusOK {
		t.Fatalf("%s:  Expected the response status to be `%d`, but instead found `%d`", instanceType, http.StatusOK, rec.Code)
	}
	// decode the response body.
	decoder := xml.NewDecoder(rec.Body)
	multipartResponse := &InitiateMultipartUploadResponse{}

	err = decoder.Decode(multipartResponse)
	if err != nil {
		t.Fatalf("Error decoding the recorded response Body")
	}
	// verify the uploadID my making an attempt to list parts.
	_, err = obj.ListObjectParts(bucketName, objectName, multipartResponse.UploadID, 0, 1)
	if err != nil {
		t.Fatalf("Invalid UploadID: <ERROR> %s", err)
	}

	// Test for Anonymous/unsigned http request.
	anonReq, err := newTestRequest("POST", getNewMultipartURL("", bucketName, objectName), 0, nil)

	if err != nil {
		t.Fatalf("Minio %s: Failed to create an anonymous request for %s/%s: <ERROR> %v",
			instanceType, bucketName, objectName, err)
	}

	// ExecObjectLayerAPIAnonTest - Calls the HTTP API handler using the anonymous request, validates the ErrAccessDeniedResponse,
	// sets the bucket policy using the policy statement generated from `getWriteOnlyObjectStatement` so that the
	// unsigned request goes through and its validated again.
	ExecObjectLayerAPIAnonTest(t, "TestAPINewMultipartHandler", bucketName, objectName, instanceType, apiRouter, anonReq, getWriteOnlyObjectStatement)

	// HTTP request to test the case of `objectLayer` being set to `nil`.
	// There is no need to use an existing bucket or valid input for creating the request,
	// since the `objectLayer==nil`  check is performed before any other checks inside the handlers.
	// The only aim is to generate an HTTP request in a way that the relevant/registered end point is evoked/called.
	nilBucket := "dummy-bucket"
	nilObject := "dummy-object"

	nilReq, err := newTestSignedRequestV4("POST", getNewMultipartURL("", nilBucket, nilObject),
		0, nil, "", "")

	if err != nil {
		t.Errorf("Minio %s: Failed to create HTTP request for testing the response when object Layer is set to `nil`.", instanceType)
	}
	// execute the object layer set to `nil` test.
	// `ExecObjectLayerAPINilTest` manages the operation.
	ExecObjectLayerAPINilTest(t, nilBucket, nilObject, instanceType, apiRouter, nilReq)

}

// Wrapper for calling NewMultipartUploadParallel tests for both XL multiple disks and single node setup.
// The objective of the test is to initialte multipart upload on the same object 10 times concurrently,
// The UploadID from the response body is parsed and its existence is asserted with an attempt to ListParts using it.
func TestAPINewMultipartHandlerParallel(t *testing.T) {
	ExecObjectLayerAPITest(t, testAPINewMultipartHandlerParallel, []string{"NewMultipart"})
}

func testAPINewMultipartHandlerParallel(obj ObjectLayer, instanceType, bucketName string, apiRouter http.Handler,
	credentials credential, t *testing.T) {
	// used for storing the uploadID's parsed on concurrent HTTP requests for NewMultipart upload on the same object.
	testUploads := struct {
		sync.Mutex
		uploads []string
	}{}

	objectName := "test-object-new-multipart-parallel"
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		// Initiate NewMultipart upload on the same object 10 times concurrrently.
		go func() {
			defer wg.Done()
			rec := httptest.NewRecorder()
			// construct HTTP request for copy object.
			req, err := newTestSignedRequestV4("POST", getNewMultipartURL("", bucketName, objectName), 0, nil, credentials.AccessKeyID, credentials.SecretAccessKey)

			if err != nil {
				t.Fatalf("Failed to create HTTP request for copy Object: <ERROR> %v", err)
			}
			// Since `apiRouter` satisfies `http.Handler` it has a ServeHTTP to execute the logic of the handler.
			// Call the ServeHTTP to executes the registered handler.
			apiRouter.ServeHTTP(rec, req)
			// Assert the response code with the expected status.
			if rec.Code != http.StatusOK {
				t.Fatalf("Minio %s:  Expected the response status to be `%d`, but instead found `%d`", instanceType, http.StatusOK, rec.Code)
			}
			// decode the response body.
			decoder := xml.NewDecoder(rec.Body)
			multipartResponse := &InitiateMultipartUploadResponse{}

			err = decoder.Decode(multipartResponse)
			if err != nil {
				t.Fatalf("Minio %s: Error decoding the recorded response Body", instanceType)
			}
			// push the obtained upload ID from the response into the array.
			testUploads.Lock()
			testUploads.uploads = append(testUploads.uploads, multipartResponse.UploadID)
			testUploads.Unlock()
		}()
	}
	// Wait till all go routines finishes execution.
	wg.Wait()
	// Validate the upload ID by an attempt to list parts using it.
	for _, uploadID := range testUploads.uploads {
		_, err := obj.ListObjectParts(bucketName, objectName, uploadID, 0, 1)
		if err != nil {
			t.Fatalf("Invalid UploadID: <ERROR> %s", err)
		}
	}
}

// The UploadID from the response body is parsed and its existence is asserted with an attempt to ListParts using it.
func TestAPICompleteMultipartHandler(t *testing.T) {
	ExecObjectLayerAPITest(t, testAPICompleteMultipartHandler, []string{"CompleteMultipart"})
}

func testAPICompleteMultipartHandler(obj ObjectLayer, instanceType, bucketName string, apiRouter http.Handler,
	credentials credential, t *testing.T) {

	// Calculates MD5 sum of the given byte array.
	findMD5 := func(toBeHashed []byte) string {
		hasher := md5.New()
		hasher.Write(toBeHashed)
		return hex.EncodeToString(hasher.Sum(nil))
	}
	// object used for the test.
	objectName := "test-object-new-multipart"

	// uploadID obtained from NewMultipart upload.
	var uploadID string
	var err error
	// upload IDs collected.
	var uploadIDs []string

	for i := 0; i < 2; i++ {
		// initiate new multipart uploadID.
		uploadID, err = obj.NewMultipartUpload(bucketName, objectName, nil)
		if err != nil {
			// Failed to create NewMultipartUpload, abort.
			t.Fatalf("Minio %s : <ERROR>  %s", instanceType, err)
		}

		uploadIDs = append(uploadIDs, uploadID)
	}

	// Parts with size greater than 5 MB.
	// Generating a 6MB byte array.
	validPart := bytes.Repeat([]byte("abcdef"), 1024*1024)
	validPartMD5 := findMD5(validPart)
	// Create multipart parts.
	// Need parts to be uploaded before CompleteMultiPartUpload can be called tested.
	parts := []struct {
		bucketName      string
		objName         string
		uploadID        string
		PartID          int
		inputReaderData string
		inputMd5        string
		intputDataSize  int64
	}{
		// Case 1-4.
		// Creating sequence of parts for same uploadID.
		{bucketName, objectName, uploadIDs[0], 1, "abcd", "e2fc714c4727ee9395f324cd2e7f331f", int64(len("abcd"))},
		{bucketName, objectName, uploadIDs[0], 2, "efgh", "1f7690ebdd9b4caf8fab49ca1757bf27", int64(len("efgh"))},
		{bucketName, objectName, uploadIDs[0], 3, "ijkl", "09a0877d04abf8759f99adec02baf579", int64(len("abcd"))},
		{bucketName, objectName, uploadIDs[0], 4, "mnop", "e132e96a5ddad6da8b07bba6f6131fef", int64(len("abcd"))},
		// Part with size larger than 5Mb.
		{bucketName, objectName, uploadIDs[0], 5, string(validPart), validPartMD5, int64(len(string(validPart)))},
		{bucketName, objectName, uploadIDs[0], 6, string(validPart), validPartMD5, int64(len(string(validPart)))},

		// Part with size larger than 5Mb.
		// Parts uploaded for anonymous/unsigned API handler test.
		{bucketName, objectName, uploadIDs[1], 1, string(validPart), validPartMD5, int64(len(string(validPart)))},
		{bucketName, objectName, uploadIDs[1], 2, string(validPart), validPartMD5, int64(len(string(validPart)))},
	}
	// Iterating over creatPartCases to generate multipart chunks.
	for _, part := range parts {
		_, err = obj.PutObjectPart(part.bucketName, part.objName, part.uploadID, part.PartID, part.intputDataSize,
			bytes.NewBufferString(part.inputReaderData), part.inputMd5, "")
		if err != nil {
			t.Fatalf("%s : %s", instanceType, err)
		}
	}
	// Parts to be sent as input for CompleteMultipartUpload.
	inputParts := []struct {
		parts []completePart
	}{
		// inputParts - 0.
		// Case for replicating ETag mismatch.
		{
			[]completePart{
				{ETag: "abcd", PartNumber: 1},
			},
		},
		// inputParts - 1.
		// should error out with part too small.
		{
			[]completePart{
				{ETag: "e2fc714c4727ee9395f324cd2e7f331f", PartNumber: 1},
				{ETag: "1f7690ebdd9b4caf8fab49ca1757bf27", PartNumber: 2},
			},
		},
		// inputParts - 2.
		// Case with invalid Part number.
		{
			[]completePart{
				{ETag: "e2fc714c4727ee9395f324cd2e7f331f", PartNumber: 10},
			},
		},
		// inputParts - 3.
		// Case with valid parts,but parts are unsorted.
		// Part size greater than 5MB.
		{
			[]completePart{
				{ETag: validPartMD5, PartNumber: 6},
				{ETag: validPartMD5, PartNumber: 5},
			},
		},
		// inputParts - 4.
		// Case with valid part.
		// Part size greater than 5MB.
		{
			[]completePart{
				{ETag: validPartMD5, PartNumber: 5},
				{ETag: validPartMD5, PartNumber: 6},
			},
		},

		// inputParts - 5.
		// Used for the case of testing for anonymous API request.
		// Part size greater than 5MB.
		{
			[]completePart{
				{ETag: validPartMD5, PartNumber: 1},
				{ETag: validPartMD5, PartNumber: 2},
			},
		},
	}

	// on succesfull complete multipart operation the s3MD5 for the parts uploaded will be returned.
	s3MD5, err := completeMultipartMD5(inputParts[3].parts...)
	if err != nil {
		t.Fatalf("Obtaining S3MD5 failed")
	}

	// generating the response body content for the success case.
	successResponse := generateCompleteMultpartUploadResponse(bucketName, objectName, getGetObjectURL("", bucketName, objectName), s3MD5)
	encodedSuccessResponse := encodeResponse(successResponse)

	testCases := []struct {
		bucket   string
		object   string
		uploadID string
		parts    []completePart
		// Expected output of CompleteMultipartUpload.
		expectedContent []byte
		// Expected HTTP Response status.
		expectedRespStatus int
	}{
		// Test case - 1.
		// Upload and PartNumber exists, But a deliberate ETag mismatch is introduced.
		{
			bucket:   bucketName,
			object:   objectName,
			uploadID: uploadIDs[0],
			parts:    inputParts[0].parts,
			expectedContent: encodeResponse(getAPIErrorResponse(getAPIError(toAPIErrorCode(BadDigest{})),
				getGetObjectURL("", bucketName, objectName))),
			expectedRespStatus: http.StatusBadRequest,
		},
		// Test case - 2.
		// No parts specified in completePart{}.
		// Should return ErrMalformedXML in the response body.
		{
			bucket:   bucketName,
			object:   objectName,
			uploadID: uploadIDs[0],
			parts:    []completePart{},
			expectedContent: encodeResponse(getAPIErrorResponse(getAPIError(ErrMalformedXML),
				getGetObjectURL("", bucketName, objectName))),
			expectedRespStatus: http.StatusBadRequest,
		},
		// Test case - 3.
		// Non-Existent uploadID.
		// 404 Not Found response status expected.
		{
			bucket:   bucketName,
			object:   objectName,
			uploadID: "abc",
			parts:    inputParts[0].parts,
			expectedContent: encodeResponse(getAPIErrorResponse(getAPIError(toAPIErrorCode(InvalidUploadID{UploadID: "abc"})),
				getGetObjectURL("", bucketName, objectName))),
			expectedRespStatus: http.StatusNotFound,
		},
		// Test case - 4.
		// Case with part size being less than minimum allowed size.
		{
			bucket:   bucketName,
			object:   objectName,
			uploadID: uploadIDs[0],
			parts:    inputParts[1].parts,
			expectedContent: encodeResponse(completeMultipartAPIError{int64(4), int64(5242880), 1, "e2fc714c4727ee9395f324cd2e7f331f",
				getAPIErrorResponse(getAPIError(toAPIErrorCode(PartTooSmall{PartNumber: 1})),
					getGetObjectURL("", bucketName, objectName))}),
			expectedRespStatus: http.StatusBadRequest,
		},
		// Test case - 5.
		// TestCase with invalid Part Number.
		{
			bucket:   bucketName,
			object:   objectName,
			uploadID: uploadIDs[0],
			parts:    inputParts[2].parts,
			expectedContent: encodeResponse(getAPIErrorResponse(getAPIError(toAPIErrorCode(InvalidPart{})),
				getGetObjectURL("", bucketName, objectName))),
			expectedRespStatus: http.StatusBadRequest,
		},
		// Test case - 6.
		// Parts are not sorted according to the part number.
		// This should return ErrInvalidPartOrder in the response body.
		{
			bucket:   bucketName,
			object:   objectName,
			uploadID: uploadIDs[0],
			parts:    inputParts[3].parts,
			expectedContent: encodeResponse(getAPIErrorResponse(getAPIError(ErrInvalidPartOrder),
				getGetObjectURL("", bucketName, objectName))),
			expectedRespStatus: http.StatusBadRequest,
		},
		// Test case - 7.
		// Test case with proper parts.
		// Should successed and the content in the response body is asserted.
		{
			bucket:             bucketName,
			object:             objectName,
			uploadID:           uploadIDs[0],
			parts:              inputParts[4].parts,
			expectedContent:    encodedSuccessResponse,
			expectedRespStatus: http.StatusOK,
		},
	}

	for i, testCase := range testCases {
		var req *http.Request
		var completeBytes, actualContent []byte
		// Complete multipart upload parts.
		completeUploads := &completeMultipartUpload{
			Parts: testCase.parts,
		}
		completeBytes, err = xml.Marshal(completeUploads)
		if err != nil {
			t.Fatalf("Error XML encoding of parts: <ERROR> %s.", err)
		}
		// Indicating that all parts are uploaded and initiating completeMultipartUpload.
		req, err = newTestSignedRequestV4("POST", getCompleteMultipartUploadURL("", bucketName, objectName, testCase.uploadID),
			int64(len(completeBytes)), bytes.NewReader(completeBytes), credentials.AccessKeyID, credentials.SecretAccessKey)
		if err != nil {
			t.Fatalf("Failed to create HTTP request for copy Object: <ERROR> %v", err)
		}
		rec := httptest.NewRecorder()
		// construct HTTP request for copy object.

		// Since `apiRouter` satisfies `http.Handler` it has a ServeHTTP to execute the logic of the handler.
		// Call the ServeHTTP to executes the registered handler.
		apiRouter.ServeHTTP(rec, req)
		// Assert the response code with the expected status.
		if rec.Code != testCase.expectedRespStatus {
			t.Errorf("Case %d: Minio %s: Expected the response status to be `%d`, but instead found `%d`", i+1, instanceType, testCase.expectedRespStatus, rec.Code)
		}

		// read the response body.
		actualContent, err = ioutil.ReadAll(rec.Body)
		if err != nil {
			t.Fatalf("Test %d : Minio %s: Failed parsing response body: <ERROR> %v", i+1, instanceType, err)
		}
		// Verify whether the bucket obtained object is same as the one created.
		if !bytes.Equal(testCase.expectedContent, actualContent) {
			t.Errorf("Test %d : Minio %s: Object content differs from expected value.", i+1, instanceType)
		}
	}

	// Testing for anonymous API request.
	var completeBytes []byte
	// Complete multipart upload parts.
	completeUploads := &completeMultipartUpload{
		Parts: inputParts[5].parts,
	}
	completeBytes, err = xml.Marshal(completeUploads)
	if err != nil {
		t.Fatalf("Error XML encoding of parts: <ERROR> %s.", err)
	}

	// create unsigned HTTP request for CompleteMultipart upload.
	anonReq, err := newTestRequest("POST", getCompleteMultipartUploadURL("", bucketName, objectName, uploadIDs[1]),
		int64(len(completeBytes)), bytes.NewReader(completeBytes))
	if err != nil {
		t.Fatalf("Minio %s: Failed to create an anonymous request for %s/%s: <ERROR> %v",
			instanceType, bucketName, objectName, err)
	}

	// ExecObjectLayerAPIAnonTest - Calls the HTTP API handler using the anonymous request, validates the ErrAccessDeniedResponse,
	// sets the bucket policy using the policy statement generated from `getWriteOnlyObjectStatement` so that the
	// unsigned request goes through and its validated again.
	ExecObjectLayerAPIAnonTest(t, "TestAPICompleteMultipartHandler", bucketName, objectName, instanceType,
		apiRouter, anonReq, getWriteOnlyObjectStatement)

	// HTTP request to test the case of `objectLayer` being set to `nil`.
	// There is no need to use an existing bucket or valid input for creating the request,
	// since the `objectLayer==nil`  check is performed before any other checks inside the handlers.
	// The only aim is to generate an HTTP request in a way that the relevant/registered end point is evoked/called.
	// Indicating that all parts are uploaded and initiating completeMultipartUpload.
	nilBucket := "dummy-bucket"
	nilObject := "dummy-object"

	nilReq, err := newTestSignedRequestV4("POST", getCompleteMultipartUploadURL("", nilBucket, nilObject, "dummy-uploadID"),
		0, nil, "", "")

	if err != nil {
		t.Errorf("Minio %s: Failed to create HTTP request for testing the response when object Layer is set to `nil`.", instanceType)
	}
	// execute the object layer set to `nil` test.
	// `ExecObjectLayerAPINilTest` manages the operation.
	ExecObjectLayerAPINilTest(t, nilBucket, nilObject, instanceType, apiRouter, nilReq)
}

// Wrapper for calling Delete Object API handler tests for both XL multiple disks and FS single drive setup.
func TestAPIDeleteObjectHandler(t *testing.T) {
	ExecObjectLayerAPITest(t, testAPIDeleteObjectHandler, []string{"DeleteObject"})
}

func testAPIDeleteObjectHandler(obj ObjectLayer, instanceType, bucketName string, apiRouter http.Handler,
	credentials credential, t *testing.T) {

	// register event notifier.
	err := initEventNotifier(obj)

	if err != nil {
		t.Fatal("Notifier initialization failed.")
	}

	objectName := "test-object"
	// Object used for anonymous API request test.
	anonObjectName := "test-anon-obj"
	// set of byte data for PutObject.
	// object has to be created before running tests for Deleting the object.
	bytesData := []struct {
		byteData []byte
	}{
		{generateBytesData(6 * 1024 * 1024)},
	}

	// set of inputs for uploading the objects before tests for deleting them is done.
	putObjectInputs := []struct {
		bucketName    string
		objectName    string
		contentLength int64
		textData      []byte
		metaData      map[string]string
	}{
		// case - 1.
		{bucketName, objectName, int64(len(bytesData[0].byteData)), bytesData[0].byteData, make(map[string]string)},
		// case - 2.
		{bucketName, anonObjectName, int64(len(bytesData[0].byteData)), bytesData[0].byteData, make(map[string]string)},
	}
	// iterate through the above set of inputs and upload the object.
	for i, input := range putObjectInputs {
		// uploading the object.
		_, err = obj.PutObject(input.bucketName, input.objectName, input.contentLength, bytes.NewBuffer(input.textData), input.metaData, "")
		// if object upload fails stop the test.
		if err != nil {
			t.Fatalf("Put Object case %d:  Error uploading object: <ERROR> %v", i+1, err)
		}
	}

	// test cases with inputs and expected result for DeleteObject.
	testCases := []struct {
		bucketName string
		objectName string

		expectedRespStatus int // expected response status body.
	}{
		// Test case - 1.
		// Deleting an existing object.
		// Expected to return HTTP resposne status code 204.
		{
			bucketName: bucketName,
			objectName: objectName,

			expectedRespStatus: http.StatusNoContent,
		},
		// Test case - 2.
		// Attempt to delete an object which is already deleted.
		// Still should return http response status 204.
		{
			bucketName: bucketName,
			objectName: objectName,

			expectedRespStatus: http.StatusNoContent,
		},
	}

	// Iterating over the cases, call DeleteObjectHandler and validate the HTTP response.
	for i, testCase := range testCases {
		var req *http.Request
		// initialize HTTP NewRecorder, this records any mutations to response writer inside the handler.
		rec := httptest.NewRecorder()
		// construct HTTP request for Get Object end point.
		req, err = newTestSignedRequestV4("DELETE", getDeleteObjectURL("", testCase.bucketName, testCase.objectName),
			0, nil, credentials.AccessKeyID, credentials.SecretAccessKey)

		if err != nil {
			t.Fatalf("Test %d: Failed to create HTTP request for Get Object: <ERROR> %v", i+1, err)
		}
		// Since `apiRouter` satisfies `http.Handler` it has a ServeHTTP to execute the logic of the handler.
		// Call the ServeHTTP to execute the handler,`func (api objectAPIHandlers) DeleteObjectHandler`  handles the request.
		apiRouter.ServeHTTP(rec, req)
		// Assert the response code with the expected status.
		if rec.Code != testCase.expectedRespStatus {
			t.Fatalf("Minio %s: Case %d: Expected the response status to be `%d`, but instead found `%d`", instanceType, i+1, testCase.expectedRespStatus, rec.Code)
		}
	}

	// Test for Anonymous/unsigned http request.
	anonReq, err := newTestRequest("DELETE", getDeleteObjectURL("", bucketName, anonObjectName), 0, nil)
	if err != nil {
		t.Fatalf("Minio %s: Failed to create an anonymous request for %s/%s: <ERROR> %v",
			instanceType, bucketName, anonObjectName, err)
	}

	// ExecObjectLayerAPIAnonTest - Calls the HTTP API handler using the anonymous request, validates the ErrAccessDeniedResponse,
	// sets the bucket policy using the policy statement generated from `getWriteOnlyObjectStatement` so that the
	// unsigned request goes through and its validated again.
	ExecObjectLayerAPIAnonTest(t, "TestAPIDeleteObjectHandler", bucketName, anonObjectName, instanceType, apiRouter, anonReq, getWriteOnlyObjectStatement)

	// HTTP request to test the case of `objectLayer` being set to `nil`.
	// There is no need to use an existing bucket or valid input for creating the request,
	// since the `objectLayer==nil`  check is performed before any other checks inside the handlers.
	// The only aim is to generate an HTTP request in a way that the relevant/registered end point is evoked/called.
	// Indicating that all parts are uploaded and initiating completeMultipartUpload.
	nilBucket := "dummy-bucket"
	nilObject := "dummy-object"

	nilReq, err := newTestSignedRequestV4("DELETE", getDeleteObjectURL("", nilBucket, nilObject),
		0, nil, "", "")

	if err != nil {
		t.Errorf("Minio %s: Failed to create HTTP request for testing the response when object Layer is set to `nil`.", instanceType)
	}
	// execute the object layer set to `nil` test.
	// `ExecObjectLayerAPINilTest` manages the operation.
	ExecObjectLayerAPINilTest(t, nilBucket, nilObject, instanceType, apiRouter, nilReq)
}

// TestAPIPutObjectPartHandlerPreSign - Tests validate the response of PutObjectPart HTTP handler
// when the request signature type is PreSign.
func TestAPIPutObjectPartHandlerPreSign(t *testing.T) {
	ExecObjectLayerAPITest(t, testAPIPutObjectPartHandlerPreSign, []string{"NewMultipart", "PutObjectPart"})
}

func testAPIPutObjectPartHandlerPreSign(obj ObjectLayer, instanceType, bucketName string, apiRouter http.Handler,
	credentials credential, t *testing.T) {
	testObject := "testobject"
	rec := httptest.NewRecorder()
	req, err := newTestSignedRequestV4("POST", getNewMultipartURL("", bucketName, "testobject"),
		0, nil, credentials.AccessKeyID, credentials.SecretAccessKey)
	if err != nil {
		t.Fatalf("[%s] - Failed to create a signed request to initiate multipart upload for %s/%s: <ERROR> %v",
			instanceType, bucketName, testObject, err)
	}
	apiRouter.ServeHTTP(rec, req)

	// Get uploadID of the mulitpart upload initiated.
	var mpartResp InitiateMultipartUploadResponse
	mpartRespBytes, err := ioutil.ReadAll(rec.Result().Body)
	if err != nil {
		t.Fatalf("[%s] Failed to read NewMultipartUpload response <ERROR> %v", instanceType, err)

	}
	err = xml.Unmarshal(mpartRespBytes, &mpartResp)
	if err != nil {
		t.Fatalf("[%s] Failed to unmarshal NewMultipartUpload response <ERROR> %v", instanceType, err)
	}

	rec = httptest.NewRecorder()
	req, err = newTestRequest("PUT", getPutObjectPartURL("", bucketName, testObject, mpartResp.UploadID, "1"),
		int64(len("hello")), bytes.NewReader([]byte("hello")))
	if err != nil {
		t.Fatalf("[%s] - Failed to create an unsigned request to put object part for %s/%s <ERROR> %v",
			instanceType, bucketName, testObject, err)
	}
	err = preSignV2(req, credentials.AccessKeyID, credentials.SecretAccessKey, int64(10*time.Minute))
	if err != nil {
		t.Fatalf("[%s] - Failed to presign an unsigned request to put object part for %s/%s <ERROR> %v",
			instanceType, bucketName, testObject, err)
	}
	apiRouter.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("Test %d %s expected to succeed but failed with HTTP status code %d", 1, instanceType, rec.Code)
	}
}

// TestAPIPutObjectPartHandlerV2 - Tests validate the response of PutObjectPart HTTP handler
// when the request signature type is V2.
func TestAPIPutObjectPartHandlerV2(t *testing.T) {
	ExecObjectLayerAPITest(t, testAPIPutObjectPartHandlerV2, []string{"NewMultipart", "PutObjectPart"})
}

func testAPIPutObjectPartHandlerV2(obj ObjectLayer, instanceType, bucketName string, apiRouter http.Handler,
	credentials credential, t *testing.T) {
	testObject := "testobject"
	rec := httptest.NewRecorder()
	req, err := newTestSignedRequestV4("POST", getNewMultipartURL("", bucketName, "testobject"),
		0, nil, credentials.AccessKeyID, credentials.SecretAccessKey)
	if err != nil {
		t.Fatalf("[%s] - Failed to create a signed request to initiate multipart upload for %s/%s: <ERROR> %v",
			instanceType, bucketName, testObject, err)
	}
	apiRouter.ServeHTTP(rec, req)

	// Get uploadID of the mulitpart upload initiated.
	var mpartResp InitiateMultipartUploadResponse
	mpartRespBytes, err := ioutil.ReadAll(rec.Result().Body)
	if err != nil {
		t.Fatalf("[%s] Failed to read NewMultipartUpload response <ERROR> %v", instanceType, err)

	}
	err = xml.Unmarshal(mpartRespBytes, &mpartResp)
	if err != nil {
		t.Fatalf("[%s] Failed to unmarshal NewMultipartUpload response <ERROR> %v", instanceType, err)
	}

	rec = httptest.NewRecorder()
	req, err = newTestSignedRequestV2("PUT",
		getPutObjectPartURL("", bucketName, testObject, mpartResp.UploadID, "1"),
		int64(len("hello")), bytes.NewReader([]byte("hello")), credentials.AccessKeyID, credentials.SecretAccessKey)
	if err != nil {
		t.Fatalf("[%s] - Failed to create a signed request to initiate multipart upload for %s/%s: <ERROR> %v",
			instanceType, bucketName, testObject, err)
	}
	signatureMismatchErr := getAPIError(ErrSignatureDoesNotMatch)
	// Reset date field in header to make signature V2 fail.
	req.Header.Set("x-amz-date", "")
	apiRouter.ServeHTTP(rec, req)
	errBytes, err := ioutil.ReadAll(rec.Result().Body)
	if err != nil {
		t.Fatalf("Test %d %s Failed to read error response from upload part request %s/%s: <ERROR> %v",
			1, instanceType, bucketName, testObject, err)
	}
	var errXML APIErrorResponse
	err = xml.Unmarshal(errBytes, &errXML)
	if err != nil {
		t.Fatalf("Test %d %s Failed to unmarshal error response from upload part request %s/%s: <ERROR> %v",
			1, instanceType, bucketName, testObject, err)
	}
	if errXML.Code != signatureMismatchErr.Code {
		t.Errorf("Test %d %s expected to fail with error %s, but received %s", 1, instanceType,
			signatureMismatchErr.Code, errXML.Code)
	}
}

// TestAPIPutObjectPartHandlerStreaming - Tests validate the response of PutObjectPart HTTP handler
// when the request signature type is `streaming signature`.
func TestAPIPutObjectPartHandlerStreaming(t *testing.T) {
	ExecObjectLayerAPITest(t, testAPIPutObjectPartHandlerStreaming, []string{"NewMultipart", "PutObjectPart"})
}

func testAPIPutObjectPartHandlerStreaming(obj ObjectLayer, instanceType, bucketName string, apiRouter http.Handler,
	credentials credential, t *testing.T) {
	testObject := "testobject"
	rec := httptest.NewRecorder()
	req, err := newTestSignedRequestV4("POST", getNewMultipartURL("", bucketName, "testobject"),
		0, nil, credentials.AccessKeyID, credentials.SecretAccessKey)
	if err != nil {
		t.Fatalf("[%s] - Failed to create a signed request to initiate multipart upload for %s/%s: <ERROR> %v",
			instanceType, bucketName, testObject, err)
	}
	apiRouter.ServeHTTP(rec, req)

	// Get uploadID of the mulitpart upload initiated.
	var mpartResp InitiateMultipartUploadResponse
	mpartRespBytes, err := ioutil.ReadAll(rec.Result().Body)
	if err != nil {
		t.Fatalf("[%s] Failed to read NewMultipartUpload response <ERROR> %v", instanceType, err)

	}
	err = xml.Unmarshal(mpartRespBytes, &mpartResp)
	if err != nil {
		t.Fatalf("[%s] Failed to unmarshal NewMultipartUpload response <ERROR> %v", instanceType, err)
	}

	noAPIErr := APIError{}
	missingDateHeaderErr := getAPIError(ErrMissingDateHeader)
	internalErr := getAPIError(ErrInternalError)
	testCases := []struct {
		fault       Fault
		expectedErr APIError
	}{
		{BadSignature, missingDateHeaderErr},
		{None, noAPIErr},
		{TooBigDecodedLength, internalErr},
	}

	for i, test := range testCases {
		rec = httptest.NewRecorder()
		req, err = newTestStreamingSignedRequest("PUT",
			getPutObjectPartURL("", bucketName, testObject, mpartResp.UploadID, "1"),
			5, 1, bytes.NewReader([]byte("hello")), credentials.AccessKeyID, credentials.SecretAccessKey)

		switch test.fault {
		case BadSignature:
			// Reset date field in header to make streaming signature fail.
			req.Header.Set("x-amz-date", "")
		case TooBigDecodedLength:
			// Set decoded length to a large value out of int64 range to simulate parse failure.
			req.Header.Set("x-amz-decoded-content-length", "9999999999999999999999")
		}
		apiRouter.ServeHTTP(rec, req)

		if test.expectedErr != noAPIErr {
			errBytes, err := ioutil.ReadAll(rec.Result().Body)
			if err != nil {
				t.Fatalf("Test %d %s Failed to read error response from upload part request %s/%s: <ERROR> %v",
					i+1, instanceType, bucketName, testObject, err)
			}
			var errXML APIErrorResponse
			err = xml.Unmarshal(errBytes, &errXML)
			if err != nil {
				t.Fatalf("Test %d %s Failed to unmarshal error response from upload part request %s/%s: <ERROR> %v",
					i+1, instanceType, bucketName, testObject, err)
			}
			if test.expectedErr.Code != errXML.Code {
				t.Errorf("Test %d %s expected to fail with error %s, but received %s", i+1, instanceType,
					test.expectedErr.Code, errXML.Code)
			}
		} else {
			if rec.Code != http.StatusOK {
				t.Errorf("Test %d %s expected to succeed, but failed with HTTP status code %d",
					i+1, instanceType, rec.Code)

			}
		}
	}
}

// TestAPIPutObjectPartHandler - Tests validate the response of PutObjectPart HTTP handler
//  for variety of inputs.
func TestAPIPutObjectPartHandler(t *testing.T) {
	ExecObjectLayerAPITest(t, testAPIPutObjectPartHandler, []string{"PutObjectPart"})
}

func testAPIPutObjectPartHandler(obj ObjectLayer, instanceType, bucketName string, apiRouter http.Handler,
	credentials credential, t *testing.T) {

	// Initiate Multipart upload for testing PutObjectPartHandler.
	testObject := "testobject"

	// PutObjectPart API HTTP Handler has to be tested in isolation,
	// that is without any other handler being registered,
	// That's why NewMultipartUpload is initiated using ObjectLayer.
	uploadID, err := obj.NewMultipartUpload(bucketName, testObject, nil)
	if err != nil {
		// Failed to create NewMultipartUpload, abort.
		t.Fatalf("Minio %s : <ERROR>  %s", instanceType, err)
	}

	uploadIDCopy := uploadID

	// expected error types for invalid inputs to PutObjectPartHandler.
	noAPIErr := APIError{}
	// expected error when content length is missing in the HTTP request.
	missingContent := getAPIError(ErrMissingContentLength)
	// expected error when content length is too large.
	entityTooLarge := getAPIError(ErrEntityTooLarge)
	// expected error when the signature check fails.
	badSigning := getAPIError(ErrSignatureDoesNotMatch)
	// expected error MD5 sum mismatch occurs.
	badChecksum := getAPIError(ErrInvalidDigest)
	// expected error when the part number in the request is invalid.
	invalidPart := getAPIError(ErrInvalidPart)
	// expected error when maxPart is beyond the limit.
	invalidMaxParts := getAPIError(ErrInvalidMaxParts)
	// expected error the when the uploadID is invalid.
	noSuchUploadID := getAPIError(ErrNoSuchUpload)

	// SignatureMismatch for various signing types
	testCases := []struct {
		objectName       string
		reader           io.ReadSeeker
		partNumber       string
		fault            Fault
		expectedAPIError APIError
	}{
		// Test case - 1.
		// Success case.
		{
			objectName:       testObject,
			reader:           bytes.NewReader([]byte("hello")),
			partNumber:       "1",
			fault:            None,
			expectedAPIError: noAPIErr,
		},
		// Test case - 2.
		// Case where part number is invalid.
		{
			objectName:       testObject,
			reader:           bytes.NewReader([]byte("hello")),
			partNumber:       "9999999999999999999",
			fault:            None,
			expectedAPIError: invalidPart,
		},
		// Test case - 3.
		// Case where the part number has exceeded the max allowed parts in an upload.
		{
			objectName:       testObject,
			reader:           bytes.NewReader([]byte("hello")),
			partNumber:       strconv.Itoa(maxPartID + 1),
			fault:            None,
			expectedAPIError: invalidMaxParts,
		},
		// Test case - 4.
		// Case where the content length is not set in the HTTP request.
		{
			objectName:       testObject,
			reader:           bytes.NewReader([]byte("hello")),
			partNumber:       "1",
			fault:            MissingContentLength,
			expectedAPIError: missingContent,
		},
		// Test case - 5.
		// case where the object size is set to a value greater than the max allowed size.
		{
			objectName:       testObject,
			reader:           bytes.NewReader([]byte("hello")),
			partNumber:       "1",
			fault:            TooBigObject,
			expectedAPIError: entityTooLarge,
		},
		// Test case - 6.
		// case where a signature mismatch is introduced and the response is validated.
		{
			objectName:       testObject,
			reader:           bytes.NewReader([]byte("hello")),
			partNumber:       "1",
			fault:            BadSignature,
			expectedAPIError: badSigning,
		},
		// Test case - 7.
		// Case where incorrect checksum is set and the error response
		// is asserted with the expected error response.
		{
			objectName:       testObject,
			reader:           bytes.NewReader([]byte("hello")),
			partNumber:       "1",
			fault:            BadMD5,
			expectedAPIError: badChecksum,
		},
		// Test case - 8.
		// case where the a non-existent uploadID is set.
		{
			objectName:       testObject,
			reader:           bytes.NewReader([]byte("hello")),
			partNumber:       "1",
			fault:            MissingUploadID,
			expectedAPIError: noSuchUploadID,
		},
	}

	for i, test := range testCases {
		// Using sub-tests introduced in Go 1.7.
		t.Run(fmt.Sprintf("Minio %s : Test case %d failed.", instanceType, i+1), func(t *testing.T) {

			var req *http.Request
			rec := httptest.NewRecorder()
			// setting a non-existent uploadID.
			// deliberately introducing the invalid value to be able to assert the response with the expected error response.
			if test.fault == MissingUploadID {
				uploadID = "upload1"
			}
			// constructing a v4 signed HTTP request.
			req, err = newTestSignedRequestV4("PUT",
				getPutObjectPartURL("", bucketName, test.objectName, uploadID, test.partNumber),
				0, test.reader, credentials.AccessKeyID, credentials.SecretAccessKey)
			if err != nil {
				t.Fatalf("Test %d %s Failed to create a signed request to upload part for %s/%s: <ERROR> %v", i+1, instanceType,
					bucketName, test.objectName, err)
			}

			// introduce faults in the request.
			// deliberately introducing the invalid value to be able to assert the response with the expected error response.
			switch test.fault {
			case MissingContentLength:
				req.ContentLength = -1
				// Setting the content length to a value greater than the max allowed size of a part.
				// Used in test case  4.
			case TooBigObject:
				req.ContentLength = maxObjectSize + 1
				// Malformed signature.
				// Used in test case  6.
			case BadSignature:
				req.Header.Set("authorization", req.Header.Get("authorization")+"a")
				// Setting an invalid Content-MD5 to force a Md5 Mismatch error.
				// Used in tesr case 7.
			case BadMD5:
				req.Header.Set("Content-MD5", "badmd5")
			}

			// invoke the PutObjectPart HTTP handler.
			apiRouter.ServeHTTP(rec, req)

			// validate the error response.
			if test.expectedAPIError != noAPIErr {
				var errBytes []byte
				// read the response body.
				errBytes, err = ioutil.ReadAll(rec.Result().Body)
				if err != nil {
					t.Fatalf("Failed to read error response from upload part request \"%s\"/\"%s\": <ERROR> %v.",
						bucketName, test.objectName, err)
				}
				// parse the XML error response.
				var errXML APIErrorResponse
				err = xml.Unmarshal(errBytes, &errXML)
				if err != nil {
					t.Fatalf("Failed to unmarshal error response from upload part request \"%s\"/\"%s\": <ERROR> %v.",
						bucketName, test.objectName, err)
				}
				// Validate whether the error has occured for the expected reason.
				if test.expectedAPIError.Code != errXML.Code {
					t.Errorf("Expected to fail with error \"%s\", but received \"%s\".",
						test.expectedAPIError.Code, errXML.Code)
				}
				// Validate the HTTP response status code  with the expected one.
				if test.expectedAPIError.HTTPStatusCode != rec.Code {
					t.Errorf("Expected the HTTP response status code to be %d, got %d.", test.expectedAPIError.HTTPStatusCode, rec.Code)
				}
			}
		})
	}

	// Test for Anonymous/unsigned http request.
	anonReq, err := newTestRequest("PUT", getPutObjectPartURL("", bucketName, testObject, uploadIDCopy, "1"),
		int64(len("hello")), bytes.NewReader([]byte("hello")))
	if err != nil {
		t.Fatalf("Minio %s: Failed to create an anonymous request for %s/%s: <ERROR> %v",
			instanceType, bucketName, testObject, err)
	}

	// ExecObjectLayerAPIAnonTest - Calls the HTTP API handler using the anonymous request, validates the ErrAccessDeniedResponse,
	// sets the bucket policy using the policy statement generated from `getWriteOnlyObjectStatement` so that the
	// unsigned request goes through and its validated again.
	ExecObjectLayerAPIAnonTest(t, "TestAPIPutObjectPartHandler", bucketName, testObject, instanceType, apiRouter, anonReq, getWriteOnlyObjectStatement)

	// HTTP request for testing when `ObjectLayer` is set to `nil`.
	// There is no need to use an existing bucket and valid input for creating the request
	// since the `objectLayer==nil`  check is performed before any other checks inside the handlers.
	// The only aim is to generate an HTTP request in a way that the relevant/registered end point is evoked/called.
	nilBucket := "dummy-bucket"
	nilObject := "dummy-object"

	nilReq, err := newTestSignedRequestV4("PUT", getPutObjectPartURL("", nilBucket, nilObject, "0", "0"),
		0, bytes.NewReader([]byte("testNilObjLayer")), "", "")

	if err != nil {
		t.Errorf("Minio %s: Failed to create http request for testing the response when object Layer is set to `nil`.", instanceType)
	}
	// execute the object layer set to `nil` test.
	// `ExecObjectLayerAPINilTest` manages the operation.
	ExecObjectLayerAPINilTest(t, nilBucket, nilObject, instanceType, apiRouter, nilReq)
}

// TestAPIListObjectPartsHandlerPreSign - Tests validate the response of ListObjectParts HTTP handler
//  when signature type of the HTTP request is `Presigned`.
func TestAPIListObjectPartsHandlerPreSign(t *testing.T) {
	ExecObjectLayerAPITest(t, testAPIListObjectPartsHandlerPreSign,
		[]string{"PutObjectPart", "NewMultipart", "ListObjectParts"})
}

func testAPIListObjectPartsHandlerPreSign(obj ObjectLayer, instanceType, bucketName string, apiRouter http.Handler,
	credentials credential, t *testing.T) {
	testObject := "testobject"
	rec := httptest.NewRecorder()
	req, err := newTestSignedRequestV4("POST", getNewMultipartURL("", bucketName, testObject),
		0, nil, credentials.AccessKeyID, credentials.SecretAccessKey)
	if err != nil {
		t.Fatalf("[%s] - Failed to create a signed request to initiate multipart upload for %s/%s: <ERROR> %v",
			instanceType, bucketName, testObject, err)
	}
	apiRouter.ServeHTTP(rec, req)

	// Get uploadID of the mulitpart upload initiated.
	var mpartResp InitiateMultipartUploadResponse
	mpartRespBytes, err := ioutil.ReadAll(rec.Result().Body)
	if err != nil {
		t.Fatalf("[%s] Failed to read NewMultipartUpload response <ERROR> %v", instanceType, err)

	}
	err = xml.Unmarshal(mpartRespBytes, &mpartResp)
	if err != nil {
		t.Fatalf("[%s] Failed to unmarshal NewMultipartUpload response <ERROR> %v", instanceType, err)
	}

	// Upload a part for listing purposes.
	rec = httptest.NewRecorder()
	req, err = newTestSignedRequestV4("PUT",
		getPutObjectPartURL("", bucketName, testObject, mpartResp.UploadID, "1"),
		int64(len("hello")), bytes.NewReader([]byte("hello")), credentials.AccessKeyID, credentials.SecretAccessKey)
	if err != nil {
		t.Fatalf("[%s] - Failed to create a signed request to initiate multipart upload for %s/%s: <ERROR> %v",
			instanceType, bucketName, testObject, err)
	}
	apiRouter.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("[%s] - Failed to PutObjectPart bucket: %s object: %s HTTP status code: %d",
			instanceType, bucketName, testObject, rec.Code)
	}
	rec = httptest.NewRecorder()
	req, err = newTestRequest("GET",
		getListMultipartURLWithParams("", bucketName, testObject, mpartResp.UploadID, "", "", ""),
		0, nil)
	if err != nil {
		t.Fatalf("[%s] - Failed to create an unsigned request to list object parts for bucket %s, uploadId %s",
			instanceType, bucketName, mpartResp.UploadID)
	}

	err = preSignV2(req, credentials.AccessKeyID, credentials.SecretAccessKey, int64(10*time.Minute))
	if err != nil {
		t.Fatalf("[%s] - Failed to presignV2 an unsigned request to list object parts for bucket %s, uploadId %s",
			instanceType, bucketName, mpartResp.UploadID)
	}
	apiRouter.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("Test %d %s expected to succeed but failed with HTTP status code %d",
			1, instanceType, rec.Code)
	}
}

// TestAPIListObjectPartsHandler - Tests validate the response of ListObjectParts HTTP handler
//  for variety of success/failure cases.
func TestAPIListObjectPartsHandler(t *testing.T) {
	ExecObjectLayerAPITest(t, testAPIListObjectPartsHandler, []string{"ListObjectParts"})
}

func testAPIListObjectPartsHandler(obj ObjectLayer, instanceType, bucketName string, apiRouter http.Handler,
	credentials credential, t *testing.T) {
	testObject := "testobject"

	// PutObjectPart API HTTP Handler has to be tested in isolation,
	// that is without any other handler being registered,
	// That's why NewMultipartUpload is initiated using ObjectLayer.
	uploadID, err := obj.NewMultipartUpload(bucketName, testObject, nil)
	if err != nil {
		// Failed to create NewMultipartUpload, abort.
		t.Fatalf("Minio %s : <ERROR>  %s", instanceType, err)
	}

	uploadIDCopy := uploadID

	// create an object Part, will be used to test list object parts.
	_, err = obj.PutObjectPart(bucketName, testObject, uploadID, 1, int64(len("hello")), bytes.NewReader([]byte("hello")),
		"5d41402abc4b2a76b9719d911017c592", "")
	if err != nil {
		t.Fatalf("Minio %s : %s.", instanceType, err)
	}

	// expected error types for invalid inputs to ListObjectParts handler.
	noAPIErr := APIError{}
	// expected error when the signature check fails.
	signatureMismatchErr := getAPIError(ErrSignatureDoesNotMatch)
	// expected error the when the uploadID is invalid.
	noSuchUploadErr := getAPIError(ErrNoSuchUpload)
	// expected error the part number marker use in the ListObjectParts request is invalid.
	invalidPartMarkerErr := getAPIError(ErrInvalidPartNumberMarker)
	// expected error when the maximum number of parts requested to listed in invalid.
	invalidMaxPartsErr := getAPIError(ErrInvalidMaxParts)

	testCases := []struct {
		fault            Fault
		partNumberMarker string
		maxParts         string
		expectedErr      APIError
	}{
		// Test case - 1.
		// case where a signature mismatch is introduced and the response is validated.
		{
			fault:            BadSignature,
			partNumberMarker: "",
			maxParts:         "",
			expectedErr:      signatureMismatchErr,
		},

		// Test case - 2.
		// Marker is set to invalid value of -1, error response is asserted.
		{
			fault:            None,
			partNumberMarker: "-1",
			maxParts:         "",
			expectedErr:      invalidPartMarkerErr,
		},
		// Test case - 3.
		// Max Parts is set a negative value, error response is validated.
		{
			fault:            None,
			partNumberMarker: "",
			maxParts:         "-1",
			expectedErr:      invalidMaxPartsErr,
		},
		// Test case - 4.
		// Invalid UploadID is set and the error response is validated.
		{
			fault:            MissingUploadID,
			partNumberMarker: "",
			maxParts:         "",
			expectedErr:      noSuchUploadErr,
		},
	}

	for i, test := range testCases {
		var req *http.Request
		// Using sub-tests introduced in Go 1.7.
		t.Run(fmt.Sprintf("Minio %s: Test case %d failed.", instanceType, i+1), func(t *testing.T) {
			rec := httptest.NewRecorder()

			// setting a non-existent uploadID.
			// deliberately introducing the invalid value to be able to assert the response with the expected error response.
			if test.fault == MissingUploadID {
				uploadID = "upload1"
			}

			// constructing a v4 signed HTTP request for ListMultipartUploads.
			req, err = newTestSignedRequestV4("GET",
				getListMultipartURLWithParams("", bucketName, testObject, uploadID, test.maxParts, test.partNumberMarker, ""),
				0, nil, credentials.AccessKeyID, credentials.SecretAccessKey)

			if err != nil {
				t.Fatalf("Failed to create a signed request to list object parts for %s/%s: <ERROR> %v.",
					bucketName, testObject, err)
			}

			// Malformed signature.
			if test.fault == BadSignature {
				req.Header.Set("authorization", req.Header.Get("authorization")+"a")
			}

			// invoke the PutObjectPart HTTP handler with the given HTTP request.
			apiRouter.ServeHTTP(rec, req)

			// validate the error response.
			if test.expectedErr != noAPIErr {

				var errBytes []byte
				// read the response body.
				errBytes, err = ioutil.ReadAll(rec.Result().Body)
				if err != nil {
					t.Fatalf("Failed to read error response list object parts request %s/%s: <ERROR> %v", bucketName, testObject, err)
				}
				// parse the error response.
				var errXML APIErrorResponse
				err = xml.Unmarshal(errBytes, &errXML)
				if err != nil {
					t.Fatalf("Failed to unmarshal error response from list object partsest %s/%s: <ERROR> %v",
						bucketName, testObject, err)
				}
				// Validate whether the error has occured for the expected reason.
				if test.expectedErr.Code != errXML.Code {
					t.Errorf("Expected to fail with %s but received %s",
						test.expectedErr.Code, errXML.Code)
				}
				// in case error is not expected response status should be 200OK.
			} else {
				if rec.Code != http.StatusOK {
					t.Errorf("Expected to succeed with response HTTP status 200OK, but failed with HTTP status code %d.", rec.Code)
				}
			}
		})
	}

	// Test for Anonymous/unsigned http request.
	anonReq, err := newTestRequest("GET",
		getListMultipartURLWithParams("", bucketName, testObject, uploadIDCopy, "", "", ""), 0, nil)
	if err != nil {
		t.Fatalf("Minio %s: Failed to create an anonymous request for %s/%s: <ERROR> %v",
			instanceType, bucketName, testObject, err)
	}

	// ExecObjectLayerAPIAnonTest - Calls the HTTP API handler using the anonymous request, validates the ErrAccessDeniedResponse,
	// sets the bucket policy using the policy statement generated from `getWriteOnlyObjectStatement` so that the
	// unsigned request goes through and its validated again.
	ExecObjectLayerAPIAnonTest(t, "TestAPIListObjectPartsHandler", bucketName, testObject, instanceType, apiRouter, anonReq, getWriteOnlyObjectStatement)

	// HTTP request for testing when `objectLayer` is set to `nil`.
	// There is no need to use an existing bucket and valid input for creating the request
	// since the `objectLayer==nil`  check is performed before any other checks inside the handlers.
	// The only aim is to generate an HTTP request in a way that the relevant/registered end point is evoked/called.
	nilBucket := "dummy-bucket"
	nilObject := "dummy-object"

	nilReq, err := newTestSignedRequestV4("GET",
		getListMultipartURLWithParams("", nilBucket, nilObject, "dummy-uploadID", "0", "0", ""),
		0, nil, "", "")
	if err != nil {
		t.Errorf("Minio %s:Failed to create http request for testing the response when object Layer is set to `nil`.", instanceType)
	}
	// execute the object layer set to `nil` test.
	// `ExecObjectLayerAPINilTest` sets the Object Layer to `nil` and calls the handler.
	ExecObjectLayerAPINilTest(t, nilBucket, nilObject, instanceType, apiRouter, nilReq)
}

// TestAPIListObjectPartsHandler - Tests validate the response of ListObjectParts HTTP handler
//  when signature type of the HTTP request is `V2`.
func TestListObjectPartsHandlerV2(t *testing.T) {
	ExecObjectLayerAPITest(t, testAPIListObjectPartsHandlerV2, []string{"PutObjectPart", "NewMultipart", "ListObjectParts"})
}

func testAPIListObjectPartsHandlerV2(obj ObjectLayer, instanceType, bucketName string, apiRouter http.Handler,
	credentials credential, t *testing.T) {
	testObject := "testobject"
	rec := httptest.NewRecorder()
	req, err := newTestSignedRequestV4("POST", getNewMultipartURL("", bucketName, testObject),
		0, nil, credentials.AccessKeyID, credentials.SecretAccessKey)
	if err != nil {
		t.Fatalf("[%s] - Failed to create a signed request to initiate multipart upload for %s/%s: <ERROR> %v",
			instanceType, bucketName, testObject, err)
	}
	apiRouter.ServeHTTP(rec, req)

	// Get uploadID of the mulitpart upload initiated.
	var mpartResp InitiateMultipartUploadResponse
	mpartRespBytes, err := ioutil.ReadAll(rec.Result().Body)
	if err != nil {
		t.Fatalf("[%s] Failed to read NewMultipartUpload response <ERROR> %v", instanceType, err)

	}
	err = xml.Unmarshal(mpartRespBytes, &mpartResp)
	if err != nil {
		t.Fatalf("[%s] Failed to unmarshal NewMultipartUpload response <ERROR> %v", instanceType, err)
	}

	// Upload a part for listing purposes.
	rec = httptest.NewRecorder()
	req, err = newTestSignedRequestV4("PUT",
		getPutObjectPartURL("", bucketName, testObject, mpartResp.UploadID, "1"),
		int64(len("hello")), bytes.NewReader([]byte("hello")), credentials.AccessKeyID, credentials.SecretAccessKey)
	if err != nil {
		t.Fatalf("[%s] - Failed to create a signed request to initiate multipart upload for %s/%s: <ERROR> %v",
			instanceType, bucketName, testObject, err)
	}
	apiRouter.ServeHTTP(rec, req)

	rec = httptest.NewRecorder()
	req, err = newTestSignedRequestV2("GET", getListMultipartURLWithParams("", bucketName, testObject, mpartResp.UploadID, "", "", ""),
		0, nil, credentials.AccessKeyID, credentials.SecretAccessKey)
	if err != nil {
		t.Fatalf("[%s] - Failed to create a signed request to list object parts for %s/%s: <ERROR> %v",
			instanceType, bucketName, testObject, err)
	}

	apiRouter.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("Test %d %s expected to succeed but failed with HTTP status code %d", 1, instanceType, rec.Code)
	}

	// Simulate signature mismatch error for V2 request.
	rec = httptest.NewRecorder()
	req, err = newTestSignedRequestV2("GET",
		getListMultipartURLWithParams("", bucketName, testObject, mpartResp.UploadID, "", "", ""),
		0, nil, credentials.AccessKeyID, credentials.SecretAccessKey)
	if err != nil {
		t.Fatalf("[%s] - Failed to create a signed request to list object parts for %s/%s: <ERROR> %v",
			instanceType, bucketName, testObject, err)
	}
	signatureMismatchErr := getAPIError(ErrSignatureDoesNotMatch)
	req.Header.Set("x-amz-date", "")
	apiRouter.ServeHTTP(rec, req)
	errBytes, err := ioutil.ReadAll(rec.Result().Body)
	if err != nil {
		t.Fatalf("Test %d %s Failed to read error response list object parts request %s/%s: <ERROR> %v",
			1, instanceType, bucketName, testObject, err)
	}
	var errXML APIErrorResponse
	err = xml.Unmarshal(errBytes, &errXML)
	if err != nil {
		t.Fatalf("Test %d %s Failed to unmarshal error response from list object partsest %s/%s: <ERROR> %v",
			1, instanceType, bucketName, testObject, err)
	}
	if errXML.Code != signatureMismatchErr.Code {
		t.Errorf("Test %d %s expected to fail with error %s, but received %s", 1, instanceType,
			signatureMismatchErr.Code, errXML.Code)
	}
}

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

	humanize "github.com/dustin/go-humanize"
	"github.com/minio/minio/pkg/auth"
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

// Wrapper for calling HeadObject API handler tests for both XL multiple disks and FS single drive setup.
func TestAPIHeadObjectHandler(t *testing.T) {
	ExecObjectLayerAPITest(t, testAPIHeadObjectHandler, []string{"HeadObject"})
}

func testAPIHeadObjectHandler(obj ObjectLayer, instanceType, bucketName string, apiRouter http.Handler,
	credentials auth.Credentials, t *testing.T) {
	objectName := "test-object"
	// set of byte data for PutObject.
	// object has to be created before running tests for HeadObject.
	// this is required even to assert the HeadObject data,
	// since dataInserted === dataFetched back is a primary criteria for any object storage this assertion is critical.
	bytesData := []struct {
		byteData []byte
	}{
		{generateBytesData(6 * humanize.MiByte)},
	}
	// set of inputs for uploading the objects before tests for downloading is done.
	putObjectInputs := []struct {
		bucketName    string
		objectName    string
		contentLength int64
		textData      []byte
		metaData      map[string]string
	}{
		{bucketName, objectName, int64(len(bytesData[0].byteData)), bytesData[0].byteData, make(map[string]string)},
	}
	// iterate through the above set of inputs and upload the object.
	for i, input := range putObjectInputs {
		// uploading the object.
		_, err := obj.PutObject(input.bucketName, input.objectName, mustGetHashReader(t, bytes.NewBuffer(input.textData), input.contentLength, input.metaData[""], ""), input.metaData)
		// if object upload fails stop the test.
		if err != nil {
			t.Fatalf("Put Object case %d:  Error uploading object: <ERROR> %v", i+1, err)
		}
	}

	// test cases with inputs and expected result for HeadObject.
	testCases := []struct {
		bucketName string
		objectName string
		accessKey  string
		secretKey  string
		// expected output.
		expectedRespStatus int // expected response status body.
	}{
		// Test case - 1.
		// Fetching stat info of object and validating it.
		{
			bucketName:         bucketName,
			objectName:         objectName,
			accessKey:          credentials.AccessKey,
			secretKey:          credentials.SecretKey,
			expectedRespStatus: http.StatusOK,
		},
		// Test case - 2.
		// Case with non-existent object name.
		{
			bucketName:         bucketName,
			objectName:         "abcd",
			accessKey:          credentials.AccessKey,
			secretKey:          credentials.SecretKey,
			expectedRespStatus: http.StatusNotFound,
		},
		// Test case - 3.
		// Test case to induce a signature mismatch.
		// Using invalid accessID.
		{
			bucketName:         bucketName,
			objectName:         objectName,
			accessKey:          "Invalid-AccessID",
			secretKey:          credentials.SecretKey,
			expectedRespStatus: http.StatusForbidden,
		},
	}

	// Iterating over the cases, fetching the object validating the response.
	for i, testCase := range testCases {
		// initialize HTTP NewRecorder, this records any mutations to response writer inside the handler.
		rec := httptest.NewRecorder()
		// construct HTTP request for Get Object end point.
		req, err := newTestSignedRequestV4("HEAD", getHeadObjectURL("", testCase.bucketName, testCase.objectName),
			0, nil, testCase.accessKey, testCase.secretKey)
		if err != nil {
			t.Fatalf("Test %d: %s: Failed to create HTTP request for Head Object: <ERROR> %v", i+1, instanceType, err)
		}
		// Since `apiRouter` satisfies `http.Handler` it has a ServeHTTP to execute the logic of the handler.
		// Call the ServeHTTP to execute the handler,`func (api objectAPIHandlers) GetObjectHandler`  handles the request.
		apiRouter.ServeHTTP(rec, req)

		// Assert the response code with the expected status.
		if rec.Code != testCase.expectedRespStatus {
			t.Fatalf("Case %d: Expected the response status to be `%d`, but instead found `%d`", i+1, testCase.expectedRespStatus, rec.Code)
		}

		// Verify response of the V2 signed HTTP request.
		// initialize HTTP NewRecorder, this records any mutations to response writer inside the handler.
		recV2 := httptest.NewRecorder()
		// construct HTTP request for Head Object endpoint.
		reqV2, err := newTestSignedRequestV2("HEAD", getHeadObjectURL("", testCase.bucketName, testCase.objectName),
			0, nil, testCase.accessKey, testCase.secretKey)

		if err != nil {
			t.Fatalf("Test %d: %s: Failed to create HTTP request for Head Object: <ERROR> %v", i+1, instanceType, err)
		}

		// Since `apiRouter` satisfies `http.Handler` it has a ServeHTTP to execute the logic of the handler.
		// Call the ServeHTTP to execute the handler.
		apiRouter.ServeHTTP(recV2, reqV2)
		if recV2.Code != testCase.expectedRespStatus {
			t.Errorf("Test %d: %s: Expected the response status to be `%d`, but instead found `%d`", i+1, instanceType, testCase.expectedRespStatus, recV2.Code)
		}
	}

	// Test for Anonymous/unsigned http request.
	anonReq, err := newTestRequest("HEAD", getHeadObjectURL("", bucketName, objectName), 0, nil)

	if err != nil {
		t.Fatalf("Minio %s: Failed to create an anonymous request for %s/%s: <ERROR> %v",
			instanceType, bucketName, objectName, err)
	}

	// ExecObjectLayerAPIAnonTest - Calls the HTTP API handler using the anonymous request, validates the ErrAccessDeniedResponse,
	// sets the bucket policy using the policy statement generated from `getWriteOnlyObjectStatement` so that the
	// unsigned request goes through and its validated again.
	ExecObjectLayerAPIAnonTest(t, obj, "TestAPIHeadObjectHandler", bucketName, objectName, instanceType, apiRouter, anonReq, getReadOnlyObjectStatement)

	// HTTP request for testing when `objectLayer` is set to `nil`.
	// There is no need to use an existing bucket and valid input for creating the request
	// since the `objectLayer==nil`  check is performed before any other checks inside the handlers.
	// The only aim is to generate an HTTP request in a way that the relevant/registered end point is evoked/called.

	nilBucket := "dummy-bucket"
	nilObject := "dummy-object"
	nilReq, err := newTestSignedRequestV4("HEAD", getGetObjectURL("", nilBucket, nilObject),
		0, nil, "", "")

	if err != nil {
		t.Errorf("Minio %s: Failed to create HTTP request for testing the response when object Layer is set to `nil`.", instanceType)
	}
	// execute the object layer set to `nil` test.
	// `ExecObjectLayerAPINilTest` manages the operation.
	ExecObjectLayerAPINilTest(t, nilBucket, nilObject, instanceType, apiRouter, nilReq)
}

// Wrapper for calling GetObject API handler tests for both XL multiple disks and FS single drive setup.
func TestAPIGetObjectHandler(t *testing.T) {
	defer DetectTestLeak(t)()
	ExecObjectLayerAPITest(t, testAPIGetObjectHandler, []string{"GetObject"})
}

func testAPIGetObjectHandler(obj ObjectLayer, instanceType, bucketName string, apiRouter http.Handler,
	credentials auth.Credentials, t *testing.T) {
	objectName := "test-object"
	// set of byte data for PutObject.
	// object has to be created before running tests for GetObject.
	// this is required even to assert the GetObject data,
	// since dataInserted === dataFetched back is a primary criteria for any object storage this assertion is critical.
	bytesData := []struct {
		byteData []byte
	}{
		{generateBytesData(6 * humanize.MiByte)},
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
		_, err := obj.PutObject(input.bucketName, input.objectName, mustGetHashReader(t, bytes.NewBuffer(input.textData), input.contentLength, input.metaData[""], ""), input.metaData)
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
		accessKey  string
		secretKey  string
		// expected output.
		expectedContent    []byte // expected response body.
		expectedRespStatus int    // expected response status body.
	}{
		// Test case - 1.
		// Fetching the entire object and validating its contents.
		{
			bucketName: bucketName,
			objectName: objectName,
			byteRange:  "",
			accessKey:  credentials.AccessKey,
			secretKey:  credentials.SecretKey,

			expectedContent:    bytesData[0].byteData,
			expectedRespStatus: http.StatusOK,
		},
		// Test case - 2.
		// Case with non-existent object name.
		{
			bucketName: bucketName,
			objectName: "abcd",
			byteRange:  "",
			accessKey:  credentials.AccessKey,
			secretKey:  credentials.SecretKey,

			expectedContent:    encodeResponse(getAPIErrorResponse(getAPIError(ErrNoSuchKey), getGetObjectURL("", bucketName, "abcd"))),
			expectedRespStatus: http.StatusNotFound,
		},
		// Test case - 3.
		// Requesting from range 10-100.
		{
			bucketName: bucketName,
			objectName: objectName,
			byteRange:  "bytes=10-100",
			accessKey:  credentials.AccessKey,
			secretKey:  credentials.SecretKey,

			expectedContent:    bytesData[0].byteData[10:101],
			expectedRespStatus: http.StatusPartialContent,
		},
		// Test case - 4.
		// Test case with invalid range.
		{
			bucketName: bucketName,
			objectName: objectName,
			byteRange:  "bytes=-0",
			accessKey:  credentials.AccessKey,
			secretKey:  credentials.SecretKey,

			expectedContent:    encodeResponse(getAPIErrorResponse(getAPIError(ErrInvalidRange), getGetObjectURL("", bucketName, objectName))),
			expectedRespStatus: http.StatusRequestedRangeNotSatisfiable,
		},
		// Test case - 5.
		// Test case with byte range exceeding the object size.
		// Expected to read till end of the object.
		{
			bucketName: bucketName,
			objectName: objectName,
			byteRange:  "bytes=10-1000000000000000",
			accessKey:  credentials.AccessKey,
			secretKey:  credentials.SecretKey,

			expectedContent:    bytesData[0].byteData[10:],
			expectedRespStatus: http.StatusPartialContent,
		},
		// Test case - 6.
		// Test case to induce a signature mismatch.
		// Using invalid accessID.
		{
			bucketName: bucketName,
			objectName: objectName,
			byteRange:  "",
			accessKey:  "Invalid-AccessID",
			secretKey:  credentials.SecretKey,

			expectedContent:    encodeResponse(getAPIErrorResponse(getAPIError(ErrInvalidAccessKeyID), getGetObjectURL("", bucketName, objectName))),
			expectedRespStatus: http.StatusForbidden,
		},
		// Test case - 7.
		// Case with bad components in object name.
		{
			bucketName: bucketName,
			objectName: "../../etc",
			byteRange:  "",
			accessKey:  credentials.AccessKey,
			secretKey:  credentials.SecretKey,

			expectedContent: encodeResponse(getAPIErrorResponse(getAPIError(ErrInvalidObjectName),
				getGetObjectURL("", bucketName, "../../etc"))),
			expectedRespStatus: http.StatusBadRequest,
		},
		// Test case - 8.
		// Case with strange components but returning error as not found.
		{
			bucketName: bucketName,
			objectName: ". ./. ./etc",
			byteRange:  "",
			accessKey:  credentials.AccessKey,
			secretKey:  credentials.SecretKey,

			expectedContent: encodeResponse(getAPIErrorResponse(getAPIError(ErrNoSuchKey),
				"/"+bucketName+"/"+". ./. ./etc")),
			expectedRespStatus: http.StatusNotFound,
		},
		// Test case - 9.
		// Case with bad components in object name.
		{
			bucketName: bucketName,
			objectName: ". ./../etc",
			byteRange:  "",
			accessKey:  credentials.AccessKey,
			secretKey:  credentials.SecretKey,

			expectedContent: encodeResponse(getAPIErrorResponse(getAPIError(ErrInvalidObjectName),
				"/"+bucketName+"/"+". ./../etc")),
			expectedRespStatus: http.StatusBadRequest,
		},
		// Test case - 10.
		// Case with proper components
		{
			bucketName: bucketName,
			objectName: "etc/path/proper/.../etc",
			byteRange:  "",
			accessKey:  credentials.AccessKey,
			secretKey:  credentials.SecretKey,

			expectedContent: encodeResponse(getAPIErrorResponse(getAPIError(ErrNoSuchKey),
				getGetObjectURL("", bucketName, "etc/path/proper/.../etc"))),
			expectedRespStatus: http.StatusNotFound,
		},
	}

	// Iterating over the cases, fetching the object validating the response.
	for i, testCase := range testCases {
		// initialize HTTP NewRecorder, this records any mutations to response writer inside the handler.
		rec := httptest.NewRecorder()
		// construct HTTP request for Get Object end point.
		req, err := newTestSignedRequestV4("GET", getGetObjectURL("", testCase.bucketName, testCase.objectName),
			0, nil, testCase.accessKey, testCase.secretKey)

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
			t.Errorf("Test %d: %s: Object content differs from expected value %s, got %s", i+1, instanceType, testCase.expectedContent, string(actualContent))
		}

		// Verify response of the V2 signed HTTP request.
		// initialize HTTP NewRecorder, this records any mutations to response writer inside the handler.
		recV2 := httptest.NewRecorder()
		// construct HTTP request for  GET Object endpoint.
		reqV2, err := newTestSignedRequestV2("GET", getGetObjectURL("", testCase.bucketName, testCase.objectName),
			0, nil, testCase.accessKey, testCase.secretKey)

		if err != nil {
			t.Fatalf("Test %d: %s: Failed to create HTTP request for GetObject: <ERROR> %v", i+1, instanceType, err)
		}

		if testCase.byteRange != "" {
			reqV2.Header.Add("Range", testCase.byteRange)
		}

		// Since `apiRouter` satisfies `http.Handler` it has a ServeHTTP to execute the logic of the handler.
		// Call the ServeHTTP to execute the handler.
		apiRouter.ServeHTTP(recV2, reqV2)
		if recV2.Code != testCase.expectedRespStatus {
			t.Errorf("Test %d: %s: Expected the response status to be `%d`, but instead found `%d`", i+1, instanceType, testCase.expectedRespStatus, recV2.Code)
		}

		// read the response body.
		actualContent, err = ioutil.ReadAll(recV2.Body)
		if err != nil {
			t.Fatalf("Test %d: %s: Failed parsing response body: <ERROR> %v", i+1, instanceType, err)
		}
		// Verify whether the bucket obtained object is same as the one created.
		if !bytes.Equal(testCase.expectedContent, actualContent) {
			t.Errorf("Test %d: %s: Object content differs from expected value.", i+1, instanceType)
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
	ExecObjectLayerAPIAnonTest(t, obj, "TestAPIGetObjectHandler", bucketName, objectName, instanceType, apiRouter, anonReq, getReadOnlyObjectStatement)

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
	defer DetectTestLeak(t)()
	ExecObjectLayerAPITest(t, testAPIPutObjectStreamSigV4Handler, []string{"PutObject"})
}

func testAPIPutObjectStreamSigV4Handler(obj ObjectLayer, instanceType, bucketName string, apiRouter http.Handler,
	credentials auth.Credentials, t *testing.T) {

	objectName := "test-object"
	bytesDataLen := 65 * humanize.KiByte
	bytesData := bytes.Repeat([]byte{'a'}, bytesDataLen)
	oneKData := bytes.Repeat([]byte("a"), 1*humanize.KiByte)

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
		tooBigDecodedLength
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
		// Custom content encoding.
		contentEncoding string
	}{
		// Test case - 1.
		// Fetching the entire object and validating its contents.
		{
			bucketName:         bucketName,
			objectName:         objectName,
			data:               bytesData,
			dataLen:            len(bytesData),
			chunkSize:          64 * humanize.KiByte,
			expectedContent:    []byte{},
			expectedRespStatus: http.StatusOK,
			accessKey:          credentials.AccessKey,
			secretKey:          credentials.SecretKey,
			shouldPass:         true,
		},
		// Test case - 2
		// Small chunk size.
		{
			bucketName:         bucketName,
			objectName:         objectName,
			data:               bytesData,
			dataLen:            len(bytesData),
			chunkSize:          1 * humanize.KiByte,
			expectedContent:    []byte{},
			expectedRespStatus: http.StatusOK,
			accessKey:          credentials.AccessKey,
			secretKey:          credentials.SecretKey,
			shouldPass:         true,
		},
		// Test case - 3
		// Empty data
		{
			bucketName:         bucketName,
			objectName:         objectName,
			data:               []byte{},
			dataLen:            0,
			chunkSize:          64 * humanize.KiByte,
			expectedContent:    []byte{},
			expectedRespStatus: http.StatusOK,
			accessKey:          credentials.AccessKey,
			secretKey:          credentials.SecretKey,
			shouldPass:         true,
		},
		// Test case - 4
		// Invalid access key id.
		{
			bucketName:         bucketName,
			objectName:         objectName,
			data:               bytesData,
			dataLen:            len(bytesData),
			chunkSize:          64 * humanize.KiByte,
			expectedContent:    []byte{},
			expectedRespStatus: http.StatusForbidden,
			accessKey:          "",
			secretKey:          "",
			shouldPass:         false,
		},
		// Test case - 5
		// Wrong auth header returns as bad request.
		{
			bucketName:         bucketName,
			objectName:         objectName,
			data:               bytesData,
			dataLen:            len(bytesData),
			chunkSize:          64 * humanize.KiByte,
			expectedContent:    []byte{},
			expectedRespStatus: http.StatusBadRequest,
			accessKey:          credentials.AccessKey,
			secretKey:          credentials.SecretKey,
			shouldPass:         false,
			removeAuthHeader:   true,
		},
		// Test case - 6
		// Large chunk size.. also passes.
		{
			bucketName:         bucketName,
			objectName:         objectName,
			data:               bytesData,
			dataLen:            len(bytesData),
			chunkSize:          100 * humanize.KiByte,
			expectedContent:    []byte{},
			expectedRespStatus: http.StatusOK,
			accessKey:          credentials.AccessKey,
			secretKey:          credentials.SecretKey,
			shouldPass:         true,
		},
		// Test case - 7
		// Chunk with malformed encoding.
		{
			bucketName:         bucketName,
			objectName:         objectName,
			data:               oneKData,
			dataLen:            1024,
			chunkSize:          1024,
			expectedContent:    []byte{},
			expectedRespStatus: http.StatusInternalServerError,
			accessKey:          credentials.AccessKey,
			secretKey:          credentials.SecretKey,
			shouldPass:         false,
			fault:              malformedEncoding,
		},
		// Test case - 8
		// Chunk with shorter than advertised chunk data.
		{
			bucketName:         bucketName,
			objectName:         objectName,
			data:               oneKData,
			dataLen:            1024,
			chunkSize:          1024,
			expectedContent:    []byte{},
			expectedRespStatus: http.StatusBadRequest,
			accessKey:          credentials.AccessKey,
			secretKey:          credentials.SecretKey,
			shouldPass:         false,
			fault:              unexpectedEOF,
		},
		// Test case - 9
		// Chunk with first chunk data byte tampered.
		{
			bucketName:         bucketName,
			objectName:         objectName,
			data:               oneKData,
			dataLen:            1024,
			chunkSize:          1024,
			expectedContent:    []byte{},
			expectedRespStatus: http.StatusForbidden,
			accessKey:          credentials.AccessKey,
			secretKey:          credentials.SecretKey,
			shouldPass:         false,
			fault:              signatureMismatch,
		},
		// Test case - 10
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
			accessKey:          credentials.AccessKey,
			secretKey:          credentials.SecretKey,
			shouldPass:         false,
			fault:              chunkDateMismatch,
		},
		// Test case - 11
		// Set x-amz-decoded-content-length to a value too big to hold in int64.
		{
			bucketName:         bucketName,
			objectName:         objectName,
			data:               oneKData,
			dataLen:            1024,
			chunkSize:          1024,
			expectedContent:    []byte{},
			expectedRespStatus: http.StatusInternalServerError,
			accessKey:          credentials.AccessKey,
			secretKey:          credentials.SecretKey,
			shouldPass:         false,
			fault:              tooBigDecodedLength,
		},
		// Test case - 12
		// Set custom content encoding should succeed and save the encoding properly.
		{
			bucketName:         bucketName,
			objectName:         objectName,
			data:               bytesData,
			dataLen:            len(bytesData),
			chunkSize:          100 * humanize.KiByte,
			expectedContent:    []byte{},
			expectedRespStatus: http.StatusOK,
			accessKey:          credentials.AccessKey,
			secretKey:          credentials.SecretKey,
			shouldPass:         true,
			contentEncoding:    "aws-chunked,gzip",
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

		} else if testCase.contentEncoding == "" {
			req, err = newTestStreamingSignedRequest("PUT",
				getPutObjectURL("", testCase.bucketName, testCase.objectName),
				int64(testCase.dataLen), testCase.chunkSize, bytes.NewReader(testCase.data),
				testCase.accessKey, testCase.secretKey)
		} else if testCase.contentEncoding != "" {
			req, err = newTestStreamingSignedCustomEncodingRequest("PUT",
				getPutObjectURL("", testCase.bucketName, testCase.objectName),
				int64(testCase.dataLen), testCase.chunkSize, bytes.NewReader(testCase.data),
				testCase.accessKey, testCase.secretKey, testCase.contentEncoding)
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
		case tooBigDecodedLength:
			// Set decoded length to a large value out of int64 range to simulate parse failure.
			req.Header.Set("x-amz-decoded-content-length", "9999999999999999999999")
		}

		if err != nil {
			t.Fatalf("Error injecting faults into the request: <ERROR> %v.", err)
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
			objInfo, err := obj.GetObjectInfo(testCase.bucketName, testCase.objectName)
			if err != nil {
				t.Fatalf("Test %d: %s: Failed to fetch the copied object: <ERROR> %s", i+1, instanceType, err)
			}
			if objInfo.ContentEncoding == streamingContentEncoding {
				t.Fatalf("Test %d: %s: ContentEncoding is set to \"aws-chunked\" which is unexpected", i+1, instanceType)
			}
			expectedContentEncoding := trimAwsChunkedContentEncoding(testCase.contentEncoding)
			if expectedContentEncoding != objInfo.ContentEncoding {
				t.Fatalf("Test %d: %s: ContentEncoding is set to \"%s\" which is unexpected, expected \"%s\"", i+1, instanceType, objInfo.ContentEncoding, expectedContentEncoding)
			}
			buffer := new(bytes.Buffer)
			err = obj.GetObject(testCase.bucketName, testCase.objectName, 0, int64(testCase.dataLen), buffer, objInfo.ETag)
			if err != nil {
				t.Fatalf("Test %d: %s: Failed to fetch the copied object: <ERROR> %s", i+1, instanceType, err)
			}
			if !bytes.Equal(testCase.data, buffer.Bytes()) {
				t.Errorf("Test %d: %s: Data Mismatch: Data fetched back from the uploaded object doesn't match the original one.", i+1, instanceType)
			}
			buffer.Reset()
		}
	}
}

// Wrapper for calling PutObject API handler tests for both XL multiple disks and FS single drive setup.
func TestAPIPutObjectHandler(t *testing.T) {
	defer DetectTestLeak(t)()
	ExecObjectLayerAPITest(t, testAPIPutObjectHandler, []string{"PutObject"})
}

func testAPIPutObjectHandler(obj ObjectLayer, instanceType, bucketName string, apiRouter http.Handler,
	credentials auth.Credentials, t *testing.T) {

	// register event notifier.
	err := initEventNotifier(obj)

	if err != nil {
		t.Fatal("Notifier initialization failed.")
	}
	objectName := "test-object"
	// byte data for PutObject.
	bytesData := generateBytesData(6 * humanize.KiByte)

	copySourceHeader := http.Header{}
	copySourceHeader.Set("X-Amz-Copy-Source", "somewhere")
	invalidMD5Header := http.Header{}
	invalidMD5Header.Set("Content-Md5", "42")
	inalidStorageClassHeader := http.Header{}
	inalidStorageClassHeader.Set(amzStorageClass, "INVALID")

	addCustomHeaders := func(req *http.Request, customHeaders http.Header) {
		for k, values := range customHeaders {
			for _, value := range values {
				req.Header.Set(k, value)
			}
		}
	}

	// test cases with inputs and expected result for GetObject.
	testCases := []struct {
		bucketName string
		objectName string
		headers    http.Header
		data       []byte
		dataLen    int
		accessKey  string
		secretKey  string
		fault      Fault
		// expected output.
		expectedRespStatus int // expected response status body.
	}{
		// Test case - 1.
		// Fetching the entire object and validating its contents.
		{
			bucketName: bucketName,
			objectName: objectName,
			data:       bytesData,
			dataLen:    len(bytesData),
			accessKey:  credentials.AccessKey,
			secretKey:  credentials.SecretKey,

			expectedRespStatus: http.StatusOK,
		},
		// Test case - 2.
		// Test Case with invalid accessID.
		{
			bucketName: bucketName,
			objectName: objectName,
			data:       bytesData,
			dataLen:    len(bytesData),
			accessKey:  "Wrong-AcessID",
			secretKey:  credentials.SecretKey,

			expectedRespStatus: http.StatusForbidden,
		},
		// Test case - 3.
		// Test Case with invalid header key X-Amz-Copy-Source.
		{
			bucketName:         bucketName,
			objectName:         objectName,
			headers:            copySourceHeader,
			data:               bytesData,
			dataLen:            len(bytesData),
			accessKey:          credentials.AccessKey,
			secretKey:          credentials.SecretKey,
			expectedRespStatus: http.StatusBadRequest,
		},
		// Test case - 4.
		// Test Case with invalid Content-Md5 value
		{
			bucketName:         bucketName,
			objectName:         objectName,
			headers:            invalidMD5Header,
			data:               bytesData,
			dataLen:            len(bytesData),
			accessKey:          credentials.AccessKey,
			secretKey:          credentials.SecretKey,
			expectedRespStatus: http.StatusBadRequest,
		},
		// Test case - 5.
		// Test Case with object greater than maximum allowed size.
		{
			bucketName:         bucketName,
			objectName:         objectName,
			data:               bytesData,
			dataLen:            len(bytesData),
			accessKey:          credentials.AccessKey,
			secretKey:          credentials.SecretKey,
			fault:              TooBigObject,
			expectedRespStatus: http.StatusBadRequest,
		},
		// Test case - 6.
		// Test Case with missing content length
		{
			bucketName:         bucketName,
			objectName:         objectName,
			data:               bytesData,
			dataLen:            len(bytesData),
			accessKey:          credentials.AccessKey,
			secretKey:          credentials.SecretKey,
			fault:              MissingContentLength,
			expectedRespStatus: http.StatusLengthRequired,
		},
		// Test case - 7.
		// Test Case with invalid header key X-Amz-Storage-Class
		{
			bucketName:         bucketName,
			objectName:         objectName,
			headers:            inalidStorageClassHeader,
			data:               bytesData,
			dataLen:            len(bytesData),
			accessKey:          credentials.AccessKey,
			secretKey:          credentials.SecretKey,
			expectedRespStatus: http.StatusBadRequest,
		},
	}
	// Iterating over the cases, fetching the object validating the response.
	for i, testCase := range testCases {
		var req, reqV2 *http.Request
		// initialize HTTP NewRecorder, this records any mutations to response writer inside the handler.
		rec := httptest.NewRecorder()
		// construct HTTP request for Get Object end point.
		req, err = newTestSignedRequestV4("PUT", getPutObjectURL("", testCase.bucketName, testCase.objectName),
			int64(testCase.dataLen), bytes.NewReader(testCase.data), testCase.accessKey, testCase.secretKey)
		if err != nil {
			t.Fatalf("Test %d: Failed to create HTTP request for Put Object: <ERROR> %v", i+1, err)
		}
		// Add test case specific headers to the request.
		addCustomHeaders(req, testCase.headers)

		// Inject faults if specified in testCase.fault
		switch testCase.fault {
		case MissingContentLength:
			req.ContentLength = -1
			req.TransferEncoding = []string{}
		case TooBigObject:
			req.ContentLength = globalMaxObjectSize + 1
		}
		// Since `apiRouter` satisfies `http.Handler` it has a ServeHTTP to execute the logic of the handler.
		// Call the ServeHTTP to execute the handler,`func (api objectAPIHandlers) GetObjectHandler`  handles the request.
		apiRouter.ServeHTTP(rec, req)
		// Assert the response code with the expected status.
		if rec.Code != testCase.expectedRespStatus {
			t.Fatalf("Case %d: Expected the response status to be `%d`, but instead found `%d`", i+1, testCase.expectedRespStatus, rec.Code)
		}
		if testCase.expectedRespStatus == http.StatusOK {
			buffer := new(bytes.Buffer)

			// Fetch the object to check whether the content is same as the one uploaded via PutObject.
			err = obj.GetObject(testCase.bucketName, testCase.objectName, 0, int64(len(bytesData)), buffer, "")
			if err != nil {
				t.Fatalf("Test %d: %s: Failed to fetch the copied object: <ERROR> %s", i+1, instanceType, err)
			}
			if !bytes.Equal(bytesData, buffer.Bytes()) {
				t.Errorf("Test %d: %s: Data Mismatch: Data fetched back from the uploaded object doesn't match the original one.", i+1, instanceType)
			}
			buffer.Reset()
		}

		// Verify response of the V2 signed HTTP request.
		// initialize HTTP NewRecorder, this records any mutations to response writer inside the handler.
		recV2 := httptest.NewRecorder()
		// construct HTTP request for PUT Object endpoint.
		reqV2, err = newTestSignedRequestV2("PUT", getPutObjectURL("", testCase.bucketName, testCase.objectName),
			int64(testCase.dataLen), bytes.NewReader(testCase.data), testCase.accessKey, testCase.secretKey)

		if err != nil {
			t.Fatalf("Test %d: %s: Failed to create HTTP request for PutObject: <ERROR> %v", i+1, instanceType, err)
		}

		// Add test case specific headers to the request.
		addCustomHeaders(reqV2, testCase.headers)

		// Inject faults if specified in testCase.fault
		switch testCase.fault {
		case MissingContentLength:
			reqV2.ContentLength = -1
			reqV2.TransferEncoding = []string{}
		case TooBigObject:
			reqV2.ContentLength = globalMaxObjectSize + 1
		}

		// Since `apiRouter` satisfies `http.Handler` it has a ServeHTTP to execute the logic of the handler.
		// Call the ServeHTTP to execute the handler.
		apiRouter.ServeHTTP(recV2, reqV2)
		if recV2.Code != testCase.expectedRespStatus {
			t.Errorf("Test %d: %s: Expected the response status to be `%d`, but instead found `%d`", i+1, instanceType, testCase.expectedRespStatus, recV2.Code)
		}

		if testCase.expectedRespStatus == http.StatusOK {
			buffer := new(bytes.Buffer)
			// Fetch the object to check whether the content is same as the one uploaded via PutObject.
			err = obj.GetObject(testCase.bucketName, testCase.objectName, 0, int64(len(bytesData)), buffer, "")
			if err != nil {
				t.Fatalf("Test %d: %s: Failed to fetch the copied object: <ERROR> %s", i+1, instanceType, err)
			}
			if !bytes.Equal(bytesData, buffer.Bytes()) {
				t.Errorf("Test %d: %s: Data Mismatch: Data fetched back from the uploaded object doesn't match the original one.", i+1, instanceType)
			}
			buffer.Reset()
		}
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
	ExecObjectLayerAPIAnonTest(t, obj, "TestAPIPutObjectHandler", bucketName, objectName, instanceType, apiRouter, anonReq, getWriteOnlyObjectStatement)

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

// Tests sanity of attempting to copying each parts at offsets from an existing
// file and create a new object. Also validates if the written is same as what we
// expected.
func TestAPICopyObjectPartHandlerSanity(t *testing.T) {
	defer DetectTestLeak(t)()
	ExecObjectLayerAPITest(t, testAPICopyObjectPartHandlerSanity, []string{"CopyObjectPart"})
}

func testAPICopyObjectPartHandlerSanity(obj ObjectLayer, instanceType, bucketName string, apiRouter http.Handler,
	credentials auth.Credentials, t *testing.T) {

	objectName := "test-object"
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
		{generateBytesData(6 * humanize.MiByte)},
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
		_, err = obj.PutObject(input.bucketName, input.objectName,
			mustGetHashReader(t, bytes.NewBuffer(input.textData), input.contentLength, input.metaData[""], ""), input.metaData)
		// if object upload fails stop the test.
		if err != nil {
			t.Fatalf("Put Object case %d:  Error uploading object: <ERROR> %v", i+1, err)
		}
	}

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

	a := 0
	b := globalMinPartSize - 1
	var parts []CompletePart
	for partNumber := 1; partNumber <= 2; partNumber++ {
		// initialize HTTP NewRecorder, this records any mutations to response writer inside the handler.
		rec := httptest.NewRecorder()
		cpPartURL := getCopyObjectPartURL("", bucketName, testObject, uploadID, fmt.Sprintf("%d", partNumber))

		// construct HTTP request for copy object.
		var req *http.Request
		req, err = newTestSignedRequestV4("PUT", cpPartURL, 0, nil, credentials.AccessKey, credentials.SecretKey)
		if err != nil {
			t.Fatalf("Test failed to create HTTP request for copy object part: <ERROR> %v", err)
		}

		// "X-Amz-Copy-Source" header contains the information about the source bucket and the object to copied.
		req.Header.Set("X-Amz-Copy-Source", url.QueryEscape(pathJoin(bucketName, objectName)))
		req.Header.Set("X-Amz-Copy-Source-Range", fmt.Sprintf("bytes=%d-%d", a, b))

		// Since `apiRouter` satisfies `http.Handler` it has a ServeHTTP to execute the logic of the handler.
		// Call the ServeHTTP to execute the handler, `func (api objectAPIHandlers) CopyObjectHandler` handles the request.
		a = globalMinPartSize
		b = len(bytesData[0].byteData) - 1
		apiRouter.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("Test failed to create HTTP request for copy %d", rec.Code)
		}

		resp := &CopyObjectPartResponse{}
		if err = xmlDecoder(rec.Body, resp, rec.Result().ContentLength); err != nil {
			t.Fatalf("Test failed to decode XML response: <ERROR> %v", err)
		}

		parts = append(parts, CompletePart{
			PartNumber: partNumber,
			ETag:       canonicalizeETag(resp.ETag),
		})
	}

	result, err := obj.CompleteMultipartUpload(bucketName, testObject, uploadID, parts)
	if err != nil {
		t.Fatalf("Test: %s complete multipart upload failed: <ERROR> %v", instanceType, err)
	}
	if result.Size != int64(len(bytesData[0].byteData)) {
		t.Fatalf("Test: %s expected size not written: expected %d, got %d", instanceType, len(bytesData[0].byteData), result.Size)
	}

	var buf bytes.Buffer
	if err = obj.GetObject(bucketName, testObject, 0, int64(len(bytesData[0].byteData)), &buf, ""); err != nil {
		t.Fatalf("Test: %s reading completed file failed: <ERROR> %v", instanceType, err)
	}
	if !bytes.Equal(buf.Bytes(), bytesData[0].byteData) {
		t.Fatalf("Test: %s returned data is not expected corruption detected:", instanceType)
	}
}

// Wrapper for calling Copy Object Part API handler tests for both XL multiple disks and single node setup.
func TestAPICopyObjectPartHandler(t *testing.T) {
	defer DetectTestLeak(t)()
	ExecObjectLayerAPITest(t, testAPICopyObjectPartHandler, []string{"CopyObjectPart"})
}

func testAPICopyObjectPartHandler(obj ObjectLayer, instanceType, bucketName string, apiRouter http.Handler,
	credentials auth.Credentials, t *testing.T) {

	objectName := "test-object"
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
		{generateBytesData(6 * humanize.KiByte)},
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
		_, err = obj.PutObject(input.bucketName, input.objectName, mustGetHashReader(t, bytes.NewBuffer(input.textData), input.contentLength, input.metaData[""], ""), input.metaData)
		// if object upload fails stop the test.
		if err != nil {
			t.Fatalf("Put Object case %d:  Error uploading object: <ERROR> %v", i+1, err)
		}
	}

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

	// test cases with inputs and expected result for Copy Object.
	testCases := []struct {
		bucketName        string
		copySourceHeader  string // data for "X-Amz-Copy-Source" header. Contains the object to be copied in the URL.
		copySourceRange   string // data for "X-Amz-Copy-Source-Range" header, contains the byte range offsets of data to be copied.
		uploadID          string // uploadID of the transaction.
		invalidPartNumber bool   // Sets an invalid multipart.
		maximumPartNumber bool   // Sets a maximum parts.
		accessKey         string
		secretKey         string
		// expected output.
		expectedRespStatus int
	}{
		// Test case - 1, copy part 1 from from newObject1, ignore request headers.
		{
			bucketName:         bucketName,
			uploadID:           uploadID,
			copySourceHeader:   url.QueryEscape("/" + bucketName + "/" + objectName),
			accessKey:          credentials.AccessKey,
			secretKey:          credentials.SecretKey,
			expectedRespStatus: http.StatusOK,
		},

		// Test case - 2.
		// Test case with invalid source object.
		{
			bucketName:       bucketName,
			uploadID:         uploadID,
			copySourceHeader: url.QueryEscape("/"),
			accessKey:        credentials.AccessKey,
			secretKey:        credentials.SecretKey,

			expectedRespStatus: http.StatusBadRequest,
		},

		// Test case - 3.
		// Test case with new object name is same as object to be copied.
		// Fail with file not found.
		{
			bucketName:       bucketName,
			uploadID:         uploadID,
			copySourceHeader: url.QueryEscape("/" + bucketName + "/" + testObject),
			accessKey:        credentials.AccessKey,
			secretKey:        credentials.SecretKey,

			expectedRespStatus: http.StatusNotFound,
		},

		// Test case - 4.
		// Test case with valid byte range.
		{
			bucketName:       bucketName,
			uploadID:         uploadID,
			copySourceHeader: url.QueryEscape("/" + bucketName + "/" + objectName),
			copySourceRange:  "bytes=500-4096",
			accessKey:        credentials.AccessKey,
			secretKey:        credentials.SecretKey,

			expectedRespStatus: http.StatusOK,
		},

		// Test case - 5.
		// Test case with invalid byte range.
		{
			bucketName:       bucketName,
			uploadID:         uploadID,
			copySourceHeader: url.QueryEscape("/" + bucketName + "/" + objectName),
			copySourceRange:  "bytes=6145-",
			accessKey:        credentials.AccessKey,
			secretKey:        credentials.SecretKey,

			expectedRespStatus: http.StatusBadRequest,
		},

		// Test case - 6.
		// Test case with ivalid byte range for exceeding source size boundaries.
		{
			bucketName:       bucketName,
			uploadID:         uploadID,
			copySourceHeader: url.QueryEscape("/" + bucketName + "/" + objectName),
			copySourceRange:  "bytes=0-6144",
			accessKey:        credentials.AccessKey,
			secretKey:        credentials.SecretKey,

			expectedRespStatus: http.StatusBadRequest,
		},

		// Test case - 7.
		// Test case with object name missing from source.
		// fail with BadRequest.
		{
			bucketName:       bucketName,
			uploadID:         uploadID,
			copySourceHeader: url.QueryEscape("//123"),
			accessKey:        credentials.AccessKey,
			secretKey:        credentials.SecretKey,

			expectedRespStatus: http.StatusBadRequest,
		},

		// Test case - 8.
		// Test case with non-existent source file.
		// Case for the purpose of failing `api.ObjectAPI.GetObjectInfo`.
		// Expecting the response status code to http.StatusNotFound (404).
		{
			bucketName:       bucketName,
			uploadID:         uploadID,
			copySourceHeader: url.QueryEscape("/" + bucketName + "/" + "non-existent-object"),
			accessKey:        credentials.AccessKey,
			secretKey:        credentials.SecretKey,

			expectedRespStatus: http.StatusNotFound,
		},

		// Test case - 9.
		// Test case with non-existent source file.
		// Case for the purpose of failing `api.ObjectAPI.PutObjectPart`.
		// Expecting the response status code to http.StatusNotFound (404).
		{
			bucketName:       "non-existent-destination-bucket",
			uploadID:         uploadID,
			copySourceHeader: url.QueryEscape("/" + bucketName + "/" + objectName),
			accessKey:        credentials.AccessKey,
			secretKey:        credentials.SecretKey,

			expectedRespStatus: http.StatusNotFound,
		},

		// Test case - 10.
		// Case with invalid AccessKey.
		{
			bucketName:       bucketName,
			uploadID:         uploadID,
			copySourceHeader: url.QueryEscape("/" + bucketName + "/" + objectName),
			accessKey:        "Invalid-AccessID",
			secretKey:        credentials.SecretKey,

			expectedRespStatus: http.StatusForbidden,
		},

		// Test case - 11.
		// Case with non-existent upload id.
		{
			bucketName:       bucketName,
			uploadID:         "-1",
			copySourceHeader: url.QueryEscape("/" + bucketName + "/" + objectName),
			accessKey:        credentials.AccessKey,
			secretKey:        credentials.SecretKey,

			expectedRespStatus: http.StatusNotFound,
		},
		// Test case - 12.
		// invalid part number.
		{
			bucketName:         bucketName,
			uploadID:           uploadID,
			copySourceHeader:   url.QueryEscape("/" + bucketName + "/" + objectName),
			invalidPartNumber:  true,
			accessKey:          credentials.AccessKey,
			secretKey:          credentials.SecretKey,
			expectedRespStatus: http.StatusOK,
		},
		// Test case - 13.
		// maximum part number.
		{
			bucketName:         bucketName,
			uploadID:           uploadID,
			copySourceHeader:   url.QueryEscape("/" + bucketName + "/" + objectName),
			maximumPartNumber:  true,
			accessKey:          credentials.AccessKey,
			secretKey:          credentials.SecretKey,
			expectedRespStatus: http.StatusOK,
		},
	}

	for i, testCase := range testCases {
		var req *http.Request
		var reqV2 *http.Request
		// initialize HTTP NewRecorder, this records any mutations to response writer inside the handler.
		rec := httptest.NewRecorder()
		if !testCase.invalidPartNumber || !testCase.maximumPartNumber {
			// construct HTTP request for copy object.
			req, err = newTestSignedRequestV4("PUT", getCopyObjectPartURL("", testCase.bucketName, testObject, testCase.uploadID, "1"), 0, nil, testCase.accessKey, testCase.secretKey)
		} else if testCase.invalidPartNumber {
			req, err = newTestSignedRequestV4("PUT", getCopyObjectPartURL("", testCase.bucketName, testObject, testCase.uploadID, "abc"), 0, nil, testCase.accessKey, testCase.secretKey)
		} else if testCase.maximumPartNumber {
			req, err = newTestSignedRequestV4("PUT", getCopyObjectPartURL("", testCase.bucketName, testObject, testCase.uploadID, "99999"), 0, nil, testCase.accessKey, testCase.secretKey)
		}
		if err != nil {
			t.Fatalf("Test %d: Failed to create HTTP request for copy Object: <ERROR> %v", i+1, err)
		}

		// "X-Amz-Copy-Source" header contains the information about the source bucket and the object to copied.
		if testCase.copySourceHeader != "" {
			req.Header.Set("X-Amz-Copy-Source", testCase.copySourceHeader)
		}
		if testCase.copySourceRange != "" {
			req.Header.Set("X-Amz-Copy-Source-Range", testCase.copySourceRange)
		}
		// Since `apiRouter` satisfies `http.Handler` it has a ServeHTTP to execute the logic of the handler.
		// Call the ServeHTTP to execute the handler, `func (api objectAPIHandlers) CopyObjectHandler` handles the request.
		apiRouter.ServeHTTP(rec, req)
		// Assert the response code with the expected status.
		if rec.Code != testCase.expectedRespStatus {
			t.Fatalf("Test %d: %s:  Expected the response status to be `%d`, but instead found `%d`", i+1, instanceType, testCase.expectedRespStatus, rec.Code)
		}
		if rec.Code == http.StatusOK {
			// See if the new part has been uploaded.
			// testing whether the copy was successful.
			var results ListPartsInfo
			results, err = obj.ListObjectParts(testCase.bucketName, testObject, testCase.uploadID, 0, 1)
			if err != nil {
				t.Fatalf("Test %d: %s: Failed to look for copied object part: <ERROR> %s", i+1, instanceType, err)
			}
			if instanceType != FSTestStr && len(results.Parts) != 1 {
				t.Fatalf("Test %d: %s: Expected only one entry returned %d entries", i+1, instanceType, len(results.Parts))
			}
		}

		// Verify response of the V2 signed HTTP request.
		// initialize HTTP NewRecorder, this records any mutations to response writer inside the handler.
		recV2 := httptest.NewRecorder()

		reqV2, err = newTestRequest("PUT", getCopyObjectPartURL("", testCase.bucketName, testObject, testCase.uploadID, "1"), 0, nil)
		if err != nil {
			t.Fatalf("Test %d: Failed to create HTTP request for copy Object: <ERROR> %v", i+1, err)
		}
		// "X-Amz-Copy-Source" header contains the information about the source bucket and the object to copied.
		if testCase.copySourceHeader != "" {
			reqV2.Header.Set("X-Amz-Copy-Source", testCase.copySourceHeader)
		}
		if testCase.copySourceRange != "" {
			reqV2.Header.Set("X-Amz-Copy-Source-Range", testCase.copySourceRange)
		}

		err = signRequestV2(reqV2, testCase.accessKey, testCase.secretKey)
		if err != nil {
			t.Fatalf("Failed to V2 Sign the HTTP request: %v.", err)
		}

		// Since `apiRouter` satisfies `http.Handler` it has a ServeHTTP to execute the logic of the handler.
		// Call the ServeHTTP to execute the handler.
		apiRouter.ServeHTTP(recV2, reqV2)
		if recV2.Code != testCase.expectedRespStatus {
			t.Errorf("Test %d: %s: Expected the response status to be `%d`, but instead found `%d`", i+1, instanceType, testCase.expectedRespStatus, recV2.Code)
		}
	}

	// HTTP request for testing when `ObjectLayer` is set to `nil`.
	// There is no need to use an existing bucket and valid input for creating the request
	// since the `objectLayer==nil`  check is performed before any other checks inside the handlers.
	// The only aim is to generate an HTTP request in a way that the relevant/registered end point is evoked/called.
	nilBucket := "dummy-bucket"
	nilObject := "dummy-object"

	nilReq, err := newTestSignedRequestV4("PUT", getCopyObjectPartURL("", nilBucket, nilObject, "0", "0"),
		0, bytes.NewReader([]byte("testNilObjLayer")), "", "")
	if err != nil {
		t.Errorf("Minio %s: Failed to create http request for testing the response when object Layer is set to `nil`.", instanceType)
	}

	// Below is how CopyObjectPartHandler is registered.
	// bucket.Methods("PUT").Path("/{object:.+}").HeadersRegexp("X-Amz-Copy-Source", ".*?(\\/|%2F).*?").HandlerFunc(api.CopyObjectPartHandler).Queries("partNumber", "{partNumber:[0-9]+}", "uploadId", "{uploadId:.*}")
	// Its necessary to set the "X-Amz-Copy-Source" header for the request to be accepted by the handler.
	nilReq.Header.Set("X-Amz-Copy-Source", url.QueryEscape("/"+nilBucket+"/"+nilObject))

	// execute the object layer set to `nil` test.
	// `ExecObjectLayerAPINilTest` manages the operation.
	ExecObjectLayerAPINilTest(t, nilBucket, nilObject, instanceType, apiRouter, nilReq)

}

// Wrapper for calling Copy Object API handler tests for both XL multiple disks and single node setup.
func TestAPICopyObjectHandler(t *testing.T) {
	defer DetectTestLeak(t)()
	ExecObjectLayerAPITest(t, testAPICopyObjectHandler, []string{"CopyObject"})
}

func testAPICopyObjectHandler(obj ObjectLayer, instanceType, bucketName string, apiRouter http.Handler,
	credentials auth.Credentials, t *testing.T) {

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
		{generateBytesData(6 * humanize.KiByte)},
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
	// iterate through the above set of inputs and upload the object.
	for i, input := range putObjectInputs {
		// uploading the object.
		_, err = obj.PutObject(input.bucketName, input.objectName, mustGetHashReader(t, bytes.NewBuffer(input.textData), input.contentLength, input.metaData[""], ""), input.metaData)
		// if object upload fails stop the test.
		if err != nil {
			t.Fatalf("Put Object case %d:  Error uploading object: <ERROR> %v", i+1, err)
		}
	}

	// test cases with inputs and expected result for Copy Object.
	testCases := []struct {
		bucketName           string
		newObjectName        string // name of the newly copied object.
		copySourceHeader     string // data for "X-Amz-Copy-Source" header. Contains the object to be copied in the URL.
		copyModifiedHeader   string // data for "X-Amz-Copy-Source-If-Modified-Since" header
		copyUnmodifiedHeader string // data for "X-Amz-Copy-Source-If-Unmodified-Since" header
		metadataGarbage      bool
		metadataReplace      bool
		metadataCopy         bool
		metadata             map[string]string
		accessKey            string
		secretKey            string
		// expected output.
		expectedRespStatus int
	}{
		// Test case - 1, copy metadata from newObject1, ignore request headers.
		{
			bucketName:       bucketName,
			newObjectName:    "newObject1",
			copySourceHeader: url.QueryEscape("/" + bucketName + "/" + objectName),
			accessKey:        credentials.AccessKey,
			secretKey:        credentials.SecretKey,
			metadata: map[string]string{
				"Content-Type": "application/json",
			},
			expectedRespStatus: http.StatusOK,
		},

		// Test case - 2.
		// Test case with invalid source object.
		{
			bucketName:       bucketName,
			newObjectName:    "newObject1",
			copySourceHeader: url.QueryEscape("/"),
			accessKey:        credentials.AccessKey,
			secretKey:        credentials.SecretKey,

			expectedRespStatus: http.StatusBadRequest,
		},

		// Test case - 3.
		// Test case with new object name is same as object to be copied.
		{
			bucketName:       bucketName,
			newObjectName:    objectName,
			copySourceHeader: url.QueryEscape("/" + bucketName + "/" + objectName),
			accessKey:        credentials.AccessKey,
			secretKey:        credentials.SecretKey,

			expectedRespStatus: http.StatusBadRequest,
		},

		// Test case - 4.
		// Test case with new object name is same as object to be copied.
		// But source copy is without leading slash
		{
			bucketName:       bucketName,
			newObjectName:    objectName,
			copySourceHeader: url.QueryEscape(bucketName + "/" + objectName),
			accessKey:        credentials.AccessKey,
			secretKey:        credentials.SecretKey,

			expectedRespStatus: http.StatusBadRequest,
		},

		// Test case - 5.
		// Test case with new object name is same as object to be copied
		// but metadata is updated.
		{
			bucketName:       bucketName,
			newObjectName:    objectName,
			copySourceHeader: url.QueryEscape("/" + bucketName + "/" + objectName),
			metadata: map[string]string{
				"Content-Type": "application/json",
			},
			metadataReplace: true,
			accessKey:       credentials.AccessKey,
			secretKey:       credentials.SecretKey,

			expectedRespStatus: http.StatusOK,
		},

		// Test case - 6.
		// Test case with invalid metadata-directive.
		{
			bucketName:       bucketName,
			newObjectName:    "newObject1",
			copySourceHeader: url.QueryEscape("/" + bucketName + "/" + objectName),
			metadata: map[string]string{
				"Content-Type": "application/json",
			},
			metadataGarbage: true,
			accessKey:       credentials.AccessKey,
			secretKey:       credentials.SecretKey,

			expectedRespStatus: http.StatusBadRequest,
		},

		// Test case - 7.
		// Test case with new object name is same as object to be copied
		// fail with BadRequest.
		{
			bucketName:       bucketName,
			newObjectName:    objectName,
			copySourceHeader: url.QueryEscape("/" + bucketName + "/" + objectName),
			metadata: map[string]string{
				"Content-Type": "application/json",
			},
			metadataCopy: true,
			accessKey:    credentials.AccessKey,
			secretKey:    credentials.SecretKey,

			expectedRespStatus: http.StatusBadRequest,
		},

		// Test case - 8.
		// Test case with non-existent source file.
		// Case for the purpose of failing `api.ObjectAPI.GetObjectInfo`.
		// Expecting the response status code to http.StatusNotFound (404).
		{
			bucketName:       bucketName,
			newObjectName:    objectName,
			copySourceHeader: url.QueryEscape("/" + bucketName + "/" + "non-existent-object"),
			accessKey:        credentials.AccessKey,
			secretKey:        credentials.SecretKey,

			expectedRespStatus: http.StatusNotFound,
		},

		// Test case - 9.
		// Test case with non-existent source file.
		// Case for the purpose of failing `api.ObjectAPI.PutObject`.
		// Expecting the response status code to http.StatusNotFound (404).
		{
			bucketName:       "non-existent-destination-bucket",
			newObjectName:    objectName,
			copySourceHeader: url.QueryEscape("/" + bucketName + "/" + objectName),
			accessKey:        credentials.AccessKey,
			secretKey:        credentials.SecretKey,

			expectedRespStatus: http.StatusNotFound,
		},

		// Test case - 10.
		// Case with invalid AccessKey.
		{
			bucketName:       bucketName,
			newObjectName:    objectName,
			copySourceHeader: url.QueryEscape("/" + bucketName + "/" + objectName),
			accessKey:        "Invalid-AccessID",
			secretKey:        credentials.SecretKey,

			expectedRespStatus: http.StatusForbidden,
		},
		// Test case - 11, copy metadata from newObject1 with satisfying modified header.
		{
			bucketName:         bucketName,
			newObjectName:      "newObject1",
			copySourceHeader:   url.QueryEscape("/" + bucketName + "/" + objectName),
			copyModifiedHeader: "Mon, 02 Jan 2006 15:04:05 GMT",
			accessKey:          credentials.AccessKey,
			secretKey:          credentials.SecretKey,
			expectedRespStatus: http.StatusOK,
		},
		// Test case - 12, copy metadata from newObject1 with unsatisfying modified header.
		{
			bucketName:         bucketName,
			newObjectName:      "newObject1",
			copySourceHeader:   url.QueryEscape("/" + bucketName + "/" + objectName),
			copyModifiedHeader: "Mon, 02 Jan 2217 15:04:05 GMT",
			accessKey:          credentials.AccessKey,
			secretKey:          credentials.SecretKey,
			expectedRespStatus: http.StatusPreconditionFailed,
		},
		// Test case - 13, copy metadata from newObject1 with wrong modified header format
		{
			bucketName:         bucketName,
			newObjectName:      "newObject1",
			copySourceHeader:   url.QueryEscape("/" + bucketName + "/" + objectName),
			copyModifiedHeader: "Mon, 02 Jan 2217 15:04:05 +00:00",
			accessKey:          credentials.AccessKey,
			secretKey:          credentials.SecretKey,
			expectedRespStatus: http.StatusOK,
		},
		// Test case - 14, copy metadata from newObject1 with satisfying unmodified header.
		{
			bucketName:           bucketName,
			newObjectName:        "newObject1",
			copySourceHeader:     url.QueryEscape("/" + bucketName + "/" + objectName),
			copyUnmodifiedHeader: "Mon, 02 Jan 2217 15:04:05 GMT",
			accessKey:            credentials.AccessKey,
			secretKey:            credentials.SecretKey,
			expectedRespStatus:   http.StatusOK,
		},
		// Test case - 15, copy metadata from newObject1 with unsatisfying unmodified header.
		{
			bucketName:           bucketName,
			newObjectName:        "newObject1",
			copySourceHeader:     url.QueryEscape("/" + bucketName + "/" + objectName),
			copyUnmodifiedHeader: "Mon, 02 Jan 2007 15:04:05 GMT",
			accessKey:            credentials.AccessKey,
			secretKey:            credentials.SecretKey,
			expectedRespStatus:   http.StatusPreconditionFailed,
		},
		// Test case - 16, copy metadata from newObject1 with incorrect unmodified header format.
		{
			bucketName:           bucketName,
			newObjectName:        "newObject1",
			copySourceHeader:     url.QueryEscape("/" + bucketName + "/" + objectName),
			copyUnmodifiedHeader: "Mon, 02 Jan 2007 15:04:05 +00:00",
			accessKey:            credentials.AccessKey,
			secretKey:            credentials.SecretKey,
			expectedRespStatus:   http.StatusOK,
		},
	}

	for i, testCase := range testCases {
		var req *http.Request
		var reqV2 *http.Request
		// initialize HTTP NewRecorder, this records any mutations to response writer inside the handler.
		rec := httptest.NewRecorder()
		// construct HTTP request for copy object.
		req, err = newTestSignedRequestV4("PUT", getCopyObjectURL("", testCase.bucketName, testCase.newObjectName),
			0, nil, testCase.accessKey, testCase.secretKey)

		if err != nil {
			t.Fatalf("Test %d: Failed to create HTTP request for copy Object: <ERROR> %v", i+1, err)
		}
		// "X-Amz-Copy-Source" header contains the information about the source bucket and the object to copied.
		if testCase.copySourceHeader != "" {
			req.Header.Set("X-Amz-Copy-Source", testCase.copySourceHeader)
		}
		if testCase.copyModifiedHeader != "" {
			req.Header.Set("X-Amz-Copy-Source-If-Modified-Since", testCase.copyModifiedHeader)
		}
		if testCase.copyUnmodifiedHeader != "" {
			req.Header.Set("X-Amz-Copy-Source-If-Unmodified-Since", testCase.copyUnmodifiedHeader)
		}
		// Add custom metadata.
		for k, v := range testCase.metadata {
			req.Header.Set(k, v)
		}
		if testCase.metadataReplace {
			req.Header.Set("X-Amz-Metadata-Directive", "REPLACE")
		}
		if testCase.metadataCopy {
			req.Header.Set("X-Amz-Metadata-Directive", "COPY")
		}
		if testCase.metadataGarbage {
			req.Header.Set("X-Amz-Metadata-Directive", "Unknown")
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
			err = obj.GetObject(testCase.bucketName, testCase.newObjectName, 0, int64(len(bytesData[0].byteData)), buffers[0], "")
			if err != nil {
				t.Fatalf("Test %d: %s: Failed to fetch the copied object: <ERROR> %s", i+1, instanceType, err)
			}
			if !bytes.Equal(bytesData[0].byteData, buffers[0].Bytes()) {
				t.Errorf("Test %d: %s: Data Mismatch: Data fetched back from the copied object doesn't match the original one.", i+1, instanceType)
			}
			buffers[0].Reset()
		}

		// Verify response of the V2 signed HTTP request.
		// initialize HTTP NewRecorder, this records any mutations to response writer inside the handler.
		recV2 := httptest.NewRecorder()

		reqV2, err = newTestRequest("PUT", getCopyObjectURL("", testCase.bucketName, testCase.newObjectName), 0, nil)
		if err != nil {
			t.Fatalf("Test %d: Failed to create HTTP request for copy Object: <ERROR> %v", i+1, err)
		}
		// "X-Amz-Copy-Source" header contains the information about the source bucket and the object to copied.
		if testCase.copySourceHeader != "" {
			reqV2.Header.Set("X-Amz-Copy-Source", testCase.copySourceHeader)
		}
		if testCase.copyModifiedHeader != "" {
			reqV2.Header.Set("X-Amz-Copy-Source-If-Modified-Since", testCase.copyModifiedHeader)
		}
		if testCase.copyUnmodifiedHeader != "" {
			reqV2.Header.Set("X-Amz-Copy-Source-If-Unmodified-Since", testCase.copyUnmodifiedHeader)
		}

		// Add custom metadata.
		for k, v := range testCase.metadata {
			reqV2.Header.Set(k, v+"+x")
		}
		if testCase.metadataReplace {
			reqV2.Header.Set("X-Amz-Metadata-Directive", "REPLACE")
		}
		if testCase.metadataCopy {
			reqV2.Header.Set("X-Amz-Metadata-Directive", "COPY")
		}
		if testCase.metadataGarbage {
			reqV2.Header.Set("X-Amz-Metadata-Directive", "Unknown")
		}

		err = signRequestV2(reqV2, testCase.accessKey, testCase.secretKey)

		if err != nil {
			t.Fatalf("Failed to V2 Sign the HTTP request: %v.", err)
		}
		// Since `apiRouter` satisfies `http.Handler` it has a ServeHTTP to execute the logic of the handler.
		// Call the ServeHTTP to execute the handler.
		apiRouter.ServeHTTP(recV2, reqV2)
		if recV2.Code != testCase.expectedRespStatus {
			t.Errorf("Test %d: %s: Expected the response status to be `%d`, but instead found `%d`", i+1, instanceType, testCase.expectedRespStatus, recV2.Code)
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
	ExecObjectLayerAPIAnonTest(t, obj, "TestAPICopyObjectHandler", bucketName, newCopyAnonObject, instanceType, apiRouter, anonReq, getWriteOnlyObjectStatement)

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
	defer DetectTestLeak(t)()
	ExecObjectLayerAPITest(t, testAPINewMultipartHandler, []string{"NewMultipart"})
}

func testAPINewMultipartHandler(obj ObjectLayer, instanceType, bucketName string, apiRouter http.Handler,
	credentials auth.Credentials, t *testing.T) {

	objectName := "test-object-new-multipart"
	rec := httptest.NewRecorder()
	// construct HTTP request for NewMultipart upload.
	req, err := newTestSignedRequestV4("POST", getNewMultipartURL("", bucketName, objectName),
		0, nil, credentials.AccessKey, credentials.SecretKey)

	if err != nil {
		t.Fatalf("Failed to create HTTP request for NewMultipart Request: <ERROR> %v", err)
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

	// Testing the response for Invalid AcccessID.
	// Forcing the signature check to fail.
	rec = httptest.NewRecorder()
	// construct HTTP request for NewMultipart upload.
	// Setting an invalid accessID.
	req, err = newTestSignedRequestV4("POST", getNewMultipartURL("", bucketName, objectName),
		0, nil, "Invalid-AccessID", credentials.SecretKey)

	if err != nil {
		t.Fatalf("Failed to create HTTP request for NewMultipart Request: <ERROR> %v", err)
	}

	// Since `apiRouter` satisfies `http.Handler` it has a ServeHTTP method to execute the logic of the handler.
	// Call the ServeHTTP to executes the registered handler.
	apiRouter.ServeHTTP(rec, req)
	// Assert the response code with the expected status.
	if rec.Code != http.StatusForbidden {
		t.Fatalf("%s:  Expected the response status to be `%d`, but instead found `%d`", instanceType, http.StatusForbidden, rec.Code)
	}

	// Verify response of the V2 signed HTTP request.
	// initialize HTTP NewRecorder, this records any mutations to response writer inside the handler.
	recV2 := httptest.NewRecorder()
	// construct HTTP request for NewMultipartUpload endpoint.
	reqV2, err := newTestSignedRequestV2("POST", getNewMultipartURL("", bucketName, objectName),
		0, nil, credentials.AccessKey, credentials.SecretKey)

	if err != nil {
		t.Fatalf("Failed to create HTTP request for NewMultipart Request: <ERROR> %v", err)
	}

	// Since `apiRouter` satisfies `http.Handler` it has a ServeHTTP to execute the logic of the handler.
	// Call the ServeHTTP to execute the handler.
	apiRouter.ServeHTTP(recV2, reqV2)
	// Assert the response code with the expected status.
	if recV2.Code != http.StatusOK {
		t.Fatalf("%s:  Expected the response status to be `%d`, but instead found `%d`", instanceType, http.StatusOK, recV2.Code)
	}
	// decode the response body.
	decoder = xml.NewDecoder(recV2.Body)
	multipartResponse = &InitiateMultipartUploadResponse{}

	err = decoder.Decode(multipartResponse)
	if err != nil {
		t.Fatalf("Error decoding the recorded response Body")
	}
	// verify the uploadID my making an attempt to list parts.
	_, err = obj.ListObjectParts(bucketName, objectName, multipartResponse.UploadID, 0, 1)
	if err != nil {
		t.Fatalf("Invalid UploadID: <ERROR> %s", err)
	}

	// Testing the response for invalid AcccessID.
	// Forcing the V2 signature check to fail.
	recV2 = httptest.NewRecorder()
	// construct HTTP request for NewMultipartUpload endpoint.
	// Setting invalid AccessID.
	reqV2, err = newTestSignedRequestV2("POST", getNewMultipartURL("", bucketName, objectName),
		0, nil, "Invalid-AccessID", credentials.SecretKey)

	if err != nil {
		t.Fatalf("Failed to create HTTP request for NewMultipart Request: <ERROR> %v", err)
	}

	// Since `apiRouter` satisfies `http.Handler` it has a ServeHTTP to execute the logic of the handler.
	// Call the ServeHTTP to execute the handler.
	apiRouter.ServeHTTP(recV2, reqV2)
	// Assert the response code with the expected status.
	if recV2.Code != http.StatusForbidden {
		t.Fatalf("%s:  Expected the response status to be `%d`, but instead found `%d`", instanceType, http.StatusForbidden, recV2.Code)
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
	ExecObjectLayerAPIAnonTest(t, obj, "TestAPINewMultipartHandler", bucketName, objectName, instanceType, apiRouter, anonReq, getWriteOnlyObjectStatement)

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
	defer DetectTestLeak(t)()
	ExecObjectLayerAPITest(t, testAPINewMultipartHandlerParallel, []string{"NewMultipart"})
}

func testAPINewMultipartHandlerParallel(obj ObjectLayer, instanceType, bucketName string, apiRouter http.Handler,
	credentials auth.Credentials, t *testing.T) {
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
			// construct HTTP request NewMultipartUpload.
			req, err := newTestSignedRequestV4("POST", getNewMultipartURL("", bucketName, objectName), 0, nil, credentials.AccessKey, credentials.SecretKey)

			if err != nil {
				t.Fatalf("Failed to create HTTP request for NewMultipart request: <ERROR> %v", err)
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
	defer DetectTestLeak(t)()
	ExecObjectLayerAPITest(t, testAPICompleteMultipartHandler, []string{"CompleteMultipart"})
}

func testAPICompleteMultipartHandler(obj ObjectLayer, instanceType, bucketName string, apiRouter http.Handler,
	credentials auth.Credentials, t *testing.T) {

	var err error
	// register event notifier.
	err = initEventNotifier(obj)

	if err != nil {
		t.Fatal("Notifier initialization failed.")
	}

	// object used for the test.
	objectName := "test-object-new-multipart"

	// uploadID obtained from NewMultipart upload.
	var uploadID string
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

	// Parts with size greater than 5 MiB.
	// Generating a 6 MiB byte array.
	validPart := bytes.Repeat([]byte("abcdef"), 1*humanize.MiByte)
	validPartMD5 := getMD5Hash(validPart)
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
		// Part with size larger than 5 MiB.
		{bucketName, objectName, uploadIDs[0], 5, string(validPart), validPartMD5, int64(len(string(validPart)))},
		{bucketName, objectName, uploadIDs[0], 6, string(validPart), validPartMD5, int64(len(string(validPart)))},

		// Part with size larger than 5 MiB.
		// Parts uploaded for anonymous/unsigned API handler test.
		{bucketName, objectName, uploadIDs[1], 1, string(validPart), validPartMD5, int64(len(string(validPart)))},
		{bucketName, objectName, uploadIDs[1], 2, string(validPart), validPartMD5, int64(len(string(validPart)))},
	}
	// Iterating over creatPartCases to generate multipart chunks.
	for _, part := range parts {
		_, err = obj.PutObjectPart(part.bucketName, part.objName, part.uploadID, part.PartID,
			mustGetHashReader(t, bytes.NewBufferString(part.inputReaderData), part.intputDataSize, part.inputMd5, ""))
		if err != nil {
			t.Fatalf("%s : %s", instanceType, err)
		}
	}
	// Parts to be sent as input for CompleteMultipartUpload.
	inputParts := []struct {
		parts []CompletePart
	}{
		// inputParts - 0.
		// Case for replicating ETag mismatch.
		{
			[]CompletePart{
				{ETag: "abcd", PartNumber: 1},
			},
		},
		// inputParts - 1.
		// should error out with part too small.
		{
			[]CompletePart{
				{ETag: "e2fc714c4727ee9395f324cd2e7f331f", PartNumber: 1},
				{ETag: "1f7690ebdd9b4caf8fab49ca1757bf27", PartNumber: 2},
			},
		},
		// inputParts - 2.
		// Case with invalid Part number.
		{
			[]CompletePart{
				{ETag: "e2fc714c4727ee9395f324cd2e7f331f", PartNumber: 10},
			},
		},
		// inputParts - 3.
		// Case with valid parts,but parts are unsorted.
		// Part size greater than 5 MiB.
		{
			[]CompletePart{
				{ETag: validPartMD5, PartNumber: 6},
				{ETag: validPartMD5, PartNumber: 5},
			},
		},
		// inputParts - 4.
		// Case with valid part.
		// Part size greater than 5 MiB.
		{
			[]CompletePart{
				{ETag: validPartMD5, PartNumber: 5},
				{ETag: validPartMD5, PartNumber: 6},
			},
		},

		// inputParts - 5.
		// Used for the case of testing for anonymous API request.
		// Part size greater than 5 MiB.
		{
			[]CompletePart{
				{ETag: validPartMD5, PartNumber: 1},
				{ETag: validPartMD5, PartNumber: 2},
			},
		},
	}

	// on successful complete multipart operation the s3MD5 for the parts uploaded will be returned.
	s3MD5, err := getCompleteMultipartMD5(inputParts[3].parts)
	if err != nil {
		t.Fatalf("Obtaining S3MD5 failed")
	}

	// generating the response body content for the success case.
	successResponse := generateCompleteMultpartUploadResponse(bucketName, objectName, getGetObjectURL("", bucketName, objectName), s3MD5)
	encodedSuccessResponse := encodeResponse(successResponse)

	testCases := []struct {
		bucket    string
		object    string
		uploadID  string
		parts     []CompletePart
		accessKey string
		secretKey string
		// Expected output of CompleteMultipartUpload.
		expectedContent []byte
		// Expected HTTP Response status.
		expectedRespStatus int
	}{
		// Test case - 1.
		// Upload and PartNumber exists, But a deliberate ETag mismatch is introduced.
		{
			bucket:    bucketName,
			object:    objectName,
			uploadID:  uploadIDs[0],
			parts:     inputParts[0].parts,
			accessKey: credentials.AccessKey,
			secretKey: credentials.SecretKey,

			expectedContent: encodeResponse(getAPIErrorResponse(getAPIError(toAPIErrorCode(InvalidPart{})),
				getGetObjectURL("", bucketName, objectName))),
			expectedRespStatus: http.StatusBadRequest,
		},
		// Test case - 2.
		// No parts specified in CompletePart{}.
		// Should return ErrMalformedXML in the response body.
		{
			bucket:    bucketName,
			object:    objectName,
			uploadID:  uploadIDs[0],
			parts:     []CompletePart{},
			accessKey: credentials.AccessKey,
			secretKey: credentials.SecretKey,

			expectedContent: encodeResponse(getAPIErrorResponse(getAPIError(ErrMalformedXML),
				getGetObjectURL("", bucketName, objectName))),
			expectedRespStatus: http.StatusBadRequest,
		},
		// Test case - 3.
		// Non-Existent uploadID.
		// 404 Not Found response status expected.
		{
			bucket:    bucketName,
			object:    objectName,
			uploadID:  "abc",
			parts:     inputParts[0].parts,
			accessKey: credentials.AccessKey,
			secretKey: credentials.SecretKey,

			expectedContent: encodeResponse(getAPIErrorResponse(getAPIError(toAPIErrorCode(InvalidUploadID{UploadID: "abc"})),
				getGetObjectURL("", bucketName, objectName))),
			expectedRespStatus: http.StatusNotFound,
		},
		// Test case - 4.
		// Case with part size being less than minimum allowed size.
		{
			bucket:    bucketName,
			object:    objectName,
			uploadID:  uploadIDs[0],
			parts:     inputParts[1].parts,
			accessKey: credentials.AccessKey,
			secretKey: credentials.SecretKey,

			expectedContent: encodeResponse(completeMultipartAPIError{int64(4), int64(5242880), 1, "e2fc714c4727ee9395f324cd2e7f331f",
				getAPIErrorResponse(getAPIError(toAPIErrorCode(PartTooSmall{PartNumber: 1})),
					getGetObjectURL("", bucketName, objectName))}),
			expectedRespStatus: http.StatusBadRequest,
		},
		// Test case - 5.
		// TestCase with invalid Part Number.
		{
			bucket:    bucketName,
			object:    objectName,
			uploadID:  uploadIDs[0],
			parts:     inputParts[2].parts,
			accessKey: credentials.AccessKey,
			secretKey: credentials.SecretKey,

			expectedContent: encodeResponse(getAPIErrorResponse(getAPIError(toAPIErrorCode(InvalidPart{})),
				getGetObjectURL("", bucketName, objectName))),
			expectedRespStatus: http.StatusBadRequest,
		},
		// Test case - 6.
		// Parts are not sorted according to the part number.
		// This should return ErrInvalidPartOrder in the response body.
		{
			bucket:    bucketName,
			object:    objectName,
			uploadID:  uploadIDs[0],
			parts:     inputParts[3].parts,
			accessKey: credentials.AccessKey,
			secretKey: credentials.SecretKey,

			expectedContent: encodeResponse(getAPIErrorResponse(getAPIError(ErrInvalidPartOrder),
				getGetObjectURL("", bucketName, objectName))),
			expectedRespStatus: http.StatusBadRequest,
		},
		// Test case - 7.
		// Test case with proper parts.
		// Should successed and the content in the response body is asserted.
		{
			bucket:    bucketName,
			object:    objectName,
			uploadID:  uploadIDs[0],
			parts:     inputParts[4].parts,
			accessKey: "Invalid-AccessID",
			secretKey: credentials.SecretKey,

			expectedContent: encodeResponse(getAPIErrorResponse(getAPIError(ErrInvalidAccessKeyID),
				getGetObjectURL("", bucketName, objectName))),
			expectedRespStatus: http.StatusForbidden,
		},
		// Test case - 8.
		// Test case with proper parts.
		// Should successed and the content in the response body is asserted.
		{
			bucket:    bucketName,
			object:    objectName,
			uploadID:  uploadIDs[0],
			parts:     inputParts[4].parts,
			accessKey: credentials.AccessKey,
			secretKey: credentials.SecretKey,

			expectedContent:    encodedSuccessResponse,
			expectedRespStatus: http.StatusOK,
		},
	}

	for i, testCase := range testCases {
		var req *http.Request
		var completeBytes, actualContent []byte
		// Complete multipart upload parts.
		completeUploads := &CompleteMultipartUpload{
			Parts: testCase.parts,
		}
		completeBytes, err = xml.Marshal(completeUploads)
		if err != nil {
			t.Fatalf("Error XML encoding of parts: <ERROR> %s.", err)
		}
		// Indicating that all parts are uploaded and initiating CompleteMultipartUpload.
		req, err = newTestSignedRequestV4("POST", getCompleteMultipartUploadURL("", bucketName, objectName, testCase.uploadID),
			int64(len(completeBytes)), bytes.NewReader(completeBytes), testCase.accessKey, testCase.secretKey)
		if err != nil {
			t.Fatalf("Failed to create HTTP request for CompleteMultipartUpload: <ERROR> %v", err)
		}

		rec := httptest.NewRecorder()

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
	completeUploads := &CompleteMultipartUpload{
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
	ExecObjectLayerAPIAnonTest(t, obj, "TestAPICompleteMultipartHandler", bucketName, objectName, instanceType,
		apiRouter, anonReq, getWriteOnlyObjectStatement)

	// HTTP request to test the case of `objectLayer` being set to `nil`.
	// There is no need to use an existing bucket or valid input for creating the request,
	// since the `objectLayer==nil`  check is performed before any other checks inside the handlers.
	// The only aim is to generate an HTTP request in a way that the relevant/registered end point is evoked/called.
	// Indicating that all parts are uploaded and initiating CompleteMultipartUpload.
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

// The UploadID from the response body is parsed and its existence is asserted with an attempt to ListParts using it.
func TestAPIAbortMultipartHandler(t *testing.T) {
	defer DetectTestLeak(t)()
	ExecObjectLayerAPITest(t, testAPIAbortMultipartHandler, []string{"AbortMultipart"})
}

func testAPIAbortMultipartHandler(obj ObjectLayer, instanceType, bucketName string, apiRouter http.Handler,
	credentials auth.Credentials, t *testing.T) {

	var err error
	// register event notifier.
	err = initEventNotifier(obj)

	if err != nil {
		t.Fatal("Notifier initialization failed.")
	}

	// object used for the test.
	objectName := "test-object-new-multipart"

	// uploadID obtained from NewMultipart upload.
	var uploadID string
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

	// Parts with size greater than 5 MiB.
	// Generating a 6 MiB byte array.
	validPart := bytes.Repeat([]byte("abcdef"), 1*humanize.MiByte)
	validPartMD5 := getMD5Hash(validPart)
	// Create multipart parts.
	// Need parts to be uploaded before AbortMultiPartUpload can be called tested.
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
		// Part with size larger than 5 MiB.
		{bucketName, objectName, uploadIDs[0], 5, string(validPart), validPartMD5, int64(len(string(validPart)))},
		{bucketName, objectName, uploadIDs[0], 6, string(validPart), validPartMD5, int64(len(string(validPart)))},

		// Part with size larger than 5 MiB.
		// Parts uploaded for anonymous/unsigned API handler test.
		{bucketName, objectName, uploadIDs[1], 1, string(validPart), validPartMD5, int64(len(string(validPart)))},
		{bucketName, objectName, uploadIDs[1], 2, string(validPart), validPartMD5, int64(len(string(validPart)))},
	}
	// Iterating over createPartCases to generate multipart chunks.
	for _, part := range parts {
		_, err = obj.PutObjectPart(part.bucketName, part.objName, part.uploadID, part.PartID,
			mustGetHashReader(t, bytes.NewBufferString(part.inputReaderData), part.intputDataSize, part.inputMd5, ""))
		if err != nil {
			t.Fatalf("%s : %s", instanceType, err)
		}
	}

	testCases := []struct {
		bucket    string
		object    string
		uploadID  string
		accessKey string
		secretKey string
		// Expected HTTP Response status.
		expectedRespStatus int
	}{
		// Test case - 1.
		// Abort existing upload ID.
		{
			bucket:             bucketName,
			object:             objectName,
			uploadID:           uploadIDs[0],
			accessKey:          credentials.AccessKey,
			secretKey:          credentials.SecretKey,
			expectedRespStatus: http.StatusNoContent,
		},
		// Test case - 2.
		// Abort non-existng upload ID.
		{
			bucket:             bucketName,
			object:             objectName,
			uploadID:           "nonexistent-upload-id",
			accessKey:          credentials.AccessKey,
			secretKey:          credentials.SecretKey,
			expectedRespStatus: http.StatusNotFound,
		},
		// Test case - 3.
		// Abort with unknown Access key.
		{
			bucket:             bucketName,
			object:             objectName,
			uploadID:           uploadIDs[0],
			accessKey:          "Invalid-AccessID",
			secretKey:          credentials.SecretKey,
			expectedRespStatus: http.StatusForbidden,
		},
	}

	for i, testCase := range testCases {
		var req *http.Request
		// Indicating that all parts are uploaded and initiating abortMultipartUpload.
		req, err = newTestSignedRequestV4("DELETE", getAbortMultipartUploadURL("", testCase.bucket, testCase.object, testCase.uploadID),
			0, nil, testCase.accessKey, testCase.secretKey)
		if err != nil {
			t.Fatalf("Failed to create HTTP request for AbortMultipartUpload: <ERROR> %v", err)
		}

		rec := httptest.NewRecorder()

		// Since `apiRouter` satisfies `http.Handler` it has a ServeHTTP to execute the logic of the handler.
		// Call the ServeHTTP to executes the registered handler.
		apiRouter.ServeHTTP(rec, req)
		// Assert the response code with the expected status.
		if rec.Code != testCase.expectedRespStatus {
			t.Errorf("Case %d: Minio %s: Expected the response status to be `%d`, but instead found `%d`", i+1, instanceType, testCase.expectedRespStatus, rec.Code)
		}
	}

	// create unsigned HTTP request for Abort multipart upload.
	anonReq, err := newTestRequest("DELETE", getAbortMultipartUploadURL("", bucketName, objectName, uploadIDs[1]),
		0, nil)
	if err != nil {
		t.Fatalf("Minio %s: Failed to create an anonymous request for %s/%s: <ERROR> %v",
			instanceType, bucketName, objectName, err)
	}

	// ExecObjectLayerAPIAnonTest - Calls the HTTP API handler using the anonymous request, validates the ErrAccessDeniedResponse,
	// sets the bucket policy using the policy statement generated from `getWriteOnlyObjectStatement` so that the
	// unsigned request goes through and its validated again.
	ExecObjectLayerAPIAnonTest(t, obj, "TestAPIAbortMultipartHandler", bucketName, objectName, instanceType,
		apiRouter, anonReq, getWriteOnlyObjectStatement)

	// HTTP request to test the case of `objectLayer` being set to `nil`.
	// There is no need to use an existing bucket or valid input for creating the request,
	// since the `objectLayer==nil`  check is performed before any other checks inside the handlers.
	// The only aim is to generate an HTTP request in a way that the relevant/registered end point is evoked/called.
	// Indicating that all parts are uploaded and initiating abortMultipartUpload.
	nilBucket := "dummy-bucket"
	nilObject := "dummy-object"

	nilReq, err := newTestSignedRequestV4("DELETE", getAbortMultipartUploadURL("", nilBucket, nilObject, "dummy-uploadID"),
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
	defer DetectTestLeak(t)()
	ExecObjectLayerAPITest(t, testAPIDeleteObjectHandler, []string{"DeleteObject"})
}

func testAPIDeleteObjectHandler(obj ObjectLayer, instanceType, bucketName string, apiRouter http.Handler,
	credentials auth.Credentials, t *testing.T) {

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
		{generateBytesData(6 * humanize.MiByte)},
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
		_, err = obj.PutObject(input.bucketName, input.objectName, mustGetHashReader(t, bytes.NewBuffer(input.textData), input.contentLength, input.metaData[""], ""), input.metaData)
		// if object upload fails stop the test.
		if err != nil {
			t.Fatalf("Put Object case %d:  Error uploading object: <ERROR> %v", i+1, err)
		}
	}

	// test cases with inputs and expected result for DeleteObject.
	testCases := []struct {
		bucketName string
		objectName string
		accessKey  string
		secretKey  string

		expectedRespStatus int // expected response status body.
	}{
		// Test case - 1.
		// Deleting an existing object.
		// Expected to return HTTP resposne status code 204.
		{
			bucketName: bucketName,
			objectName: objectName,
			accessKey:  credentials.AccessKey,
			secretKey:  credentials.SecretKey,

			expectedRespStatus: http.StatusNoContent,
		},
		// Test case - 2.
		// Attempt to delete an object which is already deleted.
		// Still should return http response status 204.
		{
			bucketName: bucketName,
			objectName: objectName,
			accessKey:  credentials.AccessKey,
			secretKey:  credentials.SecretKey,

			expectedRespStatus: http.StatusNoContent,
		},
		// Test case - 3.
		// Setting Invalid AccessKey to force signature check inside the handler to fail.
		// Should return HTTP response status 403 forbidden.
		{
			bucketName: bucketName,
			objectName: objectName,
			accessKey:  "Invalid-AccessKey",
			secretKey:  credentials.SecretKey,

			expectedRespStatus: http.StatusForbidden,
		},
	}

	// Iterating over the cases, call DeleteObjectHandler and validate the HTTP response.
	for i, testCase := range testCases {
		var req, reqV2 *http.Request
		// initialize HTTP NewRecorder, this records any mutations to response writer inside the handler.
		rec := httptest.NewRecorder()
		// construct HTTP request for Delete Object end point.
		req, err = newTestSignedRequestV4("DELETE", getDeleteObjectURL("", testCase.bucketName, testCase.objectName),
			0, nil, testCase.accessKey, testCase.secretKey)

		if err != nil {
			t.Fatalf("Test %d: Failed to create HTTP request for Delete Object: <ERROR> %v", i+1, err)
		}
		// Since `apiRouter` satisfies `http.Handler` it has a ServeHTTP to execute the logic of the handler.
		// Call the ServeHTTP to execute the handler,`func (api objectAPIHandlers) DeleteObjectHandler`  handles the request.
		apiRouter.ServeHTTP(rec, req)
		// Assert the response code with the expected status.
		if rec.Code != testCase.expectedRespStatus {
			t.Fatalf("Minio %s: Case %d: Expected the response status to be `%d`, but instead found `%d`", instanceType, i+1, testCase.expectedRespStatus, rec.Code)
		}

		// Verify response of the V2 signed HTTP request.
		// initialize HTTP NewRecorder, this records any mutations to response writer inside the handler.
		recV2 := httptest.NewRecorder()
		// construct HTTP request for Delete Object endpoint.
		reqV2, err = newTestSignedRequestV2("DELETE", getDeleteObjectURL("", testCase.bucketName, testCase.objectName),
			0, nil, testCase.accessKey, testCase.secretKey)

		if err != nil {
			t.Fatalf("Failed to create HTTP request for NewMultipart Request: <ERROR> %v", err)
		}

		// Since `apiRouter` satisfies `http.Handler` it has a ServeHTTP to execute the logic of the handler.
		// Call the ServeHTTP to execute the handler.
		apiRouter.ServeHTTP(recV2, reqV2)
		// Assert the response code with the expected status.
		if recV2.Code != testCase.expectedRespStatus {
			t.Errorf("Case %d: Minio %s: Expected the response status to be `%d`, but instead found `%d`", i+1,
				instanceType, testCase.expectedRespStatus, recV2.Code)
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
	ExecObjectLayerAPIAnonTest(t, obj, "TestAPIDeleteObjectHandler", bucketName, anonObjectName, instanceType, apiRouter, anonReq, getWriteOnlyObjectStatement)

	// HTTP request to test the case of `objectLayer` being set to `nil`.
	// There is no need to use an existing bucket or valid input for creating the request,
	// since the `objectLayer==nil`  check is performed before any other checks inside the handlers.
	// The only aim is to generate an HTTP request in a way that the relevant/registered end point is evoked/called.
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
	defer DetectTestLeak(t)()
	ExecObjectLayerAPITest(t, testAPIPutObjectPartHandlerPreSign, []string{"NewMultipart", "PutObjectPart"})
}

func testAPIPutObjectPartHandlerPreSign(obj ObjectLayer, instanceType, bucketName string, apiRouter http.Handler,
	credentials auth.Credentials, t *testing.T) {
	testObject := "testobject"
	rec := httptest.NewRecorder()
	req, err := newTestSignedRequestV4("POST", getNewMultipartURL("", bucketName, "testobject"),
		0, nil, credentials.AccessKey, credentials.SecretKey)
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

	err = preSignV2(req, credentials.AccessKey, credentials.SecretKey, int64(10*60*60))
	if err != nil {
		t.Fatalf("[%s] - Failed to presign an unsigned request to put object part for %s/%s <ERROR> %v",
			instanceType, bucketName, testObject, err)
	}
	apiRouter.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("Test %d %s expected to succeed but failed with HTTP status code %d", 1, instanceType, rec.Code)
	}

	rec = httptest.NewRecorder()
	req, err = newTestRequest("PUT", getPutObjectPartURL("", bucketName, testObject, mpartResp.UploadID, "1"),
		int64(len("hello")), bytes.NewReader([]byte("hello")))
	if err != nil {
		t.Fatalf("[%s] - Failed to create an unsigned request to put object part for %s/%s <ERROR> %v",
			instanceType, bucketName, testObject, err)
	}
	err = preSignV4(req, credentials.AccessKey, credentials.SecretKey, int64(10*60*60))
	if err != nil {
		t.Fatalf("[%s] - Failed to presign an unsigned request to put object part for %s/%s <ERROR> %v",
			instanceType, bucketName, testObject, err)
	}
	apiRouter.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("Test %d %s expected to succeed but failed with HTTP status code %d", 1, instanceType, rec.Code)
	}
}

// TestAPIPutObjectPartHandlerStreaming - Tests validate the response of PutObjectPart HTTP handler
// when the request signature type is `streaming signature`.
func TestAPIPutObjectPartHandlerStreaming(t *testing.T) {
	defer DetectTestLeak(t)()
	ExecObjectLayerAPITest(t, testAPIPutObjectPartHandlerStreaming, []string{"NewMultipart", "PutObjectPart"})
}

func testAPIPutObjectPartHandlerStreaming(obj ObjectLayer, instanceType, bucketName string, apiRouter http.Handler,
	credentials auth.Credentials, t *testing.T) {
	testObject := "testobject"
	rec := httptest.NewRecorder()
	req, err := newTestSignedRequestV4("POST", getNewMultipartURL("", bucketName, "testobject"),
		0, nil, credentials.AccessKey, credentials.SecretKey)
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
			5, 1, bytes.NewReader([]byte("hello")), credentials.AccessKey, credentials.SecretKey)

		if err != nil {
			t.Fatalf("Failed to create new streaming signed HTTP request: <ERROR> %v.", err)
		}
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
	defer DetectTestLeak(t)()
	ExecObjectLayerAPITest(t, testAPIPutObjectPartHandler, []string{"PutObjectPart"})
}

func testAPIPutObjectPartHandler(obj ObjectLayer, instanceType, bucketName string, apiRouter http.Handler,
	credentials auth.Credentials, t *testing.T) {

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
	// expected error when InvalidAccessID is set.
	invalidAccessID := getAPIError(ErrInvalidAccessKeyID)

	// SignatureMismatch for various signing types
	testCases := []struct {
		objectName string
		reader     io.ReadSeeker
		partNumber string
		fault      Fault
		accessKey  string
		secretKey  string

		expectedAPIError APIError
	}{
		// Test case - 1.
		// Success case.
		{
			objectName: testObject,
			reader:     bytes.NewReader([]byte("hello")),
			partNumber: "1",
			fault:      None,
			accessKey:  credentials.AccessKey,
			secretKey:  credentials.SecretKey,

			expectedAPIError: noAPIErr,
		},
		// Test case - 2.
		// Case where part number is invalid.
		{
			objectName: testObject,
			reader:     bytes.NewReader([]byte("hello")),
			partNumber: "9999999999999999999",
			fault:      None,
			accessKey:  credentials.AccessKey,
			secretKey:  credentials.SecretKey,

			expectedAPIError: invalidPart,
		},
		// Test case - 3.
		// Case where the part number has exceeded the max allowed parts in an upload.
		{
			objectName: testObject,
			reader:     bytes.NewReader([]byte("hello")),
			partNumber: strconv.Itoa(globalMaxPartID + 1),
			fault:      None,
			accessKey:  credentials.AccessKey,
			secretKey:  credentials.SecretKey,

			expectedAPIError: invalidMaxParts,
		},
		// Test case - 4.
		// Case where the content length is not set in the HTTP request.
		{
			objectName: testObject,
			reader:     bytes.NewReader([]byte("hello")),
			partNumber: "1",
			fault:      MissingContentLength,
			accessKey:  credentials.AccessKey,
			secretKey:  credentials.SecretKey,

			expectedAPIError: missingContent,
		},
		// Test case - 5.
		// case where the object size is set to a value greater than the max allowed size.
		{
			objectName: testObject,
			reader:     bytes.NewReader([]byte("hello")),
			partNumber: "1",
			fault:      TooBigObject,
			accessKey:  credentials.AccessKey,
			secretKey:  credentials.SecretKey,

			expectedAPIError: entityTooLarge,
		},
		// Test case - 6.
		// case where a signature mismatch is introduced and the response is validated.
		{
			objectName: testObject,
			reader:     bytes.NewReader([]byte("hello")),
			partNumber: "1",
			fault:      BadSignature,
			accessKey:  credentials.AccessKey,
			secretKey:  credentials.SecretKey,

			expectedAPIError: badSigning,
		},
		// Test case - 7.
		// Case where incorrect checksum is set and the error response
		// is asserted with the expected error response.
		{
			objectName: testObject,
			reader:     bytes.NewReader([]byte("hello")),
			partNumber: "1",
			fault:      BadMD5,
			accessKey:  credentials.AccessKey,
			secretKey:  credentials.SecretKey,

			expectedAPIError: badChecksum,
		},
		// Test case - 8.
		// case where the a non-existent uploadID is set.
		{
			objectName: testObject,
			reader:     bytes.NewReader([]byte("hello")),
			partNumber: "1",
			fault:      MissingUploadID,
			accessKey:  credentials.AccessKey,
			secretKey:  credentials.SecretKey,

			expectedAPIError: noSuchUploadID,
		},
		// Test case - 9.
		// case with invalid AccessID.
		// Forcing the signature check inside the handler to fail.
		{
			objectName: testObject,
			reader:     bytes.NewReader([]byte("hello")),
			partNumber: "1",
			fault:      None,
			accessKey:  "Invalid-AccessID",
			secretKey:  credentials.SecretKey,

			expectedAPIError: invalidAccessID,
		},
	}

	reqV2Str := "V2 Signed HTTP request"
	reqV4Str := "V4 Signed HTTP request"

	// collection of input HTTP request, ResponseRecorder and request type.
	// Used to make a collection of V4 and V4 HTTP request.
	type inputReqRec struct {
		req     *http.Request
		rec     *httptest.ResponseRecorder
		reqType string
	}

	for i, test := range testCases {
		// Using sub-tests introduced in Go 1.7.
		t.Run(fmt.Sprintf("Minio %s : Test case %d.", instanceType, i+1), func(t *testing.T) {

			var reqV4, reqV2 *http.Request
			var recV4, recV2 *httptest.ResponseRecorder

			// initialize HTTP NewRecorder, this records any mutations to response writer inside the handler.
			recV4 = httptest.NewRecorder()
			recV2 = httptest.NewRecorder()
			// setting a non-existent uploadID.
			// deliberately introducing the invalid value to be able to assert the response with the expected error response.
			if test.fault == MissingUploadID {
				uploadID = "upload1"
			}
			// constructing a v4 signed HTTP request.
			reqV4, err = newTestSignedRequestV4("PUT",
				getPutObjectPartURL("", bucketName, test.objectName, uploadID, test.partNumber),
				0, test.reader, test.accessKey, test.secretKey)
			if err != nil {
				t.Fatalf("Failed to create a signed V4 request to upload part for %s/%s: <ERROR> %v",
					bucketName, test.objectName, err)
			}
			// Verify response of the V2 signed HTTP request.
			// construct HTTP request for PutObject Part Object endpoint.
			reqV2, err = newTestSignedRequestV2("PUT",
				getPutObjectPartURL("", bucketName, test.objectName, uploadID, test.partNumber),
				0, test.reader, test.accessKey, test.secretKey)

			if err != nil {
				t.Fatalf("Test %d %s Failed to create a V2  signed request to upload part for %s/%s: <ERROR> %v", i+1, instanceType,
					bucketName, test.objectName, err)
			}

			// collection of input HTTP request, ResponseRecorder and request type.
			reqRecs := []inputReqRec{
				{
					req:     reqV4,
					rec:     recV4,
					reqType: reqV4Str,
				},
				{
					req:     reqV2,
					rec:     recV2,
					reqType: reqV2Str,
				},
			}

			for _, reqRec := range reqRecs {
				// Response recorder to record the response of the handler.
				rec := reqRec.rec
				// HTTP request used to call the handler.
				req := reqRec.req
				// HTTP request type string for V4/V2 requests.
				reqType := reqRec.reqType

				// introduce faults in the request.
				// deliberately introducing the invalid value to be able to assert the response with the expected error response.
				switch test.fault {
				case MissingContentLength:
					req.ContentLength = -1
					// Setting the content length to a value greater than the max allowed size of a part.
					// Used in test case  4.
				case TooBigObject:
					req.ContentLength = globalMaxObjectSize + 1
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
						t.Fatalf("%s, Failed to read error response from upload part request \"%s\"/\"%s\": <ERROR> %v.",
							reqType, bucketName, test.objectName, err)
					}
					// parse the XML error response.
					var errXML APIErrorResponse
					err = xml.Unmarshal(errBytes, &errXML)
					if err != nil {
						t.Fatalf("%s, Failed to unmarshal error response from upload part request \"%s\"/\"%s\": <ERROR> %v.",
							reqType, bucketName, test.objectName, err)
					}
					// Validate whether the error has occurred for the expected reason.
					if test.expectedAPIError.Code != errXML.Code {
						t.Errorf("%s, Expected to fail with error \"%s\", but received \"%s\".",
							reqType, test.expectedAPIError.Code, errXML.Code)
					}
					// Validate the HTTP response status code  with the expected one.
					if test.expectedAPIError.HTTPStatusCode != rec.Code {
						t.Errorf("%s, Expected the HTTP response status code to be %d, got %d.", reqType, test.expectedAPIError.HTTPStatusCode, rec.Code)
					}
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
	ExecObjectLayerAPIAnonTest(t, obj, "TestAPIPutObjectPartHandler", bucketName, testObject, instanceType, apiRouter, anonReq, getWriteOnlyObjectStatement)

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
	defer DetectTestLeak(t)()
	ExecObjectLayerAPITest(t, testAPIListObjectPartsHandlerPreSign,
		[]string{"PutObjectPart", "NewMultipart", "ListObjectParts"})
}

func testAPIListObjectPartsHandlerPreSign(obj ObjectLayer, instanceType, bucketName string, apiRouter http.Handler,
	credentials auth.Credentials, t *testing.T) {
	testObject := "testobject"
	rec := httptest.NewRecorder()
	req, err := newTestSignedRequestV4("POST", getNewMultipartURL("", bucketName, testObject),
		0, nil, credentials.AccessKey, credentials.SecretKey)
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
		int64(len("hello")), bytes.NewReader([]byte("hello")), credentials.AccessKey, credentials.SecretKey)
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

	req.Header = http.Header{}
	err = preSignV2(req, credentials.AccessKey, credentials.SecretKey, int64(10*60*60))
	if err != nil {
		t.Fatalf("[%s] - Failed to presignV2 an unsigned request to list object parts for bucket %s, uploadId %s",
			instanceType, bucketName, mpartResp.UploadID)
	}
	apiRouter.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("Test %d %s expected to succeed but failed with HTTP status code %d",
			1, instanceType, rec.Code)
	}

	rec = httptest.NewRecorder()
	req, err = newTestRequest("GET",
		getListMultipartURLWithParams("", bucketName, testObject, mpartResp.UploadID, "", "", ""),
		0, nil)
	if err != nil {
		t.Fatalf("[%s] - Failed to create an unsigned request to list object parts for bucket %s, uploadId %s",
			instanceType, bucketName, mpartResp.UploadID)
	}

	err = preSignV4(req, credentials.AccessKey, credentials.SecretKey, int64(10*60*60))
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
	defer DetectTestLeak(t)()
	ExecObjectLayerAPITest(t, testAPIListObjectPartsHandler, []string{"ListObjectParts"})
}

func testAPIListObjectPartsHandler(obj ObjectLayer, instanceType, bucketName string, apiRouter http.Handler,
	credentials auth.Credentials, t *testing.T) {
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
	_, err = obj.PutObjectPart(bucketName, testObject, uploadID, 1, mustGetHashReader(t, bytes.NewReader([]byte("hello")), int64(len("hello")), "5d41402abc4b2a76b9719d911017c592", ""))
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

	// string to represent V2 signed HTTP request.
	reqV2Str := "V2 Signed HTTP request"
	// string to represent V4 signed HTTP request.
	reqV4Str := "V4 Signed HTTP request"
	// Collection of HTTP request and ResponseRecorder and request type string.
	type inputReqRec struct {
		req     *http.Request
		rec     *httptest.ResponseRecorder
		reqType string
	}

	for i, test := range testCases {
		var reqV4, reqV2 *http.Request
		// Using sub-tests introduced in Go 1.7.
		t.Run(fmt.Sprintf("Minio %s: Test case %d failed.", instanceType, i+1), func(t *testing.T) {
			recV2 := httptest.NewRecorder()
			recV4 := httptest.NewRecorder()

			// setting a non-existent uploadID.
			// deliberately introducing the invalid value to be able to assert the response with the expected error response.
			if test.fault == MissingUploadID {
				uploadID = "upload1"
			}

			// constructing a v4 signed HTTP request for ListMultipartUploads.
			reqV4, err = newTestSignedRequestV4("GET",
				getListMultipartURLWithParams("", bucketName, testObject, uploadID, test.maxParts, test.partNumberMarker, ""),
				0, nil, credentials.AccessKey, credentials.SecretKey)

			if err != nil {
				t.Fatalf("Failed to create a V4 signed request to list object parts for %s/%s: <ERROR> %v.",
					bucketName, testObject, err)
			}
			// Verify response of the V2 signed HTTP request.
			// construct HTTP request for PutObject Part Object endpoint.
			reqV2, err = newTestSignedRequestV2("GET",
				getListMultipartURLWithParams("", bucketName, testObject, uploadID, test.maxParts, test.partNumberMarker, ""),
				0, nil, credentials.AccessKey, credentials.SecretKey)

			if err != nil {
				t.Fatalf("Failed to create a V2 signed request to list object parts for %s/%s: <ERROR> %v.",
					bucketName, testObject, err)
			}

			// collection of input HTTP request, ResponseRecorder and request type.
			reqRecs := []inputReqRec{
				{
					req:     reqV4,
					rec:     recV4,
					reqType: reqV4Str,
				},
				{
					req:     reqV2,
					rec:     recV2,
					reqType: reqV2Str,
				},
			}
			for _, reqRec := range reqRecs {
				// Response recorder to record the response of the handler.
				rec := reqRec.rec
				// HTTP request used to call the handler.
				req := reqRec.req
				// HTTP request type string for V4/V2 requests.
				reqType := reqRec.reqType
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
						t.Fatalf("%s,Failed to read error response list object parts request %s/%s: <ERROR> %v", reqType, bucketName, testObject, err)
					}
					// parse the error response.
					var errXML APIErrorResponse
					err = xml.Unmarshal(errBytes, &errXML)
					if err != nil {
						t.Fatalf("%s, Failed to unmarshal error response from list object partsest %s/%s: <ERROR> %v",
							reqType, bucketName, testObject, err)
					}
					// Validate whether the error has occurred for the expected reason.
					if test.expectedErr.Code != errXML.Code {
						t.Errorf("%s, Expected to fail with %s but received %s",
							reqType, test.expectedErr.Code, errXML.Code)
					}
					// in case error is not expected response status should be 200OK.
				} else {
					if rec.Code != http.StatusOK {
						t.Errorf("%s, Expected to succeed with response HTTP status 200OK, but failed with HTTP status code %d.", reqType, rec.Code)
					}
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
	ExecObjectLayerAPIAnonTest(t, obj, "TestAPIListObjectPartsHandler", bucketName, testObject, instanceType, apiRouter, anonReq, getWriteOnlyObjectStatement)

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

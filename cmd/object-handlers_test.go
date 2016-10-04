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
	credentials credential, t TestErrHandler) {
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
}

// Wrapper for calling PutObject API handler tests using streaming signature v4 for both XL multiple disks and FS single drive setup.
func TestAPIPutObjectStreamSigV4Handler(t *testing.T) {
	ExecObjectLayerAPITest(t, testAPIPutObjectStreamSigV4Handler, []string{"PutObject"})
}

func testAPIPutObjectStreamSigV4Handler(obj ObjectLayer, instanceType, bucketName string, apiRouter http.Handler,
	credentials credential, t TestErrHandler) {

	objectName := "test-object"
	bytesDataLen := 65 * 1024
	bytesData := bytes.Repeat([]byte{'a'}, bytesDataLen)

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
	}
	// Iterating over the cases, fetching the object validating the response.
	for i, testCase := range testCases {
		// initialize HTTP NewRecorder, this records any mutations to response writer inside the handler.
		rec := httptest.NewRecorder()
		// construct HTTP request for Put Object end point.
		req, err := newTestStreamingSignedRequest("PUT",
			getPutObjectURL("", testCase.bucketName, testCase.objectName),
			int64(testCase.dataLen), testCase.chunkSize, bytes.NewReader(testCase.data),
			testCase.accessKey, testCase.secretKey)
		if err != nil {
			t.Fatalf("Test %d: Failed to create HTTP request for Put Object: <ERROR> %v", i+1, err)
		}
		// Removes auth header if test case requires it.
		if testCase.removeAuthHeader {
			req.Header.Del("Authorization")
		}
		// Since `apiRouter` satisfies `http.Handler` it has a ServeHTTP to execute the logic of the handler.
		// Call the ServeHTTP to execute the handler,`func (api objectAPIHandlers) GetObjectHandler`  handles the request.
		apiRouter.ServeHTTP(rec, req)
		// Assert the response code with the expected status.
		if rec.Code != testCase.expectedRespStatus {
			t.Errorf("Test %d: Expected the response status to be `%d`, but instead found `%d`", i+1, testCase.expectedRespStatus, rec.Code)
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
	credentials credential, t TestErrHandler) {

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
		// initialize HTTP NewRecorder, this records any mutations to response writer inside the handler.
		rec := httptest.NewRecorder()
		// construct HTTP request for Get Object end point.
		req, err := newTestSignedRequestV4("PUT", getPutObjectURL("", testCase.bucketName, testCase.objectName),
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
		actualContent, err := ioutil.ReadAll(rec.Body)
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
}

// Wrapper for calling Copy Object API handler tests for both XL multiple disks and single node setup.
func TestAPICopyObjectHandler(t *testing.T) {
	ExecObjectLayerAPITest(t, testAPICopyObjectHandler, []string{"CopyObject"})
}

func testAPICopyObjectHandler(obj ObjectLayer, instanceType, bucketName string, apiRouter http.Handler,
	credentials credential, t TestErrHandler) {

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
		// initialize HTTP NewRecorder, this records any mutations to response writer inside the handler.
		rec := httptest.NewRecorder()
		// construct HTTP request for copy object.
		req, err := newTestSignedRequestV4("PUT", getCopyObjectURL("", testCase.bucketName, testCase.newObjectName),
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

// Wrapper for calling NewMultipartUpload tests for both XL multiple disks and single node setup.
// First register the HTTP handler for NewMutlipartUpload, then a HTTP request for NewMultipart upload is made.
// The UploadID from the response body is parsed and its existence is asserted with an attempt to ListParts using it.
func TestAPINewMultipartHandler(t *testing.T) {
	ExecObjectLayerAPITest(t, testAPINewMultipartHandler, []string{"NewMultipart"})
}

func testAPINewMultipartHandler(obj ObjectLayer, instanceType, bucketName string, apiRouter http.Handler,
	credentials credential, t TestErrHandler) {

	objectName := "test-object-new-multipart"
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

}

// Wrapper for calling NewMultipartUploadParallel tests for both XL multiple disks and single node setup.
// The objective of the test is to initialte multipart upload on the same object 10 times concurrently,
// The UploadID from the response body is parsed and its existence is asserted with an attempt to ListParts using it.
func TestAPINewMultipartHandlerParallel(t *testing.T) {
	ExecObjectLayerAPITest(t, testAPINewMultipartHandlerParallel, []string{"NewMultipart"})
}

func testAPINewMultipartHandlerParallel(obj ObjectLayer, instanceType, bucketName string, apiRouter http.Handler,
	credentials credential, t TestErrHandler) {
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
	credentials credential, t TestErrHandler) {

	// Calculates MD5 sum of the given byte array.
	findMD5 := func(toBeHashed []byte) string {
		hasher := md5.New()
		hasher.Write(toBeHashed)
		return hex.EncodeToString(hasher.Sum(nil))
	}

	objectName := "test-object-new-multipart"

	uploadID, err := obj.NewMultipartUpload(bucketName, objectName, nil)
	if err != nil {
		// Failed to create NewMultipartUpload, abort.
		t.Fatalf("Minio %s : <ERROR>  %s", instanceType, err)
	}
	var uploadIDs []string
	uploadIDs = append(uploadIDs, uploadID)
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
	}
	// Iterating over creatPartCases to generate multipart chunks.
	for _, part := range parts {
		_, err = obj.PutObjectPart(part.bucketName, part.objName, part.uploadID, part.PartID, part.intputDataSize, bytes.NewBufferString(part.inputReaderData), part.inputMd5, "")
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
			bucket:             bucketName,
			object:             objectName,
			uploadID:           uploadIDs[0],
			parts:              []completePart{},
			expectedContent:    encodeResponse(getAPIErrorResponse(getAPIError(ErrMalformedXML), getGetObjectURL("", bucketName, objectName))),
			expectedRespStatus: http.StatusBadRequest,
		},
		// Test case - 3.
		// Non-Existent uploadID.
		// 404 Not Found response status expected.
		{
			bucket:             bucketName,
			object:             objectName,
			uploadID:           "abc",
			parts:              inputParts[0].parts,
			expectedContent:    encodeResponse(getAPIErrorResponse(getAPIError(toAPIErrorCode(InvalidUploadID{UploadID: "abc"})), getGetObjectURL("", bucketName, objectName))),
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
			bucket:             bucketName,
			object:             objectName,
			uploadID:           uploadIDs[0],
			parts:              inputParts[2].parts,
			expectedContent:    encodeResponse(getAPIErrorResponse(getAPIError(toAPIErrorCode(InvalidPart{})), getGetObjectURL("", bucketName, objectName))),
			expectedRespStatus: http.StatusBadRequest,
		},
		// Test case - 6.
		// Parts are not sorted according to the part number.
		// This should return ErrInvalidPartOrder in the response body.
		{
			bucket:             bucketName,
			object:             objectName,
			uploadID:           uploadIDs[0],
			parts:              inputParts[3].parts,
			expectedContent:    encodeResponse(getAPIErrorResponse(getAPIError(ErrInvalidPartOrder), getGetObjectURL("", bucketName, objectName))),
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
		// Complete multipart upload parts.
		completeUploads := &completeMultipartUpload{
			Parts: testCase.parts,
		}
		completeBytes, err := xml.Marshal(completeUploads)
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
		actualContent, err := ioutil.ReadAll(rec.Body)
		if err != nil {
			t.Fatalf("Test %d : Minio %s: Failed parsing response body: <ERROR> %v", i+1, instanceType, err)
		}
		// Verify whether the bucket obtained object is same as the one created.
		if !bytes.Equal(testCase.expectedContent, actualContent) {
			t.Errorf("Test %d : Minio %s: Object content differs from expected value.", i+1, instanceType)
		}
	}
}

// Wrapper for calling Delete Object API handler tests for both XL multiple disks and FS single drive setup.
func TestAPIDeleteOjectHandler(t *testing.T) {
	ExecObjectLayerAPITest(t, testAPIDeleteOjectHandler, []string{"DeleteObject"})
}

func testAPIDeleteOjectHandler(obj ObjectLayer, instanceType, bucketName string, apiRouter http.Handler,
	credentials credential, t TestErrHandler) {

	switch obj.(type) {
	case fsObjects:
		return
	}
	objectName := "test-object"
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
	}
	// iterate through the above set of inputs and upload the object.
	for i, input := range putObjectInputs {
		// uploading the object.
		_, err := obj.PutObject(input.bucketName, input.objectName, input.contentLength, bytes.NewBuffer(input.textData), input.metaData, "")
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
		// initialize HTTP NewRecorder, this records any mutations to response writer inside the handler.
		rec := httptest.NewRecorder()
		// construct HTTP request for Get Object end point.
		req, err := newTestSignedRequestV4("DELETE", getDeleteObjectURL("", testCase.bucketName, testCase.objectName),
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
}

func testAPIPutObjectPartHandlerPreSign(obj ObjectLayer, instanceType, bucketName string, apiRouter http.Handler,
	credentials credential, t TestErrHandler) {
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

func TestAPIPutObjectPartHandlerPreSign(t *testing.T) {
	ExecObjectLayerAPITest(t, testAPIPutObjectPartHandlerPreSign, []string{"NewMultipart", "PutObjectPart"})
}

func testAPIPutObjectPartHandlerV2(obj ObjectLayer, instanceType, bucketName string, apiRouter http.Handler,
	credentials credential, t TestErrHandler) {
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

func TestAPIPutObjectPartHandlerV2(t *testing.T) {
	ExecObjectLayerAPITest(t, testAPIPutObjectPartHandlerV2, []string{"NewMultipart", "PutObjectPart"})
}

func testAPIPutObjectPartHandlerStreaming(obj ObjectLayer, instanceType, bucketName string, apiRouter http.Handler,
	credentials credential, t TestErrHandler) {
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

func TestAPIPutObjectPartHandlerStreaming(t *testing.T) {
	ExecObjectLayerAPITest(t, testAPIPutObjectPartHandlerStreaming, []string{"NewMultipart", "PutObjectPart"})
}

func testAPIPutObjectPartHandlerAnon(obj ObjectLayer, instanceType, bucketName string, apiRouter http.Handler,
	credentials credential, t TestErrHandler) {
	// Initialize bucket policies for anonymous request test
	err := initBucketPolicies(obj)
	if err != nil {
		t.Fatalf("Failed to initialize bucket policies: <ERROR> %v", err)
	}

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

	accessDeniedErr := getAPIError(ErrAccessDenied)
	anonRec := httptest.NewRecorder()
	anonReq, aErr := newTestRequest("PUT",
		getPutObjectPartURL("", bucketName, testObject, mpartResp.UploadID, "1"),
		int64(len("hello")), bytes.NewReader([]byte("hello")))
	if aErr != nil {
		t.Fatalf("Test %d %s Failed to create an anonymous request to upload part for %s/%s: <ERROR> %v",
			1, instanceType, bucketName, testObject, aErr)
	}
	apiRouter.ServeHTTP(anonRec, anonReq)

	anonErrBytes, err := ioutil.ReadAll(anonRec.Result().Body)
	if err != nil {
		t.Fatalf("Test %d %s Failed to read error response from upload part request %s/%s: <ERROR> %v",
			1, instanceType, bucketName, testObject, err)
	}
	var anonErrXML APIErrorResponse
	err = xml.Unmarshal(anonErrBytes, &anonErrXML)
	if err != nil {
		t.Fatalf("Test %d %s Failed to unmarshal error response from upload part request %s/%s: <ERROR> %v",
			1, instanceType, bucketName, testObject, err)
	}
	if accessDeniedErr.Code != anonErrXML.Code {
		t.Errorf("Test %d %s expected to fail with error %s, but received %s", 1, instanceType,
			accessDeniedErr.Code, anonErrXML.Code)
	}

	// Set write only policy on bucket to allow anonymous PutObjectPart API
	// request to go through.
	writeOnlyPolicy := bucketPolicy{
		Version:    "1.0",
		Statements: []policyStatement{getWriteOnlyObjectStatement(bucketName, "")},
	}
	globalBucketPolicies.SetBucketPolicy(bucketName, &writeOnlyPolicy)

	anonRec = httptest.NewRecorder()
	anonReq, aErr = newTestRequest("PUT",
		getPutObjectPartURL("", bucketName, testObject, mpartResp.UploadID, "1"),
		int64(len("hello")), bytes.NewReader([]byte("hello")))
	if aErr != nil {
		t.Fatalf("Test %d %s Failed to create an anonymous request to upload part for %s/%s: <ERROR> %v",
			1, instanceType, bucketName, testObject, aErr)
	}
	apiRouter.ServeHTTP(anonRec, anonReq)
	if anonRec.Code != http.StatusOK {
		t.Errorf("Test %d %s expected PutObject Part with authAnonymous type to succeed but failed with "+
			"HTTP status code %d", 1, instanceType, anonRec.Code)
	}
}

func TestAPIPutObjectPartHandlerAnon(t *testing.T) {
	ExecObjectLayerAPITest(t, testAPIPutObjectPartHandlerAnon, []string{"PutObjectPart", "NewMultipart"})
}

func testAPIPutObjectPartHandler(obj ObjectLayer, instanceType, bucketName string, apiRouter http.Handler,
	credentials credential, t TestErrHandler) {
	// Initiate Multipart upload for testing PutObjectPartHandler.
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
	missingContent := getAPIError(ErrMissingContentLength)
	entityTooLarge := getAPIError(ErrEntityTooLarge)
	badSigning := getAPIError(ErrSignatureDoesNotMatch)
	badChecksum := getAPIError(ErrInvalidDigest)
	invalidPart := getAPIError(ErrInvalidPart)
	invalidMaxParts := getAPIError(ErrInvalidMaxParts)
	noSuchUploadID := getAPIError(ErrNoSuchUpload)
	// SignatureMismatch for various signing types
	testCases := []struct {
		objectName       string
		reader           io.ReadSeeker
		partNumber       string
		fault            Fault
		expectedAPIError APIError
	}{
		// Success case
		{testObject, bytes.NewReader([]byte("hello")), "1", None, noAPIErr},
		{testObject, bytes.NewReader([]byte("hello")), "9999999999999999999", None, invalidPart},
		{testObject, bytes.NewReader([]byte("hello")), strconv.Itoa(maxPartID + 1), None, invalidMaxParts},
		{testObject, bytes.NewReader([]byte("hello")), "1", MissingContentLength, missingContent},
		{testObject, bytes.NewReader([]byte("hello")), "1", TooBigObject, entityTooLarge},
		{testObject, bytes.NewReader([]byte("hello")), "1", BadSignature, badSigning},
		{testObject, bytes.NewReader([]byte("hello")), "1", BadMD5, badChecksum},
		{testObject, bytes.NewReader([]byte("hello")), "1", MissingUploadID, noSuchUploadID},
	}

	for i, test := range testCases {
		tRec := httptest.NewRecorder()
		uploadID := mpartResp.UploadID
		// To simulate PutObjectPart failure at object layer.
		if test.fault == MissingUploadID {
			uploadID = "upload1"
		}
		tReq, tErr := newTestSignedRequestV4("PUT",
			getPutObjectPartURL("", bucketName, test.objectName, uploadID, test.partNumber),
			0, test.reader, credentials.AccessKeyID, credentials.SecretAccessKey)
		if tErr != nil {
			t.Fatalf("Test %d %s Failed to create a signed request to upload part for %s/%s: <ERROR> %v", i+1, instanceType,
				bucketName, test.objectName, tErr)
		}
		switch test.fault {
		case MissingContentLength:
			tReq.ContentLength = -1
		case TooBigObject:
			tReq.ContentLength = maxObjectSize + 1
		case BadSignature:
			// Mangle signature
			tReq.Header.Set("authorization", tReq.Header.Get("authorization")+"a")
		case BadMD5:
			tReq.Header.Set("Content-MD5", "badmd5")
		}
		apiRouter.ServeHTTP(tRec, tReq)
		if test.expectedAPIError != noAPIErr {
			errBytes, err := ioutil.ReadAll(tRec.Result().Body)
			if err != nil {
				t.Fatalf("Test %d %s Failed to read error response from upload part request %s/%s: <ERROR> %v",
					i+1, instanceType, bucketName, test.objectName, err)
			}
			var errXML APIErrorResponse
			err = xml.Unmarshal(errBytes, &errXML)
			if err != nil {
				t.Fatalf("Test %d %s Failed to unmarshal error response from upload part request %s/%s: <ERROR> %v",
					i+1, instanceType, bucketName, test.objectName, err)
			}
			if test.expectedAPIError.Code != errXML.Code {
				t.Errorf("Test %d %s expected to fail with error %s, but received %s", i+1, instanceType,
					test.expectedAPIError.Code, errXML.Code)
			}
		}
	}
}

func TestAPIPutObjectPartHandler(t *testing.T) {
	ExecObjectLayerAPITest(t, testAPIPutObjectPartHandler, []string{"PutObjectPart", "NewMultipart"})
}

func TestPutObjectPartNilObjAPI(t *testing.T) {
	configDir, err := newTestConfig("us-east-1")
	if err != nil {
		t.Fatalf("Failed to create a test config: %v", err)
	}
	defer removeAll(configDir)

	rec := httptest.NewRecorder()
	req, err := newTestSignedRequestV4("PUT",
		getPutObjectPartURL("", "testbucket", "testobject", "uploadId1", "1"),
		-1, bytes.NewReader([]byte("hello")), "abcd1", "abcd123")
	if err != nil {
		t.Fatal("Failed to create a signed UploadPart request.")
	}
	// Setup the 'nil' objectAPI router.
	nilAPIRouter := initTestNilObjAPIEndPoints([]string{"PutObjectPart"})
	nilAPIRouter.ServeHTTP(rec, req)
	serverNotInitializedErr := getAPIError(ErrServerNotInitialized).HTTPStatusCode
	if rec.Code != serverNotInitializedErr {
		t.Errorf("Test expected to fail with %d, but failed with %d", serverNotInitializedErr, rec.Code)
	}
}

func testAPIListObjectPartsHandlerPreSign(obj ObjectLayer, instanceType, bucketName string, apiRouter http.Handler,
	credentials credential, t TestErrHandler) {
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

func TestAPIListObjectPartsHandlerPreSign(t *testing.T) {
	ExecObjectLayerAPITest(t, testAPIListObjectPartsHandlerPreSign,
		[]string{"PutObjectPart", "NewMultipart", "ListObjectParts"})
}

func testAPIListObjectPartsHandler(obj ObjectLayer, instanceType, bucketName string, apiRouter http.Handler,
	credentials credential, t TestErrHandler) {
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

	noAPIErr := APIError{}
	signatureMismatchErr := getAPIError(ErrSignatureDoesNotMatch)
	noSuchUploadErr := getAPIError(ErrNoSuchUpload)
	invalidPartMarkerErr := getAPIError(ErrInvalidPartNumberMarker)
	invalidMaxPartsErr := getAPIError(ErrInvalidMaxParts)
	testCases := []struct {
		fault            Fault
		partNumberMarker string
		maxParts         string
		expectedErr      APIError
	}{
		{BadSignature, "", "", signatureMismatchErr},
		{MissingUploadID, "", "", noSuchUploadErr},
		{None, "-1", "", invalidPartMarkerErr},
		{None, "", "-1", invalidMaxPartsErr},
	}

	for i, test := range testCases {
		uploadID := mpartResp.UploadID
		tRec := httptest.NewRecorder()
		if test.fault == MissingUploadID {
			uploadID = "upload1"
		}
		tReq, tErr := newTestSignedRequestV4("GET",
			getListMultipartURLWithParams("", bucketName, testObject, uploadID, test.maxParts, test.partNumberMarker, ""),
			0, nil, credentials.AccessKeyID, credentials.SecretAccessKey)
		if tErr != nil {
			t.Fatalf("Test %d %s - Failed to create a signed request to list object parts for %s/%s: <ERROR> %v",
				i+1, instanceType, bucketName, testObject, tErr)
		}
		if test.fault == BadSignature {
			// Mangle signature
			tReq.Header.Set("authorization", tReq.Header.Get("authorization")+"a")
		}
		apiRouter.ServeHTTP(tRec, tReq)
		if test.expectedErr != noAPIErr {
			errBytes, err := ioutil.ReadAll(tRec.Result().Body)
			if err != nil {
				t.Fatalf("Test %d %s Failed to read error response list object parts request %s/%s: <ERROR> %v",
					i+1, instanceType, bucketName, testObject, err)
			}
			var errXML APIErrorResponse
			err = xml.Unmarshal(errBytes, &errXML)
			if err != nil {
				t.Fatalf("Test %d %s Failed to unmarshal error response from list object partsest %s/%s: <ERROR> %v",
					i+1, instanceType, bucketName, testObject, err)
			}
			if test.expectedErr.Code != errXML.Code {
				t.Errorf("Test %d %s expected to fail with %s but received %s",
					i+1, instanceType, test.expectedErr.Code, errXML.Code)
			}

		} else {
			if tRec.Code != http.StatusOK {
				t.Errorf("Test %d %s expected to succeed but failed with HTTP status code %d",
					i+1, instanceType, tRec.Code)
			}
		}
	}
}

func TestAPIListObjectPartsHandler(t *testing.T) {
	ExecObjectLayerAPITest(t, testAPIListObjectPartsHandler,
		[]string{"PutObjectPart", "NewMultipart", "ListObjectParts"})
}

func testAPIListObjectPartsHandlerV2(obj ObjectLayer, instanceType, bucketName string, apiRouter http.Handler,
	credentials credential, t TestErrHandler) {
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
	req, err = newTestSignedRequestV2("GET",
		getListMultipartURLWithParams("", bucketName, testObject, mpartResp.UploadID, "", "", ""),
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

func TestListObjectPartsHandlerV2(t *testing.T) {
	ExecObjectLayerAPITest(t, testAPIListObjectPartsHandlerV2, []string{"PutObjectPart", "NewMultipart", "ListObjectParts"})
}

func testAPIListObjectPartsHandlerUnknown(obj ObjectLayer, instanceType, bucketName string, apiRouter http.Handler,
	credentials credential, t TestErrHandler) {
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
	accessDeniedErr := getAPIError(ErrAccessDenied)
	unKnownRec := httptest.NewRecorder()
	unKnownReq, aErr := newTestRequest("GET",
		getListMultipartURLWithParams("", bucketName, testObject, mpartResp.UploadID, "", "", ""),
		0, nil)
	if aErr != nil {
		t.Fatalf("Test %d %s Failed to create an unKnownymous request to list multipart of an upload for %s/%s: <ERROR> %v",
			1, instanceType, bucketName, testObject, aErr)
	}
	unKnownReq.Header.Set("Authorization", "nothingElse")
	apiRouter.ServeHTTP(unKnownRec, unKnownReq)
	unKnownErrBytes, err := ioutil.ReadAll(unKnownRec.Result().Body)
	if err != nil {
		t.Fatalf("Test %d %s Failed to read error response from list object parts request %s/%s: <ERROR> %v",
			1, instanceType, bucketName, testObject, err)
	}
	var unKnownErrXML APIErrorResponse
	err = xml.Unmarshal(unKnownErrBytes, &unKnownErrXML)
	if err != nil {
		t.Fatalf("Test %d %s Failed to unmarshal error response from list object parts request %s/%s: <ERROR> %v",
			1, instanceType, bucketName, testObject, err)
	}
	if accessDeniedErr.Code != unKnownErrXML.Code {
		t.Errorf("Test %d %s expected to fail with error %s, but received %s", 1, instanceType,
			accessDeniedErr.Code, unKnownErrXML.Code)
	}
}

func TestAPIListObjectPartsHandlerUnknown(t *testing.T) {
	ExecObjectLayerAPITest(t, testAPIListObjectPartsHandlerUnknown,
		[]string{"PutObjectPart", "NewMultipart", "ListObjectParts"})
}

func testAPIListObjectPartsHandlerAnon(obj ObjectLayer, instanceType, bucketName string, apiRouter http.Handler,
	credentials credential, t TestErrHandler) {
	// Initialize bucket policies for anonymous request test
	err := initBucketPolicies(obj)
	if err != nil {
		t.Fatalf("Failed to initialize bucket policies: <ERROR> %v", err)
	}

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
	// Add a part to the new multipart upload created.
	uploadID := mpartResp.UploadID
	req, err = newTestSignedRequestV4("PUT",
		getPutObjectPartURL("", bucketName, testObject, uploadID, "1"),
		0, bytes.NewReader([]byte("hello")), credentials.AccessKeyID, credentials.SecretAccessKey)
	if err != nil {
		t.Fatalf("Test %d %s Failed to create a signed request to upload part for %s/%s: <ERROR> %v", 1, instanceType,
			bucketName, testObject, err)
	}
	// Attempt an anonymous ListObjectParts API request to trigger AccessDenied error.
	accessDeniedErr := getAPIError(ErrAccessDenied)
	anonRec := httptest.NewRecorder()
	anonReq, aErr := newTestRequest("GET",
		getListMultipartURLWithParams("", bucketName, testObject, mpartResp.UploadID, "", "", ""),
		0, nil)
	if aErr != nil {
		t.Fatalf("Test %d %s Failed to create an anonymous request to list multipart of an upload for %s/%s: <ERROR> %v",
			1, instanceType, bucketName, testObject, aErr)
	}
	apiRouter.ServeHTTP(anonRec, anonReq)
	anonErrBytes, err := ioutil.ReadAll(anonRec.Result().Body)
	if err != nil {
		t.Fatalf("Test %d %s Failed to read error response from list object parts request %s/%s: <ERROR> %v",
			1, instanceType, bucketName, testObject, err)
	}
	var anonErrXML APIErrorResponse
	err = xml.Unmarshal(anonErrBytes, &anonErrXML)
	if err != nil {
		t.Fatalf("Test %d %s Failed to unmarshal error response from list object parts request %s/%s: <ERROR> %v",
			1, instanceType, bucketName, testObject, err)
	}
	if accessDeniedErr.Code != anonErrXML.Code {
		t.Errorf("Test %d %s expected to fail with error %s, but received %s", 1, instanceType,
			accessDeniedErr.Code, anonErrXML.Code)
	}

	// Set write only policy on bucket to allow anonymous ListObjectParts API
	// request to go through.
	writeOnlyPolicy := bucketPolicy{
		Version:    "1.0",
		Statements: []policyStatement{getWriteOnlyObjectStatement(bucketName, "")},
	}
	globalBucketPolicies.SetBucketPolicy(bucketName, &writeOnlyPolicy)

	anonRec = httptest.NewRecorder()
	anonReq, aErr = newTestRequest("GET",
		getListMultipartURLWithParams("", bucketName, testObject, mpartResp.UploadID, "", "", ""),
		0, nil)
	if aErr != nil {
		t.Fatalf("Test %d %s Failed to create an anonymous request to list multipart of an upload for %s/%s: <ERROR> %v",
			1, instanceType, bucketName, testObject, aErr)
	}
	apiRouter.ServeHTTP(anonRec, anonReq)
	if anonRec.Code != http.StatusOK {
		t.Errorf("Test %d %s expected ListObjectParts with authAnonymous type to succeed but failed with "+
			"HTTP status code %d", 1, instanceType, anonRec.Code)
	}
}

func TestListObjectPartsHandlerAnon(t *testing.T) {
	ExecObjectLayerAPITest(t, testAPIListObjectPartsHandlerAnon, []string{"PutObjectPart", "NewMultipart", "ListObjectParts"})
}

func TestListObjectPartsHandlerNilObjAPI(t *testing.T) {
	configDir, err := newTestConfig("us-east-1")
	if err != nil {
		t.Fatalf("Failed to create a test config: %v", err)
	}
	defer removeAll(configDir)

	rec := httptest.NewRecorder()
	req, err := newTestSignedRequestV4("GET",
		getListMultipartURLWithParams("", "testbucket", "testobject", "fakeuploadId", "", "", ""),
		0, bytes.NewReader([]byte("")), "abcd1", "abcd123")
	if err != nil {
		t.Fatal("Failed to create a signed UploadPart request.")
	}
	// Setup the 'nil' objectAPI router.
	nilAPIRouter := initTestNilObjAPIEndPoints([]string{"ListObjectParts"})
	nilAPIRouter.ServeHTTP(rec, req)
	serverNotInitializedErr := getAPIError(ErrServerNotInitialized).HTTPStatusCode
	if rec.Code != serverNotInitializedErr {
		t.Errorf("Test expected to fail with %d, but failed with %d", serverNotInitializedErr, rec.Code)
	}
}

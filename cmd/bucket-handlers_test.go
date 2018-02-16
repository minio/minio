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
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/minio/minio/pkg/auth"
)

// Wrapper for calling GetBucketPolicy HTTP handler tests for both XL multiple disks and single node setup.
func TestGetBucketLocationHandler(t *testing.T) {
	ExecObjectLayerAPITest(t, testGetBucketLocationHandler, []string{"GetBucketLocation"})
}

func testGetBucketLocationHandler(obj ObjectLayer, instanceType, bucketName string, apiRouter http.Handler,
	credentials auth.Credentials, t *testing.T) {

	// test cases with sample input and expected output.
	testCases := []struct {
		bucketName string
		accessKey  string
		secretKey  string
		// expected Response.
		expectedRespStatus int
		locationResponse   []byte
		errorResponse      APIErrorResponse
		shouldPass         bool
	}{
		// Test case - 1.
		// Tests for authenticated request and proper response.
		{
			bucketName:         bucketName,
			accessKey:          credentials.AccessKey,
			secretKey:          credentials.SecretKey,
			expectedRespStatus: http.StatusOK,
			locationResponse: []byte(`<?xml version="1.0" encoding="UTF-8"?>
<LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></LocationConstraint>`),
			errorResponse: APIErrorResponse{},
			shouldPass:    true,
		},
		// Test case - 2.
		// Tests for signature mismatch error.
		{
			bucketName:         bucketName,
			accessKey:          "abcd",
			secretKey:          "abcd",
			expectedRespStatus: http.StatusForbidden,
			locationResponse:   []byte(""),
			errorResponse: APIErrorResponse{
				Resource: "/" + bucketName + "/",
				Code:     "InvalidAccessKeyId",
				Message:  "The access key ID you provided does not exist in our records.",
			},
			shouldPass: false,
		},
	}

	for i, testCase := range testCases {
		if i != 1 {
			continue
		}
		// initialize httptest Recorder, this records any mutations to response writer inside the handler.
		rec := httptest.NewRecorder()
		// construct HTTP request for Get bucket location.
		req, err := newTestSignedRequestV4("GET", getBucketLocationURL("", testCase.bucketName), 0, nil, testCase.accessKey, testCase.secretKey)
		if err != nil {
			t.Fatalf("Test %d: %s: Failed to create HTTP request for GetBucketLocationHandler: <ERROR> %v", i+1, instanceType, err)
		}
		// Since `apiRouter` satisfies `http.Handler` it has a ServeHTTP to execute the logic of the handler.
		// Call the ServeHTTP to execute the handler.
		apiRouter.ServeHTTP(rec, req)
		if rec.Code != testCase.expectedRespStatus {
			t.Errorf("Test %d: %s: Expected the response status to be `%d`, but instead found `%d`", i+1, instanceType, testCase.expectedRespStatus, rec.Code)
		}
		if !bytes.Equal(testCase.locationResponse, rec.Body.Bytes()) && testCase.shouldPass {
			t.Errorf("Test %d: %s: Expected the response to be `%s`, but instead found `%s`", i+1, instanceType, string(testCase.locationResponse), string(rec.Body.Bytes()))
		}
		errorResponse := APIErrorResponse{}
		err = xml.Unmarshal(rec.Body.Bytes(), &errorResponse)
		if err != nil && !testCase.shouldPass {
			t.Fatalf("Test %d: %s: Unable to marshal response body %s", i+1, instanceType, string(rec.Body.Bytes()))
		}
		if errorResponse.Resource != testCase.errorResponse.Resource {
			t.Errorf("Test %d: %s: Expected the error resource to be `%s`, but instead found `%s`", i+1, instanceType, testCase.errorResponse.Resource, errorResponse.Resource)
		}
		if errorResponse.Message != testCase.errorResponse.Message {
			t.Errorf("Test %d: %s: Expected the error message to be `%s`, but instead found `%s`", i+1, instanceType, testCase.errorResponse.Message, errorResponse.Message)
		}
		if errorResponse.Code != testCase.errorResponse.Code {
			t.Errorf("Test %d: %s: Expected the error code to be `%s`, but instead found `%s`", i+1, instanceType, testCase.errorResponse.Code, errorResponse.Code)
		}

		// Verify response of the V2 signed HTTP request.
		// initialize HTTP NewRecorder, this records any mutations to response writer inside the handler.
		recV2 := httptest.NewRecorder()
		// construct HTTP request for PUT bucket policy endpoint.
		reqV2, err := newTestSignedRequestV2("GET", getBucketLocationURL("", testCase.bucketName), 0, nil, testCase.accessKey, testCase.secretKey)

		if err != nil {
			t.Fatalf("Test %d: %s: Failed to create HTTP request for PutBucketPolicyHandler: <ERROR> %v", i+1, instanceType, err)
		}
		// Since `apiRouter` satisfies `http.Handler` it has a ServeHTTP to execute the logic of the handler.
		// Call the ServeHTTP to execute the handler.
		apiRouter.ServeHTTP(recV2, reqV2)
		if recV2.Code != testCase.expectedRespStatus {
			t.Errorf("Test %d: %s: Expected the response status to be `%d`, but instead found `%d`", i+1, instanceType, testCase.expectedRespStatus, recV2.Code)
		}

		errorResponse = APIErrorResponse{}
		err = xml.Unmarshal(recV2.Body.Bytes(), &errorResponse)
		if err != nil && !testCase.shouldPass {
			t.Fatalf("Test %d: %s: Unable to marshal response body %s", i+1, instanceType, string(recV2.Body.Bytes()))
		}
		if errorResponse.Resource != testCase.errorResponse.Resource {
			t.Errorf("Test %d: %s: Expected the error resource to be `%s`, but instead found `%s`", i+1, instanceType, testCase.errorResponse.Resource, errorResponse.Resource)
		}
		if errorResponse.Message != testCase.errorResponse.Message {
			t.Errorf("Test %d: %s: Expected the error message to be `%s`, but instead found `%s`", i+1, instanceType, testCase.errorResponse.Message, errorResponse.Message)
		}
		if errorResponse.Code != testCase.errorResponse.Code {
			t.Errorf("Test %d: %s: Expected the error code to be `%s`, but instead found `%s`", i+1, instanceType, testCase.errorResponse.Code, errorResponse.Code)
		}

	}

	// Test for Anonymous/unsigned http request.
	// ListBucketsHandler doesn't support bucket policies, setting the policies shouldn't make any difference.
	anonReq, err := newTestRequest("GET", getBucketLocationURL("", bucketName), 0, nil)
	if err != nil {
		t.Fatalf("Minio %s: Failed to create an anonymous request.", instanceType)
	}

	// ExecObjectLayerAPIAnonTest - Calls the HTTP API handler using the anonymous request, validates the ErrAccessDeniedResponse,
	// sets the bucket policy using the policy statement generated from `getReadOnlyBucketStatement` so that the
	// unsigned request goes through and its validated again.
	ExecObjectLayerAPIAnonTest(t, obj, "TestGetBucketLocationHandler", bucketName, "", instanceType, apiRouter, anonReq, getReadOnlyBucketStatement)

	// HTTP request for testing when `objectLayer` is set to `nil`.
	// There is no need to use an existing bucket and valid input for creating the request
	// since the `objectLayer==nil`  check is performed before any other checks inside the handlers.
	// The only aim is to generate an HTTP request in a way that the relevant/registered end point is evoked/called.

	nilBucket := "dummy-bucket"
	nilReq, err := newTestRequest("GET", getBucketLocationURL("", nilBucket), 0, nil)

	if err != nil {
		t.Errorf("Minio %s: Failed to create HTTP request for testing the response when object Layer is set to `nil`.", instanceType)
	}
	// Executes the object layer set to `nil` test.
	// `ExecObjectLayerAPINilTest` manages the operation.
	ExecObjectLayerAPINilTest(t, nilBucket, "", instanceType, apiRouter, nilReq)
}

// Wrapper for calling HeadBucket HTTP handler tests for both XL multiple disks and single node setup.
func TestHeadBucketHandler(t *testing.T) {
	ExecObjectLayerAPITest(t, testHeadBucketHandler, []string{"HeadBucket"})
}

func testHeadBucketHandler(obj ObjectLayer, instanceType, bucketName string, apiRouter http.Handler,
	credentials auth.Credentials, t *testing.T) {

	// test cases with sample input and expected output.
	testCases := []struct {
		bucketName string
		accessKey  string
		secretKey  string
		// expected Response.
		expectedRespStatus int
	}{
		// Test case - 1.
		// Bucket exists.
		{
			bucketName:         bucketName,
			accessKey:          credentials.AccessKey,
			secretKey:          credentials.SecretKey,
			expectedRespStatus: http.StatusOK,
		},
		// Test case - 2.
		// Non-existent bucket name.
		{
			bucketName:         "2333",
			accessKey:          credentials.AccessKey,
			secretKey:          credentials.SecretKey,
			expectedRespStatus: http.StatusNotFound,
		},
		// Test case - 3.
		// Testing for signature mismatch error.
		// setting invalid acess and secret key.
		{
			bucketName:         bucketName,
			accessKey:          "abcd",
			secretKey:          "abcd",
			expectedRespStatus: http.StatusForbidden,
		},
	}

	for i, testCase := range testCases {
		// initialize HTTP NewRecorder, this records any mutations to response writer inside the handler.
		rec := httptest.NewRecorder()
		// construct HTTP request for HEAD bucket.
		req, err := newTestSignedRequestV4("HEAD", getHEADBucketURL("", testCase.bucketName), 0, nil, testCase.accessKey, testCase.secretKey)
		if err != nil {
			t.Fatalf("Test %d: %s: Failed to create HTTP request for HeadBucketHandler: <ERROR> %v", i+1, instanceType, err)
		}
		// Since `apiRouter` satisfies `http.Handler` it has a ServeHTTP to execute the logic of the handler.
		// Call the ServeHTTP to execute the handler.
		apiRouter.ServeHTTP(rec, req)
		if rec.Code != testCase.expectedRespStatus {
			t.Errorf("Test %d: %s: Expected the response status to be `%d`, but instead found `%d`", i+1, instanceType, testCase.expectedRespStatus, rec.Code)
		}

		// Verify response the V2 signed HTTP request.
		// initialize HTTP NewRecorder, this records any mutations to response writer inside the handler.
		recV2 := httptest.NewRecorder()
		// construct HTTP request for PUT bucket policy endpoint.
		reqV2, err := newTestSignedRequestV2("HEAD", getHEADBucketURL("", testCase.bucketName), 0, nil, testCase.accessKey, testCase.secretKey)

		if err != nil {
			t.Fatalf("Test %d: %s: Failed to create HTTP request for PutBucketPolicyHandler: <ERROR> %v", i+1, instanceType, err)
		}
		// Since `apiRouter` satisfies `http.Handler` it has a ServeHTTP to execute the logic of the handler.
		// Call the ServeHTTP to execute the handler.
		apiRouter.ServeHTTP(recV2, reqV2)
		if recV2.Code != testCase.expectedRespStatus {
			t.Errorf("Test %d: %s: Expected the response status to be `%d`, but instead found `%d`", i+1, instanceType, testCase.expectedRespStatus, recV2.Code)
		}

	}

	// Test for Anonymous/unsigned http request.
	anonReq, err := newTestRequest("HEAD", getHEADBucketURL("", bucketName), 0, nil)

	if err != nil {
		t.Fatalf("Minio %s: Failed to create an anonymous request for bucket \"%s\": <ERROR> %v",
			instanceType, bucketName, err)
	}

	// ExecObjectLayerAPIAnonTest - Calls the HTTP API handler using the anonymous request, validates the ErrAccessDeniedResponse,
	// sets the bucket policy using the policy statement generated from `getReadOnlyBucketStatement` so that the
	// unsigned request goes through and its validated again.
	ExecObjectLayerAPIAnonTest(t, obj, "TestHeadBucketHandler", bucketName, "", instanceType, apiRouter, anonReq, getReadOnlyBucketStatement)

	// HTTP request for testing when `objectLayer` is set to `nil`.
	// There is no need to use an existing bucket and valid input for creating the request
	// since the `objectLayer==nil`  check is performed before any other checks inside the handlers.
	// The only aim is to generate an HTTP request in a way that the relevant/registered end point is evoked/called.

	nilBucket := "dummy-bucket"
	nilReq, err := newTestRequest("HEAD", getHEADBucketURL("", nilBucket), 0, nil)

	if err != nil {
		t.Errorf("Minio %s: Failed to create HTTP request for testing the response when object Layer is set to `nil`.", instanceType)
	}
	// execute the object layer set to `nil` test.
	// `ExecObjectLayerAPINilTest` manages the operation.
	ExecObjectLayerAPINilTest(t, nilBucket, "", instanceType, apiRouter, nilReq)
}

// Wrapper for calling TestListMultipartUploadsHandler tests for both XL multiple disks and single node setup.
func TestListMultipartUploadsHandler(t *testing.T) {
	ExecObjectLayerAPITest(t, testListMultipartUploadsHandler, []string{"ListMultipartUploads"})
}

// testListMultipartUploadsHandler - Tests validate listing of multipart uploads.
func testListMultipartUploadsHandler(obj ObjectLayer, instanceType, bucketName string, apiRouter http.Handler,
	credentials auth.Credentials, t *testing.T) {

	// Collection of non-exhaustive ListMultipartUploads test cases, valid errors
	// and success responses.
	testCases := []struct {
		// Inputs to ListMultipartUploads.
		bucket             string
		prefix             string
		keyMarker          string
		uploadIDMarker     string
		delimiter          string
		maxUploads         string
		accessKey          string
		secretKey          string
		expectedRespStatus int
		shouldPass         bool
	}{
		// Test case - 1.
		// Setting invalid bucket name.
		{
			bucket:             ".test",
			prefix:             "",
			keyMarker:          "",
			uploadIDMarker:     "",
			delimiter:          "",
			maxUploads:         "0",
			accessKey:          credentials.AccessKey,
			secretKey:          credentials.SecretKey,
			expectedRespStatus: http.StatusBadRequest,
			shouldPass:         false,
		},
		// Test case - 2.
		// Setting a non-existent bucket.
		{
			bucket:             "volatile-bucket-1",
			prefix:             "",
			keyMarker:          "",
			uploadIDMarker:     "",
			delimiter:          "",
			maxUploads:         "0",
			accessKey:          credentials.AccessKey,
			secretKey:          credentials.SecretKey,
			expectedRespStatus: http.StatusNotFound,
			shouldPass:         false,
		},
		// Test case -3.
		// Setting invalid delimiter, expecting the HTTP response status to be http.StatusNotImplemented.
		{
			bucket:             bucketName,
			prefix:             "",
			keyMarker:          "",
			uploadIDMarker:     "",
			delimiter:          "-",
			maxUploads:         "0",
			accessKey:          credentials.AccessKey,
			secretKey:          credentials.SecretKey,
			expectedRespStatus: http.StatusNotImplemented,
			shouldPass:         false,
		},
		// Test case - 4.
		// Setting Invalid prefix and marker combination.
		{
			bucket:             bucketName,
			prefix:             "asia",
			keyMarker:          "europe-object",
			uploadIDMarker:     "",
			delimiter:          "",
			maxUploads:         "0",
			accessKey:          credentials.AccessKey,
			secretKey:          credentials.SecretKey,
			expectedRespStatus: http.StatusNotImplemented,
			shouldPass:         false,
		},
		// Test case - 5.
		// Invalid upload id and marker combination.
		{
			bucket:             bucketName,
			prefix:             "asia",
			keyMarker:          "asia/europe/",
			uploadIDMarker:     "abc",
			delimiter:          "",
			maxUploads:         "0",
			accessKey:          credentials.AccessKey,
			secretKey:          credentials.SecretKey,
			expectedRespStatus: http.StatusNotImplemented,
			shouldPass:         false,
		},
		// Test case - 6.
		// Setting a negative value to max-uploads paramater, should result in http.StatusBadRequest.
		{
			bucket:             bucketName,
			prefix:             "",
			keyMarker:          "",
			uploadIDMarker:     "",
			delimiter:          "",
			maxUploads:         "-1",
			accessKey:          credentials.AccessKey,
			secretKey:          credentials.SecretKey,
			expectedRespStatus: http.StatusBadRequest,
			shouldPass:         false,
		},
		// Test case - 7.
		// Case with right set of parameters,
		// should result in success 200OK.
		{
			bucket:             bucketName,
			prefix:             "",
			keyMarker:          "",
			uploadIDMarker:     "",
			delimiter:          "/",
			maxUploads:         "100",
			accessKey:          credentials.AccessKey,
			secretKey:          credentials.SecretKey,
			expectedRespStatus: http.StatusOK,
			shouldPass:         true,
		},
		// Test case - 8.
		// Good case without delimiter.
		{
			bucket:             bucketName,
			prefix:             "",
			keyMarker:          "",
			uploadIDMarker:     "",
			delimiter:          "",
			maxUploads:         "100",
			accessKey:          credentials.AccessKey,
			secretKey:          credentials.SecretKey,
			expectedRespStatus: http.StatusOK,
			shouldPass:         true,
		},
		// Test case - 9.
		// Setting Invalid AccessKey and SecretKey to induce and verify Signature Mismatch error.
		{
			bucket:             bucketName,
			prefix:             "",
			keyMarker:          "",
			uploadIDMarker:     "",
			delimiter:          "",
			maxUploads:         "100",
			accessKey:          "abcd",
			secretKey:          "abcd",
			expectedRespStatus: http.StatusForbidden,
			shouldPass:         true,
		},
	}

	for i, testCase := range testCases {
		// initialize HTTP NewRecorder, this records any mutations to response writer inside the handler.
		rec := httptest.NewRecorder()

		// construct HTTP request for List multipart uploads endpoint.
		u := getListMultipartUploadsURLWithParams("", testCase.bucket, testCase.prefix, testCase.keyMarker, testCase.uploadIDMarker, testCase.delimiter, testCase.maxUploads)
		req, gerr := newTestSignedRequestV4("GET", u, 0, nil, testCase.accessKey, testCase.secretKey)
		if gerr != nil {
			t.Fatalf("Test %d: %s: Failed to create HTTP request for ListMultipartUploadsHandler: <ERROR> %v", i+1, instanceType, gerr)
		}
		// Since `apiRouter` satisfies `http.Handler` it has a ServeHTTP to execute the logic of the handler.
		// Call the ServeHTTP to execute the handler.
		apiRouter.ServeHTTP(rec, req)
		if rec.Code != testCase.expectedRespStatus {
			t.Errorf("Test %d: %s: Expected the response status to be `%d`, but instead found `%d`", i+1, instanceType, testCase.expectedRespStatus, rec.Code)
		}

		// Verify response the V2 signed HTTP request.
		// initialize HTTP NewRecorder, this records any mutations to response writer inside the handler.
		recV2 := httptest.NewRecorder()
		// construct HTTP request for PUT bucket policy endpoint.

		// verify response for V2 signed HTTP request.
		reqV2, err := newTestSignedRequestV2("GET", u, 0, nil, testCase.accessKey, testCase.secretKey)
		if err != nil {
			t.Fatalf("Test %d: %s: Failed to create HTTP request for PutBucketPolicyHandler: <ERROR> %v", i+1, instanceType, err)
		}
		// Since `apiRouter` satisfies `http.Handler` it has a ServeHTTP to execute the logic of the handler.
		// Call the ServeHTTP to execute the handler.
		apiRouter.ServeHTTP(recV2, reqV2)
		if recV2.Code != testCase.expectedRespStatus {
			t.Errorf("Test %d: %s: Expected the response status to be `%d`, but instead found `%d`", i+1, instanceType, testCase.expectedRespStatus, recV2.Code)
		}
	}

	// initialize HTTP NewRecorder, this records any mutations to response writer inside the handler.
	rec := httptest.NewRecorder()

	// construct HTTP request for List multipart uploads endpoint.
	u := getListMultipartUploadsURLWithParams("", bucketName, "", "", "", "", "")
	req, err := newTestSignedRequestV4("GET", u, 0, nil, "", "") // Generate an anonymous request.
	if err != nil {
		t.Fatalf("Test %s: Failed to create HTTP request for ListMultipartUploadsHandler: <ERROR> %v", instanceType, err)
	}
	// Since `apiRouter` satisfies `http.Handler` it has a ServeHTTP to execute the logic of the handler.
	// Call the ServeHTTP to execute the handler.
	apiRouter.ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Errorf("Test %s: Expected the response status to be `http.StatusForbidden`, but instead found `%d`", instanceType, rec.Code)
	}

	url := getListMultipartUploadsURLWithParams("", testCases[6].bucket, testCases[6].prefix, testCases[6].keyMarker,
		testCases[6].uploadIDMarker, testCases[6].delimiter, testCases[6].maxUploads)
	// Test for Anonymous/unsigned http request.
	anonReq, err := newTestRequest("GET", url, 0, nil)
	if err != nil {
		t.Fatalf("Minio %s: Failed to create an anonymous request for bucket \"%s\": <ERROR> %v",
			instanceType, bucketName, err)
	}

	// ExecObjectLayerAPIAnonTest - Calls the HTTP API handler using the anonymous request, validates the ErrAccessDeniedResponse,
	// sets the bucket policy using the policy statement generated from `getWriteOnlyBucketStatement` so that the
	// unsigned request goes through and its validated again.
	ExecObjectLayerAPIAnonTest(t, obj, "TestListMultipartUploadsHandler", bucketName, "", instanceType, apiRouter, anonReq, getWriteOnlyBucketStatement)

	// HTTP request for testing when `objectLayer` is set to `nil`.
	// There is no need to use an existing bucket and valid input for creating the request
	// since the `objectLayer==nil`  check is performed before any other checks inside the handlers.
	// The only aim is to generate an HTTP request in a way that the relevant/registered end point is evoked/called.

	nilBucket := "dummy-bucket"
	url = getListMultipartUploadsURLWithParams("", nilBucket, "dummy-prefix", testCases[6].keyMarker,
		testCases[6].uploadIDMarker, testCases[6].delimiter, testCases[6].maxUploads)

	nilReq, err := newTestRequest("GET", url, 0, nil)

	if err != nil {
		t.Errorf("Minio %s: Failed to create HTTP request for testing the response when object Layer is set to `nil`.", instanceType)
	}
	// execute the object layer set to `nil` test.
	// `ExecObjectLayerAPINilTest` manages the operation.
	ExecObjectLayerAPINilTest(t, nilBucket, "", instanceType, apiRouter, nilReq)
}

// Wrapper for calling TestListBucketsHandler tests for both XL multiple disks and single node setup.
func TestListBucketsHandler(t *testing.T) {
	ExecObjectLayerAPITest(t, testListBucketsHandler, []string{"ListBuckets"})
}

// testListBucketsHandler - Tests validate listing of buckets.
func testListBucketsHandler(obj ObjectLayer, instanceType, bucketName string, apiRouter http.Handler,
	credentials auth.Credentials, t *testing.T) {

	testCases := []struct {
		bucketName         string
		accessKey          string
		secretKey          string
		expectedRespStatus int
	}{
		// Test case - 1.
		// Validate a good case request succeeds.
		{
			bucketName:         bucketName,
			accessKey:          credentials.AccessKey,
			secretKey:          credentials.SecretKey,
			expectedRespStatus: http.StatusOK,
		},
		// Test case - 2.
		// Test case with invalid accessKey to produce and validate Signature MisMatch error.
		{
			bucketName:         bucketName,
			accessKey:          "abcd",
			secretKey:          "abcd",
			expectedRespStatus: http.StatusForbidden,
		},
	}

	for i, testCase := range testCases {
		// initialize HTTP NewRecorder, this records any mutations to response writer inside the handler.
		rec := httptest.NewRecorder()
		req, lerr := newTestSignedRequestV4("GET", getListBucketURL(""), 0, nil, testCase.accessKey, testCase.secretKey)
		if lerr != nil {
			t.Fatalf("Test %d: %s: Failed to create HTTP request for ListBucketsHandler: <ERROR> %v", i+1, instanceType, lerr)
		}
		// Since `apiRouter` satisfies `http.Handler` it has a ServeHTTP to execute the logic of the handler.
		// Call the ServeHTTP to execute the handler.
		apiRouter.ServeHTTP(rec, req)
		if rec.Code != testCase.expectedRespStatus {
			t.Errorf("Test %d: %s: Expected the response status to be `%d`, but instead found `%d`", i+1, instanceType, testCase.expectedRespStatus, rec.Code)
		}

		// Verify response of the V2 signed HTTP request.
		// initialize HTTP NewRecorder, this records any mutations to response writer inside the handler.
		recV2 := httptest.NewRecorder()
		// construct HTTP request for PUT bucket policy endpoint.

		// verify response for V2 signed HTTP request.
		reqV2, err := newTestSignedRequestV2("GET", getListBucketURL(""), 0, nil, testCase.accessKey, testCase.secretKey)

		if err != nil {
			t.Fatalf("Test %d: %s: Failed to create HTTP request for PutBucketPolicyHandler: <ERROR> %v", i+1, instanceType, err)
		}
		// Since `apiRouter` satisfies `http.Handler` it has a ServeHTTP to execute the logic of the handler.
		// Call the ServeHTTP to execute the handler.
		apiRouter.ServeHTTP(recV2, reqV2)
		if recV2.Code != testCase.expectedRespStatus {
			t.Errorf("Test %d: %s: Expected the response status to be `%d`, but instead found `%d`", i+1, instanceType, testCase.expectedRespStatus, recV2.Code)
		}
	}

	// Test for Anonymous/unsigned http request.
	// ListBucketsHandler doesn't support bucket policies, setting the policies shouldn't make a difference.
	anonReq, err := newTestRequest("GET", getListBucketURL(""), 0, nil)

	if err != nil {
		t.Fatalf("Minio %s: Failed to create an anonymous request.", instanceType)
	}

	// ExecObjectLayerAPIAnonTest - Calls the HTTP API handler using the anonymous request, validates the ErrAccessDeniedResponse,
	// sets the bucket policy using the policy statement generated from `getWriteOnlyObjectStatement` so that the
	// unsigned request goes through and its validated again.
	ExecObjectLayerAPIAnonTest(t, obj, "ListBucketsHandler", "", "", instanceType, apiRouter, anonReq, getWriteOnlyObjectStatement)

	// HTTP request for testing when `objectLayer` is set to `nil`.
	// There is no need to use an existing bucket and valid input for creating the request
	// since the `objectLayer==nil`  check is performed before any other checks inside the handlers.
	// The only aim is to generate an HTTP request in a way that the relevant/registered end point is evoked/called.

	nilReq, err := newTestRequest("GET", getListBucketURL(""), 0, nil)

	if err != nil {
		t.Errorf("Minio %s: Failed to create HTTP request for testing the response when object Layer is set to `nil`.", instanceType)
	}
	// execute the object layer set to `nil` test.
	// `ExecObjectLayerAPINilTest` manages the operation.
	ExecObjectLayerAPINilTest(t, "", "", instanceType, apiRouter, nilReq)
}

// Wrapper for calling DeleteMultipleObjects HTTP handler tests for both XL multiple disks and single node setup.
func TestAPIDeleteMultipleObjectsHandler(t *testing.T) {
	ExecObjectLayerAPITest(t, testAPIDeleteMultipleObjectsHandler, []string{"DeleteMultipleObjects"})
}

func testAPIDeleteMultipleObjectsHandler(obj ObjectLayer, instanceType, bucketName string, apiRouter http.Handler,
	credentials auth.Credentials, t *testing.T) {

	var err error
	// register event notifier.
	err = initEventNotifier(obj)
	if err != nil {
		t.Fatal("Notifier initialization failed.")
	}

	contentBytes := []byte("hello")
	sha256sum := ""
	var objectNames []string
	for i := 0; i < 10; i++ {
		objectName := "test-object-" + strconv.Itoa(i)
		// uploading the object.
		_, err = obj.PutObject(bucketName, objectName, mustGetHashReader(t, bytes.NewBuffer(contentBytes), int64(len(contentBytes)), "", sha256sum), nil)
		// if object upload fails stop the test.
		if err != nil {
			t.Fatalf("Put Object %d:  Error uploading object: <ERROR> %v", i, err)
		}

		// object used for the test.
		objectNames = append(objectNames, objectName)
	}

	getObjectIdentifierList := func(objectNames []string) (objectIdentifierList []ObjectIdentifier) {
		for _, objectName := range objectNames {
			objectIdentifierList = append(objectIdentifierList, ObjectIdentifier{objectName})
		}

		return objectIdentifierList
	}
	getDeleteErrorList := func(objects []ObjectIdentifier) (deleteErrorList []DeleteError) {
		for _, obj := range objects {
			deleteErrorList = append(deleteErrorList, DeleteError{
				Code:    errorCodeResponse[ErrAccessDenied].Code,
				Message: errorCodeResponse[ErrAccessDenied].Description,
				Key:     obj.ObjectName,
			})
		}

		return deleteErrorList
	}

	requestList := []DeleteObjectsRequest{
		{Quiet: false, Objects: getObjectIdentifierList(objectNames[:5])},
		{Quiet: true, Objects: getObjectIdentifierList(objectNames[5:])},
	}

	// generate multi objects delete response.
	successRequest0 := encodeResponse(requestList[0])
	successResponse0 := generateMultiDeleteResponse(requestList[0].Quiet, requestList[0].Objects, nil)
	encodedSuccessResponse0 := encodeResponse(successResponse0)

	successRequest1 := encodeResponse(requestList[1])
	successResponse1 := generateMultiDeleteResponse(requestList[1].Quiet, requestList[1].Objects, nil)
	encodedSuccessResponse1 := encodeResponse(successResponse1)

	// generate multi objects delete response for errors.
	// errorRequest := encodeResponse(requestList[1])
	errorResponse := generateMultiDeleteResponse(requestList[1].Quiet, requestList[1].Objects, nil)
	encodedErrorResponse := encodeResponse(errorResponse)

	anonRequest := encodeResponse(requestList[0])
	anonResponse := generateMultiDeleteResponse(requestList[0].Quiet, nil, getDeleteErrorList(requestList[0].Objects))
	encodedAnonResponse := encodeResponse(anonResponse)

	testCases := []struct {
		bucket             string
		objects            []byte
		accessKey          string
		secretKey          string
		expectedContent    []byte
		expectedRespStatus int
	}{
		// Test case - 1.
		// Delete objects with invalid access key.
		{
			bucket:             bucketName,
			objects:            successRequest0,
			accessKey:          "Invalid-AccessID",
			secretKey:          credentials.SecretKey,
			expectedContent:    nil,
			expectedRespStatus: http.StatusForbidden,
		},
		// Test case - 2.
		// Delete valid objects with quiet flag off.
		{
			bucket:             bucketName,
			objects:            successRequest0,
			accessKey:          credentials.AccessKey,
			secretKey:          credentials.SecretKey,
			expectedContent:    encodedSuccessResponse0,
			expectedRespStatus: http.StatusOK,
		},
		// Test case - 3.
		// Delete valid objects with quiet flag on.
		{
			bucket:             bucketName,
			objects:            successRequest1,
			accessKey:          credentials.AccessKey,
			secretKey:          credentials.SecretKey,
			expectedContent:    encodedSuccessResponse1,
			expectedRespStatus: http.StatusOK,
		},
		// Test case - 4.
		// Delete previously deleted objects.
		{
			bucket:             bucketName,
			objects:            successRequest1,
			accessKey:          credentials.AccessKey,
			secretKey:          credentials.SecretKey,
			expectedContent:    encodedErrorResponse,
			expectedRespStatus: http.StatusOK,
		},
		// Test case - 5.
		// Anonymous user access denied response
		// Currently anonymous users cannot delete multiple objects in Minio server
		{
			bucket:             bucketName,
			objects:            anonRequest,
			accessKey:          "",
			secretKey:          "",
			expectedContent:    encodedAnonResponse,
			expectedRespStatus: http.StatusOK,
		},
	}

	for i, testCase := range testCases {
		var req *http.Request
		var actualContent []byte

		// Generate a signed or anonymous request based on the testCase
		if testCase.accessKey != "" {
			req, err = newTestSignedRequestV4("POST", getDeleteMultipleObjectsURL("", bucketName),
				int64(len(testCase.objects)), bytes.NewReader(testCase.objects), testCase.accessKey, testCase.secretKey)
		} else {
			req, err = newTestRequest("POST", getDeleteMultipleObjectsURL("", bucketName),
				int64(len(testCase.objects)), bytes.NewReader(testCase.objects))
		}

		if err != nil {
			t.Fatalf("Failed to create HTTP request for DeleteMultipleObjects: <ERROR> %v", err)
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
		if testCase.expectedContent != nil && !bytes.Equal(testCase.expectedContent, actualContent) {
			t.Errorf("Test %d : Minio %s: Object content differs from expected value.", i+1, instanceType)
		}
	}

	// HTTP request to test the case of `objectLayer` being set to `nil`.
	// There is no need to use an existing bucket or valid input for creating the request,
	// since the `objectLayer==nil`  check is performed before any other checks inside the handlers.
	// The only aim is to generate an HTTP request in a way that the relevant/registered end point is evoked/called.
	// Indicating that all parts are uploaded and initiating completeMultipartUpload.
	nilBucket := "dummy-bucket"
	nilObject := ""

	nilReq, err := newTestSignedRequestV4("POST", getDeleteMultipleObjectsURL("", nilBucket), 0, nil, "", "")
	if err != nil {
		t.Errorf("Minio %s: Failed to create HTTP request for testing the response when object Layer is set to `nil`.", instanceType)
	}
	// execute the object layer set to `nil` test.
	// `ExecObjectLayerAPINilTest` manages the operation.
	ExecObjectLayerAPINilTest(t, nilBucket, nilObject, instanceType, apiRouter, nilReq)
}

func TestIsBucketActionAllowed(t *testing.T) {
	ExecObjectLayerAPITest(t, testIsBucketActionAllowedHandler, []string{"BucketLocation"})
}

func testIsBucketActionAllowedHandler(obj ObjectLayer, instanceType, bucketName string, apiRouter http.Handler,
	credentials auth.Credentials, t *testing.T) {

	testCases := []struct {
		// input.
		action              string
		bucket              string
		prefix              string
		isGlobalPoliciesNil bool
		// flag indicating whether the test should pass.
		shouldPass bool
	}{
		{"s3:GetBucketLocation", "mybucket", "abc", true, false},
		{"s3:ListObject", "mybucket", "abc", false, false},
	}
	for i, testCase := range testCases {
		isAllowed := isBucketActionAllowed(testCase.action, testCase.bucket, testCase.prefix, obj)
		if isAllowed != testCase.shouldPass {
			t.Errorf("Case %d: Expected the response status to be `%t`, but instead found `%t`", i+1, testCase.shouldPass, isAllowed)
		}

	}
}

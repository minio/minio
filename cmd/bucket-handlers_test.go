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

package cmd

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/minio/minio/internal/auth"
)

// Wrapper for calling RemoveBucket HTTP handler tests for both Erasure multiple disks and single node setup.
func TestRemoveBucketHandler(t *testing.T) {
	ExecObjectLayerAPITest(ExecObjectLayerAPITestArgs{t: t, objAPITest: testRemoveBucketHandler, endpoints: []string{"RemoveBucket"}})
}

func testRemoveBucketHandler(obj ObjectLayer, instanceType, bucketName string, apiRouter http.Handler,
	credentials auth.Credentials, t *testing.T,
) {
	_, err := obj.PutObject(GlobalContext, bucketName, "test-object", mustGetPutObjReader(t, bytes.NewReader([]byte{}), int64(0), "", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"), ObjectOptions{})
	// if object upload fails stop the test.
	if err != nil {
		t.Fatalf("Error uploading object: <ERROR> %v", err)
	}

	// initialize httptest Recorder, this records any mutations to response writer inside the handler.
	rec := httptest.NewRecorder()
	// construct HTTP request for DELETE bucket.
	req, err := newTestSignedRequestV4(http.MethodDelete, getBucketLocationURL("", bucketName), 0, nil, credentials.AccessKey, credentials.SecretKey, nil)
	if err != nil {
		t.Fatalf("Test %s: Failed to create HTTP request for RemoveBucketHandler: <ERROR> %v", instanceType, err)
	}
	// Since `apiRouter` satisfies `http.Handler` it has a ServeHTTP to execute the logic of the handler.
	// Call the ServeHTTP to execute the handler.
	apiRouter.ServeHTTP(rec, req)
	switch rec.Code {
	case http.StatusOK, http.StatusCreated, http.StatusAccepted, http.StatusNoContent:
		t.Fatalf("Test %v: expected failure, but succeeded with %v", instanceType, rec.Code)
	}

	// Verify response of the V2 signed HTTP request.
	// initialize HTTP NewRecorder, this records any mutations to response writer inside the handler.
	recV2 := httptest.NewRecorder()
	// construct HTTP request for DELETE bucket.
	reqV2, err := newTestSignedRequestV2(http.MethodDelete, getBucketLocationURL("", bucketName), 0, nil, credentials.AccessKey, credentials.SecretKey, nil)
	if err != nil {
		t.Fatalf("Test %s: Failed to create HTTP request for RemoveBucketHandler: <ERROR> %v", instanceType, err)
	}
	// Since `apiRouter` satisfies `http.Handler` it has a ServeHTTP to execute the logic of the handler.
	// Call the ServeHTTP to execute the handler.
	apiRouter.ServeHTTP(recV2, reqV2)
	switch recV2.Code {
	case http.StatusOK, http.StatusCreated, http.StatusAccepted, http.StatusNoContent:
		t.Fatalf("Test %v: expected failure, but succeeded with %v", instanceType, recV2.Code)
	}
}

// Wrapper for calling GetBucketPolicy HTTP handler tests for both Erasure multiple disks and single node setup.
func TestGetBucketLocationHandler(t *testing.T) {
	ExecObjectLayerAPITest(ExecObjectLayerAPITestArgs{t: t, objAPITest: testGetBucketLocationHandler, endpoints: []string{"GetBucketLocation"}})
}

func testGetBucketLocationHandler(obj ObjectLayer, instanceType, bucketName string, apiRouter http.Handler,
	credentials auth.Credentials, t *testing.T,
) {
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
				Resource: SlashSeparator + bucketName + SlashSeparator,
				Code:     "InvalidAccessKeyId",
				Message:  "The Access Key Id you provided does not exist in our records.",
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
		req, err := newTestSignedRequestV4(http.MethodGet, getBucketLocationURL("", testCase.bucketName), 0, nil, testCase.accessKey, testCase.secretKey, nil)
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
			t.Errorf("Test %d: %s: Expected the response to be `%s`, but instead found `%s`", i+1, instanceType, string(testCase.locationResponse), rec.Body.String())
		}
		errorResponse := APIErrorResponse{}
		err = xml.Unmarshal(rec.Body.Bytes(), &errorResponse)
		if err != nil && !testCase.shouldPass {
			t.Fatalf("Test %d: %s: Unable to marshal response body %s", i+1, instanceType, rec.Body.String())
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
		reqV2, err := newTestSignedRequestV2(http.MethodGet, getBucketLocationURL("", testCase.bucketName), 0, nil, testCase.accessKey, testCase.secretKey, nil)
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
			t.Fatalf("Test %d: %s: Unable to marshal response body %s", i+1, instanceType, recV2.Body.String())
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
	anonReq, err := newTestRequest(http.MethodGet, getBucketLocationURL("", bucketName), 0, nil)
	if err != nil {
		t.Fatalf("MinIO %s: Failed to create an anonymous request.", instanceType)
	}

	// ExecObjectLayerAPIAnonTest - Calls the HTTP API handler using the anonymous request, validates the ErrAccessDeniedResponse,
	// sets the bucket policy using the policy statement generated from `getReadOnlyBucketStatement` so that the
	// unsigned request goes through and its validated again.
	ExecObjectLayerAPIAnonTest(t, obj, "TestGetBucketLocationHandler", bucketName, "", instanceType, apiRouter, anonReq, getAnonReadOnlyBucketPolicy(bucketName))

	// HTTP request for testing when `objectLayer` is set to `nil`.
	// There is no need to use an existing bucket and valid input for creating the request
	// since the `objectLayer==nil`  check is performed before any other checks inside the handlers.
	// The only aim is to generate an HTTP request in a way that the relevant/registered end point is evoked/called.

	nilBucket := "dummy-bucket"
	nilReq, err := newTestRequest(http.MethodGet, getBucketLocationURL("", nilBucket), 0, nil)
	if err != nil {
		t.Errorf("MinIO %s: Failed to create HTTP request for testing the response when object Layer is set to `nil`.", instanceType)
	}
	// Executes the object layer set to `nil` test.
	// `ExecObjectLayerAPINilTest` manages the operation.
	ExecObjectLayerAPINilTest(t, nilBucket, "", instanceType, apiRouter, nilReq)
}

// Wrapper for calling HeadBucket HTTP handler tests for both Erasure multiple disks and single node setup.
func TestHeadBucketHandler(t *testing.T) {
	ExecObjectLayerAPITest(ExecObjectLayerAPITestArgs{t: t, objAPITest: testHeadBucketHandler, endpoints: []string{"HeadBucket"}})
}

func testHeadBucketHandler(obj ObjectLayer, instanceType, bucketName string, apiRouter http.Handler,
	credentials auth.Credentials, t *testing.T,
) {
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
		// setting invalid access and secret key.
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
		req, err := newTestSignedRequestV4(http.MethodHead, getHEADBucketURL("", testCase.bucketName), 0, nil, testCase.accessKey, testCase.secretKey, nil)
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
		reqV2, err := newTestSignedRequestV2(http.MethodHead, getHEADBucketURL("", testCase.bucketName), 0, nil, testCase.accessKey, testCase.secretKey, nil)
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
	anonReq, err := newTestRequest(http.MethodHead, getHEADBucketURL("", bucketName), 0, nil)
	if err != nil {
		t.Fatalf("MinIO %s: Failed to create an anonymous request for bucket \"%s\": <ERROR> %v",
			instanceType, bucketName, err)
	}

	// ExecObjectLayerAPIAnonTest - Calls the HTTP API handler using the anonymous request, validates the ErrAccessDeniedResponse,
	// sets the bucket policy using the policy statement generated from `getReadOnlyBucketStatement` so that the
	// unsigned request goes through and its validated again.
	ExecObjectLayerAPIAnonTest(t, obj, "TestHeadBucketHandler", bucketName, "", instanceType, apiRouter, anonReq, getAnonReadOnlyBucketPolicy(bucketName))

	// HTTP request for testing when `objectLayer` is set to `nil`.
	// There is no need to use an existing bucket and valid input for creating the request
	// since the `objectLayer==nil`  check is performed before any other checks inside the handlers.
	// The only aim is to generate an HTTP request in a way that the relevant/registered end point is evoked/called.

	nilBucket := "dummy-bucket"
	nilReq, err := newTestRequest(http.MethodHead, getHEADBucketURL("", nilBucket), 0, nil)
	if err != nil {
		t.Errorf("MinIO %s: Failed to create HTTP request for testing the response when object Layer is set to `nil`.", instanceType)
	}
	// execute the object layer set to `nil` test.
	// `ExecObjectLayerAPINilTest` manages the operation.
	ExecObjectLayerAPINilTest(t, nilBucket, "", instanceType, apiRouter, nilReq)
}

// Wrapper for calling TestListMultipartUploadsHandler tests for both Erasure multiple disks and single node setup.
func TestListMultipartUploadsHandler(t *testing.T) {
	ExecObjectLayerAPITest(ExecObjectLayerAPITestArgs{t: t, objAPITest: testListMultipartUploadsHandler, endpoints: []string{"ListMultipartUploads"}})
}

// testListMultipartUploadsHandler - Tests validate listing of multipart uploads.
func testListMultipartUploadsHandler(obj ObjectLayer, instanceType, bucketName string, apiRouter http.Handler,
	credentials auth.Credentials, t *testing.T,
) {
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
		// Delimiter unsupported, but response is empty.
		{
			bucket:             bucketName,
			prefix:             "",
			keyMarker:          "",
			uploadIDMarker:     "",
			delimiter:          "-",
			maxUploads:         "0",
			accessKey:          credentials.AccessKey,
			secretKey:          credentials.SecretKey,
			expectedRespStatus: http.StatusOK,
			shouldPass:         true,
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
		// Setting a negative value to max-uploads parameter, should result in http.StatusBadRequest.
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
			delimiter:          SlashSeparator,
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
		req, gerr := newTestSignedRequestV4(http.MethodGet, u, 0, nil, testCase.accessKey, testCase.secretKey, nil)
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
		reqV2, err := newTestSignedRequestV2(http.MethodGet, u, 0, nil, testCase.accessKey, testCase.secretKey, nil)
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
	req, err := newTestSignedRequestV4(http.MethodGet, u, 0, nil, "", "", nil) // Generate an anonymous request.
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
	anonReq, err := newTestRequest(http.MethodGet, url, 0, nil)
	if err != nil {
		t.Fatalf("MinIO %s: Failed to create an anonymous request for bucket \"%s\": <ERROR> %v",
			instanceType, bucketName, err)
	}

	// ExecObjectLayerAPIAnonTest - Calls the HTTP API handler using the anonymous request, validates the ErrAccessDeniedResponse,
	// sets the bucket policy using the policy statement generated from `getWriteOnlyBucketStatement` so that the
	// unsigned request goes through and its validated again.
	ExecObjectLayerAPIAnonTest(t, obj, "TestListMultipartUploadsHandler", bucketName, "", instanceType, apiRouter, anonReq, getAnonWriteOnlyBucketPolicy(bucketName))

	// HTTP request for testing when `objectLayer` is set to `nil`.
	// There is no need to use an existing bucket and valid input for creating the request
	// since the `objectLayer==nil`  check is performed before any other checks inside the handlers.
	// The only aim is to generate an HTTP request in a way that the relevant/registered end point is evoked/called.

	nilBucket := "dummy-bucket"
	url = getListMultipartUploadsURLWithParams("", nilBucket, "dummy-prefix", testCases[6].keyMarker,
		testCases[6].uploadIDMarker, testCases[6].delimiter, testCases[6].maxUploads)

	nilReq, err := newTestRequest(http.MethodGet, url, 0, nil)
	if err != nil {
		t.Errorf("MinIO %s: Failed to create HTTP request for testing the response when object Layer is set to `nil`.", instanceType)
	}
	// execute the object layer set to `nil` test.
	// `ExecObjectLayerAPINilTest` manages the operation.
	ExecObjectLayerAPINilTest(t, nilBucket, "", instanceType, apiRouter, nilReq)
}

// Wrapper for calling TestListBucketsHandler tests for both Erasure multiple disks and single node setup.
func TestListBucketsHandler(t *testing.T) {
	ExecObjectLayerAPITest(ExecObjectLayerAPITestArgs{t: t, objAPITest: testListBucketsHandler, endpoints: []string{"ListBuckets"}})
}

// testListBucketsHandler - Tests validate listing of buckets.
func testListBucketsHandler(obj ObjectLayer, instanceType, bucketName string, apiRouter http.Handler,
	credentials auth.Credentials, t *testing.T,
) {
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
		// Test case with invalid accessKey to produce and validate Signature Mismatch error.
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
		req, lerr := newTestSignedRequestV4(http.MethodGet, getListBucketURL(""), 0, nil, testCase.accessKey, testCase.secretKey, nil)
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
		reqV2, err := newTestSignedRequestV2(http.MethodGet, getListBucketURL(""), 0, nil, testCase.accessKey, testCase.secretKey, nil)
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
	anonReq, err := newTestRequest(http.MethodGet, getListBucketURL(""), 0, nil)
	if err != nil {
		t.Fatalf("MinIO %s: Failed to create an anonymous request.", instanceType)
	}

	// ExecObjectLayerAPIAnonTest - Calls the HTTP API handler using the anonymous request, validates the ErrAccessDeniedResponse,
	// sets the bucket policy using the policy statement generated from `getWriteOnlyObjectStatement` so that the
	// unsigned request goes through and its validated again.
	ExecObjectLayerAPIAnonTest(t, obj, "ListBucketsHandler", "", "", instanceType, apiRouter, anonReq, getAnonWriteOnlyBucketPolicy("*"))

	// HTTP request for testing when `objectLayer` is set to `nil`.
	// There is no need to use an existing bucket and valid input for creating the request
	// since the `objectLayer==nil`  check is performed before any other checks inside the handlers.
	// The only aim is to generate an HTTP request in a way that the relevant/registered end point is evoked/called.

	nilReq, err := newTestRequest(http.MethodGet, getListBucketURL(""), 0, nil)
	if err != nil {
		t.Errorf("MinIO %s: Failed to create HTTP request for testing the response when object Layer is set to `nil`.", instanceType)
	}
	// execute the object layer set to `nil` test.
	// `ExecObjectLayerAPINilTest` manages the operation.
	ExecObjectLayerAPINilTest(t, "", "", instanceType, apiRouter, nilReq)
}

// Wrapper for calling DeleteMultipleObjects HTTP handler tests for both Erasure multiple disks and single node setup.
func TestAPIDeleteMultipleObjectsHandler(t *testing.T) {
	ExecObjectLayerAPITest(ExecObjectLayerAPITestArgs{t: t, objAPITest: testAPIDeleteMultipleObjectsHandler, endpoints: []string{"DeleteMultipleObjects", "PutBucketPolicy"}})
}

func testAPIDeleteMultipleObjectsHandler(obj ObjectLayer, instanceType, bucketName string, apiRouter http.Handler,
	credentials auth.Credentials, t *testing.T,
) {
	var err error

	sha256sum := ""
	var objectNames []string
	for i := range 10 {
		contentBytes := []byte("hello")
		objectName := "test-object-" + strconv.Itoa(i)
		if i == 0 {
			objectName += "/"
			contentBytes = []byte{}
		}
		// uploading the object.
		_, err = obj.PutObject(GlobalContext, bucketName, objectName, mustGetPutObjReader(t, bytes.NewReader(contentBytes), int64(len(contentBytes)), "", sha256sum), ObjectOptions{})
		// if object upload fails stop the test.
		if err != nil {
			t.Fatalf("Put Object %d:  Error uploading object: <ERROR> %v", i, err)
		}

		// object used for the test.
		objectNames = append(objectNames, objectName)
	}

	contentBytes := []byte("hello")
	for _, name := range []string{"private/object", "public/object"} {
		// Uploading the object with retention enabled
		_, err = obj.PutObject(GlobalContext, bucketName, name, mustGetPutObjReader(t, bytes.NewReader(contentBytes), int64(len(contentBytes)), "", sha256sum), ObjectOptions{})
		// if object upload fails stop the test.
		if err != nil {
			t.Fatalf("Put Object %s:  Error uploading object: <ERROR> %v", name, err)
		}
	}

	// The following block will create a bucket policy with delete object to 'public/*'. This is
	// to test a mixed response of a successful & failure while deleting objects in a single request
	policyBytes := fmt.Appendf(nil, `{"Id": "Policy1637752602639", "Version": "2012-10-17", "Statement": [{"Sid": "Stmt1637752600730", "Action": "s3:DeleteObject", "Effect": "Allow", "Resource": "arn:aws:s3:::%s/public/*", "Principal": "*"}]}`, bucketName)
	rec := httptest.NewRecorder()
	req, err := newTestSignedRequestV4(http.MethodPut, getPutPolicyURL("", bucketName), int64(len(policyBytes)), bytes.NewReader(policyBytes),
		credentials.AccessKey, credentials.SecretKey, nil)
	if err != nil {
		t.Fatalf("Failed to create HTTP request for PutBucketPolicyHandler: <ERROR> %v", err)
	}
	apiRouter.ServeHTTP(rec, req)
	if rec.Code != http.StatusNoContent {
		t.Errorf("Expected the response status to be `%d`, but instead found `%d`", 200, rec.Code)
	}

	getObjectToDeleteList := func(objectNames []string) (objectList []ObjectToDelete) {
		for _, objectName := range objectNames {
			objectList = append(objectList, ObjectToDelete{
				ObjectV: ObjectV{
					ObjectName: objectName,
				},
			})
		}

		return objectList
	}

	getDeleteErrorList := func(objects []ObjectToDelete) (deleteErrorList []DeleteError) {
		for _, obj := range objects {
			deleteErrorList = append(deleteErrorList, DeleteError{
				Code:    errorCodes[ErrAccessDenied].Code,
				Message: errorCodes[ErrAccessDenied].Description,
				Key:     obj.ObjectName,
			})
		}

		return deleteErrorList
	}

	objects := []ObjectToDelete{}
	objects = append(objects, ObjectToDelete{
		ObjectV: ObjectV{
			ObjectName: "private/object",
		},
	})
	objects = append(objects, ObjectToDelete{
		ObjectV: ObjectV{
			ObjectName: "public/object",
		},
	})
	requestList := []DeleteObjectsRequest{
		{Quiet: false, Objects: getObjectToDeleteList(objectNames[:5])},
		{Quiet: true, Objects: getObjectToDeleteList(objectNames[5:])},
		{Quiet: false, Objects: objects},
	}

	// generate multi objects delete response.
	successRequest0 := encodeResponse(requestList[0])

	deletedObjects := make([]DeletedObject, len(requestList[0].Objects))
	for i := range requestList[0].Objects {
		var vid string
		if isDirObject(requestList[0].Objects[i].ObjectName) {
			vid = ""
		}
		deletedObjects[i] = DeletedObject{
			ObjectName: requestList[0].Objects[i].ObjectName,
			VersionID:  vid,
		}
	}

	successResponse0 := generateMultiDeleteResponse(requestList[0].Quiet, deletedObjects, nil)
	encodedSuccessResponse0 := encodeResponse(successResponse0)

	successRequest1 := encodeResponse(requestList[1])

	deletedObjects = make([]DeletedObject, len(requestList[1].Objects))
	for i := range requestList[1].Objects {
		var vid string
		if isDirObject(requestList[0].Objects[i].ObjectName) {
			vid = ""
		}
		deletedObjects[i] = DeletedObject{
			ObjectName: requestList[1].Objects[i].ObjectName,
			VersionID:  vid,
		}
	}

	successResponse1 := generateMultiDeleteResponse(requestList[1].Quiet, deletedObjects, nil)
	encodedSuccessResponse1 := encodeResponse(successResponse1)

	// generate multi objects delete response for errors.
	// errorRequest := encodeResponse(requestList[1])
	errorResponse := generateMultiDeleteResponse(requestList[1].Quiet, deletedObjects, nil)
	encodedErrorResponse := encodeResponse(errorResponse)

	anonRequest := encodeResponse(requestList[0])
	anonResponse := generateMultiDeleteResponse(requestList[0].Quiet, nil, getDeleteErrorList(requestList[0].Objects))
	encodedAnonResponse := encodeResponse(anonResponse)

	anonRequestWithPartialPublicAccess := encodeResponse(requestList[2])
	anonResponseWithPartialPublicAccess := generateMultiDeleteResponse(requestList[2].Quiet,
		[]DeletedObject{
			{ObjectName: "public/object"},
		},
		[]DeleteError{
			{
				Code:    errorCodes[ErrAccessDenied].Code,
				Message: errorCodes[ErrAccessDenied].Description,
				Key:     "private/object",
			},
		})
	encodedAnonResponseWithPartialPublicAccess := encodeResponse(anonResponseWithPartialPublicAccess)

	testCases := []struct {
		bucket             string
		objects            []byte
		accessKey          string
		secretKey          string
		expectedContent    []byte
		expectedRespStatus int
	}{
		// Test case - 0.
		// Delete objects with invalid access key.
		0: {
			bucket:             bucketName,
			objects:            successRequest0,
			accessKey:          "Invalid-AccessID",
			secretKey:          credentials.SecretKey,
			expectedContent:    nil,
			expectedRespStatus: http.StatusForbidden,
		},
		// Test case - 1.
		// Delete valid objects with quiet flag off.
		1: {
			bucket:             bucketName,
			objects:            successRequest0,
			accessKey:          credentials.AccessKey,
			secretKey:          credentials.SecretKey,
			expectedContent:    encodedSuccessResponse0,
			expectedRespStatus: http.StatusOK,
		},
		// Test case - 2.
		// Delete deleted objects with quiet flag off.
		2: {
			bucket:             bucketName,
			objects:            successRequest0,
			accessKey:          credentials.AccessKey,
			secretKey:          credentials.SecretKey,
			expectedContent:    encodedSuccessResponse0,
			expectedRespStatus: http.StatusOK,
		},
		// Test case - 3.
		// Delete valid objects with quiet flag on.
		3: {
			bucket:             bucketName,
			objects:            successRequest1,
			accessKey:          credentials.AccessKey,
			secretKey:          credentials.SecretKey,
			expectedContent:    encodedSuccessResponse1,
			expectedRespStatus: http.StatusOK,
		},
		// Test case - 4.
		// Delete previously deleted objects.
		4: {
			bucket:             bucketName,
			objects:            successRequest1,
			accessKey:          credentials.AccessKey,
			secretKey:          credentials.SecretKey,
			expectedContent:    encodedErrorResponse,
			expectedRespStatus: http.StatusOK,
		},
		// Test case - 5.
		// Anonymous user access denied response
		// Currently anonymous users cannot delete multiple objects in MinIO server
		5: {
			bucket:             bucketName,
			objects:            anonRequest,
			accessKey:          "",
			secretKey:          "",
			expectedContent:    encodedAnonResponse,
			expectedRespStatus: http.StatusOK,
		},
		// Test case - 6.
		// Anonymous user has access to some public folder, issue removing with
		// another private object as well
		6: {
			bucket:             bucketName,
			objects:            anonRequestWithPartialPublicAccess,
			accessKey:          "",
			secretKey:          "",
			expectedContent:    encodedAnonResponseWithPartialPublicAccess,
			expectedRespStatus: http.StatusOK,
		},
		// Test case - 7.
		// Bucket does not exist.
		7: {
			bucket:             "unknown-bucket-name",
			objects:            successRequest0,
			accessKey:          credentials.AccessKey,
			secretKey:          credentials.SecretKey,
			expectedRespStatus: http.StatusNotFound,
		},
	}

	for i, testCase := range testCases {
		var req *http.Request
		var actualContent []byte

		// Generate a signed or anonymous request based on the testCase
		if testCase.accessKey != "" {
			req, err = newTestSignedRequestV4(http.MethodPost, getDeleteMultipleObjectsURL("", testCase.bucket),
				int64(len(testCase.objects)), bytes.NewReader(testCase.objects), testCase.accessKey, testCase.secretKey, nil)
		} else {
			req, err = newTestRequest(http.MethodPost, getDeleteMultipleObjectsURL("", testCase.bucket),
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
			t.Errorf("Test %d: MinIO %s: Expected the response status to be `%d`, but instead found `%d`", i, instanceType, testCase.expectedRespStatus, rec.Code)
		}

		// read the response body.
		actualContent, err = io.ReadAll(rec.Body)
		if err != nil {
			t.Fatalf("Test %d : MinIO %s: Failed parsing response body: <ERROR> %v", i, instanceType, err)
		}

		// Verify whether the bucket obtained object is same as the one created.
		if testCase.expectedContent != nil && !bytes.Equal(testCase.expectedContent, actualContent) {
			t.Log(string(testCase.expectedContent), string(actualContent))
			t.Errorf("Test %d : MinIO %s: Object content differs from expected value.", i, instanceType)
		}
	}

	// HTTP request to test the case of `objectLayer` being set to `nil`.
	// There is no need to use an existing bucket or valid input for creating the request,
	// since the `objectLayer==nil`  check is performed before any other checks inside the handlers.
	// The only aim is to generate an HTTP request in a way that the relevant/registered end point is evoked/called.
	// Indicating that all parts are uploaded and initiating completeMultipartUpload.
	nilBucket := "dummy-bucket"
	nilObject := ""

	nilReq, err := newTestSignedRequestV4(http.MethodPost, getDeleteMultipleObjectsURL("", nilBucket), 0, nil, "", "", nil)
	if err != nil {
		t.Errorf("MinIO %s: Failed to create HTTP request for testing the response when object Layer is set to `nil`.", instanceType)
	}
	// execute the object layer set to `nil` test.
	// `ExecObjectLayerAPINilTest` manages the operation.
	ExecObjectLayerAPINilTest(t, nilBucket, nilObject, instanceType, apiRouter, nilReq)
}

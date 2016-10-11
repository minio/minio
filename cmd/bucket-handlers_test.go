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
	"net/http"
	"net/http/httptest"
	"testing"
)

// Wrapper for calling GetBucketPolicy HTTP handler tests for both XL multiple disks and single node setup.
func TestGetBucketLocationHandler(t *testing.T) {
	ExecObjectLayerAPITest(t, testGetBucketLocationHandler, []string{"GetBucketLocation"})
}

func testGetBucketLocationHandler(obj ObjectLayer, instanceType, bucketName string, apiRouter http.Handler,
	credentials credential, t *testing.T) {
	initBucketPolicies(obj)

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
		// Tests for authenticated request and proper response.
		{
			bucketName,
			credentials.AccessKeyID,
			credentials.SecretAccessKey,
			http.StatusOK,
			[]byte(`<?xml version="1.0" encoding="UTF-8"?>
<LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></LocationConstraint>`),
			APIErrorResponse{},
			true,
		},
		// Tests for anonymous requests.
		{
			bucketName,
			"",
			"",
			http.StatusForbidden,
			[]byte(""),
			APIErrorResponse{
				Resource: "/" + bucketName + "/",
				Code:     "AccessDenied",
				Message:  "Access Denied.",
			},
			false,
		},
	}

	for i, testCase := range testCases {
		// initialize httptest Recorder, this records any mutations to response writer inside the handler.
		rec := httptest.NewRecorder()
		// construct HTTP request for Get bucket location.
		req, err := newTestSignedRequestV4("GET", getBucketLocationURL("", testCase.bucketName), 0, nil, testCase.accessKey, testCase.secretKey)
		if err != nil {
			t.Fatalf("Test %d: %s: Failed to create HTTP request for GetBucketLocationHandler: <ERROR> %v", i+1, instanceType, err)
		}
		// Since `apiRouter` satisfies `http.Handler` it has a ServeHTTP to execute the logic ofthe handler.
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
	ExecObjectLayerAPIAnonTest(t, "TestGetBucketLocationHandler", bucketName, "", instanceType, apiRouter, anonReq, getReadOnlyBucketStatement)

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
	credentials credential, t *testing.T) {
	initBucketPolicies(obj)

	// test cases with sample input and expected output.
	testCases := []struct {
		bucketName string
		accessKey  string
		secretKey  string
		// expected Response.
		expectedRespStatus int
	}{
		// Bucket exists.
		{
			bucketName:         bucketName,
			accessKey:          credentials.AccessKeyID,
			secretKey:          credentials.SecretAccessKey,
			expectedRespStatus: http.StatusOK,
		},
		// Non-existent bucket name.
		{
			bucketName:         "2333",
			accessKey:          credentials.AccessKeyID,
			secretKey:          credentials.SecretAccessKey,
			expectedRespStatus: http.StatusNotFound,
		},
		// Un-authenticated request.
		{
			bucketName:         bucketName,
			accessKey:          "",
			secretKey:          "",
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
		// Since `apiRouter` satisfies `http.Handler` it has a ServeHTTP to execute the logic ofthe handler.
		// Call the ServeHTTP to execute the handler.
		apiRouter.ServeHTTP(rec, req)
		if rec.Code != testCase.expectedRespStatus {
			t.Errorf("Test %d: %s: Expected the response status to be `%d`, but instead found `%d`", i+1, instanceType, testCase.expectedRespStatus, rec.Code)
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
	ExecObjectLayerAPIAnonTest(t, "TestHeadBucketHandler", bucketName, "", instanceType, apiRouter, anonReq, getReadOnlyBucketStatement)

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
	credentials credential, t *testing.T) {
	initBucketPolicies(obj)

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
		expectedRespStatus int
		shouldPass         bool
	}{
		// 1 - invalid bucket name.
		{".test", "", "", "", "", "0", http.StatusBadRequest, false},
		// 2 - bucket not found.
		{"volatile-bucket-1", "", "", "", "", "0", http.StatusNotFound, false},
		// 3 - invalid delimiter.
		{bucketName, "", "", "", "-", "0", http.StatusNotImplemented, false},
		// 4 - invalid prefix and marker combination.
		{bucketName, "asia", "europe-object", "", "", "0", http.StatusNotImplemented, false},
		// 5 - invalid upload id and marker combination.
		{bucketName, "asia", "asia/europe/", "abc", "", "0", http.StatusNotImplemented, false},
		// 6 - invalid max uploads.
		{bucketName, "", "", "", "", "-1", http.StatusBadRequest, false},
		// 7 - good case delimiter.
		{bucketName, "", "", "", "/", "100", http.StatusOK, true},
		// 8 - good case without delimiter.
		{bucketName, "", "", "", "", "100", http.StatusOK, true},
	}

	for i, testCase := range testCases {
		// initialize HTTP NewRecorder, this records any mutations to response writer inside the handler.
		rec := httptest.NewRecorder()

		// construct HTTP request for List multipart uploads endpoint.
		u := getListMultipartUploadsURLWithParams("", testCase.bucket, testCase.prefix, testCase.keyMarker, testCase.uploadIDMarker, testCase.delimiter, testCase.maxUploads)
		req, gerr := newTestSignedRequestV4("GET", u, 0, nil, credentials.AccessKeyID, credentials.SecretAccessKey)
		if gerr != nil {
			t.Fatalf("Test %d: %s: Failed to create HTTP request for ListMultipartUploadsHandler: <ERROR> %v", i+1, instanceType, gerr)
		}
		// Since `apiRouter` satisfies `http.Handler` it has a ServeHTTP to execute the logic ofthe handler.
		// Call the ServeHTTP to execute the handler.
		apiRouter.ServeHTTP(rec, req)
		if rec.Code != testCase.expectedRespStatus {
			t.Errorf("Test %d: %s: Expected the response status to be `%d`, but instead found `%d`", i+1, instanceType, testCase.expectedRespStatus, rec.Code)
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
	// Since `apiRouter` satisfies `http.Handler` it has a ServeHTTP to execute the logic ofthe handler.
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
	ExecObjectLayerAPIAnonTest(t, "TestListMultipartUploadsHandler", bucketName, "", instanceType, apiRouter, anonReq, getWriteOnlyBucketStatement)

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
	credentials credential, t *testing.T) {

	testCases := []struct {
		bucketName         string
		accessKey          string
		secretKey          string
		expectedRespStatus int
	}{
		// Validate a good case request succeeds.
		{
			bucketName:         bucketName,
			accessKey:          credentials.AccessKeyID,
			secretKey:          credentials.SecretAccessKey,
			expectedRespStatus: http.StatusOK,
		},
		// Validate a bad case request fails with http.StatusForbidden.
		{
			bucketName:         bucketName,
			accessKey:          "",
			secretKey:          "",
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
		// Since `apiRouter` satisfies `http.Handler` it has a ServeHTTP to execute the logic ofthe handler.
		// Call the ServeHTTP to execute the handler.
		apiRouter.ServeHTTP(rec, req)
		if rec.Code != testCase.expectedRespStatus {
			t.Errorf("Test %d: %s: Expected the response status to be `%d`, but instead found `%d`", i+1, instanceType, testCase.expectedRespStatus, rec.Code)
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
	ExecObjectLayerAPIAnonTest(t, "ListBucketsHandler", "", "", instanceType, apiRouter, anonReq, getWriteOnlyObjectStatement)

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

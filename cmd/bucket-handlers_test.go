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
	ExecObjectLayerTest(t, testGetBucketLocationHandler)
}

func testGetBucketLocationHandler(obj ObjectLayer, instanceType string, t TestErrHandler) {
	// get random bucket name.
	bucketName := getRandomBucketName()
	// Create bucket.
	err := obj.MakeBucket(bucketName)
	if err != nil {
		// failed to create newbucket, abort.
		t.Fatalf("%s : %s", instanceType, err)
	}
	// Register the API end points with XL/FS object layer.
	apiRouter := initTestAPIEndPoints(obj, []string{"GetBucketLocation"})
	// initialize the server and obtain the credentials and root.
	// credentials are necessary to sign the HTTP request.
	rootPath, err := newTestConfig("us-east-1")
	if err != nil {
		t.Fatalf("Init Test config failed")
	}
	// remove the root folder after the test ends.
	defer removeAll(rootPath)

	credentials := serverConfig.GetCredential()
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
		// initialize HTTP NewRecorder, this records any mutations to response writer inside the handler.
		rec := httptest.NewRecorder()
		// construct HTTP request for Get bucket location.
		req, err := newTestSignedRequest("GET", getBucketLocationURL("", testCase.bucketName), 0, nil, testCase.accessKey, testCase.secretKey)
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
}

// Wrapper for calling HeadBucket HTTP handler tests for both XL multiple disks and single node setup.
func TestHeadBucketHandler(t *testing.T) {
	ExecObjectLayerTest(t, testHeadBucketHandler)
}

func testHeadBucketHandler(obj ObjectLayer, instanceType string, t TestErrHandler) {
	// get random bucket name.
	bucketName := getRandomBucketName()
	// Create bucket.
	err := obj.MakeBucket(bucketName)
	if err != nil {
		// failed to create newbucket, abort.
		t.Fatalf("%s : %s", instanceType, err)
	}
	// Register the API end points with XL/FS object layer.
	apiRouter := initTestAPIEndPoints(obj, []string{"HeadBucket"})
	// initialize the server and obtain the credentials and root.
	// credentials are necessary to sign the HTTP request.
	rootPath, err := newTestConfig("us-east-1")
	if err != nil {
		t.Fatalf("Init Test config failed")
	}
	// remove the root folder after the test ends.
	defer removeAll(rootPath)

	credentials := serverConfig.GetCredential()
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
		req, err := newTestSignedRequest("HEAD", getHEADBucketURL("", testCase.bucketName), 0, nil, testCase.accessKey, testCase.secretKey)
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
}

// Wrapper for calling TestListMultipartUploadsHandler tests for both XL multiple disks and single node setup.
func TestListMultipartUploadsHandler(t *testing.T) {
	ExecObjectLayerTest(t, testListMultipartUploadsHandler)
}

// testListMultipartUploadsHandler - Tests validate listing of multipart uploads.
func testListMultipartUploadsHandler(obj ObjectLayer, instanceType string, t TestErrHandler) {
	// get random bucket name.
	bucketName := getRandomBucketName()

	// Register the API end points with XL/FS object layer.
	apiRouter := initTestAPIEndPoints(obj, []string{"ListMultipartUploads"})
	// initialize the server and obtain the credentials and root.
	// credentials are necessary to sign the HTTP request.
	rootPath, err := newTestConfig("us-east-1")
	if err != nil {
		t.Fatalf("Init Test config failed")
	}
	// remove the root folder after the test ends.
	defer removeAll(rootPath)

	credentials := serverConfig.GetCredential()

	// bucketnames[0].
	// objectNames[0].
	// uploadIds [0].
	// Create bucket before initiating NewMultipartUpload.
	err = obj.MakeBucket(bucketName)
	if err != nil {
		// Failed to create newbucket, abort.
		t.Fatalf("%s : %s", instanceType, err.Error())
	}

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
		req, gerr := newTestSignedRequest("GET", u, 0, nil, credentials.AccessKeyID, credentials.SecretAccessKey)
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
	req, err := newTestSignedRequest("GET", u, 0, nil, "", "") // Generate an anonymous request.
	if err != nil {
		t.Fatalf("Test %s: Failed to create HTTP request for ListMultipartUploadsHandler: <ERROR> %v", instanceType, err)
	}
	// Since `apiRouter` satisfies `http.Handler` it has a ServeHTTP to execute the logic ofthe handler.
	// Call the ServeHTTP to execute the handler.
	apiRouter.ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Errorf("Test %s: Expected the response status to be `http.StatusForbidden`, but instead found `%d`", instanceType, rec.Code)
	}
}

// Wrapper for calling TestListBucketsHandler tests for both XL multiple disks and single node setup.
func TestListBucketsHandler(t *testing.T) {
	ExecObjectLayerTest(t, testListBuckets)
}

// testListBucketsHandler - Tests validate listing of buckets.
func testListBucketsHandler(obj ObjectLayer, instanceType string, t TestErrHandler) {
	// get random bucket name.
	bucketName := getRandomBucketName()

	// Register the API end points with XL/FS object layer.
	apiRouter := initTestAPIEndPoints(obj, []string{"ListBuckets"})
	// initialize the server and obtain the credentials and root.
	// credentials are necessary to sign the HTTP request.
	rootPath, err := newTestConfig("us-east-1")
	if err != nil {
		t.Fatalf("Init Test config failed")
	}
	// remove the root folder after the test ends.
	defer removeAll(rootPath)

	credentials := serverConfig.GetCredential()

	// bucketnames[0].
	// objectNames[0].
	// uploadIds [0].
	// Create bucket before initiating NewMultipartUpload.
	err = obj.MakeBucket(bucketName)
	if err != nil {
		// Failed to create newbucket, abort.
		t.Fatalf("%s : %s", instanceType, err.Error())
	}

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
		req, lerr := newTestSignedRequest("GET", getListBucketURL(""), 0, nil, testCase.accessKey, testCase.secretKey)
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
}

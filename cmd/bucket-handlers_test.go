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
	initBucketPolicies(obj)

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
		// construct HTTP request for PUT bucket policy endpoint.
		req, err := newTestSignedRequest("GET", getBucketLocationURL("", testCase.bucketName), 0, nil, testCase.accessKey, testCase.secretKey)
		if err != nil {
			t.Fatalf("Test %d: %s: Failed to create HTTP request for PutBucketPolicyHandler: <ERROR> %v", i+1, instanceType, err)
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
	initBucketPolicies(obj)

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
		// construct HTTP request for PUT bucket policy endpoint.
		req, err := newTestSignedRequest("HEAD", getHEADBucketURL("", testCase.bucketName), 0, nil, testCase.accessKey, testCase.secretKey)
		if err != nil {
			t.Fatalf("Test %d: %s: Failed to create HTTP request for PutBucketPolicyHandler: <ERROR> %v", i+1, instanceType, err)
		}
		// Since `apiRouter` satisfies `http.Handler` it has a ServeHTTP to execute the logic ofthe handler.
		// Call the ServeHTTP to execute the handler.
		apiRouter.ServeHTTP(rec, req)
		if rec.Code != testCase.expectedRespStatus {
			t.Errorf("Test %d: %s: Expected the response status to be `%d`, but instead found `%d`", i+1, instanceType, testCase.expectedRespStatus, rec.Code)
		}
	}
}

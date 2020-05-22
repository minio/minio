/*
 * MinIO Cloud Storage, (C) 2020 MinIO, Inc.
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

	"github.com/minio/minio/pkg/auth"
)

// Test S3 Bucket lifecycle APIs with wrong credentials
func TestBucketLifecycleWrongCredentials(t *testing.T) {
	ExecObjectLayerAPITest(t, testBucketLifecycleHandlersWrongCredentials, []string{"GetBucketLifecycle", "PutBucketLifecycle", "DeleteBucketLifecycle"})
}

// Test for authentication
func testBucketLifecycleHandlersWrongCredentials(obj ObjectLayer, instanceType, bucketName string, apiRouter http.Handler,
	credentials auth.Credentials, t *testing.T) {
	// test cases with sample input and expected output.
	testCases := []struct {
		method     string
		bucketName string
		accessKey  string
		secretKey  string
		// Sent body
		body []byte
		// Expected response
		expectedRespStatus int
		lifecycleResponse  []byte
		errorResponse      APIErrorResponse
		shouldPass         bool
	}{
		// GET empty credentials
		{
			method: "GET", bucketName: bucketName,
			accessKey:          "",
			secretKey:          "",
			expectedRespStatus: http.StatusForbidden,
			lifecycleResponse:  []byte(""),
			errorResponse: APIErrorResponse{
				Resource: SlashSeparator + bucketName + SlashSeparator,
				Code:     "AccessDenied",
				Message:  "Access Denied.",
			},
			shouldPass: false,
		},
		// GET wrong credentials
		{
			method: "GET", bucketName: bucketName,
			accessKey:          "abcd",
			secretKey:          "abcd",
			expectedRespStatus: http.StatusForbidden,
			lifecycleResponse:  []byte(""),
			errorResponse: APIErrorResponse{
				Resource: SlashSeparator + bucketName + SlashSeparator,
				Code:     "InvalidAccessKeyId",
				Message:  "The Access Key Id you provided does not exist in our records.",
			},
			shouldPass: false,
		},
		// PUT empty credentials
		{
			method:             "PUT",
			bucketName:         bucketName,
			accessKey:          "",
			secretKey:          "",
			expectedRespStatus: http.StatusForbidden,
			lifecycleResponse:  []byte(""),
			errorResponse: APIErrorResponse{
				Resource: SlashSeparator + bucketName + SlashSeparator,
				Code:     "AccessDenied",
				Message:  "Access Denied.",
			},
			shouldPass: false,
		},
		// PUT wrong credentials
		{
			method:             "PUT",
			bucketName:         bucketName,
			accessKey:          "abcd",
			secretKey:          "abcd",
			expectedRespStatus: http.StatusForbidden,
			lifecycleResponse:  []byte(""),
			errorResponse: APIErrorResponse{
				Resource: SlashSeparator + bucketName + SlashSeparator,
				Code:     "InvalidAccessKeyId",
				Message:  "The Access Key Id you provided does not exist in our records.",
			},
			shouldPass: false,
		},
		// DELETE empty credentials
		{
			method:             "DELETE",
			bucketName:         bucketName,
			accessKey:          "",
			secretKey:          "",
			expectedRespStatus: http.StatusForbidden,
			lifecycleResponse:  []byte(""),
			errorResponse: APIErrorResponse{
				Resource: SlashSeparator + bucketName + SlashSeparator,
				Code:     "AccessDenied",
				Message:  "Access Denied.",
			},
			shouldPass: false,
		},
		// DELETE wrong credentials
		{
			method:             "DELETE",
			bucketName:         bucketName,
			accessKey:          "abcd",
			secretKey:          "abcd",
			expectedRespStatus: http.StatusForbidden,
			lifecycleResponse:  []byte(""),
			errorResponse: APIErrorResponse{
				Resource: SlashSeparator + bucketName + SlashSeparator,
				Code:     "InvalidAccessKeyId",
				Message:  "The Access Key Id you provided does not exist in our records.",
			},
			shouldPass: false,
		},
	}

	testBucketLifecycle(obj, instanceType, bucketName, apiRouter, t, testCases)
}

// Test S3 Bucket lifecycle APIs
func TestBucketLifecycle(t *testing.T) {
	ExecObjectLayerAPITest(t, testBucketLifecycleHandlers, []string{"GetBucketLifecycle", "PutBucketLifecycle", "DeleteBucketLifecycle"})
}

// Simple tests of bucket lifecycle: PUT, GET, DELETE.
// Tests are related and the order is important.
func testBucketLifecycleHandlers(obj ObjectLayer, instanceType, bucketName string, apiRouter http.Handler,
	creds auth.Credentials, t *testing.T) {

	// test cases with sample input and expected output.
	testCases := []struct {
		method     string
		bucketName string
		accessKey  string
		secretKey  string
		// Sent body
		body []byte
		// Expected response
		expectedRespStatus int
		lifecycleResponse  []byte
		errorResponse      APIErrorResponse
		shouldPass         bool
	}{
		// Test case - 1.
		// Filter contains more than (Prefix,Tag,And) rule
		{
			method:             "PUT",
			bucketName:         bucketName,
			accessKey:          creds.AccessKey,
			secretKey:          creds.SecretKey,
			body:               []byte(`<LifecycleConfiguration><Rule><ID>id</ID><Filter><Prefix>logs/</Prefix><Tag><Key>Key1</Key><Value>Value1</Value></Tag></Filter><Status>Enabled</Status><Expiration><Days>365</Days></Expiration></Rule></LifecycleConfiguration>`),
			expectedRespStatus: http.StatusBadRequest,
			lifecycleResponse:  []byte(``),
			errorResponse: APIErrorResponse{
				Resource: SlashSeparator + bucketName + SlashSeparator,
				Code:     "InvalidRequest",
				Message:  "Filter must have exactly one of Prefix, Tag, or And specified",
			},

			shouldPass: false,
		},
		// Date contains wrong format
		{
			method:             "PUT",
			bucketName:         bucketName,
			accessKey:          creds.AccessKey,
			secretKey:          creds.SecretKey,
			body:               []byte(`<LifecycleConfiguration><Rule><ID>id</ID><Filter><Prefix>logs/</Prefix><Tag><Key>Key1</Key><Value>Value1</Value></Tag></Filter><Status>Enabled</Status><Expiration><Date>365</Date></Expiration></Rule></LifecycleConfiguration>`),
			expectedRespStatus: http.StatusBadRequest,
			lifecycleResponse:  []byte(``),
			errorResponse: APIErrorResponse{
				Resource: SlashSeparator + bucketName + SlashSeparator,
				Code:     "InvalidRequest",
				Message:  "Date must be provided in ISO 8601 format",
			},

			shouldPass: false,
		},
		{
			method:             "PUT",
			bucketName:         bucketName,
			accessKey:          creds.AccessKey,
			secretKey:          creds.SecretKey,
			body:               []byte(`<?xml version="1.0" encoding="UTF-8"?><LifecycleConfiguration><Rule><ID>id</ID><Filter><Prefix>logs/</Prefix></Filter><Status>Enabled</Status><Expiration><Days>365</Days></Expiration></Rule></LifecycleConfiguration>`),
			expectedRespStatus: http.StatusOK,
			lifecycleResponse:  []byte(``),
			errorResponse:      APIErrorResponse{},
			shouldPass:         true,
		},
		{
			method:             "GET",
			accessKey:          creds.AccessKey,
			secretKey:          creds.SecretKey,
			bucketName:         bucketName,
			body:               []byte(``),
			expectedRespStatus: http.StatusOK,
			lifecycleResponse:  []byte(`<LifecycleConfiguration><Rule><ID>id</ID><Status>Enabled</Status><Filter><Prefix>logs/</Prefix></Filter><Expiration><Days>365</Days></Expiration></Rule></LifecycleConfiguration>`),
			errorResponse:      APIErrorResponse{},
			shouldPass:         true,
		},
		{
			method:             "DELETE",
			accessKey:          creds.AccessKey,
			secretKey:          creds.SecretKey,
			bucketName:         bucketName,
			body:               []byte(``),
			expectedRespStatus: http.StatusNoContent,
			lifecycleResponse:  []byte(``),
			errorResponse:      APIErrorResponse{},
			shouldPass:         true,
		},
		{
			method:             "GET",
			accessKey:          creds.AccessKey,
			secretKey:          creds.SecretKey,
			bucketName:         bucketName,
			body:               []byte(``),
			expectedRespStatus: http.StatusNotFound,
			lifecycleResponse:  []byte(``),
			errorResponse: APIErrorResponse{
				Resource: SlashSeparator + bucketName + SlashSeparator,
				Code:     "NoSuchLifecycleConfiguration",
				Message:  "The lifecycle configuration does not exist",
			},
			shouldPass: false,
		},
	}

	testBucketLifecycle(obj, instanceType, bucketName, apiRouter, t, testCases)
}

// testBucketLifecycle is a generic testing of lifecycle requests
func testBucketLifecycle(obj ObjectLayer, instanceType, bucketName string, apiRouter http.Handler,
	t *testing.T, testCases []struct {
		method             string
		bucketName         string
		accessKey          string
		secretKey          string
		body               []byte
		expectedRespStatus int
		lifecycleResponse  []byte
		errorResponse      APIErrorResponse
		shouldPass         bool
	}) {

	for i, testCase := range testCases {
		// initialize httptest Recorder, this records any mutations to response writer inside the handler.
		rec := httptest.NewRecorder()
		// construct HTTP request
		req, err := newTestSignedRequestV4(testCase.method, getBucketLifecycleURL("", testCase.bucketName),
			int64(len(testCase.body)), bytes.NewReader(testCase.body), testCase.accessKey, testCase.secretKey, nil)
		if err != nil {
			t.Fatalf("Test %d: %s: Failed to create HTTP request for GetBucketLocationHandler: <ERROR> %v", i+1, instanceType, err)
		}
		// Since `apiRouter` satisfies `http.Handler` it has a ServeHTTP to execute the logic of the handler.
		// Call the ServeHTTP to execute the handler.
		apiRouter.ServeHTTP(rec, req)
		if rec.Code != testCase.expectedRespStatus {
			t.Errorf("Test %d: %s: Expected the response status to be `%d`, but instead found `%d`", i+1, instanceType, testCase.expectedRespStatus, rec.Code)
		}
		if testCase.shouldPass && !bytes.Equal(testCase.lifecycleResponse, rec.Body.Bytes()) {
			t.Errorf("Test %d: %s: Expected the response to be `%s`, but instead found `%s`", i+1, instanceType, string(testCase.lifecycleResponse), rec.Body.String())
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
	}
}

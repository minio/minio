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
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/minio/minio/internal/auth"
)

// Test S3 Bucket lifecycle APIs with wrong credentials
func TestBucketLifecycleWrongCredentials(t *testing.T) {
	ExecObjectLayerAPITest(ExecObjectLayerAPITestArgs{t: t, objAPITest: testBucketLifecycleHandlersWrongCredentials, endpoints: []string{"GetBucketLifecycle", "PutBucketLifecycle", "DeleteBucketLifecycle"}})
}

// Test for authentication
func testBucketLifecycleHandlersWrongCredentials(obj ObjectLayer, instanceType, bucketName string, apiRouter http.Handler,
	credentials auth.Credentials, t *testing.T,
) {
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
			method: http.MethodGet, bucketName: bucketName,
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
			method: http.MethodGet, bucketName: bucketName,
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
			method:             http.MethodPut,
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
			method:             http.MethodPut,
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
			method:             http.MethodDelete,
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
			method:             http.MethodDelete,
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
	ExecObjectLayerAPITest(ExecObjectLayerAPITestArgs{t: t, objAPITest: testBucketLifecycleHandlers, endpoints: []string{"GetBucketLifecycle", "PutBucketLifecycle", "DeleteBucketLifecycle"}})
}

// Simple tests of bucket lifecycle: PUT, GET, DELETE.
// Tests are related and the order is important.
func testBucketLifecycleHandlers(obj ObjectLayer, instanceType, bucketName string, apiRouter http.Handler,
	creds auth.Credentials, t *testing.T,
) {
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
			method:             http.MethodPut,
			bucketName:         bucketName,
			accessKey:          creds.AccessKey,
			secretKey:          creds.SecretKey,
			body:               []byte(`<LifecycleConfiguration><Rule><ID>id</ID><Filter><Prefix>logs/</Prefix><Tag><Key>Key1</Key><Value>Value1</Value></Tag></Filter><Status>Enabled</Status><Expiration><Days>365</Days></Expiration></Rule></LifecycleConfiguration>`),
			expectedRespStatus: http.StatusBadRequest,
			lifecycleResponse:  []byte(``),
			errorResponse: APIErrorResponse{
				Resource: SlashSeparator + bucketName + SlashSeparator,
				Code:     "InvalidArgument",
				Message:  "Filter must have exactly one of Prefix, Tag, or And specified",
			},

			shouldPass: false,
		},
		// Date contains wrong format
		{
			method:             http.MethodPut,
			bucketName:         bucketName,
			accessKey:          creds.AccessKey,
			secretKey:          creds.SecretKey,
			body:               []byte(`<LifecycleConfiguration><Rule><ID>id</ID><Filter><Prefix>logs/</Prefix><Tag><Key>Key1</Key><Value>Value1</Value></Tag></Filter><Status>Enabled</Status><Expiration><Date>365</Date></Expiration></Rule></LifecycleConfiguration>`),
			expectedRespStatus: http.StatusBadRequest,
			lifecycleResponse:  []byte(``),
			errorResponse: APIErrorResponse{
				Resource: SlashSeparator + bucketName + SlashSeparator,
				Code:     "InvalidArgument",
				Message:  "Date must be provided in ISO 8601 format",
			},

			shouldPass: false,
		},
		{
			method:             http.MethodPut,
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
			method:             http.MethodGet,
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
			method:             http.MethodDelete,
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
			method:             http.MethodGet,
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
	},
) {
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

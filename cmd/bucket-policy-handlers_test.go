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
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/minio/minio/internal/auth"
	"github.com/minio/pkg/v3/policy"
	"github.com/minio/pkg/v3/policy/condition"
)

func getAnonReadOnlyBucketPolicy(bucketName string) *policy.BucketPolicy {
	return &policy.BucketPolicy{
		Version: policy.DefaultVersion,
		Statements: []policy.BPStatement{
			policy.NewBPStatement(
				"",
				policy.Allow,
				policy.NewPrincipal("*"),
				policy.NewActionSet(policy.GetBucketLocationAction, policy.ListBucketAction),
				policy.NewResourceSet(policy.NewResource(bucketName)),
				condition.NewFunctions(),
			),
		},
	}
}

func getAnonWriteOnlyBucketPolicy(bucketName string) *policy.BucketPolicy {
	return &policy.BucketPolicy{
		Version: policy.DefaultVersion,
		Statements: []policy.BPStatement{
			policy.NewBPStatement(
				"",
				policy.Allow,
				policy.NewPrincipal("*"),
				policy.NewActionSet(
					policy.GetBucketLocationAction,
					policy.ListBucketMultipartUploadsAction,
				),
				policy.NewResourceSet(policy.NewResource(bucketName)),
				condition.NewFunctions(),
			),
		},
	}
}

func getAnonReadOnlyObjectPolicy(bucketName, prefix string) *policy.BucketPolicy {
	return &policy.BucketPolicy{
		Version: policy.DefaultVersion,
		Statements: []policy.BPStatement{
			policy.NewBPStatement(
				"",
				policy.Allow,
				policy.NewPrincipal("*"),
				policy.NewActionSet(policy.GetObjectAction),
				policy.NewResourceSet(policy.NewResource(bucketName+"/"+prefix)),
				condition.NewFunctions(),
			),
		},
	}
}

func getAnonWriteOnlyObjectPolicy(bucketName, prefix string) *policy.BucketPolicy {
	return &policy.BucketPolicy{
		Version: policy.DefaultVersion,
		Statements: []policy.BPStatement{
			policy.NewBPStatement(
				"",
				policy.Allow,
				policy.NewPrincipal("*"),
				policy.NewActionSet(
					policy.AbortMultipartUploadAction,
					policy.DeleteObjectAction,
					policy.ListMultipartUploadPartsAction,
					policy.PutObjectAction,
				),
				policy.NewResourceSet(policy.NewResource(bucketName+"/"+prefix)),
				condition.NewFunctions(),
			),
		},
	}
}

// Wrapper for calling Create Bucket and ensure we get one and only one success.
func TestCreateBucket(t *testing.T) {
	ExecObjectLayerAPITest(ExecObjectLayerAPITestArgs{t: t, objAPITest: testCreateBucket, endpoints: []string{"MakeBucket"}})
}

// testCreateBucket - Test for calling Create Bucket and ensure we get one and only one success.
func testCreateBucket(obj ObjectLayer, instanceType, bucketName string, apiRouter http.Handler,
	credentials auth.Credentials, t *testing.T,
) {
	bucketName1 := fmt.Sprintf("%s-1", bucketName)

	const n = 100
	start := make(chan struct{})
	var ok, errs int
	var wg sync.WaitGroup
	var mu sync.Mutex
	wg.Add(n)
	for range n {
		go func() {
			defer wg.Done()
			// Sync start.
			<-start
			if err := obj.MakeBucket(GlobalContext, bucketName1, MakeBucketOptions{}); err != nil {
				if _, ok := err.(BucketExists); !ok {
					t.Logf("unexpected error: %T: %v", err, err)
					return
				}
				mu.Lock()
				errs++
				mu.Unlock()
				return
			}
			mu.Lock()
			ok++
			mu.Unlock()
		}()
	}
	close(start)
	wg.Wait()
	if ok != 1 {
		t.Fatalf("want 1 ok, got %d", ok)
	}
	if errs != n-1 {
		t.Fatalf("want %d errors, got %d", n-1, errs)
	}
}

// Wrapper for calling Put Bucket Policy HTTP handler tests for both Erasure multiple disks and single node setup.
func TestPutBucketPolicyHandler(t *testing.T) {
	ExecObjectLayerAPITest(ExecObjectLayerAPITestArgs{t: t, objAPITest: testPutBucketPolicyHandler, endpoints: []string{"PutBucketPolicy"}})
}

// testPutBucketPolicyHandler - Test for Bucket policy end point.
func testPutBucketPolicyHandler(obj ObjectLayer, instanceType, bucketName string, apiRouter http.Handler,
	credentials auth.Credentials, t *testing.T,
) {
	bucketName1 := fmt.Sprintf("%s-1", bucketName)
	if err := obj.MakeBucket(GlobalContext, bucketName1, MakeBucketOptions{}); err != nil {
		t.Fatal(err)
	}

	// template for constructing HTTP request body for PUT bucket policy.
	bucketPolicyTemplate := `{"Version":"2012-10-17","Statement":[{"Sid":"","Effect":"Allow","Principal":{"AWS":["*"]},"Action":["s3:GetBucketLocation","s3:ListBucket"],"Resource":["arn:aws:s3:::%s"]},{"Sid":"","Effect":"Allow","Principal":{"AWS":["*"]},"Action":["s3:GetObject"],"Resource":["arn:aws:s3:::%s/this*"]}]}`

	bucketPolicyTemplateWithoutVersion := `{"Version":"","Statement":[{"Sid":"","Effect":"Allow","Principal":{"AWS":["*"]},"Action":["s3:GetBucketLocation","s3:ListBucket"],"Resource":["arn:aws:s3:::%s"]},{"Sid":"","Effect":"Allow","Principal":{"AWS":["*"]},"Action":["s3:GetObject"],"Resource":["arn:aws:s3:::%s/this*"]}]}`

	// test cases with sample input and expected output.
	testCases := []struct {
		bucketName string
		// bucket policy to be set,
		// set as request body.
		bucketPolicyReader io.ReadSeeker
		// length in bytes of the bucket policy being set.
		policyLen int
		accessKey string
		secretKey string
		// expected Response.
		expectedRespStatus int
	}{
		// Test case - 1.
		{
			bucketName:         bucketName,
			bucketPolicyReader: bytes.NewReader(fmt.Appendf(nil, bucketPolicyTemplate, bucketName, bucketName)),

			policyLen:          len(fmt.Sprintf(bucketPolicyTemplate, bucketName, bucketName)),
			accessKey:          credentials.AccessKey,
			secretKey:          credentials.SecretKey,
			expectedRespStatus: http.StatusNoContent,
		},
		// Test case - 2.
		// Setting the content length to be more than max allowed size.
		// Expecting StatusBadRequest (400).
		{
			bucketName:         bucketName,
			bucketPolicyReader: bytes.NewReader(fmt.Appendf(nil, bucketPolicyTemplate, bucketName, bucketName)),

			policyLen:          maxBucketPolicySize + 1,
			accessKey:          credentials.AccessKey,
			secretKey:          credentials.SecretKey,
			expectedRespStatus: http.StatusBadRequest,
		},
		// Test case - 3.
		// Case with content-length of the HTTP request set to 0.
		// Expecting the HTTP response status to be StatusLengthRequired (411).
		{
			bucketName:         bucketName,
			bucketPolicyReader: bytes.NewReader(fmt.Appendf(nil, bucketPolicyTemplate, bucketName, bucketName)),

			policyLen:          0,
			accessKey:          credentials.AccessKey,
			secretKey:          credentials.SecretKey,
			expectedRespStatus: http.StatusLengthRequired,
		},
		// Test case - 4.
		// setting the readSeeker to `nil`, bucket policy parser will fail.
		{
			bucketName:         bucketName,
			bucketPolicyReader: nil,

			policyLen:          10,
			accessKey:          credentials.AccessKey,
			secretKey:          credentials.SecretKey,
			expectedRespStatus: http.StatusBadRequest,
		},
		// Test case - 5.
		// setting the keys to be empty.
		// Expecting statusForbidden.
		{
			bucketName:         bucketName,
			bucketPolicyReader: nil,

			policyLen:          10,
			accessKey:          "",
			secretKey:          "",
			expectedRespStatus: http.StatusForbidden,
		},
		// Test case - 6.
		// setting an invalid bucket policy.
		// the bucket policy parser will fail.
		{
			bucketName:         bucketName,
			bucketPolicyReader: bytes.NewReader([]byte("dummy-policy")),

			policyLen:          len([]byte("dummy-policy")),
			accessKey:          credentials.AccessKey,
			secretKey:          credentials.SecretKey,
			expectedRespStatus: http.StatusBadRequest,
		},
		// Test case - 7.
		// Different bucket name used in the HTTP request and the policy string.
		// checkBucketPolicyResources should fail.
		{
			bucketName:         bucketName1,
			bucketPolicyReader: bytes.NewReader(fmt.Appendf(nil, bucketPolicyTemplate, bucketName, bucketName)),

			policyLen:          len(fmt.Sprintf(bucketPolicyTemplate, bucketName, bucketName)),
			accessKey:          credentials.AccessKey,
			secretKey:          credentials.SecretKey,
			expectedRespStatus: http.StatusBadRequest,
		},
		// Test case - 8.
		// non-existent bucket is used.
		// writing BucketPolicy should fail.
		// should result in 404 StatusNotFound
		{
			bucketName:         "non-existent-bucket",
			bucketPolicyReader: bytes.NewReader(fmt.Appendf(nil, bucketPolicyTemplate, "non-existent-bucket", "non-existent-bucket")),

			policyLen:          len(fmt.Sprintf(bucketPolicyTemplate, bucketName, bucketName)),
			accessKey:          credentials.AccessKey,
			secretKey:          credentials.SecretKey,
			expectedRespStatus: http.StatusNotFound,
		},
		// Test case - 9.
		// non-existent bucket is used (with invalid bucket name)
		// writing BucketPolicy should fail.
		// should result in 404 StatusNotFound
		{
			bucketName:         ".invalid-bucket",
			bucketPolicyReader: bytes.NewReader(fmt.Appendf(nil, bucketPolicyTemplate, ".invalid-bucket", ".invalid-bucket")),

			policyLen:          len(fmt.Sprintf(bucketPolicyTemplate, bucketName, bucketName)),
			accessKey:          credentials.AccessKey,
			secretKey:          credentials.SecretKey,
			expectedRespStatus: http.StatusBadRequest,
		},
		// Test case - 10.
		// Existent bucket with policy with Version field empty.
		// writing BucketPolicy should fail.
		// should result in 400 StatusBadRequest.
		{
			bucketName:         bucketName,
			bucketPolicyReader: bytes.NewReader(fmt.Appendf(nil, bucketPolicyTemplateWithoutVersion, bucketName, bucketName)),

			policyLen:          len(fmt.Sprintf(bucketPolicyTemplateWithoutVersion, bucketName, bucketName)),
			accessKey:          credentials.AccessKey,
			secretKey:          credentials.SecretKey,
			expectedRespStatus: http.StatusBadRequest,
		},
	}

	// Iterating over the test cases, calling the function under test and asserting the response.
	for i, testCase := range testCases {
		// obtain the put bucket policy request body.
		// initialize HTTP NewRecorder, this records any mutations to response writer inside the handler.
		recV4 := httptest.NewRecorder()
		// construct HTTP request for PUT bucket policy endpoint.
		reqV4, err := newTestSignedRequestV4(http.MethodPut, getPutPolicyURL("", testCase.bucketName),
			int64(testCase.policyLen), testCase.bucketPolicyReader, testCase.accessKey, testCase.secretKey, nil)
		if err != nil {
			t.Fatalf("Test %d: %s: Failed to create HTTP request for PutBucketPolicyHandler: <ERROR> %v", i+1, instanceType, err)
		}
		// Since `apiRouter` satisfies `http.Handler` it has a ServeHTTP to execute the logic of the handler.
		// Call the ServeHTTP to execute the handler.
		apiRouter.ServeHTTP(recV4, reqV4)
		if recV4.Code != testCase.expectedRespStatus {
			t.Errorf("Test %d: %s: Expected the response status to be `%d`, but instead found `%d`", i+1, instanceType, testCase.expectedRespStatus, recV4.Code)
		}
		// initialize HTTP NewRecorder, this records any mutations to response writer inside the handler.
		recV2 := httptest.NewRecorder()
		// construct HTTP request for PUT bucket policy endpoint.
		reqV2, err := newTestSignedRequestV2(http.MethodPut, getPutPolicyURL("", testCase.bucketName),
			int64(testCase.policyLen), testCase.bucketPolicyReader, testCase.accessKey, testCase.secretKey, nil)
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
	// Bucket policy related functions doesn't support anonymous requests, setting policies shouldn't make a difference.
	bucketPolicyStr := fmt.Sprintf(bucketPolicyTemplate, bucketName, bucketName)
	// create unsigned HTTP request for PutBucketPolicyHandler.
	anonReq, err := newTestRequest(http.MethodPut, getPutPolicyURL("", bucketName),
		int64(len(bucketPolicyStr)), bytes.NewReader([]byte(bucketPolicyStr)))
	if err != nil {
		t.Fatalf("MinIO %s: Failed to create an anonymous request for bucket \"%s\": <ERROR> %v",
			instanceType, bucketName, err)
	}

	// ExecObjectLayerAPIAnonTest - Calls the HTTP API handler using the anonymous request, validates the ErrAccessDeniedResponse,
	// sets the bucket policy using the policy statement generated from `getWriteOnlyObjectStatement` so that the
	// unsigned request goes through and its validated again.
	ExecObjectLayerAPIAnonTest(t, obj, "PutBucketPolicyHandler", bucketName, "", instanceType, apiRouter, anonReq, getAnonWriteOnlyBucketPolicy(bucketName))

	// HTTP request for testing when `objectLayer` is set to `nil`.
	// There is no need to use an existing bucket and valid input for creating the request
	// since the `objectLayer==nil`  check is performed before any other checks inside the handlers.
	// The only aim is to generate an HTTP request in a way that the relevant/registered end point is evoked/called.
	nilBucket := "dummy-bucket"

	nilReq, err := newTestSignedRequestV4(http.MethodPut, getPutPolicyURL("", nilBucket),
		0, nil, "", "", nil)
	if err != nil {
		t.Errorf("MinIO %s: Failed to create HTTP request for testing the response when object Layer is set to `nil`.", instanceType)
	}
	// execute the object layer set to `nil` test.
	// `ExecObjectLayerAPINilTest` manages the operation.
	ExecObjectLayerAPINilTest(t, nilBucket, "", instanceType, apiRouter, nilReq)
}

// Wrapper for calling Get Bucket Policy HTTP handler tests for both Erasure multiple disks and single node setup.
func TestGetBucketPolicyHandler(t *testing.T) {
	ExecObjectLayerAPITest(ExecObjectLayerAPITestArgs{t: t, objAPITest: testGetBucketPolicyHandler, endpoints: []string{"PutBucketPolicy", "GetBucketPolicy"}})
}

// testGetBucketPolicyHandler - Test for end point which fetches the access policy json of the given bucket.
func testGetBucketPolicyHandler(obj ObjectLayer, instanceType, bucketName string, apiRouter http.Handler,
	credentials auth.Credentials, t *testing.T,
) {
	// template for constructing HTTP request body for PUT bucket policy.
	bucketPolicyTemplate := `{"Version":"2012-10-17","Statement":[{"Action":["s3:GetBucketLocation","s3:ListBucket"],"Effect":"Allow","Principal":{"AWS":["*"]},"Resource":["arn:aws:s3:::%s"]},{"Action":["s3:GetObject"],"Effect":"Allow","Principal":{"AWS":["*"]},"Resource":["arn:aws:s3:::%s/this*"]}]}`

	// Writing bucket policy before running test on GetBucketPolicy.
	putTestPolicies := []struct {
		bucketName string
		accessKey  string
		secretKey  string
		// expected Response.
		expectedRespStatus int
	}{
		{bucketName, credentials.AccessKey, credentials.SecretKey, http.StatusNoContent},
	}

	// Iterating over the cases and writing the bucket policy.
	// its required to write the policies first before running tests on GetBucketPolicy.
	for i, testPolicy := range putTestPolicies {
		// obtain the put bucket policy request body.
		bucketPolicyStr := fmt.Sprintf(bucketPolicyTemplate, testPolicy.bucketName, testPolicy.bucketName)
		// initialize HTTP NewRecorder, this records any mutations to response writer inside the handler.
		recV4 := httptest.NewRecorder()
		// construct HTTP request for PUT bucket policy endpoint.
		reqV4, err := newTestSignedRequestV4(http.MethodPut, getPutPolicyURL("", testPolicy.bucketName),
			int64(len(bucketPolicyStr)), bytes.NewReader([]byte(bucketPolicyStr)), testPolicy.accessKey, testPolicy.secretKey, nil)
		if err != nil {
			t.Fatalf("Test %d: Failed to create HTTP request for PutBucketPolicyHandler: <ERROR> %v", i+1, err)
		}
		// Since `apiRouter` satisfies `http.Handler` it has a ServeHTTP to execute the logic of the handler.
		// Call the ServeHTTP to execute the handler.
		apiRouter.ServeHTTP(recV4, reqV4)
		if recV4.Code != testPolicy.expectedRespStatus {
			t.Fatalf("Case %d: Expected the response status to be `%d`, but instead found `%d`", i+1, testPolicy.expectedRespStatus, recV4.Code)
		}
		// initialize HTTP NewRecorder, this records any mutations to response writer inside the handler.
		recV2 := httptest.NewRecorder()
		// construct HTTP request for PUT bucket policy endpoint.
		reqV2, err := newTestSignedRequestV2(http.MethodPut, getPutPolicyURL("", testPolicy.bucketName),
			int64(len(bucketPolicyStr)), bytes.NewReader([]byte(bucketPolicyStr)), testPolicy.accessKey, testPolicy.secretKey, nil)
		if err != nil {
			t.Fatalf("Test %d: Failed to create HTTP request for PutBucketPolicyHandler: <ERROR> %v", i+1, err)
		}
		// Since `apiRouter` satisfies `http.Handler` it has a ServeHTTP to execute the logic of the handler.
		// Call the ServeHTTP to execute the handler.
		apiRouter.ServeHTTP(recV2, reqV2)
		if recV2.Code != testPolicy.expectedRespStatus {
			t.Fatalf("Case %d: Expected the response status to be `%d`, but instead found `%d`", i+1, testPolicy.expectedRespStatus, recV2.Code)
		}
	}

	// test cases with inputs and expected result for GetBucketPolicyHandler.
	testCases := []struct {
		bucketName string
		accessKey  string
		secretKey  string
		// expected output.
		expectedBucketPolicy string
		expectedRespStatus   int
	}{
		// Test case - 1.
		// Case which valid inputs, expected to return success status of 200OK.
		{
			bucketName:           bucketName,
			accessKey:            credentials.AccessKey,
			secretKey:            credentials.SecretKey,
			expectedBucketPolicy: bucketPolicyTemplate,
			expectedRespStatus:   http.StatusOK,
		},
		// Test case - 2.
		// Case with non-existent bucket name.
		{
			bucketName:           "non-existent-bucket",
			accessKey:            credentials.AccessKey,
			secretKey:            credentials.SecretKey,
			expectedBucketPolicy: bucketPolicyTemplate,
			expectedRespStatus:   http.StatusNotFound,
		},
		// Test case - 3.
		// Case with non-existent bucket name.
		{
			bucketName:           ".invalid-bucket-name",
			accessKey:            credentials.AccessKey,
			secretKey:            credentials.SecretKey,
			expectedBucketPolicy: "",
			expectedRespStatus:   http.StatusBadRequest,
		},
	}
	// Iterating over the cases, fetching the policy and validating the response.
	for i, testCase := range testCases {
		// expected bucket policy json string.
		expectedBucketPolicyStr := fmt.Sprintf(testCase.expectedBucketPolicy, testCase.bucketName, testCase.bucketName)
		// initialize HTTP NewRecorder, this records any mutations to response writer inside the handler.
		recV4 := httptest.NewRecorder()
		// construct HTTP request for PUT bucket policy endpoint.
		reqV4, err := newTestSignedRequestV4(http.MethodGet, getGetPolicyURL("", testCase.bucketName),
			0, nil, testCase.accessKey, testCase.secretKey, nil)
		if err != nil {
			t.Fatalf("Test %d: Failed to create HTTP request for GetBucketPolicyHandler: <ERROR> %v", i+1, err)
		}
		// Since `apiRouter` satisfies `http.Handler` it has a ServeHTTP to execute the logic of the handler.
		// Call the ServeHTTP to execute the handler, GetBucketPolicyHandler handles the request.
		apiRouter.ServeHTTP(recV4, reqV4)
		// Assert the response code with the expected status.
		if recV4.Code != testCase.expectedRespStatus {
			t.Fatalf("Case %d: Expected the response status to be `%d`, but instead found `%d`", i+1, testCase.expectedRespStatus, recV4.Code)
		}
		// read the response body.
		bucketPolicyReadBuf, err := io.ReadAll(recV4.Body)
		if err != nil {
			t.Fatalf("Test %d: %s: Failed parsing response body: <ERROR> %v", i+1, instanceType, err)
		}

		if recV4.Code != testCase.expectedRespStatus {
			// Verify whether the bucket policy fetched is same as the one inserted.
			var expectedPolicy *policy.BucketPolicy
			expectedPolicy, err = policy.ParseBucketPolicyConfig(strings.NewReader(expectedBucketPolicyStr), testCase.bucketName)
			if err != nil {
				t.Fatalf("unexpected error. %v", err)
			}
			var gotPolicy *policy.BucketPolicy
			gotPolicy, err = policy.ParseBucketPolicyConfig(bytes.NewReader(bucketPolicyReadBuf), testCase.bucketName)
			if err != nil {
				t.Fatalf("unexpected error. %v", err)
			}

			if !reflect.DeepEqual(expectedPolicy, gotPolicy) {
				t.Errorf("Test %d: %s: Bucket policy differs from expected value.", i+1, instanceType)
			}
		}
		// initialize HTTP NewRecorder, this records any mutations to response writer inside the handler.
		recV2 := httptest.NewRecorder()
		// construct HTTP request for PUT bucket policy endpoint.
		reqV2, err := newTestSignedRequestV2(http.MethodGet, getGetPolicyURL("", testCase.bucketName),
			0, nil, testCase.accessKey, testCase.secretKey, nil)
		if err != nil {
			t.Fatalf("Test %d: Failed to create HTTP request for GetBucketPolicyHandler: <ERROR> %v", i+1, err)
		}
		// Since `apiRouter` satisfies `http.Handler` it has a ServeHTTP to execute the logic of the handler.
		// Call the ServeHTTP to execute the handler, GetBucketPolicyHandler handles the request.
		apiRouter.ServeHTTP(recV2, reqV2)
		// Assert the response code with the expected status.
		if recV2.Code != testCase.expectedRespStatus {
			t.Fatalf("Case %d: Expected the response status to be `%d`, but instead found `%d`", i+1, testCase.expectedRespStatus, recV2.Code)
		}
		// read the response body.
		bucketPolicyReadBuf, err = io.ReadAll(recV2.Body)
		if err != nil {
			t.Fatalf("Test %d: %s: Failed parsing response body: <ERROR> %v", i+1, instanceType, err)
		}
		if recV2.Code == http.StatusOK {
			// Verify whether the bucket policy fetched is same as the one inserted.
			expectedPolicy, err := policy.ParseBucketPolicyConfig(strings.NewReader(expectedBucketPolicyStr), testCase.bucketName)
			if err != nil {
				t.Fatalf("unexpected error. %v", err)
			}
			gotPolicy, err := policy.ParseBucketPolicyConfig(bytes.NewReader(bucketPolicyReadBuf), testCase.bucketName)
			if err != nil {
				t.Fatalf("unexpected error. %v", err)
			}

			if !reflect.DeepEqual(expectedPolicy, gotPolicy) {
				t.Errorf("Test %d: %s: Bucket policy differs from expected value.", i+1, instanceType)
			}
		}
	}

	// Test for Anonymous/unsigned http request.
	// Bucket policy related functions doesn't support anonymous requests, setting policies shouldn't make a difference.
	// create unsigned HTTP request for PutBucketPolicyHandler.
	anonReq, err := newTestRequest(http.MethodGet, getPutPolicyURL("", bucketName), 0, nil)
	if err != nil {
		t.Fatalf("MinIO %s: Failed to create an anonymous request for bucket \"%s\": <ERROR> %v",
			instanceType, bucketName, err)
	}

	// ExecObjectLayerAPIAnonTest - Calls the HTTP API handler using the anonymous request, validates the ErrAccessDeniedResponse,
	// sets the bucket policy using the policy statement generated from `getWriteOnlyObjectStatement` so that the
	// unsigned request goes through and its validated again.
	ExecObjectLayerAPIAnonTest(t, obj, "GetBucketPolicyHandler", bucketName, "", instanceType, apiRouter, anonReq, getAnonReadOnlyBucketPolicy(bucketName))

	// HTTP request for testing when `objectLayer` is set to `nil`.
	// There is no need to use an existing bucket and valid input for creating the request
	// since the `objectLayer==nil`  check is performed before any other checks inside the handlers.
	// The only aim is to generate an HTTP request in a way that the relevant/registered end point is evoked/called.
	nilBucket := "dummy-bucket"

	nilReq, err := newTestSignedRequestV4(http.MethodGet, getGetPolicyURL("", nilBucket),
		0, nil, "", "", nil)
	if err != nil {
		t.Errorf("MinIO %s: Failed to create HTTP request for testing the response when object Layer is set to `nil`.", instanceType)
	}
	// execute the object layer set to `nil` test.
	// `ExecObjectLayerAPINilTest` manages the operation.
	ExecObjectLayerAPINilTest(t, nilBucket, "", instanceType, apiRouter, nilReq)
}

// Wrapper for calling Delete Bucket Policy HTTP handler tests for both Erasure multiple disks and single node setup.
func TestDeleteBucketPolicyHandler(t *testing.T) {
	ExecObjectLayerAPITest(ExecObjectLayerAPITestArgs{t: t, objAPITest: testDeleteBucketPolicyHandler, endpoints: []string{"PutBucketPolicy", "DeleteBucketPolicy"}})
}

// testDeleteBucketPolicyHandler - Test for Delete bucket policy end point.
func testDeleteBucketPolicyHandler(obj ObjectLayer, instanceType, bucketName string, apiRouter http.Handler,
	credentials auth.Credentials, t *testing.T,
) {
	// template for constructing HTTP request body for PUT bucket policy.
	bucketPolicyTemplate := `{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Action": [
                "s3:GetBucketLocation",
                "s3:ListBucket"
            ],
            "Effect": "Allow",
            "Principal": {
                "AWS": [
                    "*"
                ]
            },
            "Resource": [
                "arn:aws:s3:::%s"
            ]
        },
        {
            "Action": [
                "s3:GetObject"
            ],
            "Effect": "Allow",
            "Principal": {
                "AWS": [
                    "*"
                ]
            },
            "Resource": [
                "arn:aws:s3:::%s/this*"
            ]
        }
    ]
}`

	// Writing bucket policy before running test on DeleteBucketPolicy.
	putTestPolicies := []struct {
		bucketName string
		accessKey  string
		secretKey  string
		// expected Response.
		expectedRespStatus int
	}{
		{
			bucketName:         bucketName,
			accessKey:          credentials.AccessKey,
			secretKey:          credentials.SecretKey,
			expectedRespStatus: http.StatusNoContent,
		},
	}

	// Iterating over the cases and writing the bucket policy.
	// its required to write the policies first before running tests on GetBucketPolicy.
	for i, testPolicy := range putTestPolicies {
		// obtain the put bucket policy request body.
		bucketPolicyStr := fmt.Sprintf(bucketPolicyTemplate, testPolicy.bucketName, testPolicy.bucketName)
		// initialize HTTP NewRecorder, this records any mutations to response writer inside the handler.
		recV4 := httptest.NewRecorder()
		// construct HTTP request for PUT bucket policy endpoint.
		reqV4, err := newTestSignedRequestV4(http.MethodPut, getPutPolicyURL("", testPolicy.bucketName),
			int64(len(bucketPolicyStr)), bytes.NewReader([]byte(bucketPolicyStr)), testPolicy.accessKey, testPolicy.secretKey, nil)
		if err != nil {
			t.Fatalf("Test %d: Failed to create HTTP request for PutBucketPolicyHandler: <ERROR> %v", i+1, err)
		}
		// Since `apiRouter` satisfies `http.Handler` it has a ServeHTTP to execute the logic of the handler.
		// Call the ServeHTTP to execute the handler.
		apiRouter.ServeHTTP(recV4, reqV4)
		if recV4.Code != testPolicy.expectedRespStatus {
			t.Fatalf("Case %d: Expected the response status to be `%d`, but instead found `%d`", i+1, testPolicy.expectedRespStatus, recV4.Code)
		}
	}

	// testcases with input and expected output for DeleteBucketPolicyHandler.
	testCases := []struct {
		bucketName string
		accessKey  string
		secretKey  string
		// expected response.
		expectedRespStatus int
	}{
		// Test case - 1.
		{
			bucketName:         bucketName,
			accessKey:          credentials.AccessKey,
			secretKey:          credentials.SecretKey,
			expectedRespStatus: http.StatusNoContent,
		},
		// Test case - 2.
		// Case with non-existent-bucket.
		{
			bucketName:         "non-existent-bucket",
			accessKey:          credentials.AccessKey,
			secretKey:          credentials.SecretKey,
			expectedRespStatus: http.StatusNotFound,
		},
		// Test case - 3.
		// Case with non-existent-bucket.
		{
			bucketName:         ".invalid-bucket-name",
			accessKey:          credentials.AccessKey,
			secretKey:          credentials.SecretKey,
			expectedRespStatus: http.StatusBadRequest,
		},
	}
	// Iterating over the cases and deleting the bucket policy and then asserting response.
	for i, testCase := range testCases {
		// initialize HTTP NewRecorder, this records any mutations to response writer inside the handler.
		recV4 := httptest.NewRecorder()
		// construct HTTP request for Delete bucket policy endpoint.
		reqV4, err := newTestSignedRequestV4(http.MethodDelete, getDeletePolicyURL("", testCase.bucketName),
			0, nil, testCase.accessKey, testCase.secretKey, nil)
		if err != nil {
			t.Fatalf("Test %d: Failed to create HTTP request for GetBucketPolicyHandler: <ERROR> %v", i+1, err)
		}
		// Since `apiRouter` satisfies `http.Handler` it has a ServeHTTP to execute the logic of the handler.
		// Call the ServeHTTP to execute the handler, DeleteBucketPolicyHandler  handles the request.
		apiRouter.ServeHTTP(recV4, reqV4)
		// Assert the response code with the expected status.
		if recV4.Code != testCase.expectedRespStatus {
			t.Fatalf("Case %d: Expected the response status to be `%d`, but instead found `%d`", i+1, testCase.expectedRespStatus, recV4.Code)
		}
	}

	// Iterating over the cases and writing the bucket policy.
	// its required to write the policies first before running tests on GetBucketPolicy.
	for i, testPolicy := range putTestPolicies {
		// obtain the put bucket policy request body.
		bucketPolicyStr := fmt.Sprintf(bucketPolicyTemplate, testPolicy.bucketName, testPolicy.bucketName)
		// initialize HTTP NewRecorder, this records any mutations to response writer inside the handler.
		recV2 := httptest.NewRecorder()
		// construct HTTP request for PUT bucket policy endpoint.
		reqV2, err := newTestSignedRequestV2(http.MethodPut, getPutPolicyURL("", testPolicy.bucketName),
			int64(len(bucketPolicyStr)), bytes.NewReader([]byte(bucketPolicyStr)), testPolicy.accessKey, testPolicy.secretKey, nil)
		if err != nil {
			t.Fatalf("Test %d: Failed to create HTTP request for PutBucketPolicyHandler: <ERROR> %v", i+1, err)
		}
		// Since `apiRouter` satisfies `http.Handler` it has a ServeHTTP to execute the logic of the handler.
		// Call the ServeHTTP to execute the handler.
		apiRouter.ServeHTTP(recV2, reqV2)
		if recV2.Code != testPolicy.expectedRespStatus {
			t.Fatalf("Case %d: Expected the response status to be `%d`, but instead found `%d`", i+1, testPolicy.expectedRespStatus, recV2.Code)
		}
	}

	for i, testCase := range testCases {
		// initialize HTTP NewRecorder, this records any mutations to response writer inside the handler.
		recV2 := httptest.NewRecorder()
		// construct HTTP request for Delete bucket policy endpoint.
		reqV2, err := newTestSignedRequestV2(http.MethodDelete, getDeletePolicyURL("", testCase.bucketName),
			0, nil, testCase.accessKey, testCase.secretKey, nil)
		if err != nil {
			t.Fatalf("Test %d: Failed to create HTTP request for GetBucketPolicyHandler: <ERROR> %v", i+1, err)
		}
		// Since `apiRouter` satisfies `http.Handler` it has a ServeHTTP to execute the logic of the handler.
		// Call the ServeHTTP to execute the handler, DeleteBucketPolicyHandler  handles the request.
		apiRouter.ServeHTTP(recV2, reqV2)
		// Assert the response code with the expected status.
		if recV2.Code != testCase.expectedRespStatus {
			t.Fatalf("Case %d: Expected the response status to be `%d`, but instead found `%d`", i+1, testCase.expectedRespStatus, recV2.Code)
		}
	}
	// Test for Anonymous/unsigned http request.
	// Bucket policy related functions doesn't support anonymous requests, setting policies shouldn't make a difference.
	// create unsigned HTTP request for PutBucketPolicyHandler.
	anonReq, err := newTestRequest(http.MethodDelete, getPutPolicyURL("", bucketName), 0, nil)
	if err != nil {
		t.Fatalf("MinIO %s: Failed to create an anonymous request for bucket \"%s\": <ERROR> %v",
			instanceType, bucketName, err)
	}

	// ExecObjectLayerAPIAnonTest - Calls the HTTP API handler using the anonymous request, validates the ErrAccessDeniedResponse,
	// sets the bucket policy using the policy statement generated from `getWriteOnlyObjectStatement` so that the
	// unsigned request goes through and its validated again.
	ExecObjectLayerAPIAnonTest(t, obj, "DeleteBucketPolicyHandler", bucketName, "", instanceType, apiRouter, anonReq, getAnonWriteOnlyBucketPolicy(bucketName))

	// HTTP request for testing when `objectLayer` is set to `nil`.
	// There is no need to use an existing bucket and valid input for creating the request
	// since the `objectLayer==nil`  check is performed before any other checks inside the handlers.
	// The only aim is to generate an HTTP request in a way that the relevant/registered end point is evoked/called.
	nilBucket := "dummy-bucket"

	nilReq, err := newTestSignedRequestV4(http.MethodDelete, getDeletePolicyURL("", nilBucket),
		0, nil, "", "", nil)
	if err != nil {
		t.Errorf("MinIO %s: Failed to create HTTP request for testing the response when object Layer is set to `nil`.", instanceType)
	}
	// execute the object layer set to `nil` test.
	// `ExecObjectLayerAPINilTest` manages the operation.
	ExecObjectLayerAPINilTest(t, nilBucket, "", instanceType, apiRouter, nilReq)
}

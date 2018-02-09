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
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/minio/minio-go/pkg/policy"
	"github.com/minio/minio-go/pkg/set"
	"github.com/minio/minio/pkg/auth"
)

// Tests validate Bucket policy resource matcher.
func TestBucketPolicyResourceMatch(t *testing.T) {

	// generates statement with given resource..
	generateStatement := func(resource string) policy.Statement {
		statement := policy.Statement{}
		statement.Resources = set.CreateStringSet([]string{resource}...)
		return statement
	}

	// generates resource prefix.
	generateResource := func(bucketName, objectName string) string {
		return bucketARNPrefix + bucketName + "/" + objectName
	}

	testCases := []struct {
		resourceToMatch       string
		statement             policy.Statement
		expectedResourceMatch bool
	}{
		// Test case 1-4.
		// Policy with resource ending with bucket/* allows access to all objects inside the given bucket.
		{generateResource("minio-bucket", ""), generateStatement(fmt.Sprintf("%s%s", bucketARNPrefix, "minio-bucket"+"/*")), true},
		{generateResource("minio-bucket", ""), generateStatement(fmt.Sprintf("%s%s", bucketARNPrefix, "minio-bucket"+"/*")), true},
		{generateResource("minio-bucket", ""), generateStatement(fmt.Sprintf("%s%s", bucketARNPrefix, "minio-bucket"+"/*")), true},
		{generateResource("minio-bucket", ""), generateStatement(fmt.Sprintf("%s%s", bucketARNPrefix, "minio-bucket"+"/*")), true},
		// Test case - 5.
		// Policy with resource ending with bucket/oo* should not allow access to bucket/output.txt.
		{generateResource("minio-bucket", "output.txt"), generateStatement(fmt.Sprintf("%s%s", bucketARNPrefix, "minio-bucket"+"/oo*")), false},
		// Test case - 6.
		// Policy with resource ending with bucket/oo* should allow access to bucket/ootput.txt.
		{generateResource("minio-bucket", "ootput.txt"), generateStatement(fmt.Sprintf("%s%s", bucketARNPrefix, "minio-bucket"+"/oo*")), true},
		// Test case - 7.
		// Policy with resource ending with bucket/oo* allows access to all sub-dirs starting with "oo" inside given bucket.
		{generateResource("minio-bucket", "oop-bucket/my-file"), generateStatement(fmt.Sprintf("%s%s", bucketARNPrefix, "minio-bucket"+"/oo*")), true},
		// Test case - 8.
		{generateResource("minio-bucket", "Asia/India/1.pjg"), generateStatement(fmt.Sprintf("%s%s", bucketARNPrefix, "minio-bucket"+"/Asia/Japan/*")), false},
		// Test case - 9.
		{generateResource("minio-bucket", "Asia/India/1.pjg"), generateStatement(fmt.Sprintf("%s%s", bucketARNPrefix, "minio-bucket"+"/Asia/Japan/*")), false},
		// Test case - 10.
		// Proves that the name space is flat.
		{generateResource("minio-bucket", "Africa/Bihar/India/design_info.doc/Bihar"), generateStatement(fmt.Sprintf("%s%s", bucketARNPrefix,
			"minio-bucket"+"/*/India/*/Bihar")), true},
		// Test case - 11.
		// Proves that the name space is flat.
		{generateResource("minio-bucket", "Asia/China/India/States/Bihar/output.txt"), generateStatement(fmt.Sprintf("%s%s", bucketARNPrefix,
			"minio-bucket"+"/*/India/*/Bihar/*")), true},
	}
	for i, testCase := range testCases {
		actualResourceMatch := bucketPolicyResourceMatch(testCase.resourceToMatch, testCase.statement)
		if testCase.expectedResourceMatch != actualResourceMatch {
			t.Errorf("Test %d: Expected Resource match to be `%v`, but instead found it to be `%v`", i+1, testCase.expectedResourceMatch, actualResourceMatch)
		}
	}
}

// TestBucketPolicyActionMatch - Test validates whether given action on the
// bucket/object matches the allowed actions in policy.Statement.
// This test preserves the allowed actions for all 3 sets of policies, that is read-write,read-only, write-only.
// The intention of the test is to catch any changes made to allowed action for on eof the above 3 major policy groups mentioned.
func TestBucketPolicyActionMatch(t *testing.T) {
	bucketName := getRandomBucketName()
	objectPrefix := "test-object"

	testCases := []struct {
		action         string
		statement      policy.Statement
		expectedResult bool
	}{
		// s3:GetBucketLocation is the action necessary to be present in the bucket policy to allow
		// fetching of bucket location on an Anonymous/unsigned request.

		//r ead-write bucket policy is expected to allow GetBucketLocation operation on an anonymous request (Test case - 1).
		{"s3:GetBucketLocation", getReadWriteBucketStatement(bucketName, objectPrefix), true},
		//	write-only bucket policy is expected to allow GetBucketLocation operation on an anonymous request (Test case - 2).
		{"s3:GetBucketLocation", getWriteOnlyBucketStatement(bucketName, objectPrefix), true},
		//	read-only bucket policy is expected to allow GetBucketLocation operation on an anonymous request (Test case - 3).
		{"s3:GetBucketLocation", getReadOnlyBucketStatement(bucketName, objectPrefix), true},

		// Any of the Object level access permissions shouldn't allow for GetBucketLocation operation  on an Anonymous/unsigned request (Test cases 4-6).
		{"s3:GetBucketLocation", getReadWriteObjectStatement(bucketName, objectPrefix), false},
		{"s3:GetBucketLocation", getWriteOnlyObjectStatement(bucketName, objectPrefix), false},
		{"s3:GetBucketLocation", getReadOnlyObjectStatement(bucketName, objectPrefix), false},

		// s3:ListBucketMultipartUploads is the action necessary to be present in the bucket policy to allow
		// Listing of multipart uploads in a given bucket for an Anonymous/unsigned request.

		//read-write bucket policy is expected to allow  ListBucketMultipartUploads operation on an anonymous request (Test case 7).
		{"s3:ListBucketMultipartUploads", getReadWriteBucketStatement(bucketName, objectPrefix), true},
		//	write-only bucket policy is expected to allow  ListBucketMultipartUploads operation on an anonymous request (Test case 8).
		{"s3:ListBucketMultipartUploads", getWriteOnlyBucketStatement(bucketName, objectPrefix), true},
		// read-only bucket policy is expected to not allow ListBucketMultipartUploads operation on an anonymous request (Test case 9).
		// the allowed actions in read-only bucket statement are  "s3:GetBucketLocation","s3:ListBucket",
		// this should not allow for ListBucketMultipartUploads operations.
		{"s3:ListBucketMultipartUploads", getReadOnlyBucketStatement(bucketName, objectPrefix), false},

		// Any of the object level policy will not allow for s3:ListBucketMultipartUploads (Test cases 10-12).
		{"s3:ListBucketMultipartUploads", getReadWriteObjectStatement(bucketName, objectPrefix), false},
		{"s3:ListBucketMultipartUploads", getWriteOnlyObjectStatement(bucketName, objectPrefix), false},
		{"s3:ListBucketMultipartUploads", getReadOnlyObjectStatement(bucketName, objectPrefix), false},

		// s3:ListBucket is the action necessary to be present in the bucket policy to allow
		// listing of all objects inside a given bucket on an Anonymous/unsigned request.

		// Cases for testing ListBucket access for different Bucket level access permissions.
		// read-only bucket policy is expected to allow ListBucket operation on an anonymous request (Test case 13).
		{"s3:ListBucket", getReadOnlyBucketStatement(bucketName, objectPrefix), true},
		// read-write bucket policy is expected to allow ListBucket operation on an anonymous request (Test case 14).
		{"s3:ListBucket", getReadWriteBucketStatement(bucketName, objectPrefix), true},
		// write-only  bucket policy is expected to not allow ListBucket operation on an anonymous request (Test case 15).
		// the allowed actions in write-only  bucket statement are "s3:GetBucketLocation",	"s3:ListBucketMultipartUploads",
		// this should not allow for ListBucket operations.
		{"s3:ListBucket", getWriteOnlyBucketStatement(bucketName, objectPrefix), false},

		// Cases for testing ListBucket access for different Object level access permissions (Test cases 16-18).
		// Any of the Object level access permissions shouldn't allow for ListBucket operation  on an Anonymous/unsigned request.
		{"s3:ListBucket", getReadOnlyObjectStatement(bucketName, objectPrefix), false},
		{"s3:ListBucket", getReadWriteObjectStatement(bucketName, objectPrefix), false},
		{"s3:ListBucket", getWriteOnlyObjectStatement(bucketName, objectPrefix), false},

		// s3:DeleteObject is the action necessary to be present in the bucket policy to allow
		// deleting/removal of  objects inside a given bucket for an Anonymous/unsigned request.

		// Cases for testing DeleteObject access for different Bucket level access permissions (Test cases 19-21).
		// Any of the Bucket level access permissions shouldn't allow for DeleteObject operation  on an Anonymous/unsigned request.
		{"s3:DeleteObject", getReadOnlyBucketStatement(bucketName, objectPrefix), false},
		{"s3:DeleteObject", getReadWriteBucketStatement(bucketName, objectPrefix), false},
		{"s3:DeleteObject", getWriteOnlyBucketStatement(bucketName, objectPrefix), false},

		// Cases for testing DeleteObject access for different Object level access permissions (Test cases 22).
		// read-only bucket policy is expected to not allow Delete Object operation on an anonymous request.
		{"s3:DeleteObject", getReadOnlyObjectStatement(bucketName, objectPrefix), false},
		// read-write bucket policy is expected to allow Delete Bucket operation on an anonymous request (Test cases 23).
		{"s3:DeleteObject", getReadWriteObjectStatement(bucketName, objectPrefix), true},
		// write-only  bucket policy is expected to allow Delete Object operation on an anonymous request (Test cases 24).
		{"s3:DeleteObject", getWriteOnlyObjectStatement(bucketName, objectPrefix), true},

		// s3:AbortMultipartUpload is the action necessary to be present in the bucket policy to allow
		// cancelling or abortion of an already initiated multipart upload operation for an Anonymous/unsigned request.

		// Cases for testing AbortMultipartUpload access for different Bucket level access permissions (Test cases 25-27).
		// Any of the Bucket level access permissions shouldn't allow for AbortMultipartUpload operation on an Anonymous/unsigned request.
		{"s3:AbortMultipartUpload", getReadOnlyBucketStatement(bucketName, objectPrefix), false},
		{"s3:AbortMultipartUpload", getReadWriteBucketStatement(bucketName, objectPrefix), false},
		{"s3:AbortMultipartUpload", getWriteOnlyBucketStatement(bucketName, objectPrefix), false},

		// Cases for testing AbortMultipartUpload access for different Object level access permissions.
		// read-only object policy is expected to not allow AbortMultipartUpload operation on an anonymous request (Test case 28).
		{"s3:AbortMultipartUpload", getReadOnlyObjectStatement(bucketName, objectPrefix), false},
		// read-write object policy is expected to allow AbortMultipartUpload operation on an anonymous request (Test case 29).
		{"s3:AbortMultipartUpload", getReadWriteObjectStatement(bucketName, objectPrefix), true},
		// write-only object policy is expected to allow AbortMultipartUpload operation on an anonymous request (Test case 30).
		{"s3:AbortMultipartUpload", getWriteOnlyObjectStatement(bucketName, objectPrefix), true},

		// s3:PutObject is the action necessary to be present in the bucket policy to allow
		// uploading of an object for an Anonymous/unsigned request.

		// Cases for testing PutObject access for different Bucket level access permissions (Test cases 31-33).
		// Any of the Bucket level access permissions shouldn't allow for PutObject operation on an Anonymous/unsigned request.
		{"s3:PutObject", getReadOnlyBucketStatement(bucketName, objectPrefix), false},
		{"s3:PutObject", getReadWriteBucketStatement(bucketName, objectPrefix), false},
		{"s3:PutObject", getWriteOnlyBucketStatement(bucketName, objectPrefix), false},

		// Cases for testing PutObject access for different Object level access permissions.
		// read-only object policy is expected to not allow PutObject operation on an anonymous request (Test case 34).
		{"s3:PutObject", getReadOnlyObjectStatement(bucketName, objectPrefix), false},
		// read-write object policy is expected to allow PutObject operation on an anonymous request (Test case 35).
		{"s3:PutObject", getReadWriteObjectStatement(bucketName, objectPrefix), true},
		// write-only  object policy is expected to allow PutObject operation on an anonymous request (Test case 36).
		{"s3:PutObject", getWriteOnlyObjectStatement(bucketName, objectPrefix), true},

		// s3:GetObject is the action necessary to be present in the bucket policy to allow
		// downloading of an object for an Anonymous/unsigned request.

		// Cases for testing GetObject access for different Bucket level access permissions (Test cases 37-39).
		// Any of the Bucket level access permissions shouldn't allow for GetObject operation on an Anonymous/unsigned request.
		{"s3:GetObject", getReadOnlyBucketStatement(bucketName, objectPrefix), false},
		{"s3:GetObject", getReadWriteBucketStatement(bucketName, objectPrefix), false},
		{"s3:GetObject", getWriteOnlyBucketStatement(bucketName, objectPrefix), false},

		// Cases for testing GetObject access for different Object level access permissions.
		// read-only bucket policy is expected to allow  downloading of an Object on an anonymous request (Test case 40).
		{"s3:GetObject", getReadOnlyObjectStatement(bucketName, objectPrefix), true},
		// read-write bucket policy is expected to allow  downloading of an Object on an anonymous request (Test case 41).
		{"s3:GetObject", getReadWriteObjectStatement(bucketName, objectPrefix), true},
		// write-only  bucket policy is expected to not allow  downloading of an Object on an anonymous request (Test case 42).
		{"s3:GetObject", getWriteOnlyObjectStatement(bucketName, objectPrefix), false},

		// s3:ListMultipartUploadParts is the action necessary to be present in the bucket policy to allow
		// Listing of uploaded parts for an Anonymous/unsigned request.

		// Any of the Bucket level access permissions shouldn't allow for ListMultipartUploadParts operation on an Anonymous/unsigned request.
		// read-only bucket policy is expected to not allow ListMultipartUploadParts operation on an anonymous request (Test cases 43-45).
		{"s3:ListMultipartUploadParts", getReadOnlyBucketStatement(bucketName, objectPrefix), false},
		{"s3:ListMultipartUploadParts", getReadWriteBucketStatement(bucketName, objectPrefix), false},
		{"s3:ListMultipartUploadParts", getWriteOnlyBucketStatement(bucketName, objectPrefix), false},

		// read-only object policy is expected to not allow ListMultipartUploadParts operation on an anonymous request (Test case 46).
		{"s3:ListMultipartUploadParts", getReadOnlyObjectStatement(bucketName, objectPrefix), false},
		// read-write object policy is expected to allow ListMultipartUploadParts operation on an anonymous request (Test case 47).
		{"s3:ListMultipartUploadParts", getReadWriteObjectStatement(bucketName, objectPrefix), true},
		// write-only  object policy is expected to allow ListMultipartUploadParts operation on an anonymous request (Test case 48).
		{"s3:ListMultipartUploadParts", getWriteOnlyObjectStatement(bucketName, objectPrefix), true},
	}
	for i, testCase := range testCases {
		actualResult := bucketPolicyActionMatch(testCase.action, testCase.statement)
		if testCase.expectedResult != actualResult {
			t.Errorf("Test %d: Expected the result to be `%v`, but instead found it to be `%v`", i+1, testCase.expectedResult, actualResult)
		}
	}
}

// Wrapper for calling Put Bucket Policy HTTP handler tests for both XL multiple disks and single node setup.
func TestPutBucketPolicyHandler(t *testing.T) {
	ExecObjectLayerAPITest(t, testPutBucketPolicyHandler, []string{"PutBucketPolicy"})
}

// testPutBucketPolicyHandler - Test for Bucket policy end point.
func testPutBucketPolicyHandler(obj ObjectLayer, instanceType, bucketName string, apiRouter http.Handler,
	credentials auth.Credentials, t *testing.T) {
	initBucketPolicies(obj)

	bucketName1 := fmt.Sprintf("%s-1", bucketName)
	if err := obj.MakeBucketWithLocation(bucketName1, ""); err != nil {
		t.Fatal(err)
	}

	// template for constructing HTTP request body for PUT bucket policy.
	bucketPolicyTemplate := `{"Version":"2012-10-17","Statement":[{"Sid":"","Effect":"Allow","Principal":{"AWS":["*"]},"Action":["s3:GetBucketLocation","s3:ListBucket"],"Resource":["arn:aws:s3:::%s"]},{"Sid":"","Effect":"Allow","Principal":{"AWS":["*"]},"Action":["s3:GetObject"],"Resource":["arn:aws:s3:::%s/this*"]}]}`

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
			bucketPolicyReader: bytes.NewReader([]byte(fmt.Sprintf(bucketPolicyTemplate, bucketName, bucketName))),

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
			bucketPolicyReader: bytes.NewReader([]byte(fmt.Sprintf(bucketPolicyTemplate, bucketName, bucketName))),

			policyLen:          maxAccessPolicySize + 1,
			accessKey:          credentials.AccessKey,
			secretKey:          credentials.SecretKey,
			expectedRespStatus: http.StatusBadRequest,
		},
		// Test case - 3.
		// Case with content-length of the HTTP request set to 0.
		// Expecting the HTTP response status to be StatusLengthRequired (411).
		{
			bucketName:         bucketName,
			bucketPolicyReader: bytes.NewReader([]byte(fmt.Sprintf(bucketPolicyTemplate, bucketName, bucketName))),

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
			bucketPolicyReader: bytes.NewReader([]byte(fmt.Sprintf(bucketPolicyTemplate, bucketName, bucketName))),

			policyLen:          len(fmt.Sprintf(bucketPolicyTemplate, bucketName, bucketName)),
			accessKey:          credentials.AccessKey,
			secretKey:          credentials.SecretKey,
			expectedRespStatus: http.StatusBadRequest,
		},
		// Test case - 8.
		// non-existent bucket is used.
		// writing BucketPolicy should fail.
		// should result is 500 InternalServerError.
		{
			bucketName:         "non-existent-bucket",
			bucketPolicyReader: bytes.NewReader([]byte(fmt.Sprintf(bucketPolicyTemplate, "non-existent-bucket", "non-existent-bucket"))),

			policyLen:          len(fmt.Sprintf(bucketPolicyTemplate, bucketName, bucketName)),
			accessKey:          credentials.AccessKey,
			secretKey:          credentials.SecretKey,
			expectedRespStatus: http.StatusNotFound,
		},
		// Test case - 9.
		// invalid bucket name is used.
		// writing BucketPolicy should fail.
		// should result is 400 StatusBadRequest.
		{
			bucketName:         ".invalid-bucket",
			bucketPolicyReader: bytes.NewReader([]byte(fmt.Sprintf(bucketPolicyTemplate, ".invalid-bucket", ".invalid-bucket"))),

			policyLen:          len(fmt.Sprintf(bucketPolicyTemplate, bucketName, bucketName)),
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
		reqV4, err := newTestSignedRequestV4("PUT", getPutPolicyURL("", testCase.bucketName),
			int64(testCase.policyLen), testCase.bucketPolicyReader, testCase.accessKey, testCase.secretKey)
		if err != nil {
			t.Fatalf("Test %d: %s: Failed to create HTTP request for PutBucketPolicyHandler: <ERROR> %v", i+1, instanceType, err)
		}
		// Since `apiRouter` satisfies `http.Handler` it has a ServeHTTP to execute the logic ofthe handler.
		// Call the ServeHTTP to execute the handler.
		apiRouter.ServeHTTP(recV4, reqV4)
		if recV4.Code != testCase.expectedRespStatus {
			t.Errorf("Test %d: %s: Expected the response status to be `%d`, but instead found `%d`", i+1, instanceType, testCase.expectedRespStatus, recV4.Code)
		}
		// initialize HTTP NewRecorder, this records any mutations to response writer inside the handler.
		recV2 := httptest.NewRecorder()
		// construct HTTP request for PUT bucket policy endpoint.
		reqV2, err := newTestSignedRequestV2("PUT", getPutPolicyURL("", testCase.bucketName),
			int64(testCase.policyLen), testCase.bucketPolicyReader, testCase.accessKey, testCase.secretKey)
		if err != nil {
			t.Fatalf("Test %d: %s: Failed to create HTTP request for PutBucketPolicyHandler: <ERROR> %v", i+1, instanceType, err)
		}
		// Since `apiRouter` satisfies `http.Handler` it has a ServeHTTP to execute the logic ofthe handler.
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
	anonReq, err := newTestRequest("PUT", getPutPolicyURL("", bucketName),
		int64(len(bucketPolicyStr)), bytes.NewReader([]byte(bucketPolicyStr)))

	if err != nil {
		t.Fatalf("Minio %s: Failed to create an anonymous request for bucket \"%s\": <ERROR> %v",
			instanceType, bucketName, err)
	}

	// ExecObjectLayerAPIAnonTest - Calls the HTTP API handler using the anonymous request, validates the ErrAccessDeniedResponse,
	// sets the bucket policy using the policy statement generated from `getWriteOnlyObjectStatement` so that the
	// unsigned request goes through and its validated again.
	ExecObjectLayerAPIAnonTest(t, obj, "PutBucketPolicyHandler", bucketName, "", instanceType, apiRouter, anonReq, getWriteOnlyObjectStatement)

	// HTTP request for testing when `objectLayer` is set to `nil`.
	// There is no need to use an existing bucket and valid input for creating the request
	// since the `objectLayer==nil`  check is performed before any other checks inside the handlers.
	// The only aim is to generate an HTTP request in a way that the relevant/registered end point is evoked/called.
	nilBucket := "dummy-bucket"

	nilReq, err := newTestSignedRequestV4("PUT", getPutPolicyURL("", nilBucket),
		0, nil, "", "")

	if err != nil {
		t.Errorf("Minio %s: Failed to create HTTP request for testing the response when object Layer is set to `nil`.", instanceType)
	}
	// execute the object layer set to `nil` test.
	// `ExecObjectLayerAPINilTest` manages the operation.
	ExecObjectLayerAPINilTest(t, nilBucket, "", instanceType, apiRouter, nilReq)

}

// Wrapper for calling Get Bucket Policy HTTP handler tests for both XL multiple disks and single node setup.
func TestGetBucketPolicyHandler(t *testing.T) {
	ExecObjectLayerAPITest(t, testGetBucketPolicyHandler, []string{"PutBucketPolicy", "GetBucketPolicy"})
}

// testGetBucketPolicyHandler - Test for end point which fetches the access policy json of the given bucket.
func testGetBucketPolicyHandler(obj ObjectLayer, instanceType, bucketName string, apiRouter http.Handler,
	credentials auth.Credentials, t *testing.T) {
	// initialize bucket policy.
	initBucketPolicies(obj)

	// template for constructing HTTP request body for PUT bucket policy.
	bucketPolicyTemplate := `{"Version":"2012-10-17","Statement":[{"Action":["s3:GetBucketLocation","s3:ListBucket"],"Effect":"Allow","Principal":{"AWS":["*"]},"Resource":["arn:aws:s3:::%s"],"Sid":""},{"Action":["s3:GetObject"],"Effect":"Allow","Principal":{"AWS":["*"]},"Resource":["arn:aws:s3:::%s/this*"],"Sid":""}]}`

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
		reqV4, err := newTestSignedRequestV4("PUT", getPutPolicyURL("", testPolicy.bucketName),
			int64(len(bucketPolicyStr)), bytes.NewReader([]byte(bucketPolicyStr)), testPolicy.accessKey, testPolicy.secretKey)
		if err != nil {
			t.Fatalf("Test %d: Failed to create HTTP request for PutBucketPolicyHandler: <ERROR> %v", i+1, err)
		}
		// Since `apiRouter` satisfies `http.Handler` it has a ServeHTTP to execute the logic ofthe handler.
		// Call the ServeHTTP to execute the handler.
		apiRouter.ServeHTTP(recV4, reqV4)
		if recV4.Code != testPolicy.expectedRespStatus {
			t.Fatalf("Case %d: Expected the response status to be `%d`, but instead found `%d`", i+1, testPolicy.expectedRespStatus, recV4.Code)
		}
		// initialize HTTP NewRecorder, this records any mutations to response writer inside the handler.
		recV2 := httptest.NewRecorder()
		// construct HTTP request for PUT bucket policy endpoint.
		reqV2, err := newTestSignedRequestV2("PUT", getPutPolicyURL("", testPolicy.bucketName),
			int64(len(bucketPolicyStr)), bytes.NewReader([]byte(bucketPolicyStr)), testPolicy.accessKey, testPolicy.secretKey)
		if err != nil {
			t.Fatalf("Test %d: Failed to create HTTP request for PutBucketPolicyHandler: <ERROR> %v", i+1, err)
		}
		// Since `apiRouter` satisfies `http.Handler` it has a ServeHTTP to execute the logic ofthe handler.
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
		// Case with invalid bucket name.
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
		reqV4, err := newTestSignedRequestV4("GET", getGetPolicyURL("", testCase.bucketName),
			0, nil, testCase.accessKey, testCase.secretKey)

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
		bucketPolicyReadBuf, err := ioutil.ReadAll(recV4.Body)
		if err != nil {
			t.Fatalf("Test %d: %s: Failed parsing response body: <ERROR> %v", i+1, instanceType, err)
		}

		if recV4.Code != testCase.expectedRespStatus {
			// Verify whether the bucket policy fetched is same as the one inserted.
			if expectedBucketPolicyStr != string(bucketPolicyReadBuf) {
				t.Errorf("Test %d: %s: Bucket policy differs from expected value.", i+1, instanceType)
			}
		}
		// initialize HTTP NewRecorder, this records any mutations to response writer inside the handler.
		recV2 := httptest.NewRecorder()
		// construct HTTP request for PUT bucket policy endpoint.
		reqV2, err := newTestSignedRequestV2("GET", getGetPolicyURL("", testCase.bucketName),
			0, nil, testCase.accessKey, testCase.secretKey)
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
		bucketPolicyReadBuf, err = ioutil.ReadAll(recV2.Body)
		if err != nil {
			t.Fatalf("Test %d: %s: Failed parsing response body: <ERROR> %v", i+1, instanceType, err)
		}
		if recV2.Code == http.StatusOK {
			// Verify whether the bucket policy fetched is same as the one inserted.
			if expectedBucketPolicyStr != string(bucketPolicyReadBuf) {
				t.Errorf("Test %d: %s: Bucket policy differs from expected value.", i+1, instanceType)
			}
		}
	}

	// Test for Anonymous/unsigned http request.
	// Bucket policy related functions doesn't support anonymous requests, setting policies shouldn't make a difference.
	// create unsigned HTTP request for PutBucketPolicyHandler.
	anonReq, err := newTestRequest("GET", getPutPolicyURL("", bucketName), 0, nil)

	if err != nil {
		t.Fatalf("Minio %s: Failed to create an anonymous request for bucket \"%s\": <ERROR> %v",
			instanceType, bucketName, err)
	}

	// ExecObjectLayerAPIAnonTest - Calls the HTTP API handler using the anonymous request, validates the ErrAccessDeniedResponse,
	// sets the bucket policy using the policy statement generated from `getWriteOnlyObjectStatement` so that the
	// unsigned request goes through and its validated again.
	ExecObjectLayerAPIAnonTest(t, obj, "GetBucketPolicyHandler", bucketName, "", instanceType, apiRouter, anonReq, getReadOnlyObjectStatement)

	// HTTP request for testing when `objectLayer` is set to `nil`.
	// There is no need to use an existing bucket and valid input for creating the request
	// since the `objectLayer==nil`  check is performed before any other checks inside the handlers.
	// The only aim is to generate an HTTP request in a way that the relevant/registered end point is evoked/called.
	nilBucket := "dummy-bucket"

	nilReq, err := newTestSignedRequestV4("GET", getGetPolicyURL("", nilBucket),
		0, nil, "", "")

	if err != nil {
		t.Errorf("Minio %s: Failed to create HTTP request for testing the response when object Layer is set to `nil`.", instanceType)
	}
	// execute the object layer set to `nil` test.
	// `ExecObjectLayerAPINilTest` manages the operation.
	ExecObjectLayerAPINilTest(t, nilBucket, "", instanceType, apiRouter, nilReq)
}

// Wrapper for calling Delete Bucket Policy HTTP handler tests for both XL multiple disks and single node setup.
func TestDeleteBucketPolicyHandler(t *testing.T) {
	ExecObjectLayerAPITest(t, testDeleteBucketPolicyHandler, []string{"PutBucketPolicy", "DeleteBucketPolicy"})
}

// testDeleteBucketPolicyHandler - Test for Delete bucket policy end point.
func testDeleteBucketPolicyHandler(obj ObjectLayer, instanceType, bucketName string, apiRouter http.Handler,
	credentials auth.Credentials, t *testing.T) {
	// initialize bucket policy.
	initBucketPolicies(obj)

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
		reqV4, err := newTestSignedRequestV4("PUT", getPutPolicyURL("", testPolicy.bucketName),
			int64(len(bucketPolicyStr)), bytes.NewReader([]byte(bucketPolicyStr)), testPolicy.accessKey, testPolicy.secretKey)
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
		// Case with invalid bucket name.
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
		reqV4, err := newTestSignedRequestV4("DELETE", getDeletePolicyURL("", testCase.bucketName),
			0, nil, testCase.accessKey, testCase.secretKey)
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
		reqV2, err := newTestSignedRequestV2("PUT", getPutPolicyURL("", testPolicy.bucketName),
			int64(len(bucketPolicyStr)), bytes.NewReader([]byte(bucketPolicyStr)), testPolicy.accessKey, testPolicy.secretKey)
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
		reqV2, err := newTestSignedRequestV2("DELETE", getDeletePolicyURL("", testCase.bucketName),
			0, nil, testCase.accessKey, testCase.secretKey)
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
	anonReq, err := newTestRequest("DELETE", getPutPolicyURL("", bucketName), 0, nil)

	if err != nil {
		t.Fatalf("Minio %s: Failed to create an anonymous request for bucket \"%s\": <ERROR> %v",
			instanceType, bucketName, err)
	}

	// ExecObjectLayerAPIAnonTest - Calls the HTTP API handler using the anonymous request, validates the ErrAccessDeniedResponse,
	// sets the bucket policy using the policy statement generated from `getWriteOnlyObjectStatement` so that the
	// unsigned request goes through and its validated again.
	ExecObjectLayerAPIAnonTest(t, obj, "DeleteBucketPolicyHandler", bucketName, "", instanceType, apiRouter, anonReq, getReadOnlyObjectStatement)

	// HTTP request for testing when `objectLayer` is set to `nil`.
	// There is no need to use an existing bucket and valid input for creating the request
	// since the `objectLayer==nil`  check is performed before any other checks inside the handlers.
	// The only aim is to generate an HTTP request in a way that the relevant/registered end point is evoked/called.
	nilBucket := "dummy-bucket"

	nilReq, err := newTestSignedRequestV4("DELETE", getDeletePolicyURL("", nilBucket),
		0, nil, "", "")

	if err != nil {
		t.Errorf("Minio %s: Failed to create HTTP request for testing the response when object Layer is set to `nil`.", instanceType)
	}
	// execute the object layer set to `nil` test.
	// `ExecObjectLayerAPINilTest` manages the operation.
	ExecObjectLayerAPINilTest(t, nilBucket, "", instanceType, apiRouter, nilReq)
}

// TestBucketPolicyConditionMatch - Tests to validate whether bucket policy conditions match.
func TestBucketPolicyConditionMatch(t *testing.T) {
	// obtain the inner map[string]set.StringSet for policy.Statement.Conditions.
	getInnerMap := func(key2, value string) map[string]set.StringSet {
		innerMap := make(map[string]set.StringSet)
		innerMap[key2] = set.CreateStringSet(value)
		return innerMap
	}

	// obtain policy.Statement with Conditions set.
	getStatementWithCondition := func(key1, key2, value string) policy.Statement {
		innerMap := getInnerMap(key2, value)
		// to set policyStatment.Conditions .
		conditions := make(policy.ConditionMap)
		conditions[key1] = innerMap
		// new policy statement.
		statement := policy.Statement{}
		// set the condition.
		statement.Conditions = conditions
		return statement
	}

	testCases := []struct {
		statementCondition policy.Statement
		condition          map[string]set.StringSet

		expectedMatch bool
	}{

		// Test case - 1.
		// StringEquals condition matches.
		{

			statementCondition: getStatementWithCondition("StringEquals", "s3:prefix", "Asia/"),
			condition:          getInnerMap("prefix", "Asia/"),

			expectedMatch: true,
		},
		// Test case - 2.
		// StringEquals condition doesn't match.
		{

			statementCondition: getStatementWithCondition("StringEquals", "s3:prefix", "Asia/"),
			condition:          getInnerMap("prefix", "Africa/"),

			expectedMatch: false,
		},
		// Test case - 3.
		// StringEquals condition matches.
		{

			statementCondition: getStatementWithCondition("StringEquals", "s3:max-keys", "Asia/"),
			condition:          getInnerMap("max-keys", "Asia/"),

			expectedMatch: true,
		},
		// Test case - 4.
		// StringEquals condition doesn't match.
		{

			statementCondition: getStatementWithCondition("StringEquals", "s3:max-keys", "Asia/"),
			condition:          getInnerMap("max-keys", "Africa/"),

			expectedMatch: false,
		},
		// Test case - 5.
		// StringNotEquals condition matches.
		{

			statementCondition: getStatementWithCondition("StringNotEquals", "s3:prefix", "Asia/"),
			condition:          getInnerMap("prefix", "Asia/"),

			expectedMatch: false,
		},
		// Test case - 6.
		// StringNotEquals condition doesn't match.
		{

			statementCondition: getStatementWithCondition("StringNotEquals", "s3:prefix", "Asia/"),
			condition:          getInnerMap("prefix", "Africa/"),

			expectedMatch: true,
		},
		// Test case - 7.
		// StringNotEquals condition matches.
		{

			statementCondition: getStatementWithCondition("StringNotEquals", "s3:max-keys", "Asia/"),
			condition:          getInnerMap("max-keys", "Asia/"),

			expectedMatch: false,
		},
		// Test case - 8.
		// StringNotEquals condition doesn't match.
		{

			statementCondition: getStatementWithCondition("StringNotEquals", "s3:max-keys", "Asia/"),
			condition:          getInnerMap("max-keys", "Africa/"),

			expectedMatch: true,
		},
		// Test case - 9.
		// StringLike condition matches.
		{
			statementCondition: getStatementWithCondition("StringLike", "aws:Referer", "http://www.example.com/"),
			condition:          getInnerMap("referer", "http://www.example.com/"),
			expectedMatch:      true,
		},
		// Test case - 10.
		// StringLike condition doesn't match.
		{
			statementCondition: getStatementWithCondition("StringLike", "aws:Referer", "http://www.example.com/"),
			condition:          getInnerMap("referer", "www.somethingelse.com"),
			expectedMatch:      false,
		},
		// Test case - 11.
		// StringNotLike condition evaluates to false.
		{
			statementCondition: getStatementWithCondition("StringNotLike", "aws:Referer", "http://www.example.com/"),
			condition:          getInnerMap("referer", "http://www.example.com/"),
			expectedMatch:      false,
		},
		// Test case - 12.
		// StringNotLike condition evaluates to true.
		{
			statementCondition: getStatementWithCondition("StringNotLike", "aws:Referer", "http://www.example.com/"),
			condition:          getInnerMap("referer", "http://somethingelse.com/"),
			expectedMatch:      true,
		},
		// Test case 13.
		// IpAddress condition evaluates to true.
		{
			statementCondition: getStatementWithCondition("IpAddress", "aws:SourceIp", "54.240.143.0/24"),
			condition:          getInnerMap("ip", "54.240.143.2"),
			expectedMatch:      true,
		},
		// Test case 14.
		// IpAddress condition evaluates to false.
		{
			statementCondition: getStatementWithCondition("IpAddress", "aws:SourceIp", "54.240.143.0/24"),
			condition:          getInnerMap("ip", "127.240.143.224"),
			expectedMatch:      false,
		},
		// Test case 15.
		// NotIpAddress condition evaluates to true.
		{
			statementCondition: getStatementWithCondition("NotIpAddress", "aws:SourceIp", "54.240.143.0/24"),
			condition:          getInnerMap("ip", "54.240.144.188"),
			expectedMatch:      true,
		},
		// Test case 16.
		// NotIpAddress condition evaluates to false.
		{
			statementCondition: getStatementWithCondition("NotIpAddress", "aws:SourceIp", "54.240.143.0/24"),
			condition:          getInnerMap("ip", "54.240.143.243"),
			expectedMatch:      false,
		},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("Case %d", i+1), func(t *testing.T) {
			// call the function under test and assert the result with the expected result.
			doesMatch := bucketPolicyConditionMatch(tc.condition, tc.statementCondition)
			if tc.expectedMatch != doesMatch {
				t.Errorf("Expected the match to be `%v`; got `%v` - %v %v.",
					tc.expectedMatch, doesMatch, tc.condition, tc.statementCondition)
			}
		})
	}
}

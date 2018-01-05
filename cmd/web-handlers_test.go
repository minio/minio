/*
 * Minio Cloud Storage, (C) 2016, 2017 Minio, Inc.
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
	"archive/zip"
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"

	jwtgo "github.com/dgrijalva/jwt-go"
	humanize "github.com/dustin/go-humanize"
	"github.com/minio/minio-go/pkg/policy"
	"github.com/minio/minio-go/pkg/set"
	"github.com/minio/minio/pkg/hash"
)

// Tests private function writeWebErrorResponse.
func TestWriteWebErrorResponse(t *testing.T) {
	var buffer bytes.Buffer
	testCases := []struct {
		webErr     error
		apiErrCode APIErrorCode
	}{
		// List of various errors and their corresponding API errors.
		{
			webErr:     StorageFull{},
			apiErrCode: ErrStorageFull,
		},
		{
			webErr:     BucketNotFound{},
			apiErrCode: ErrNoSuchBucket,
		},
		{
			webErr:     BucketNameInvalid{},
			apiErrCode: ErrInvalidBucketName,
		},
		{
			webErr:     hash.BadDigest{},
			apiErrCode: ErrBadDigest,
		},
		{
			webErr:     IncompleteBody{},
			apiErrCode: ErrIncompleteBody,
		},
		{
			webErr:     ObjectExistsAsDirectory{},
			apiErrCode: ErrObjectExistsAsDirectory,
		},
		{
			webErr:     ObjectNotFound{},
			apiErrCode: ErrNoSuchKey,
		},
		{
			webErr:     ObjectNameInvalid{},
			apiErrCode: ErrNoSuchKey,
		},
		{
			webErr:     InsufficientWriteQuorum{},
			apiErrCode: ErrWriteQuorum,
		},
		{
			webErr:     InsufficientReadQuorum{},
			apiErrCode: ErrReadQuorum,
		},
		{
			webErr:     NotImplemented{},
			apiErrCode: ErrNotImplemented,
		},
	}

	// Validate all the test cases.
	for i, testCase := range testCases {
		writeWebErrorResponse(newFlushWriter(&buffer), testCase.webErr)
		desc := getAPIError(testCase.apiErrCode).Description
		if testCase.apiErrCode == ErrNotImplemented {
			desc = "Functionality not implemented"
		}
		recvDesc := buffer.Bytes()
		// Check if the written desc is same as the one expected.
		if !bytes.Equal(recvDesc, []byte(desc)) {
			t.Errorf("Test %d: Unexpected response, expecting %s, got %s", i+1, desc, string(buffer.Bytes()))
		}
		buffer.Reset()
	}
}

// Authenticate and get JWT token - will be called before every webrpc handler invocation
func getWebRPCToken(apiRouter http.Handler, accessKey, secretKey string) (token string, err error) {
	rec := httptest.NewRecorder()
	request := LoginArgs{Username: accessKey, Password: secretKey}
	reply := &LoginRep{}
	req, err := newTestWebRPCRequest("Web"+loginMethodName, "", request)
	if err != nil {
		return "", err
	}
	apiRouter.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		return "", errors.New("Auth failed")
	}
	err = getTestWebRPCResponse(rec, &reply)
	if err != nil {
		return "", err
	}
	if reply.Token == "" {
		return "", errors.New("Auth failed")
	}
	return reply.Token, nil
}

// Wrapper for calling Login Web Handler
func TestWebHandlerLogin(t *testing.T) {
	ExecObjectLayerTest(t, testLoginWebHandler)
}

// testLoginWebHandler - Test login web handler
func testLoginWebHandler(obj ObjectLayer, instanceType string, t TestErrHandler) {
	// Register the API end points with XL/FS object layer.
	apiRouter := initTestWebRPCEndPoint(obj)
	credentials := globalServerConfig.GetCredential()

	// test cases with sample input and expected output.
	testCases := []struct {
		username string
		password string
		success  bool
	}{
		{"", "", false},
		{"azerty", "foo", false},
		{"", "foo", false},
		{"azerty", "", false},
		{"azerty", "foo", false},
		{"azerty", "azerty123", false},
		{credentials.AccessKey, credentials.SecretKey, true},
	}

	// Iterating over the test cases, calling the function under test and asserting the response.
	for i, testCase := range testCases {
		_, err := getWebRPCToken(apiRouter, testCase.username, testCase.password)
		if err != nil && testCase.success {
			t.Fatalf("Test %d: Expected to succeed but it failed, %v", i+1, err)
		}
		if err == nil && !testCase.success {
			t.Fatalf("Test %d: Expected to fail but it didn't, %v", i+1, err)
		}

	}
}

// Wrapper for calling StorageInfo Web Handler
func TestWebHandlerStorageInfo(t *testing.T) {
	ExecObjectLayerTest(t, testStorageInfoWebHandler)
}

// testStorageInfoWebHandler - Test StorageInfo web handler
func testStorageInfoWebHandler(obj ObjectLayer, instanceType string, t TestErrHandler) {
	// get random bucket name.
	// Register the API end points with XL/FS object layer.
	apiRouter := initTestWebRPCEndPoint(obj)
	credentials := globalServerConfig.GetCredential()

	authorization, err := getWebRPCToken(apiRouter, credentials.AccessKey, credentials.SecretKey)
	if err != nil {
		t.Fatal("Cannot authenticate")
	}

	rec := httptest.NewRecorder()

	storageInfoRequest := AuthRPCArgs{}
	storageInfoReply := &StorageInfoRep{}
	req, err := newTestWebRPCRequest("Web.StorageInfo", authorization, storageInfoRequest)
	if err != nil {
		t.Fatalf("Failed to create HTTP request: <ERROR> %v", err)
	}
	apiRouter.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("Expected the response status to be 200, but instead found `%d`", rec.Code)
	}
	err = getTestWebRPCResponse(rec, &storageInfoReply)
	if err != nil {
		t.Fatalf("Failed %v", err)
	}
	if storageInfoReply.StorageInfo.Total <= 0 {
		t.Fatalf("Got a zero or negative total free space disk")
	}
}

// Wrapper for calling ServerInfo Web Handler
func TestWebHandlerServerInfo(t *testing.T) {
	ExecObjectLayerTest(t, testServerInfoWebHandler)
}

// testServerInfoWebHandler - Test ServerInfo web handler
func testServerInfoWebHandler(obj ObjectLayer, instanceType string, t TestErrHandler) {
	// Register the API end points with XL/FS object layer.
	apiRouter := initTestWebRPCEndPoint(obj)
	credentials := globalServerConfig.GetCredential()

	authorization, err := getWebRPCToken(apiRouter, credentials.AccessKey, credentials.SecretKey)
	if err != nil {
		t.Fatal("Cannot authenticate")
	}

	rec := httptest.NewRecorder()

	serverInfoRequest := AuthRPCArgs{}
	serverInfoReply := &ServerInfoRep{}
	req, err := newTestWebRPCRequest("Web.ServerInfo", authorization, serverInfoRequest)
	if err != nil {
		t.Fatalf("Failed to create HTTP request: <ERROR> %v", err)
	}
	apiRouter.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("Expected the response status to be 200, but instead found `%d`", rec.Code)
	}
	err = getTestWebRPCResponse(rec, &serverInfoReply)
	if err != nil {
		t.Fatalf("Failed, %v", err)
	}
	if serverInfoReply.MinioVersion != Version {
		t.Fatalf("Cannot get minio version from server info handler")
	}
	globalInfo := getGlobalInfo()
	if !reflect.DeepEqual(serverInfoReply.MinioGlobalInfo, globalInfo) {
		t.Fatalf("Global info did not match got %#v, expected %#v", serverInfoReply.MinioGlobalInfo, globalInfo)
	}
}

// Wrapper for calling MakeBucket Web Handler
func TestWebHandlerMakeBucket(t *testing.T) {
	ExecObjectLayerTest(t, testMakeBucketWebHandler)
}

// testMakeBucketWebHandler - Test MakeBucket web handler
func testMakeBucketWebHandler(obj ObjectLayer, instanceType string, t TestErrHandler) {
	// Register the API end points with XL/FS object layer.
	apiRouter := initTestWebRPCEndPoint(obj)
	credentials := globalServerConfig.GetCredential()

	authorization, err := getWebRPCToken(apiRouter, credentials.AccessKey, credentials.SecretKey)
	if err != nil {
		t.Fatal("Cannot authenticate")
	}

	rec := httptest.NewRecorder()

	bucketName := getRandomBucketName()

	testCases := []struct {
		bucketName string
		success    bool
	}{
		{"", false},
		{".", false},
		{"ab", false},
		{"minio", false},
		{minioMetaBucket, false},
		{bucketName, true},
	}

	for i, testCase := range testCases {
		makeBucketRequest := MakeBucketArgs{BucketName: testCase.bucketName}
		makeBucketReply := &WebGenericRep{}
		req, err := newTestWebRPCRequest("Web.MakeBucket", authorization, makeBucketRequest)
		if err != nil {
			t.Fatalf("Test %d: Failed to create HTTP request: <ERROR> %v", i+1, err)
		}
		apiRouter.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("Test %d: Expected the response status to be 200, but instead found `%d`", i+1, rec.Code)
		}
		err = getTestWebRPCResponse(rec, &makeBucketReply)
		if testCase.success && err != nil {
			t.Fatalf("Test %d: Should succeed but it didn't, %v", i+1, err)
		}
		if !testCase.success && err == nil {
			t.Fatalf("Test %d: Should fail but it didn't (%s)", i+1, testCase.bucketName)
		}
	}
}

// Wrapper for calling DeleteBucket handler
func TestWebHandlerDeleteBucket(t *testing.T) {
	ExecObjectLayerTest(t, testDeleteBucketWebHandler)
}

// testDeleteBucketWebHandler - Test DeleteBucket web handler
func testDeleteBucketWebHandler(obj ObjectLayer, instanceType string, t TestErrHandler) {
	apiRouter := initTestWebRPCEndPoint(obj)

	credentials := globalServerConfig.GetCredential()
	token, err := getWebRPCToken(apiRouter, credentials.AccessKey, credentials.SecretKey)
	if err != nil {
		t.Fatalf("could not get RPC token, %s", err.Error())
	}

	bucketName := getRandomBucketName()
	err = obj.MakeBucketWithLocation(bucketName, "")
	if err != nil {
		t.Fatalf("failed to create bucket: %s (%s)", err.Error(), instanceType)
	}

	testCases := []struct {
		bucketName string
		// Whether or not to put an object into the bucket.
		initWithObject bool
		token          string
		// Expected error (error must only contain this string to pass test)
		// Empty string = no error
		expect string
	}{
		{"", false, token, "Bucket Name  is invalid"},
		{".", false, "auth", "Authentication failed"},
		{".", false, token, "Bucket Name . is invalid"},
		{"ab", false, token, "Bucket Name ab is invalid"},
		{"minio", false, "false token", "Authentication failed"},
		{"minio", false, token, "specified bucket minio does not exist"},
		{bucketName, false, token, ""},
		{bucketName, true, token, "Bucket not empty"},
		{bucketName, false, "", "Authentication failed"},
	}

	for _, test := range testCases {
		if test.initWithObject {
			data := bytes.NewBufferString("hello")
			_, err = obj.PutObject(test.bucketName, "object", mustGetHashReader(t, data, int64(data.Len()), "", ""), nil)
			// _, err = obj.PutObject(test.bucketName, "object", int64(data.Len()), data, nil, "")
			if err != nil {
				t.Fatalf("could not put object to %s, %s", test.bucketName, err.Error())
			}
		}

		rec := httptest.NewRecorder()

		makeBucketRequest := MakeBucketArgs{BucketName: test.bucketName}
		makeBucketReply := &WebGenericRep{}

		req, err := newTestWebRPCRequest("Web.DeleteBucket", test.token, makeBucketRequest)
		if err != nil {
			t.Errorf("failed to create HTTP request: <ERROR> %v", err)
		}

		apiRouter.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Errorf("expected the response status to be `%d`, but instead found `%d`", http.StatusOK, rec.Code)
		}
		err = getTestWebRPCResponse(rec, &makeBucketReply)

		if test.expect != "" {
			if err == nil {
				// If we expected an error, but didn't get one.
				t.Errorf("expected `..%s..` but got nil error", test.expect)
			} else if !strings.Contains(err.Error(), test.expect) {
				// If we got an error that wasn't what we expected.
				t.Errorf("expected `..%s..` but got `%s`", test.expect, err.Error())
			}
		} else if test.expect == "" && err != nil {
			t.Errorf("expected test success, but got `%s`", err.Error())
		}

		// If we created the bucket with an object, now delete the object to cleanup.
		if test.initWithObject {
			err = obj.DeleteObject(test.bucketName, "object")
			if err != nil {
				t.Fatalf("could not delete object, %s", err.Error())
			}
		}

		// If it did not succeed in deleting the bucket, don't try and recreate it.
		// Or, it'll fail if there was an object.
		if err != nil || test.initWithObject {
			continue
		}

		err = obj.MakeBucketWithLocation(bucketName, "")
		if err != nil {
			// failed to create new bucket, abort.
			t.Fatalf("failed to create new bucket (%s): %s", instanceType, err.Error())
		}
	}
}

// Wrapper for calling ListBuckets Web Handler
func TestWebHandlerListBuckets(t *testing.T) {
	ExecObjectLayerTest(t, testListBucketsWebHandler)
}

// testListBucketsHandler - Test ListBuckets web handler
func testListBucketsWebHandler(obj ObjectLayer, instanceType string, t TestErrHandler) {
	// Register the API end points with XL/FS object layer.
	apiRouter := initTestWebRPCEndPoint(obj)
	credentials := globalServerConfig.GetCredential()

	authorization, err := getWebRPCToken(apiRouter, credentials.AccessKey, credentials.SecretKey)
	if err != nil {
		t.Fatal("Cannot authenticate")
	}

	rec := httptest.NewRecorder()

	bucketName := getRandomBucketName()
	// Create bucket.
	err = obj.MakeBucketWithLocation(bucketName, "")
	if err != nil {
		// failed to create newbucket, abort.
		t.Fatalf("%s : %s", instanceType, err)
	}

	listBucketsRequest := WebGenericArgs{}
	listBucketsReply := &ListBucketsRep{}
	req, err := newTestWebRPCRequest("Web.ListBuckets", authorization, listBucketsRequest)
	if err != nil {
		t.Fatalf("Failed to create HTTP request: <ERROR> %v", err)
	}
	apiRouter.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("Expected the response status to be 200, but instead found `%d`", rec.Code)
	}
	err = getTestWebRPCResponse(rec, &listBucketsReply)
	if err != nil {
		t.Fatalf("Failed, %v", err)
	}
	if len(listBucketsReply.Buckets) == 0 {
		t.Fatalf("Cannot find the bucket already created by MakeBucket")
	}
	if listBucketsReply.Buckets[0].Name != bucketName {
		t.Fatalf("Found another bucket other than already created by MakeBucket")
	}
}

// Wrapper for calling ListObjects Web Handler
func TestWebHandlerListObjects(t *testing.T) {
	ExecObjectLayerTest(t, testListObjectsWebHandler)
}

// testListObjectsHandler - Test ListObjects web handler
func testListObjectsWebHandler(obj ObjectLayer, instanceType string, t TestErrHandler) {
	// Register the API end points with XL/FS object layer.
	apiRouter := initTestWebRPCEndPoint(obj)
	credentials := globalServerConfig.GetCredential()

	rec := httptest.NewRecorder()

	authorization, err := getWebRPCToken(apiRouter, credentials.AccessKey, credentials.SecretKey)
	if err != nil {
		t.Fatal("Cannot authenticate")
	}

	bucketName := getRandomBucketName()
	objectName := "object"
	objectSize := 1 * humanize.KiByte

	// Create bucket.
	err = obj.MakeBucketWithLocation(bucketName, "")
	if err != nil {
		// failed to create newbucket, abort.
		t.Fatalf("%s : %s", instanceType, err)
	}

	data := bytes.Repeat([]byte("a"), objectSize)
	metadata := map[string]string{"etag": "c9a34cfc85d982698c6ac89f76071abd"}
	_, err = obj.PutObject(bucketName, objectName, mustGetHashReader(t, bytes.NewReader(data), int64(len(data)), metadata["etag"], ""), metadata)

	if err != nil {
		t.Fatalf("Was not able to upload an object, %v", err)
	}

	test := func(token string) (error, *ListObjectsRep) {
		listObjectsRequest := ListObjectsArgs{BucketName: bucketName, Prefix: ""}
		listObjectsReply := &ListObjectsRep{}
		var req *http.Request
		req, err = newTestWebRPCRequest("Web.ListObjects", token, listObjectsRequest)
		if err != nil {
			t.Fatalf("Failed to create HTTP request: <ERROR> %v", err)
		}
		apiRouter.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			return fmt.Errorf("Expected the response status to be 200, but instead found `%d`", rec.Code), listObjectsReply
		}
		err = getTestWebRPCResponse(rec, &listObjectsReply)
		if err != nil {
			return err, listObjectsReply
		}
		return nil, listObjectsReply
	}
	verifyReply := func(reply *ListObjectsRep) {
		if len(reply.Objects) == 0 {
			t.Fatalf("Cannot find the object")
		}
		if reply.Objects[0].Key != objectName {
			t.Fatalf("Found another object other than already created by PutObject")
		}
		if reply.Objects[0].Size != int64(objectSize) {
			t.Fatalf("Found a object with the same name but with a different size")
		}
	}

	// Authenticated ListObjects should succeed.
	err, reply := test(authorization)
	if err != nil {
		t.Fatal(err)
	}
	verifyReply(reply)

	// Unauthenticated ListObjects should fail.
	err, _ = test("")
	if err == nil {
		t.Fatalf("Expected error `%s`", err)
	}

	policy := policy.BucketAccessPolicy{
		Version:    "1.0",
		Statements: []policy.Statement{getReadOnlyObjectStatement(bucketName, "")},
	}

	globalBucketPolicies.SetBucketPolicy(bucketName, policyChange{false, policy})

	// Unauthenticated ListObjects with READ bucket policy should succeed.
	err, reply = test("")
	if err != nil {
		t.Fatal(err)
	}
	verifyReply(reply)
}

// Wrapper for calling RemoveObject Web Handler
func TestWebHandlerRemoveObject(t *testing.T) {
	ExecObjectLayerTest(t, testRemoveObjectWebHandler)
}

// testRemoveObjectWebHandler - Test RemoveObjectObject web handler
func testRemoveObjectWebHandler(obj ObjectLayer, instanceType string, t TestErrHandler) {
	// Register the API end points with XL/FS object layer.
	apiRouter := initTestWebRPCEndPoint(obj)
	credentials := globalServerConfig.GetCredential()

	rec := httptest.NewRecorder()
	authorization, err := getWebRPCToken(apiRouter, credentials.AccessKey, credentials.SecretKey)
	if err != nil {
		t.Fatal("Cannot authenticate")
	}

	bucketName := getRandomBucketName()
	objectName := "object"
	objectSize := 1 * humanize.KiByte

	// Create bucket.
	err = obj.MakeBucketWithLocation(bucketName, "")
	if err != nil {
		// failed to create newbucket, abort.
		t.Fatalf("%s : %s", instanceType, err)
	}

	data := bytes.Repeat([]byte("a"), objectSize)
	metadata := map[string]string{"etag": "c9a34cfc85d982698c6ac89f76071abd"}
	_, err = obj.PutObject(bucketName, objectName, mustGetHashReader(t, bytes.NewReader(data), int64(len(data)), metadata["etag"], ""), metadata)
	if err != nil {
		t.Fatalf("Was not able to upload an object, %v", err)
	}

	objectName = "a/object"
	metadata = map[string]string{"etag": "c9a34cfc85d982698c6ac89f76071abd"}
	_, err = obj.PutObject(bucketName, objectName, mustGetHashReader(t, bytes.NewReader(data), int64(len(data)), metadata["etag"], ""), metadata)
	if err != nil {
		t.Fatalf("Was not able to upload an object, %v", err)
	}

	removeRequest := RemoveObjectArgs{BucketName: bucketName, Objects: []string{"a/", "object"}}
	removeReply := &WebGenericRep{}
	req, err := newTestWebRPCRequest("Web.RemoveObject", authorization, removeRequest)
	if err != nil {
		t.Fatalf("Failed to create HTTP request: <ERROR> %v", err)
	}
	apiRouter.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("Expected the response status to be 200, but instead found `%d`", rec.Code)
	}
	err = getTestWebRPCResponse(rec, &removeReply)
	if err != nil {
		t.Fatalf("Failed, %v", err)
	}

	removeRequest = RemoveObjectArgs{BucketName: bucketName, Objects: []string{"a/", "object"}}
	removeReply = &WebGenericRep{}
	req, err = newTestWebRPCRequest("Web.RemoveObject", authorization, removeRequest)
	if err != nil {
		t.Fatalf("Failed to create HTTP request: <ERROR> %v", err)
	}
	apiRouter.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("Expected the response status to be 200, but instead found `%d`", rec.Code)
	}
	err = getTestWebRPCResponse(rec, &removeReply)
	if err != nil {
		t.Fatalf("Failed, %v", err)
	}

	removeRequest = RemoveObjectArgs{BucketName: bucketName}
	removeReply = &WebGenericRep{}
	req, err = newTestWebRPCRequest("Web.RemoveObject", authorization, removeRequest)
	if err != nil {
		t.Fatalf("Failed to create HTTP request: <ERROR> %v", err)
	}
	apiRouter.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("Expected the response status to be 200, but instead found `%d`", rec.Code)
	}
	b, err := ioutil.ReadAll(rec.Body)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(b, []byte("Invalid arguments specified")) {
		t.Fatalf("Expected response wrong %s", string(b))
	}
}

// Wrapper for calling Generate Auth Handler
func TestWebHandlerGenerateAuth(t *testing.T) {
	ExecObjectLayerTest(t, testGenerateAuthWebHandler)
}

// testGenerateAuthWebHandler - Test GenerateAuth web handler
func testGenerateAuthWebHandler(obj ObjectLayer, instanceType string, t TestErrHandler) {
	// Register the API end points with XL/FS object layer.
	apiRouter := initTestWebRPCEndPoint(obj)
	credentials := globalServerConfig.GetCredential()

	rec := httptest.NewRecorder()
	authorization, err := getWebRPCToken(apiRouter, credentials.AccessKey, credentials.SecretKey)
	if err != nil {
		t.Fatal("Cannot authenticate")
	}

	generateAuthRequest := WebGenericArgs{}
	generateAuthReply := &GenerateAuthReply{}
	req, err := newTestWebRPCRequest("Web.GenerateAuth", authorization, generateAuthRequest)
	if err != nil {
		t.Fatalf("Failed to create HTTP request: <ERROR> %v", err)
	}
	apiRouter.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("Expected the response status to be 200, but instead found `%d`", rec.Code)
	}
	err = getTestWebRPCResponse(rec, &generateAuthReply)
	if err != nil {
		t.Fatalf("Failed, %v", err)
	}

	if generateAuthReply.AccessKey == "" || generateAuthReply.SecretKey == "" {
		t.Fatalf("Failed to generate auth keys")
	}
}

// Wrapper for calling Set Auth Handler
func TestWebHandlerSetAuth(t *testing.T) {
	ExecObjectLayerTest(t, testSetAuthWebHandler)
}

// testSetAuthWebHandler - Test SetAuth web handler
func testSetAuthWebHandler(obj ObjectLayer, instanceType string, t TestErrHandler) {
	// Register the API end points with XL/FS object layer.
	apiRouter := initTestWebRPCEndPoint(obj)
	credentials := globalServerConfig.GetCredential()

	rec := httptest.NewRecorder()
	authorization, err := getWebRPCToken(apiRouter, credentials.AccessKey, credentials.SecretKey)
	if err != nil {
		t.Fatal("Cannot authenticate")
	}

	testCases := []struct {
		username string
		password string
		success  bool
	}{
		{"", "", false},
		{"1", "1", false},
		{"azerty", "foooooooooooooo", true},
	}

	// Iterating over the test cases, calling the function under test and asserting the response.
	for i, testCase := range testCases {
		setAuthRequest := SetAuthArgs{AccessKey: testCase.username, SecretKey: testCase.password}
		setAuthReply := &SetAuthReply{}
		req, err := newTestWebRPCRequest("Web.SetAuth", authorization, setAuthRequest)
		if err != nil {
			t.Fatalf("Test %d: Failed to create HTTP request: <ERROR> %v", i+1, err)
		}
		apiRouter.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("Test %d: Expected the response status to be 200, but instead found `%d`", i+1, rec.Code)
		}
		err = getTestWebRPCResponse(rec, &setAuthReply)
		if testCase.success && err != nil {
			t.Fatalf("Test %d: Supposed to succeed but failed, %v", i+1, err)
		}
		if !testCase.success && err == nil {
			t.Fatalf("Test %d: Supposed to fail but succeeded, %v", i+1, err)
		}
		if testCase.success && setAuthReply.Token == "" {
			t.Fatalf("Test %d: Failed to set auth keys", i+1)
		}
	}
}

// Wrapper for calling Get Auth Handler
func TestWebHandlerGetAuth(t *testing.T) {
	ExecObjectLayerTest(t, testGetAuthWebHandler)
}

// testGetAuthWebHandler - Test GetAuth web handler
func testGetAuthWebHandler(obj ObjectLayer, instanceType string, t TestErrHandler) {
	// Register the API end points with XL/FS object layer.
	apiRouter := initTestWebRPCEndPoint(obj)
	credentials := globalServerConfig.GetCredential()

	rec := httptest.NewRecorder()
	authorization, err := getWebRPCToken(apiRouter, credentials.AccessKey, credentials.SecretKey)
	if err != nil {
		t.Fatal("Cannot authenticate")
	}

	getAuthRequest := WebGenericArgs{}
	getAuthReply := &GetAuthReply{}
	req, err := newTestWebRPCRequest("Web.GetAuth", authorization, getAuthRequest)
	if err != nil {
		t.Fatalf("Failed to create HTTP request: <ERROR> %v", err)
	}
	apiRouter.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("Expected the response status to be 200, but instead found `%d`", rec.Code)
	}
	err = getTestWebRPCResponse(rec, &getAuthReply)
	if err != nil {
		t.Fatalf("Failed, %v", err)
	}
	if getAuthReply.AccessKey != credentials.AccessKey || getAuthReply.SecretKey != credentials.SecretKey {
		t.Fatalf("Failed to get correct auth keys")
	}
}

func TestWebCreateURLToken(t *testing.T) {
	ExecObjectLayerTest(t, testCreateURLToken)
}

func getTokenString(accessKey, secretKey string) (string, error) {
	utcNow := UTCNow()
	token := jwtgo.NewWithClaims(jwtgo.SigningMethodHS512, jwtgo.StandardClaims{
		ExpiresAt: utcNow.Add(defaultJWTExpiry).Unix(),
		IssuedAt:  utcNow.Unix(),
		Subject:   accessKey,
	})
	return token.SignedString([]byte(secretKey))
}

func testCreateURLToken(obj ObjectLayer, instanceType string, t TestErrHandler) {
	apiRouter := initTestWebRPCEndPoint(obj)
	credentials := globalServerConfig.GetCredential()

	authorization, err := getWebRPCToken(apiRouter, credentials.AccessKey, credentials.SecretKey)
	if err != nil {
		t.Fatal(err)
	}

	args := WebGenericArgs{}
	tokenReply := &URLTokenReply{}

	req, err := newTestWebRPCRequest("Web.CreateURLToken", authorization, args)
	if err != nil {
		t.Fatal(err)
	}

	rec := httptest.NewRecorder()
	apiRouter.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("Expected the response status to be 200, but instead found `%d`", rec.Code)
	}

	err = getTestWebRPCResponse(rec, &tokenReply)
	if err != nil {
		t.Fatal(err)
	}

	// Ensure the token is valid now. It will expire later.
	if !isAuthTokenValid(tokenReply.Token) {
		t.Fatalf("token is not valid")
	}

	// Token is invalid.
	if isAuthTokenValid("") {
		t.Fatalf("token shouldn't be valid, but it is")
	}

	token, err := getTokenString("invalid-access", credentials.SecretKey)
	if err != nil {
		t.Fatal(err)
	}

	// Token has invalid access key.
	if isAuthTokenValid(token) {
		t.Fatalf("token shouldn't be valid, but it is")
	}
}

// Wrapper for calling Upload Handler
func TestWebHandlerUpload(t *testing.T) {
	ExecObjectLayerTest(t, testUploadWebHandler)
}

// testUploadWebHandler - Test Upload web handler
func testUploadWebHandler(obj ObjectLayer, instanceType string, t TestErrHandler) {
	// Register the API end points with XL/FS object layer.
	apiRouter := initTestWebRPCEndPoint(obj)
	credentials := globalServerConfig.GetCredential()

	content := []byte("temporary file's content")
	authorization, err := getWebRPCToken(apiRouter, credentials.AccessKey, credentials.SecretKey)
	if err != nil {
		t.Fatal("Cannot authenticate")
	}

	objectName := "test.file"
	bucketName := getRandomBucketName()

	test := func(token string, sendContentLength bool) int {
		rec := httptest.NewRecorder()
		req, rErr := http.NewRequest("PUT", "/minio/upload/"+bucketName+"/"+objectName, nil)
		if rErr != nil {
			t.Fatalf("Cannot create upload request, %v", rErr)
		}

		req.Header.Set("x-amz-date", "20160814T114029Z")
		req.Header.Set("Accept", "*/*")

		req.Body = ioutil.NopCloser(bytes.NewReader(content))

		if !sendContentLength {
			req.ContentLength = -1
		} else {
			req.ContentLength = int64(len(content))
		}

		if token != "" {
			req.Header.Set("Authorization", "Bearer "+authorization)
		}
		apiRouter.ServeHTTP(rec, req)
		return rec.Code
	}
	// Create bucket.
	err = obj.MakeBucketWithLocation(bucketName, "")
	if err != nil {
		// failed to create newbucket, abort.
		t.Fatalf("%s : %s", instanceType, err)
	}

	// Authenticated upload should succeed.
	code := test(authorization, true)
	if code != http.StatusOK {
		t.Fatalf("Expected the response status to be 200, but instead found `%d`", code)
	}

	var byteBuffer bytes.Buffer
	err = obj.GetObject(bucketName, objectName, 0, int64(len(content)), &byteBuffer)
	if err != nil {
		t.Fatalf("Failed, %v", err)
	}

	if !bytes.Equal(byteBuffer.Bytes(), content) {
		t.Fatalf("The upload file is different from the download file")
	}

	// Authenticated upload without content-length should fail
	code = test(authorization, false)
	if code != http.StatusBadRequest {
		t.Fatalf("Expected the response status to be 200, but instead found `%d`", code)
	}

	// Unauthenticated upload should fail.
	code = test("", true)
	if code != http.StatusForbidden {
		t.Fatalf("Expected the response status to be 403, but instead found `%d`", code)
	}

	bp := policy.BucketAccessPolicy{
		Version:    "1.0",
		Statements: []policy.Statement{getWriteOnlyObjectStatement(bucketName, "")},
	}

	globalBucketPolicies.SetBucketPolicy(bucketName, policyChange{false, bp})

	// Unauthenticated upload with WRITE policy should succeed.
	code = test("", true)
	if code != http.StatusOK {
		t.Fatalf("Expected the response status to be 200, but instead found `%d`", code)
	}
}

// Wrapper for calling Download Handler
func TestWebHandlerDownload(t *testing.T) {
	ExecObjectLayerTest(t, testDownloadWebHandler)
}

// testDownloadWebHandler - Test Download web handler
func testDownloadWebHandler(obj ObjectLayer, instanceType string, t TestErrHandler) {
	// Register the API end points with XL/FS object layer.
	apiRouter := initTestWebRPCEndPoint(obj)
	credentials := globalServerConfig.GetCredential()

	authorization, err := getWebRPCToken(apiRouter, credentials.AccessKey, credentials.SecretKey)
	if err != nil {
		t.Fatal("Cannot authenticate")
	}

	objectName := "test.file"
	bucketName := getRandomBucketName()

	test := func(token string) (int, []byte) {
		rec := httptest.NewRecorder()
		path := "/minio/download/" + bucketName + "/" + objectName + "?token="
		if token != "" {
			path = path + token
		}
		var req *http.Request
		req, err = http.NewRequest("GET", path, nil)

		if err != nil {
			t.Fatalf("Cannot create upload request, %v", err)
		}

		apiRouter.ServeHTTP(rec, req)
		return rec.Code, rec.Body.Bytes()
	}

	// Create bucket.
	err = obj.MakeBucketWithLocation(bucketName, "")
	if err != nil {
		// failed to create newbucket, abort.
		t.Fatalf("%s : %s", instanceType, err)
	}

	content := []byte("temporary file's content")
	metadata := map[string]string{"etag": "01ce59706106fe5e02e7f55fffda7f34"}
	_, err = obj.PutObject(bucketName, objectName, mustGetHashReader(t, bytes.NewReader(content), int64(len(content)), metadata["etag"], ""), metadata)
	if err != nil {
		t.Fatalf("Was not able to upload an object, %v", err)
	}

	// Authenticated download should succeed.
	code, bodyContent := test(authorization)

	if code != http.StatusOK {
		t.Fatalf("Expected the response status to be 200, but instead found `%d`", code)
	}

	if !bytes.Equal(bodyContent, content) {
		t.Fatalf("The downloaded file is corrupted")
	}

	// Temporary token should succeed.
	tmpToken, err := authenticateURL(credentials.AccessKey, credentials.SecretKey)
	if err != nil {
		t.Fatal(err)
	}

	code, bodyContent = test(tmpToken)

	if code != http.StatusOK {
		t.Fatalf("Expected the response status to be 200, but instead found `%d`", code)
	}

	if !bytes.Equal(bodyContent, content) {
		t.Fatalf("The downloaded file is corrupted")
	}

	// Old token should fail.
	code, bodyContent = test("eyJhbGciOiJIUzUxMiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE1MDAzMzIwOTUsImlhdCI6MTUwMDMzMjAzNSwic3ViIjoiRFlLSU01VlRZNDBJMVZQSE5VMTkifQ.tXQ45GJc8eOFet_a4VWVyeqJEOPWybotQYNr2zVxBpEOICkGbu_YWGhd9TkLLe1E65oeeiLHPdXSN8CzcbPoRA")
	if code != http.StatusForbidden {
		t.Fatalf("Expected the response status to be 403, but instead found `%d`", code)
	}

	if !bytes.Equal(bodyContent, bytes.NewBufferString("Authentication failed, check your access credentials").Bytes()) {
		t.Fatalf("Expected authentication error message, got %v", bodyContent)
	}

	// Unauthenticated download should fail.
	code, _ = test("")
	if code != http.StatusForbidden {
		t.Fatalf("Expected the response status to be 403, but instead found `%d`", code)
	}

	bp := policy.BucketAccessPolicy{
		Version:    "1.0",
		Statements: []policy.Statement{getReadOnlyObjectStatement(bucketName, "")},
	}

	globalBucketPolicies.SetBucketPolicy(bucketName, policyChange{false, bp})

	// Unauthenticated download with READ policy should succeed.
	code, bodyContent = test("")
	if code != http.StatusOK {
		t.Fatalf("Expected the response status to be 200, but instead found `%d`", code)
	}

	if !bytes.Equal(bodyContent, content) {
		t.Fatalf("The downloaded file is corrupted")
	}
}

// Test web.DownloadZip
func TestWebHandlerDownloadZip(t *testing.T) {
	ExecObjectLayerTest(t, testWebHandlerDownloadZip)
}

func testWebHandlerDownloadZip(obj ObjectLayer, instanceType string, t TestErrHandler) {
	apiRouter := initTestWebRPCEndPoint(obj)
	credentials := globalServerConfig.GetCredential()

	authorization, err := authenticateURL(credentials.AccessKey, credentials.SecretKey)
	if err != nil {
		t.Fatal("Cannot authenticate")
	}

	bucket := getRandomBucketName()
	fileOne := "aaaaaaaaaaaaaa"
	fileTwo := "bbbbbbbbbbbbbb"
	fileThree := "cccccccccccccc"

	// Create bucket.
	err = obj.MakeBucketWithLocation(bucket, "")
	if err != nil {
		// failed to create newbucket, abort.
		t.Fatalf("%s : %s", instanceType, err)
	}

	obj.PutObject(bucket, "a/one", mustGetHashReader(t, strings.NewReader(fileOne), int64(len(fileOne)), "", ""), nil)
	obj.PutObject(bucket, "a/b/two", mustGetHashReader(t, strings.NewReader(fileTwo), int64(len(fileTwo)), "", ""), nil)
	obj.PutObject(bucket, "a/c/three", mustGetHashReader(t, strings.NewReader(fileThree), int64(len(fileThree)), "", ""), nil)

	test := func(token string) (int, []byte) {
		rec := httptest.NewRecorder()
		path := "/minio/zip" + "?token="
		if token != "" {
			path = path + token
		}
		args := DownloadZipArgs{
			Objects:    []string{"one", "b/", "c/"},
			Prefix:     "a/",
			BucketName: bucket,
		}

		var argsData []byte
		argsData, err = json.Marshal(args)
		if err != nil {
			return 0, nil
		}
		var req *http.Request
		req, err = http.NewRequest("POST", path, bytes.NewBuffer(argsData))

		if err != nil {
			t.Fatalf("Cannot create upload request, %v", err)
		}

		apiRouter.ServeHTTP(rec, req)
		return rec.Code, rec.Body.Bytes()
	}
	code, _ := test("")
	if code != 403 {
		t.Fatal("Expected to receive authentication error")
	}
	code, data := test(authorization)
	if code != 200 {
		t.Fatal("web.DownloadsZip() failed")
	}
	reader, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	h := md5.New()
	for _, file := range reader.File {
		fileReader, err := file.Open()
		if err != nil {
			t.Fatal(err)
		}
		io.Copy(h, fileReader)
	}
	// Verify the md5 of the response.
	if hex.EncodeToString(h.Sum(nil)) != "ac7196449b14bea42775d29e8bb29f50" {
		t.Fatal("Incorrect zip contents")
	}
}

// Wrapper for calling PresignedGet handler
func TestWebHandlerPresignedGetHandler(t *testing.T) {
	ExecObjectLayerTest(t, testWebPresignedGetHandler)
}

func testWebPresignedGetHandler(obj ObjectLayer, instanceType string, t TestErrHandler) {
	// Register the API end points with XL/FS object layer.
	apiRouter := initTestWebRPCEndPoint(obj)
	credentials := globalServerConfig.GetCredential()

	authorization, err := getWebRPCToken(apiRouter, credentials.AccessKey, credentials.SecretKey)
	if err != nil {
		t.Fatal("Cannot authenticate")
	}

	rec := httptest.NewRecorder()

	bucketName := getRandomBucketName()
	objectName := "object"
	objectSize := 1 * humanize.KiByte

	// Create bucket.
	err = obj.MakeBucketWithLocation(bucketName, "")
	if err != nil {
		// failed to create newbucket, abort.
		t.Fatalf("%s : %s", instanceType, err)
	}

	data := bytes.Repeat([]byte("a"), objectSize)
	metadata := map[string]string{"etag": "c9a34cfc85d982698c6ac89f76071abd"}
	_, err = obj.PutObject(bucketName, objectName, mustGetHashReader(t, bytes.NewReader(data), int64(len(data)), metadata["etag"], ""), metadata)
	if err != nil {
		t.Fatalf("Was not able to upload an object, %v", err)
	}

	presignGetReq := PresignedGetArgs{
		HostName:   "",
		BucketName: bucketName,
		ObjectName: objectName,
		Expiry:     1000,
	}
	presignGetRep := &PresignedGetRep{}
	req, err := newTestWebRPCRequest("Web.PresignedGet", authorization, presignGetReq)
	if err != nil {
		t.Fatalf("Failed to create HTTP request: <ERROR> %v", err)
	}
	apiRouter.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("Expected the response status to be 200, but instead found `%d`", rec.Code)
	}
	err = getTestWebRPCResponse(rec, &presignGetRep)
	if err != nil {
		t.Fatalf("Failed, %v", err)
	}

	// Register the API end points with XL/FS object layer.
	apiRouter = initTestAPIEndPoints(obj, []string{"GetObject"})

	// Initialize a new api recorder.
	arec := httptest.NewRecorder()

	req, err = newTestRequest("GET", presignGetRep.URL, 0, nil)
	req.Header.Del("x-amz-content-sha256")
	if err != nil {
		t.Fatal("Failed to initialized a new request", err)
	}
	apiRouter.ServeHTTP(arec, req)
	if arec.Code != http.StatusOK {
		t.Fatalf("Expected the response status to be 200, but instead found `%d`", arec.Code)
	}
	savedData, err := ioutil.ReadAll(arec.Body)
	if err != nil {
		t.Fatal("Reading body failed", err)
	}
	if !bytes.Equal(data, savedData) {
		t.Fatal("Read data is not equal was what was expected")
	}

	// Register the API end points with XL/FS object layer.
	apiRouter = initTestWebRPCEndPoint(obj)

	presignGetReq = PresignedGetArgs{
		HostName:   "",
		BucketName: "",
		ObjectName: "",
	}
	presignGetRep = &PresignedGetRep{}
	req, err = newTestWebRPCRequest("Web.PresignedGet", authorization, presignGetReq)
	if err != nil {
		t.Fatalf("Failed to create HTTP request: <ERROR> %v", err)
	}
	apiRouter.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("Expected the response status to be 200, but instead found `%d`", rec.Code)
	}
	err = getTestWebRPCResponse(rec, &presignGetRep)
	if err == nil {
		t.Fatalf("Failed, %v", err)
	}
	if err.Error() != "Bucket and Object are mandatory arguments." {
		t.Fatalf("Unexpected, expected `Bucket and Object are mandatory arguments`, got %s", err)
	}
}

// Wrapper for calling GetBucketPolicy Handler
func TestWebHandlerGetBucketPolicyHandler(t *testing.T) {
	ExecObjectLayerTest(t, testWebGetBucketPolicyHandler)
}

// testWebGetBucketPolicyHandler - Test GetBucketPolicy web handler
func testWebGetBucketPolicyHandler(obj ObjectLayer, instanceType string, t TestErrHandler) {
	// Register the API end points with XL/FS object layer.
	apiRouter := initTestWebRPCEndPoint(obj)
	credentials := globalServerConfig.GetCredential()

	authorization, err := getWebRPCToken(apiRouter, credentials.AccessKey, credentials.SecretKey)
	if err != nil {
		t.Fatal("Cannot authenticate")
	}

	rec := httptest.NewRecorder()

	bucketName := getRandomBucketName()
	if err := obj.MakeBucketWithLocation(bucketName, ""); err != nil {
		t.Fatal("Unexpected error: ", err)
	}

	policyVal := policy.BucketAccessPolicy{
		Version: "2012-10-17",
		Statements: []policy.Statement{
			{
				Actions: set.CreateStringSet("s3:GetBucketLocation", "s3:ListBucket"),
				Effect:  "Allow",
				Principal: policy.User{
					AWS: set.CreateStringSet("*"),
				},
				Resources: set.CreateStringSet(bucketARNPrefix + bucketName),
				Sid:       "",
			},
			{
				Actions: set.CreateStringSet("s3:GetObject"),
				Effect:  "Allow",
				Principal: policy.User{
					AWS: set.CreateStringSet("*"),
				},
				Resources: set.CreateStringSet(bucketARNPrefix + bucketName + "/*"),
				Sid:       "",
			},
		},
	}
	if err := writeBucketPolicy(bucketName, obj, policyVal); err != nil {
		t.Fatal("Unexpected error: ", err)
	}

	testCases := []struct {
		bucketName     string
		prefix         string
		expectedResult policy.BucketPolicy
	}{
		{bucketName, "", policy.BucketPolicyReadOnly},
	}

	for i, testCase := range testCases {
		args := &GetBucketPolicyArgs{BucketName: testCase.bucketName, Prefix: testCase.prefix}
		reply := &GetBucketPolicyRep{}
		req, err := newTestWebRPCRequest("Web.GetBucketPolicy", authorization, args)
		if err != nil {
			t.Fatalf("Test %d: Failed to create HTTP request: <ERROR> %v", i+1, err)
		}
		apiRouter.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("Test %d: Expected the response status to be 200, but instead found `%d`", i+1, rec.Code)
		}
		if err = getTestWebRPCResponse(rec, &reply); err != nil {
			t.Fatalf("Test %d: Should succeed but it didn't, %v", i+1, err)
		}
		if testCase.expectedResult != reply.Policy {
			t.Fatalf("Test %d: expected: %v, got: %v", i+1, testCase.expectedResult, reply.Policy)
		}
	}
}

// Wrapper for calling ListAllBucketPolicies Handler
func TestWebHandlerListAllBucketPoliciesHandler(t *testing.T) {
	ExecObjectLayerTest(t, testWebListAllBucketPoliciesHandler)
}

// testWebListAllBucketPoliciesHandler - Test ListAllBucketPolicies web handler
func testWebListAllBucketPoliciesHandler(obj ObjectLayer, instanceType string, t TestErrHandler) {
	// Register the API end points with XL/FS object layer.
	apiRouter := initTestWebRPCEndPoint(obj)
	credentials := globalServerConfig.GetCredential()

	authorization, err := getWebRPCToken(apiRouter, credentials.AccessKey, credentials.SecretKey)
	if err != nil {
		t.Fatal("Cannot authenticate")
	}

	rec := httptest.NewRecorder()

	bucketName := getRandomBucketName()
	if err := obj.MakeBucketWithLocation(bucketName, ""); err != nil {
		t.Fatal("Unexpected error: ", err)
	}

	stringEqualsConditions := policy.ConditionMap{}
	stringEqualsConditions["StringEquals"] = make(policy.ConditionKeyMap)
	stringEqualsConditions["StringEquals"].Add("s3:prefix", set.CreateStringSet("hello"))

	policyVal := policy.BucketAccessPolicy{
		Version: "2012-10-17",
		Statements: []policy.Statement{
			{
				Actions:   set.CreateStringSet("s3:GetBucketLocation"),
				Effect:    "Allow",
				Principal: policy.User{AWS: set.CreateStringSet("*")},
				Resources: set.CreateStringSet(bucketARNPrefix + bucketName),
				Sid:       "",
			},
			{
				Actions:    set.CreateStringSet("s3:ListBucket"),
				Conditions: stringEqualsConditions,
				Effect:     "Allow",
				Principal:  policy.User{AWS: set.CreateStringSet("*")},
				Resources:  set.CreateStringSet(bucketARNPrefix + bucketName),
				Sid:        "",
			},
			{
				Actions:   set.CreateStringSet("s3:ListBucketMultipartUploads"),
				Effect:    "Allow",
				Principal: policy.User{AWS: set.CreateStringSet("*")},
				Resources: set.CreateStringSet(bucketARNPrefix + bucketName),
				Sid:       "",
			},
			{
				Actions: set.CreateStringSet("s3:AbortMultipartUpload", "s3:DeleteObject",
					"s3:GetObject", "s3:ListMultipartUploadParts", "s3:PutObject"),
				Effect:    "Allow",
				Principal: policy.User{AWS: set.CreateStringSet("*")},
				Resources: set.CreateStringSet(bucketARNPrefix + bucketName + "/hello*"),
				Sid:       "",
			},
		},
	}
	if err := writeBucketPolicy(bucketName, obj, policyVal); err != nil {
		t.Fatal("Unexpected error: ", err)
	}

	testCaseResult1 := []BucketAccessPolicy{{
		Prefix: bucketName + "/hello*",
		Policy: policy.BucketPolicyReadWrite,
	}}
	testCases := []struct {
		bucketName     string
		expectedResult []BucketAccessPolicy
	}{
		{bucketName, testCaseResult1},
	}

	for i, testCase := range testCases {
		args := &ListAllBucketPoliciesArgs{BucketName: testCase.bucketName}
		reply := &ListAllBucketPoliciesRep{}
		req, err := newTestWebRPCRequest("Web.ListAllBucketPolicies", authorization, args)
		if err != nil {
			t.Fatalf("Test %d: Failed to create HTTP request: <ERROR> %v", i+1, err)
		}
		apiRouter.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("Test %d: Expected the response status to be 200, but instead found `%d`", i+1, rec.Code)
		}
		if err = getTestWebRPCResponse(rec, &reply); err != nil {
			t.Fatalf("Test %d: Should succeed but it didn't, %v", i+1, err)
		}
		if !reflect.DeepEqual(testCase.expectedResult, reply.Policies) {
			t.Fatalf("Test %d: expected: %v, got: %v", i+1, testCase.expectedResult, reply.Policies)
		}
	}
}

// Wrapper for calling SetBucketPolicy Handler
func TestWebHandlerSetBucketPolicyHandler(t *testing.T) {
	ExecObjectLayerTest(t, testWebSetBucketPolicyHandler)
}

// testWebSetBucketPolicyHandler - Test SetBucketPolicy web handler
func testWebSetBucketPolicyHandler(obj ObjectLayer, instanceType string, t TestErrHandler) {
	// Register the API end points with XL/FS object layer.
	apiRouter := initTestWebRPCEndPoint(obj)
	credentials := globalServerConfig.GetCredential()

	authorization, err := getWebRPCToken(apiRouter, credentials.AccessKey, credentials.SecretKey)
	if err != nil {
		t.Fatal("Cannot authenticate")
	}

	rec := httptest.NewRecorder()

	// Create a bucket
	bucketName := getRandomBucketName()
	if err = obj.MakeBucketWithLocation(bucketName, ""); err != nil {
		t.Fatal("Unexpected error: ", err)
	}

	testCases := []struct {
		bucketName string
		prefix     string
		policy     string
		pass       bool
	}{
		// Invalid bucket name
		{"", "", "readonly", false},
		// Invalid policy
		{bucketName, "", "foo", false},
		// Valid parameters
		{bucketName, "", "readwrite", true},
		// None is valid and policy should be removed.
		{bucketName, "", "none", true},
		// Setting none again meants should return an error.
		{bucketName, "", "none", false},
	}

	for i, testCase := range testCases {
		args := &SetBucketPolicyArgs{BucketName: testCase.bucketName, Prefix: testCase.prefix, Policy: testCase.policy}
		reply := &WebGenericRep{}
		// Call SetBucketPolicy RPC
		req, err := newTestWebRPCRequest("Web.SetBucketPolicy", authorization, args)
		if err != nil {
			t.Fatalf("Test %d: Failed to create HTTP request: <ERROR> %v", i+1, err)
		}
		apiRouter.ServeHTTP(rec, req)
		// Check if we have 200 OK
		if testCase.pass && rec.Code != http.StatusOK {
			t.Fatalf("Test %d: Expected the response status to be 200, but instead found `%d`", i+1, rec.Code)
		}
		// Parse RPC response
		err = getTestWebRPCResponse(rec, &reply)
		if testCase.pass && err != nil {
			t.Fatalf("Test %d: Should succeed but it didn't, %#v", i+1, err)
		}
		if !testCase.pass && err == nil {
			t.Fatalf("Test %d: Should fail it didn't", i+1)
		}
	}
}

// TestWebCheckAuthorization - Test Authorization for all web handlers
func TestWebCheckAuthorization(t *testing.T) {
	// Prepare XL backend
	obj, fsDirs, err := prepareXL16()
	if err != nil {
		t.Fatalf("Initialization of object layer failed for XL setup: %s", err)
	}
	// Executing the object layer tests for XL.
	defer removeRoots(fsDirs)

	// Register the API end points with XL/FS object layer.
	apiRouter := initTestWebRPCEndPoint(obj)
	// initialize the server and obtain the credentials and root.
	// credentials are necessary to sign the HTTP request.
	rootPath, err := newTestConfig(globalMinioDefaultRegion)
	if err != nil {
		t.Fatal("Init Test config failed", err)
	}
	// remove the root directory after the test ends.
	defer os.RemoveAll(rootPath)

	rec := httptest.NewRecorder()

	// Check if web rpc calls return unauthorized request with an incorrect token
	webRPCs := []string{
		"ServerInfo", "StorageInfo", "MakeBucket",
		"ListBuckets", "ListObjects", "RemoveObject",
		"GenerateAuth", "SetAuth", "GetAuth",
		"GetBucketPolicy", "SetBucketPolicy", "ListAllBucketPolicies",
		"PresignedGet",
	}
	for _, rpcCall := range webRPCs {
		args := &AuthRPCArgs{}
		reply := &WebGenericRep{}
		req, nerr := newTestWebRPCRequest("Web."+rpcCall, "Bearer fooauthorization", args)
		if nerr != nil {
			t.Fatalf("Test %s: Failed to create HTTP request: <ERROR> %v", rpcCall, nerr)
		}
		apiRouter.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("Test %s: Expected the response status to be 200, but instead found `%d`", rpcCall, rec.Code)
		}
		err = getTestWebRPCResponse(rec, &reply)
		if err == nil {
			t.Fatalf("Test %s: Should fail", rpcCall)
		} else {
			if !strings.Contains(err.Error(), errAuthentication.Error()) {
				t.Fatalf("Test %s: should fail with Unauthorized request. Found error: %v", rpcCall, err)
			}
		}
	}

	rec = httptest.NewRecorder()
	// Test authorization of Web.Download
	req, err := http.NewRequest("GET", "/minio/download/bucket/object?token=wrongauth", nil)
	if err != nil {
		t.Fatalf("Cannot create upload request, %v", err)
	}
	apiRouter.ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("Expected the response status to be 403, but instead found `%d`", rec.Code)
	}
	resp := string(rec.Body.Bytes())
	if !strings.EqualFold(resp, errAuthentication.Error()) {
		t.Fatalf("Unexpected error message, expected: %s, found: `%s`", errAuthentication, resp)
	}

	rec = httptest.NewRecorder()
	// Test authorization of Web.Upload
	content := []byte("temporary file's content")
	req, err = http.NewRequest("PUT", "/minio/upload/bucket/object", nil)
	req.Header.Set("Authorization", "Bearer foo-authorization")
	req.Header.Set("Content-Length", strconv.Itoa(len(content)))
	req.Header.Set("x-amz-date", "20160814T114029Z")
	req.Header.Set("Accept", "*/*")
	req.Body = ioutil.NopCloser(bytes.NewReader(content))
	if err != nil {
		t.Fatalf("Cannot create upload request, %v", err)
	}
	apiRouter.ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("Expected the response status to be 403, but instead found `%d`", rec.Code)
	}
	resp = string(rec.Body.Bytes())
	if !strings.EqualFold(resp, errAuthentication.Error()) {
		t.Fatalf("Unexpected error message, expected: `%s`, found: `%s`", errAuthentication, resp)
	}
}

// TestWebObjectLayerNotReady - Test RPCs responses when disks are not ready
func TestWebObjectLayerNotReady(t *testing.T) {
	// Initialize web rpc endpoint.
	apiRouter := initTestWebRPCEndPoint(nil)

	// initialize the server and obtain the credentials and root.
	// credentials are necessary to sign the HTTP request.
	rootPath, err := newTestConfig(globalMinioDefaultRegion)
	if err != nil {
		t.Fatal("Init Test config failed", err)
	}
	// remove the root directory after the test ends.
	defer os.RemoveAll(rootPath)

	rec := httptest.NewRecorder()

	credentials := globalServerConfig.GetCredential()
	authorization, err := getWebRPCToken(apiRouter, credentials.AccessKey, credentials.SecretKey)
	if err != nil {
		t.Fatal("Cannot authenticate", err)
	}

	// Check if web rpc calls return Server not initialized. ServerInfo, GenerateAuth,
	// SetAuth and GetAuth are not concerned
	webRPCs := []string{"StorageInfo", "MakeBucket", "ListBuckets", "ListObjects", "RemoveObject",
		"GetBucketPolicy", "SetBucketPolicy", "ListAllBucketPolicies"}
	for _, rpcCall := range webRPCs {
		args := &AuthRPCArgs{}
		reply := &WebGenericRep{}
		req, nerr := newTestWebRPCRequest("Web."+rpcCall, authorization, args)
		if nerr != nil {
			t.Fatalf("Test %s: Failed to create HTTP request: <ERROR> %v", rpcCall, nerr)
		}
		apiRouter.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("Test %s: Expected the response status to be 200, but instead found `%d`", rpcCall, rec.Code)
		}
		err = getTestWebRPCResponse(rec, &reply)
		if err == nil {
			t.Fatalf("Test %s: Should fail", rpcCall)
		} else {
			if !strings.EqualFold(err.Error(), errServerNotInitialized.Error()) {
				t.Fatalf("Test %s: should fail with %s Found error: %v", rpcCall, errServerNotInitialized, err)
			}
		}
	}

	rec = httptest.NewRecorder()
	// Test authorization of Web.Download
	req, err := http.NewRequest("GET", "/minio/download/bucket/object?token="+authorization, nil)
	if err != nil {
		t.Fatalf("Cannot create upload request, %v", err)
	}
	apiRouter.ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("Expected the response status to be 503, but instead found `%d`", rec.Code)
	}
	resp := string(rec.Body.Bytes())
	if !strings.EqualFold(resp, errServerNotInitialized.Error()) {
		t.Fatalf("Unexpected error message, expected: `%s`, found: `%s`", errServerNotInitialized, resp)
	}

	rec = httptest.NewRecorder()
	// Test authorization of Web.Upload
	content := []byte("temporary file's content")
	req, err = http.NewRequest("PUT", "/minio/upload/bucket/object", nil)
	req.Header.Set("Authorization", "Bearer "+authorization)
	req.Header.Set("Content-Length", strconv.Itoa(len(content)))
	req.Header.Set("x-amz-date", "20160814T114029Z")
	req.Header.Set("Accept", "*/*")
	req.Body = ioutil.NopCloser(bytes.NewReader(content))
	if err != nil {
		t.Fatalf("Cannot create upload request, %v", err)
	}
	apiRouter.ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("Expected the response status to be 503, but instead found `%d`", rec.Code)
	}
	resp = string(rec.Body.Bytes())
	if !strings.EqualFold(resp, errServerNotInitialized.Error()) {
		t.Fatalf("Unexpected error message, expected: `%s`, found: `%s`", errServerNotInitialized, resp)
	}
}

// TestWebObjectLayerFaultyDisks - Test Web RPC responses with faulty disks
func TestWebObjectLayerFaultyDisks(t *testing.T) {
	root, err := newTestConfig(globalMinioDefaultRegion)
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(root)

	// Prepare XL backend
	obj, fsDirs, err := prepareXL16()
	if err != nil {
		t.Fatalf("Initialization of object layer failed for XL setup: %s", err)
	}
	// Executing the object layer tests for XL.
	defer removeRoots(fsDirs)

	bucketName := "mybucket"
	err = obj.MakeBucketWithLocation(bucketName, "")
	if err != nil {
		t.Fatal("Cannot make bucket:", err)
	}

	// Set faulty disks to XL backend
	xl := obj.(*xlObjects)
	for i, d := range xl.storageDisks {
		xl.storageDisks[i] = newNaughtyDisk(d.(*retryStorage), nil, errFaultyDisk)
	}

	// Initialize web rpc endpoint.
	apiRouter := initTestWebRPCEndPoint(obj)

	// initialize the server and obtain the credentials and root.
	// credentials are necessary to sign the HTTP request.
	rootPath, err := newTestConfig(globalMinioDefaultRegion)
	if err != nil {
		t.Fatal("Init Test config failed", err)
	}
	// remove the root directory after the test ends.
	defer os.RemoveAll(rootPath)

	rec := httptest.NewRecorder()

	credentials := globalServerConfig.GetCredential()
	authorization, err := getWebRPCToken(apiRouter, credentials.AccessKey, credentials.SecretKey)
	if err != nil {
		t.Fatal("Cannot authenticate", err)
	}

	// Check if web rpc calls return errors with faulty disks.  ServerInfo, GenerateAuth, SetAuth, GetAuth are not concerned
	// RemoveObject is also not concerned since it always returns success.
	webRPCs := []struct {
		webRPCName string
		ReqArgs    interface{}
		RepArgs    interface{}
	}{
		{"MakeBucket", MakeBucketArgs{BucketName: bucketName}, WebGenericRep{}},
		{"ListBuckets", AuthRPCArgs{}, ListBucketsRep{}},
		{"ListObjects", ListObjectsArgs{BucketName: bucketName, Prefix: ""}, ListObjectsRep{}},
		{"GetBucketPolicy", GetBucketPolicyArgs{BucketName: bucketName, Prefix: ""}, GetBucketPolicyRep{}},
		{"SetBucketPolicy", SetBucketPolicyArgs{BucketName: bucketName, Prefix: "", Policy: "none"}, WebGenericRep{}},
	}

	for _, rpcCall := range webRPCs {
		args := &rpcCall.ReqArgs
		reply := &rpcCall.RepArgs
		req, nerr := newTestWebRPCRequest("Web."+rpcCall.webRPCName, authorization, args)
		if nerr != nil {
			t.Fatalf("Test %s: Failed to create HTTP request: <ERROR> %v", rpcCall, nerr)
		}
		apiRouter.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("Test %s: Expected the response status to be 200, but instead found `%d`", rpcCall, rec.Code)
		}
		err = getTestWebRPCResponse(rec, &reply)
		if err == nil {
			t.Errorf("Test %s: Should fail", rpcCall)
		}
	}

	// Test Web.StorageInfo
	storageInfoRequest := AuthRPCArgs{}
	storageInfoReply := &StorageInfoRep{}
	req, err := newTestWebRPCRequest("Web.StorageInfo", authorization, storageInfoRequest)
	if err != nil {
		t.Fatalf("Failed to create HTTP request: <ERROR> %v", err)
	}
	apiRouter.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("Expected the response status to be 200, but instead found `%d`", rec.Code)
	}
	err = getTestWebRPCResponse(rec, &storageInfoReply)
	if err != nil {
		t.Fatalf("Failed %v", err)
	}
	// if Total size is 0 it indicates faulty disk.
	if storageInfoReply.StorageInfo.Total != 0 {
		t.Fatalf("Should get zero Total size since disks are faulty ")
	}

	// Test authorization of Web.Download
	req, err = http.NewRequest("GET", "/minio/download/bucket/object?token="+authorization, nil)
	if err != nil {
		t.Fatalf("Cannot create upload request, %v", err)
	}
	apiRouter.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("Expected the response status to be 200, but instead found `%d`", rec.Code)
	}

	// Test authorization of Web.Upload
	content := []byte("temporary file's content")
	req, err = http.NewRequest("PUT", "/minio/upload/bucket/object", nil)
	req.Header.Set("Authorization", "Bearer "+authorization)
	req.Header.Set("Content-Length", strconv.Itoa(len(content)))
	req.Header.Set("x-amz-date", "20160814T114029Z")
	req.Header.Set("Accept", "*/*")
	req.Body = ioutil.NopCloser(bytes.NewReader(content))
	if err != nil {
		t.Fatalf("Cannot create upload request, %v", err)
	}
	apiRouter.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("Expected the response status to be 200, but instead found `%d`", rec.Code)
	}
}

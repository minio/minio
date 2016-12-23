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
	"errors"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"testing"

	humanize "github.com/dustin/go-humanize"
	"github.com/minio/minio-go/pkg/policy"
	"github.com/minio/minio-go/pkg/set"
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
			webErr:     BadDigest{},
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
	}

	// Validate all the test cases.
	for i, testCase := range testCases {
		writeWebErrorResponse(newFlushWriter(&buffer), testCase.webErr)
		desc := getAPIError(testCase.apiErrCode).Description
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
	// initialize the server and obtain the credentials and root.
	// credentials are necessary to sign the HTTP request.
	rootPath, err := newTestConfig("us-east-1")
	if err != nil {
		t.Fatalf("Init Test config failed")
	}
	// remove the root directory after the test ends.
	defer removeAll(rootPath)

	credentials := serverConfig.GetCredential()

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
	// initialize the server and obtain the credentials and root.
	// credentials are necessary to sign the HTTP request.
	rootPath, err := newTestConfig("us-east-1")
	if err != nil {
		t.Fatalf("Init Test config failed")
	}
	// remove the root directory after the test ends.
	defer removeAll(rootPath)

	credentials := serverConfig.GetCredential()

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
	// initialize the server and obtain the credentials and root.
	// credentials are necessary to sign the HTTP request.
	rootPath, err := newTestConfig("us-east-1")
	if err != nil {
		t.Fatalf("Init Test config failed")
	}
	// remove the root directory after the test ends.
	defer removeAll(rootPath)

	credentials := serverConfig.GetCredential()

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
}

// Wrapper for calling MakeBucket Web Handler
func TestWebHandlerMakeBucket(t *testing.T) {
	ExecObjectLayerTest(t, testMakeBucketWebHandler)
}

// testMakeBucketWebHandler - Test MakeBucket web handler
func testMakeBucketWebHandler(obj ObjectLayer, instanceType string, t TestErrHandler) {
	// Register the API end points with XL/FS object layer.
	apiRouter := initTestWebRPCEndPoint(obj)
	// initialize the server and obtain the credentials and root.
	// credentials are necessary to sign the HTTP request.
	rootPath, err := newTestConfig("us-east-1")
	if err != nil {
		t.Fatalf("Init Test config failed")
	}
	// remove the root directory after the test ends.
	defer removeAll(rootPath)

	credentials := serverConfig.GetCredential()

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

// Wrapper for calling ListBuckets Web Handler
func TestWebHandlerListBuckets(t *testing.T) {
	ExecObjectLayerTest(t, testListBucketsWebHandler)
}

// testListBucketsHandler - Test ListBuckets web handler
func testListBucketsWebHandler(obj ObjectLayer, instanceType string, t TestErrHandler) {
	// Register the API end points with XL/FS object layer.
	apiRouter := initTestWebRPCEndPoint(obj)
	// initialize the server and obtain the credentials and root.
	// credentials are necessary to sign the HTTP request.
	rootPath, err := newTestConfig("us-east-1")
	if err != nil {
		t.Fatalf("Init Test config failed")
	}
	// remove the root directory after the test ends.
	defer removeAll(rootPath)

	credentials := serverConfig.GetCredential()

	authorization, err := getWebRPCToken(apiRouter, credentials.AccessKey, credentials.SecretKey)
	if err != nil {
		t.Fatal("Cannot authenticate")
	}

	rec := httptest.NewRecorder()

	bucketName := getRandomBucketName()
	// Create bucket.
	err = obj.MakeBucket(bucketName)
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
	// initialize the server and obtain the credentials and root.
	// credentials are necessary to sign the HTTP request.
	rootPath, err := newTestConfig("us-east-1")
	if err != nil {
		t.Fatalf("Init Test config failed")
	}
	// remove the root directory after the test ends.
	defer removeAll(rootPath)

	credentials := serverConfig.GetCredential()

	rec := httptest.NewRecorder()

	authorization, err := getWebRPCToken(apiRouter, credentials.AccessKey, credentials.SecretKey)
	if err != nil {
		t.Fatal("Cannot authenticate")
	}

	bucketName := getRandomBucketName()
	objectName := "object"
	objectSize := 1 * humanize.KiByte

	// Create bucket.
	err = obj.MakeBucket(bucketName)
	if err != nil {
		// failed to create newbucket, abort.
		t.Fatalf("%s : %s", instanceType, err)
	}

	data := bytes.Repeat([]byte("a"), objectSize)

	_, err = obj.PutObject(bucketName, objectName, int64(len(data)), bytes.NewReader(data), map[string]string{"md5Sum": "c9a34cfc85d982698c6ac89f76071abd"}, "")

	if err != nil {
		t.Fatalf("Was not able to upload an object, %v", err)
	}

	listObjectsRequest := ListObjectsArgs{BucketName: bucketName, Prefix: ""}
	listObjectsReply := &ListObjectsRep{}
	req, err := newTestWebRPCRequest("Web.ListObjects", authorization, listObjectsRequest)
	if err != nil {
		t.Fatalf("Failed to create HTTP request: <ERROR> %v", err)
	}
	apiRouter.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("Expected the response status to be 200, but instead found `%d`", rec.Code)
	}
	err = getTestWebRPCResponse(rec, &listObjectsReply)
	if err != nil {
		t.Fatalf("Failed, %v", err)
	}
	if len(listObjectsReply.Objects) == 0 {
		t.Fatalf("Cannot find the object")
	}
	if listObjectsReply.Objects[0].Key != objectName {
		t.Fatalf("Found another object other than already created by PutObject")
	}
	if listObjectsReply.Objects[0].Size != int64(objectSize) {
		t.Fatalf("Found a object with the same name but with a different size")
	}

}

// Wrapper for calling RemoveObject Web Handler
func TestWebHandlerRemoveObject(t *testing.T) {
	ExecObjectLayerTest(t, testRemoveObjectWebHandler)
}

// testRemoveObjectWebHandler - Test RemoveObject web handler
func testRemoveObjectWebHandler(obj ObjectLayer, instanceType string, t TestErrHandler) {
	// Register the API end points with XL/FS object layer.
	apiRouter := initTestWebRPCEndPoint(obj)
	// initialize the server and obtain the credentials and root.
	// credentials are necessary to sign the HTTP request.
	rootPath, err := newTestConfig("us-east-1")
	if err != nil {
		t.Fatalf("Init Test config failed")
	}
	// remove the root directory after the test ends.
	defer removeAll(rootPath)

	credentials := serverConfig.GetCredential()

	rec := httptest.NewRecorder()
	authorization, err := getWebRPCToken(apiRouter, credentials.AccessKey, credentials.SecretKey)
	if err != nil {
		t.Fatal("Cannot authenticate")
	}

	bucketName := getRandomBucketName()
	objectName := "object"
	objectSize := 1 * humanize.KiByte

	// Create bucket.
	err = obj.MakeBucket(bucketName)
	if err != nil {
		// failed to create newbucket, abort.
		t.Fatalf("%s : %s", instanceType, err)
	}

	data := bytes.Repeat([]byte("a"), objectSize)

	_, err = obj.PutObject(bucketName, objectName, int64(len(data)), bytes.NewReader(data),
		map[string]string{"md5Sum": "c9a34cfc85d982698c6ac89f76071abd"}, "")
	if err != nil {
		t.Fatalf("Was not able to upload an object, %v", err)
	}

	removeObjectRequest := RemoveObjectArgs{BucketName: bucketName, ObjectName: objectName}
	removeObjectReply := &WebGenericRep{}
	req, err := newTestWebRPCRequest("Web.RemoveObject", authorization, removeObjectRequest)
	if err != nil {
		t.Fatalf("Failed to create HTTP request: <ERROR> %v", err)
	}
	apiRouter.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("Expected the response status to be 200, but instead found `%d`", rec.Code)
	}
	err = getTestWebRPCResponse(rec, &removeObjectReply)
	if err != nil {
		t.Fatalf("Failed, %v", err)
	}

	removeObjectRequest = RemoveObjectArgs{BucketName: bucketName, ObjectName: objectName}
	removeObjectReply = &WebGenericRep{}
	req, err = newTestWebRPCRequest("Web.RemoveObject", authorization, removeObjectRequest)
	if err != nil {
		t.Fatalf("Failed to create HTTP request: <ERROR> %v", err)
	}
	apiRouter.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("Expected the response status to be 200, but instead found `%d`", rec.Code)
	}
	err = getTestWebRPCResponse(rec, &removeObjectReply)
	if err != nil {
		t.Fatalf("Failed, %v", err)
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
	// initialize the server and obtain the credentials and root.
	// credentials are necessary to sign the HTTP request.
	rootPath, err := newTestConfig("us-east-1")
	if err != nil {
		t.Fatalf("Init Test config failed")
	}
	// remove the root directory after the test ends.
	defer removeAll(rootPath)

	credentials := serverConfig.GetCredential()

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
	// initialize the server and obtain the credentials and root.
	// credentials are necessary to sign the HTTP request.
	rootPath, err := newTestConfig("us-east-1")
	if err != nil {
		t.Fatalf("Init Test config failed")
	}
	// remove the root directory after the test ends.
	defer removeAll(rootPath)

	credentials := serverConfig.GetCredential()

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
	// initialize the server and obtain the credentials and root.
	// credentials are necessary to sign the HTTP request.
	rootPath, err := newTestConfig("us-east-1")
	if err != nil {
		t.Fatalf("Init Test config failed")
	}
	// remove the root directory after the test ends.
	defer removeAll(rootPath)

	credentials := serverConfig.GetCredential()

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

// Wrapper for calling Upload Handler
func TestWebHandlerUpload(t *testing.T) {
	ExecObjectLayerTest(t, testUploadWebHandler)
}

// testUploadWebHandler - Test Upload web handler
func testUploadWebHandler(obj ObjectLayer, instanceType string, t TestErrHandler) {
	// Register the API end points with XL/FS object layer.
	apiRouter := initTestWebRPCEndPoint(obj)
	// initialize the server and obtain the credentials and root.
	// credentials are necessary to sign the HTTP request.
	rootPath, err := newTestConfig("us-east-1")
	if err != nil {
		t.Fatalf("Init Test config failed")
	}
	// remove the root directory after the test ends.
	defer removeAll(rootPath)

	credentials := serverConfig.GetCredential()

	rec := httptest.NewRecorder()
	authorization, err := getWebRPCToken(apiRouter, credentials.AccessKey, credentials.SecretKey)
	if err != nil {
		t.Fatal("Cannot authenticate")
	}

	objectName := "test.file"
	bucketName := getRandomBucketName()
	// Create bucket.
	err = obj.MakeBucket(bucketName)
	if err != nil {
		// failed to create newbucket, abort.
		t.Fatalf("%s : %s", instanceType, err)
	}

	content := []byte("temporary file's content")

	req, err := http.NewRequest("PUT", "/minio/upload/"+bucketName+"/"+objectName, nil)
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

	var byteBuffer bytes.Buffer
	err = obj.GetObject(bucketName, objectName, 0, int64(len(content)), &byteBuffer)
	if err != nil {
		t.Fatalf("Failed, %v", err)
	}

	if bytes.Compare(byteBuffer.Bytes(), content) != 0 {
		t.Fatalf("The upload file is different from the download file")
	}
}

// Wrapper for calling Upload Handler
func TestWebHandlerDownload(t *testing.T) {
	ExecObjectLayerTest(t, testDownloadWebHandler)
}

// testDownloadWebHandler - Test Download web handler
func testDownloadWebHandler(obj ObjectLayer, instanceType string, t TestErrHandler) {
	// Register the API end points with XL/FS object layer.
	apiRouter := initTestWebRPCEndPoint(obj)
	// initialize the server and obtain the credentials and root.
	// credentials are necessary to sign the HTTP request.
	rootPath, err := newTestConfig("us-east-1")
	if err != nil {
		t.Fatalf("Init Test config failed")
	}
	// remove the root directory after the test ends.
	defer removeAll(rootPath)

	credentials := serverConfig.GetCredential()

	rec := httptest.NewRecorder()
	authorization, err := getWebRPCToken(apiRouter, credentials.AccessKey, credentials.SecretKey)
	if err != nil {
		t.Fatal("Cannot authenticate")
	}

	objectName := "test.file"
	bucketName := getRandomBucketName()
	// Create bucket.
	err = obj.MakeBucket(bucketName)
	if err != nil {
		// failed to create newbucket, abort.
		t.Fatalf("%s : %s", instanceType, err)
	}

	content := []byte("temporary file's content")
	_, err = obj.PutObject(bucketName, objectName, int64(len(content)), bytes.NewReader(content), map[string]string{"md5Sum": "01ce59706106fe5e02e7f55fffda7f34"}, "")
	if err != nil {
		t.Fatalf("Was not able to upload an object, %v", err)
	}

	req, err := http.NewRequest("GET", "/minio/download/"+bucketName+"/"+objectName+"?token="+authorization, nil)

	if err != nil {
		t.Fatalf("Cannot create upload request, %v", err)
	}

	apiRouter.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("Expected the response status to be 200, but instead found `%d`", rec.Code)
	}

	if bytes.Compare(rec.Body.Bytes(), content) != 0 {
		t.Fatalf("The downloaded file is corrupted")
	}
}

// Wrapper for calling PresignedGet handler
func TestWebHandlerPresignedGetHandler(t *testing.T) {
	ExecObjectLayerTest(t, testWebPresignedGetHandler)
}

func testWebPresignedGetHandler(obj ObjectLayer, instanceType string, t TestErrHandler) {
	// Register the API end points with XL/FS object layer.
	apiRouter := initTestWebRPCEndPoint(obj)
	// initialize the server and obtain the credentials and root.
	// credentials are necessary to sign the HTTP request.
	rootPath, err := newTestConfig("us-east-1")
	if err != nil {
		t.Fatalf("Init Test config failed")
	}
	// remove the root directory after the test ends.
	defer removeAll(rootPath)

	credentials := serverConfig.GetCredential()

	authorization, err := getWebRPCToken(apiRouter, credentials.AccessKey, credentials.SecretKey)
	if err != nil {
		t.Fatal("Cannot authenticate")
	}

	rec := httptest.NewRecorder()

	bucketName := getRandomBucketName()
	objectName := "object"
	objectSize := 1 * humanize.KiByte

	// Create bucket.
	err = obj.MakeBucket(bucketName)
	if err != nil {
		// failed to create newbucket, abort.
		t.Fatalf("%s : %s", instanceType, err)
	}

	data := bytes.Repeat([]byte("a"), objectSize)
	_, err = obj.PutObject(bucketName, objectName, int64(len(data)), bytes.NewReader(data), map[string]string{"md5Sum": "c9a34cfc85d982698c6ac89f76071abd"}, "")
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
	// initialize the server and obtain the credentials and root.
	// credentials are necessary to sign the HTTP request.
	rootPath, err := newTestConfig("us-east-1")
	if err != nil {
		t.Fatalf("Init Test config failed")
	}
	// remove the root directory after the test ends.
	defer removeAll(rootPath)

	credentials := serverConfig.GetCredential()

	authorization, err := getWebRPCToken(apiRouter, credentials.AccessKey, credentials.SecretKey)
	if err != nil {
		t.Fatal("Cannot authenticate")
	}

	rec := httptest.NewRecorder()

	bucketName := getRandomBucketName()
	if err := obj.MakeBucket(bucketName); err != nil {
		t.Fatal("Unexpected error: ", err)
	}

	policyVal := bucketPolicy{
		Version: "2012-10-17",
		Statements: []policyStatement{
			{
				Actions:   set.CreateStringSet("s3:GetBucketLocation", "s3:ListBucket"),
				Effect:    "Allow",
				Principal: map[string][]string{"AWS": {"*"}},
				Resources: set.CreateStringSet("arn:aws:s3:::" + bucketName),
				Sid:       "",
			},
			{
				Actions:   set.CreateStringSet("s3:GetObject"),
				Effect:    "Allow",
				Principal: map[string][]string{"AWS": {"*"}},
				Resources: set.CreateStringSet("arn:aws:s3:::" + bucketName + "/*"),
				Sid:       "",
			},
		},
	}
	if err := writeBucketPolicy(bucketName, obj, &policyVal); err != nil {
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
	// initialize the server and obtain the credentials and root.
	// credentials are necessary to sign the HTTP request.
	rootPath, err := newTestConfig("us-east-1")
	if err != nil {
		t.Fatalf("Init Test config failed")
	}
	// remove the root directory after the test ends.
	defer removeAll(rootPath)

	credentials := serverConfig.GetCredential()

	authorization, err := getWebRPCToken(apiRouter, credentials.AccessKey, credentials.SecretKey)
	if err != nil {
		t.Fatal("Cannot authenticate")
	}

	rec := httptest.NewRecorder()

	bucketName := getRandomBucketName()
	if err := obj.MakeBucket(bucketName); err != nil {
		t.Fatal("Unexpected error: ", err)
	}

	policyVal := bucketPolicy{
		Version: "2012-10-17",
		Statements: []policyStatement{
			{
				Actions:   set.CreateStringSet("s3:GetBucketLocation"),
				Effect:    "Allow",
				Principal: map[string][]string{"AWS": {"*"}},
				Resources: set.CreateStringSet("arn:aws:s3:::" + bucketName),
				Sid:       "",
			},
			{
				Actions: set.CreateStringSet("s3:ListBucket"),
				Conditions: map[string]map[string]set.StringSet{
					"StringEquals": {
						"s3:prefix": set.CreateStringSet("hello"),
					},
				},
				Effect:    "Allow",
				Principal: map[string][]string{"AWS": {"*"}},
				Resources: set.CreateStringSet("arn:aws:s3:::" + bucketName),
				Sid:       "",
			},
			{
				Actions:   set.CreateStringSet("s3:ListBucketMultipartUploads"),
				Effect:    "Allow",
				Principal: map[string][]string{"AWS": {"*"}},
				Resources: set.CreateStringSet("arn:aws:s3:::" + bucketName),
				Sid:       "",
			},
			{
				Actions: set.CreateStringSet("s3:AbortMultipartUpload", "s3:DeleteObject",
					"s3:GetObject", "s3:ListMultipartUploadParts", "s3:PutObject"),
				Effect:    "Allow",
				Principal: map[string][]string{"AWS": {"*"}},
				Resources: set.CreateStringSet("arn:aws:s3:::" + bucketName + "/hello*"),
				Sid:       "",
			},
		},
	}
	if err := writeBucketPolicy(bucketName, obj, &policyVal); err != nil {
		t.Fatal("Unexpected error: ", err)
	}

	testCaseResult1 := []bucketAccessPolicy{{
		Prefix: bucketName + "/hello*",
		Policy: policy.BucketPolicyReadWrite,
	}}
	testCases := []struct {
		bucketName     string
		expectedResult []bucketAccessPolicy
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
	// initialize the server and obtain the credentials and root.
	// credentials are necessary to sign the HTTP request.
	rootPath, err := newTestConfig("us-east-1")
	if err != nil {
		t.Fatalf("Init Test config failed")
	}
	// remove the root directory after the test ends.
	defer removeAll(rootPath)

	credentials := serverConfig.GetCredential()

	authorization, err := getWebRPCToken(apiRouter, credentials.AccessKey, credentials.SecretKey)
	if err != nil {
		t.Fatal("Cannot authenticate")
	}

	rec := httptest.NewRecorder()

	// Create a bucket
	bucketName := getRandomBucketName()
	if err = obj.MakeBucket(bucketName); err != nil {
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
			t.Fatalf("Test %d: Should succeed but it didn't, %v", i+1, err)
		}
		if !testCase.pass && err == nil {
			t.Fatalf("Test %d: Should fail it didn't", i+1)
		}
	}
}

// TestWebCheckAuthorization - Test Authorization for all web handlers
func TestWebCheckAuthorization(t *testing.T) {
	// Prepare XL backend
	obj, fsDirs, err := prepareXL()
	if err != nil {
		t.Fatalf("Initialization of object layer failed for XL setup: %s", err)
	}
	// Executing the object layer tests for XL.
	defer removeRoots(fsDirs)

	// Register the API end points with XL/FS object layer.
	apiRouter := initTestWebRPCEndPoint(obj)
	// initialize the server and obtain the credentials and root.
	// credentials are necessary to sign the HTTP request.
	rootPath, err := newTestConfig("us-east-1")
	if err != nil {
		t.Fatal("Init Test config failed", err)
	}
	// remove the root directory after the test ends.
	defer removeAll(rootPath)

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
	rootPath, err := newTestConfig("us-east-1")
	if err != nil {
		t.Fatal("Init Test config failed", err)
	}
	// remove the root directory after the test ends.
	defer removeAll(rootPath)

	rec := httptest.NewRecorder()

	credentials := serverConfig.GetCredential()
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
	root, err := newTestConfig("us-east-1")
	if err != nil {
		t.Fatal(err)
	}
	defer removeAll(root)

	// Prepare XL backend
	obj, fsDirs, err := prepareXL()
	if err != nil {
		t.Fatalf("Initialization of object layer failed for XL setup: %s", err)
	}
	// Executing the object layer tests for XL.
	defer removeRoots(fsDirs)

	// Set faulty disks to XL backend
	xl := obj.(*xlObjects)
	for i, d := range xl.storageDisks {
		xl.storageDisks[i] = newNaughtyDisk(d.(*retryStorage), nil, errFaultyDisk)
	}

	// Initialize web rpc endpoint.
	apiRouter := initTestWebRPCEndPoint(obj)

	// initialize the server and obtain the credentials and root.
	// credentials are necessary to sign the HTTP request.
	rootPath, err := newTestConfig("us-east-1")
	if err != nil {
		t.Fatal("Init Test config failed", err)
	}
	// remove the root directory after the test ends.
	defer removeAll(rootPath)

	rec := httptest.NewRecorder()

	credentials := serverConfig.GetCredential()
	authorization, err := getWebRPCToken(apiRouter, credentials.AccessKey, credentials.SecretKey)
	if err != nil {
		t.Fatal("Cannot authenticate", err)
	}

	// Check if web rpc calls return errors with faulty disks.  ServerInfo, GenerateAuth, SetAuth, GetAuth are not concerned
	webRPCs := []string{"MakeBucket", "ListBuckets", "ListObjects", "RemoveObject",
		"GetBucketPolicy", "SetBucketPolicy"}

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
	if storageInfoReply.StorageInfo.Total != -1 || storageInfoReply.StorageInfo.Free != -1 {
		t.Fatalf("Should get negative values of Total and Free since disks are faulty ")
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
	resp := string(rec.Body.Bytes())
	if !strings.Contains(resp, "We encountered an internal error, please try again.") {
		t.Fatalf("Unexpected error message, expected: `Invalid token`, found: `%s`", resp)
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
	resp = string(rec.Body.Bytes())
	if !strings.Contains(resp, "We encountered an internal error, please try again.") {
		t.Fatalf("Unexpected error message, expected: `Invalid token`, found: `%s`", resp)
	}
}

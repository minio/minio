/*
 * MinIO Cloud Storage, (C) 2016, 2017, 2018 MinIO, Inc.
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
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"testing"

	jwtgo "github.com/dgrijalva/jwt-go"
	humanize "github.com/dustin/go-humanize"
	xjwt "github.com/minio/minio/cmd/jwt"
	"github.com/minio/minio/pkg/hash"
)

// Implement a dummy flush writer.
type flushWriter struct {
	io.Writer
}

// Flush writer is a dummy writer compatible with http.Flusher and http.ResponseWriter.
func (f *flushWriter) Flush()                            {}
func (f *flushWriter) Write(b []byte) (n int, err error) { return f.Writer.Write(b) }
func (f *flushWriter) Header() http.Header               { return http.Header{} }
func (f *flushWriter) WriteHeader(code int)              {}

func newFlushWriter(writer io.Writer) http.ResponseWriter {
	return &flushWriter{writer}
}

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
			t.Errorf("Test %d: Unexpected response, expecting %s, got %s", i+1, desc, buffer.String())
		}
		buffer.Reset()
	}
}

// Authenticate and get JWT token - will be called before every webrpc handler invocation
func getWebRPCToken(apiRouter http.Handler, accessKey, secretKey string) (token string, err error) {
	return authenticateWeb(accessKey, secretKey)
}

// Wrapper for calling Login Web Handler
func TestWebHandlerLogin(t *testing.T) {
	ExecObjectLayerTest(t, testLoginWebHandler)
}

// testLoginWebHandler - Test login web handler
func testLoginWebHandler(obj ObjectLayer, instanceType string, t TestErrHandler) {
	// Register the API end points with Erasure/FS object layer.
	apiRouter := initTestWebRPCEndPoint(obj)
	credentials := globalActiveCred

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
	// Register the API end points with Erasure/FS object layer.
	apiRouter := initTestWebRPCEndPoint(obj)
	credentials := globalActiveCred

	authorization, err := getWebRPCToken(apiRouter, credentials.AccessKey, credentials.SecretKey)
	if err != nil {
		t.Fatal("Cannot authenticate")
	}

	rec := httptest.NewRecorder()

	storageInfoRequest := &WebGenericArgs{}
	storageInfoReply := &StorageInfoRep{}
	req, err := newTestWebRPCRequest("Web.StorageInfo", authorization, storageInfoRequest)
	if err != nil {
		t.Fatalf("Failed to create HTTP request: <ERROR> %v", err)
	}
	apiRouter.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("Expected the response status to be 200, but instead found `%d`", rec.Code)
	}
	if err = getTestWebRPCResponse(rec, &storageInfoReply); err != nil {
		t.Fatalf("Failed %v", err)
	}
}

// Wrapper for calling ServerInfo Web Handler
func TestWebHandlerServerInfo(t *testing.T) {
	ExecObjectLayerTest(t, testServerInfoWebHandler)
}

// testServerInfoWebHandler - Test ServerInfo web handler
func testServerInfoWebHandler(obj ObjectLayer, instanceType string, t TestErrHandler) {
	// Register the API end points with Erasure/FS object layer.
	apiRouter := initTestWebRPCEndPoint(obj)
	credentials := globalActiveCred

	authorization, err := getWebRPCToken(apiRouter, credentials.AccessKey, credentials.SecretKey)
	if err != nil {
		t.Fatal("Cannot authenticate")
	}

	rec := httptest.NewRecorder()

	serverInfoRequest := &WebGenericArgs{}
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
	serverInfoReply.MinioGlobalInfo["domains"] = []string(nil)
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
	// Register the API end points with Erasure/FS object layer.
	apiRouter := initTestWebRPCEndPoint(obj)
	credentials := globalActiveCred

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

	credentials := globalActiveCred
	token, err := getWebRPCToken(apiRouter, credentials.AccessKey, credentials.SecretKey)
	if err != nil {
		t.Fatalf("could not get RPC token, %s", err.Error())
	}

	bucketName := getRandomBucketName()
	var opts ObjectOptions

	err = obj.MakeBucketWithLocation(context.Background(), bucketName, BucketOptions{})
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
		{"", false, token, "The specified bucket is not valid"},
		{".", false, "auth", "Authentication failed"},
		{".", false, token, "The specified bucket is not valid"},
		{"..", false, token, "The specified bucket is not valid"},
		{"ab", false, token, "The specified bucket is not valid"},
		{"minio", false, "false token", "Authentication failed"},
		{"minio", false, token, "The specified bucket is not valid"},
		{bucketName, false, token, ""},
		{bucketName, true, token, "The bucket you tried to delete is not empty"},
		{bucketName, false, "", "JWT token missing"},
	}

	for _, test := range testCases {
		if test.initWithObject {
			data := bytes.NewBufferString("hello")
			_, err = obj.PutObject(context.Background(), test.bucketName, "object", mustGetPutObjReader(t, data, int64(data.Len()), "", ""), opts)
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
			_, err = obj.DeleteObject(context.Background(), test.bucketName, "object", ObjectOptions{})
			if err != nil {
				t.Fatalf("could not delete object, %s", err.Error())
			}
		}

		// If it did not succeed in deleting the bucket, don't try and recreate it.
		// Or, it'll fail if there was an object.
		if err != nil || test.initWithObject {
			continue
		}

		err = obj.MakeBucketWithLocation(context.Background(), bucketName, BucketOptions{})
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
	// Register the API end points with Erasure/FS object layer.
	apiRouter := initTestWebRPCEndPoint(obj)
	credentials := globalActiveCred

	authorization, err := getWebRPCToken(apiRouter, credentials.AccessKey, credentials.SecretKey)
	if err != nil {
		t.Fatal("Cannot authenticate")
	}

	rec := httptest.NewRecorder()

	bucketName := getRandomBucketName()
	// Create bucket.
	err = obj.MakeBucketWithLocation(context.Background(), bucketName, BucketOptions{})
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
		t.Fatalf("Found another bucket %q other than already created by MakeBucket", listBucketsReply.Buckets[0].Name)
	}
}

// Wrapper for calling ListObjects Web Handler
func TestWebHandlerListObjects(t *testing.T) {
	ExecObjectLayerTest(t, testListObjectsWebHandler)
}

// testListObjectsHandler - Test ListObjects web handler
func testListObjectsWebHandler(obj ObjectLayer, instanceType string, t TestErrHandler) {
	// Register the API end points with Erasure/FS object layer.
	apiRouter := initTestWebRPCEndPoint(obj)
	credentials := globalActiveCred

	rec := httptest.NewRecorder()

	authorization, err := getWebRPCToken(apiRouter, credentials.AccessKey, credentials.SecretKey)
	if err != nil {
		t.Fatal("Cannot authenticate")
	}

	bucketName := getRandomBucketName()
	objectName := "object"
	objectSize := 1 * humanize.KiByte

	// Create bucket.
	err = obj.MakeBucketWithLocation(context.Background(), bucketName, BucketOptions{})
	if err != nil {
		// failed to create newbucket, abort.
		t.Fatalf("%s : %s", instanceType, err)
	}

	data := bytes.Repeat([]byte("a"), objectSize)
	metadata := map[string]string{"etag": "c9a34cfc85d982698c6ac89f76071abd"}
	_, err = obj.PutObject(context.Background(), bucketName, objectName, mustGetPutObjReader(t, bytes.NewReader(data), int64(len(data)), metadata["etag"], ""), ObjectOptions{UserDefined: metadata})

	if err != nil {
		t.Fatalf("Was not able to upload an object, %v", err)
	}

	test := func(token string) (*ListObjectsRep, error) {
		listObjectsRequest := ListObjectsArgs{BucketName: bucketName, Prefix: ""}
		listObjectsReply := &ListObjectsRep{}
		var req *http.Request
		req, err = newTestWebRPCRequest("Web.ListObjects", token, listObjectsRequest)
		if err != nil {
			return nil, err
		}
		apiRouter.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			return listObjectsReply, fmt.Errorf("Expected the response status to be 200, but instead found `%d`", rec.Code)
		}
		err = getTestWebRPCResponse(rec, &listObjectsReply)
		if err != nil {
			return listObjectsReply, err
		}
		return listObjectsReply, nil
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
	reply, err := test(authorization)
	if err != nil {
		t.Fatal(err)
	}
	verifyReply(reply)

	// Unauthenticated ListObjects should fail.
	_, err = test("")
	if err == nil {
		t.Fatalf("Expected error `%s`", err)
	}
}

// Wrapper for calling RemoveObject Web Handler
func TestWebHandlerRemoveObject(t *testing.T) {
	ExecObjectLayerTest(t, testRemoveObjectWebHandler)
}

// testRemoveObjectWebHandler - Test RemoveObjectObject web handler
func testRemoveObjectWebHandler(obj ObjectLayer, instanceType string, t TestErrHandler) {
	// Register the API end points with Erasure/FS object layer.
	apiRouter := initTestWebRPCEndPoint(obj)
	credentials := globalActiveCred

	rec := httptest.NewRecorder()
	authorization, err := getWebRPCToken(apiRouter, credentials.AccessKey, credentials.SecretKey)
	if err != nil {
		t.Fatal("Cannot authenticate")
	}

	bucketName := getRandomBucketName()
	objectName := "object"
	objectSize := 1 * humanize.KiByte

	// Create bucket.
	err = obj.MakeBucketWithLocation(context.Background(), bucketName, BucketOptions{})
	if err != nil {
		// failed to create newbucket, abort.
		t.Fatalf("%s : %s", instanceType, err)
	}

	data := bytes.Repeat([]byte("a"), objectSize)
	metadata := map[string]string{"etag": "c9a34cfc85d982698c6ac89f76071abd"}
	_, err = obj.PutObject(context.Background(), bucketName, objectName, mustGetPutObjReader(t, bytes.NewReader(data), int64(len(data)), metadata["etag"], ""), ObjectOptions{UserDefined: metadata})
	if err != nil {
		t.Fatalf("Was not able to upload an object, %v", err)
	}

	objectName = "a/object"
	metadata = map[string]string{"etag": "c9a34cfc85d982698c6ac89f76071abd"}
	_, err = obj.PutObject(context.Background(), bucketName, objectName, mustGetPutObjReader(t, bytes.NewReader(data), int64(len(data)), metadata["etag"], ""), ObjectOptions{UserDefined: metadata})
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
	// Register the API end points with Erasure/FS object layer.
	apiRouter := initTestWebRPCEndPoint(obj)
	credentials := globalActiveCred

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

func TestWebCreateURLToken(t *testing.T) {
	ExecObjectLayerTest(t, testCreateURLToken)
}

func getTokenString(accessKey, secretKey string) (string, error) {
	claims := xjwt.NewMapClaims()
	claims.SetExpiry(UTCNow().Add(defaultJWTExpiry))
	claims.SetAccessKey(accessKey)
	token := jwtgo.NewWithClaims(jwtgo.SigningMethodHS512, claims)
	return token.SignedString([]byte(secretKey))
}

func testCreateURLToken(obj ObjectLayer, instanceType string, t TestErrHandler) {
	apiRouter := initTestWebRPCEndPoint(obj)
	credentials := globalActiveCred

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
	// Register the API end points with Erasure/FS object layer.
	apiRouter := initTestWebRPCEndPoint(obj)
	credentials := globalActiveCred

	content := []byte("temporary file's content")
	authorization, err := getWebRPCToken(apiRouter, credentials.AccessKey, credentials.SecretKey)
	if err != nil {
		t.Fatal("Cannot authenticate")
	}

	objectName := "test.file"
	bucketName := getRandomBucketName()

	test := func(token string, sendContentLength bool) int {
		rec := httptest.NewRecorder()
		req, rErr := http.NewRequest("PUT", "/minio/upload/"+bucketName+SlashSeparator+objectName, nil)
		if rErr != nil {
			t.Fatalf("Cannot create upload request, %v", rErr)
		}

		req.Header.Set("x-amz-date", "20160814T114029Z")
		req.Header.Set("Accept", "*/*")
		req.Header.Set("User-Agent", "Mozilla")

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
	err = obj.MakeBucketWithLocation(context.Background(), bucketName, BucketOptions{})
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
	err = obj.GetObject(context.Background(), bucketName, objectName, 0, int64(len(content)), &byteBuffer, "", ObjectOptions{})
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

}

// Wrapper for calling Download Handler
func TestWebHandlerDownload(t *testing.T) {
	ExecObjectLayerTest(t, testDownloadWebHandler)
}

// testDownloadWebHandler - Test Download web handler
func testDownloadWebHandler(obj ObjectLayer, instanceType string, t TestErrHandler) {
	// Register the API end points with Erasure/FS object layer.
	apiRouter := initTestWebRPCEndPoint(obj)
	credentials := globalActiveCred

	authorization, err := getWebRPCToken(apiRouter, credentials.AccessKey, credentials.SecretKey)
	if err != nil {
		t.Fatal("Cannot authenticate")
	}

	objectName := "test.file"
	bucketName := getRandomBucketName()

	test := func(token string) (int, []byte) {
		rec := httptest.NewRecorder()
		path := "/minio/download/" + bucketName + SlashSeparator + objectName + "?token="
		if token != "" {
			path = path + token
		}
		var req *http.Request
		req, err = http.NewRequest("GET", path, nil)

		if err != nil {
			t.Fatalf("Cannot create upload request, %v", err)
		}

		req.Header.Set("User-Agent", "Mozilla")

		apiRouter.ServeHTTP(rec, req)
		return rec.Code, rec.Body.Bytes()
	}

	// Create bucket.
	err = obj.MakeBucketWithLocation(context.Background(), bucketName, BucketOptions{})
	if err != nil {
		// failed to create newbucket, abort.
		t.Fatalf("%s : %s", instanceType, err)
	}

	content := []byte("temporary file's content")
	metadata := map[string]string{"etag": "01ce59706106fe5e02e7f55fffda7f34"}
	_, err = obj.PutObject(context.Background(), bucketName, objectName, mustGetPutObjReader(t, bytes.NewReader(content), int64(len(content)), metadata["etag"], ""), ObjectOptions{UserDefined: metadata})
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
		t.Fatalf("Expected authentication error message, got %s", string(bodyContent))
	}

	// Unauthenticated download should fail.
	code, _ = test("")
	if code != http.StatusForbidden {
		t.Fatalf("Expected the response status to be 403, but instead found `%d`", code)
	}
}

// Test web.DownloadZip
func TestWebHandlerDownloadZip(t *testing.T) {
	ExecObjectLayerTest(t, testWebHandlerDownloadZip)
}

func testWebHandlerDownloadZip(obj ObjectLayer, instanceType string, t TestErrHandler) {
	apiRouter := initTestWebRPCEndPoint(obj)
	credentials := globalActiveCred
	var opts ObjectOptions

	authorization, err := authenticateURL(credentials.AccessKey, credentials.SecretKey)
	if err != nil {
		t.Fatal("Cannot authenticate")
	}

	bucket := getRandomBucketName()
	fileOne := "aaaaaaaaaaaaaa"
	fileTwo := "bbbbbbbbbbbbbb"
	fileThree := "cccccccccccccc"

	// Create bucket.
	err = obj.MakeBucketWithLocation(context.Background(), bucket, BucketOptions{})
	if err != nil {
		// failed to create newbucket, abort.
		t.Fatalf("%s : %s", instanceType, err)
	}

	obj.PutObject(context.Background(), bucket, "a/one", mustGetPutObjReader(t, strings.NewReader(fileOne), int64(len(fileOne)), "", ""), opts)
	obj.PutObject(context.Background(), bucket, "a/b/two", mustGetPutObjReader(t, strings.NewReader(fileTwo), int64(len(fileTwo)), "", ""), opts)
	obj.PutObject(context.Background(), bucket, "a/c/three", mustGetPutObjReader(t, strings.NewReader(fileThree), int64(len(fileThree)), "", ""), opts)

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

		req.Header.Set("User-Agent", "Mozilla")

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
	// Register the API end points with Erasure/FS object layer.
	apiRouter := initTestWebRPCEndPoint(obj)
	credentials := globalActiveCred

	authorization, err := getWebRPCToken(apiRouter, credentials.AccessKey, credentials.SecretKey)
	if err != nil {
		t.Fatal("Cannot authenticate")
	}

	rec := httptest.NewRecorder()

	bucketName := getRandomBucketName()
	objectName := "object"
	objectSize := 1 * humanize.KiByte

	// Create bucket.
	err = obj.MakeBucketWithLocation(context.Background(), bucketName, BucketOptions{})
	if err != nil {
		// failed to create newbucket, abort.
		t.Fatalf("%s : %s", instanceType, err)
	}

	data := bytes.Repeat([]byte("a"), objectSize)
	metadata := map[string]string{"etag": "c9a34cfc85d982698c6ac89f76071abd"}
	_, err = obj.PutObject(context.Background(), bucketName, objectName, mustGetPutObjReader(t, bytes.NewReader(data), int64(len(data)), metadata["etag"], ""), ObjectOptions{UserDefined: metadata})
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

	// Register the API end points with Erasure/FS object layer.
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

	// Register the API end points with Erasure/FS object layer.
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

// TestWebCheckAuthorization - Test Authorization for all web handlers
func TestWebCheckAuthorization(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Prepare Erasure backend
	obj, fsDirs, err := prepareErasure16(ctx)
	if err != nil {
		t.Fatalf("Initialization of object layer failed for Erasure setup: %s", err)
	}
	// Executing the object layer tests for Erasure.
	defer removeRoots(fsDirs)

	// Register the API end points with Erasure/FS object layer.
	apiRouter := initTestWebRPCEndPoint(obj)

	// initialize the server and obtain the credentials and root.
	// credentials are necessary to sign the HTTP request.
	err = newTestConfig(globalMinioDefaultRegion, obj)
	if err != nil {
		t.Fatal("Init Test config failed", err)
	}

	rec := httptest.NewRecorder()

	// Check if web rpc calls return unauthorized request with an incorrect token
	webRPCs := []string{
		"ServerInfo", "StorageInfo", "MakeBucket",
		"ListBuckets", "ListObjects", "RemoveObject",
		"GenerateAuth", "SetAuth",
		"GetBucketPolicy", "SetBucketPolicy", "ListAllBucketPolicies",
		"PresignedGet",
	}
	for _, rpcCall := range webRPCs {
		reply := &WebGenericRep{}
		req, nerr := newTestWebRPCRequest("Web."+rpcCall, "Bearer fooauthorization", &WebGenericArgs{})
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
	req.Header.Set("User-Agent", "Mozilla")
	apiRouter.ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("Expected the response status to be 403, but instead found `%d`", rec.Code)
	}
	resp := rec.Body.String()
	if !strings.EqualFold(resp, errAuthentication.Error()) {
		t.Fatalf("Unexpected error message, expected: %s, found: `%s`", errAuthentication, resp)
	}

	rec = httptest.NewRecorder()
	// Test authorization of Web.Upload
	content := []byte("temporary file's content")
	req, err = http.NewRequest("PUT", "/minio/upload/bucket/object", nil)
	req.Header.Set("Authorization", "Bearer foo-authorization")
	req.Header.Set("User-Agent", "Mozilla")
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
	resp = rec.Body.String()
	if !strings.EqualFold(resp, errAuthentication.Error()) {
		t.Fatalf("Unexpected error message, expected: `%s`, found: `%s`", errAuthentication, resp)
	}
}

// TestWebObjectLayerFaultyDisks - Test Web RPC responses with faulty disks
func TestWebObjectLayerFaultyDisks(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Prepare Erasure backend
	obj, fsDirs, err := prepareErasure16(ctx)
	if err != nil {
		t.Fatalf("Initialization of object layer failed for Erasure setup: %s", err)
	}
	// Executing the object layer tests for Erasure.
	defer removeRoots(fsDirs)

	// initialize the server and obtain the credentials and root.
	// credentials are necessary to sign the HTTP request.
	err = newTestConfig(globalMinioDefaultRegion, obj)
	if err != nil {
		t.Fatal("Init Test config failed", err)
	}

	bucketName := "mybucket"
	err = obj.MakeBucketWithLocation(context.Background(), bucketName, BucketOptions{})
	if err != nil {
		t.Fatal("Cannot make bucket:", err)
	}

	// Set faulty disks to Erasure backend
	z := obj.(*erasureZones)
	xl := z.zones[0].sets[0]
	erasureDisks := xl.getDisks()
	z.zones[0].erasureDisksMu.Lock()
	xl.getDisks = func() []StorageAPI {
		for i, d := range erasureDisks {
			erasureDisks[i] = newNaughtyDisk(d, nil, errFaultyDisk)
		}
		return erasureDisks
	}
	z.zones[0].erasureDisksMu.Unlock()

	// Initialize web rpc endpoint.
	apiRouter := initTestWebRPCEndPoint(obj)

	rec := httptest.NewRecorder()

	credentials := globalActiveCred
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
		{"ListBuckets", WebGenericArgs{}, ListBucketsRep{}},
		{"ListObjects", ListObjectsArgs{BucketName: bucketName, Prefix: ""}, ListObjectsRep{}},
		{"GetBucketPolicy", GetBucketPolicyArgs{BucketName: bucketName, Prefix: ""}, GetBucketPolicyRep{}},
		{"SetBucketPolicy", SetBucketPolicyWebArgs{BucketName: bucketName, Prefix: "", Policy: "none"}, WebGenericRep{}},
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
	storageInfoRequest := &WebGenericArgs{}
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

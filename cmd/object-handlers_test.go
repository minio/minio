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
	"context"
	"crypto/md5"
	"crypto/sha1"
	"encoding/base64"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/dustin/go-humanize"
	"github.com/minio/minio/internal/auth"
	"github.com/minio/minio/internal/hash/sha256"
	xhttp "github.com/minio/minio/internal/http"
	ioutilx "github.com/minio/minio/internal/ioutil"
)

// Type to capture different modifications to API request to simulate failure cases.
type Fault int

const (
	None Fault = iota
	MissingContentLength
	TooBigObject
	TooBigDecodedLength
	BadSignature
	BadMD5
	MissingUploadID
)

// Wrapper for calling HeadObject API handler tests for both Erasure multiple disks and FS single drive setup.
func TestAPIHeadObjectHandler(t *testing.T) {
	ExecObjectLayerAPITest(ExecObjectLayerAPITestArgs{t: t, objAPITest: testAPIHeadObjectHandler, endpoints: []string{"HeadObject"}})
}

func testAPIHeadObjectHandler(obj ObjectLayer, instanceType, bucketName string, apiRouter http.Handler,
	credentials auth.Credentials, t *testing.T,
) {
	objectName := "test-object"
	// set of byte data for PutObject.
	// object has to be created before running tests for HeadObject.
	// this is required even to assert the HeadObject data,
	// since dataInserted === dataFetched back is a primary criteria for any object storage this assertion is critical.
	bytesData := []struct {
		byteData []byte
	}{
		{generateBytesData(6 * humanize.MiByte)},
	}
	// set of inputs for uploading the objects before tests for downloading is done.
	putObjectInputs := []struct {
		bucketName    string
		objectName    string
		contentLength int64
		textData      []byte
		metaData      map[string]string
	}{
		{bucketName, objectName, int64(len(bytesData[0].byteData)), bytesData[0].byteData, make(map[string]string)},
	}
	// iterate through the above set of inputs and upload the object.
	for i, input := range putObjectInputs {
		// uploading the object.
		_, err := obj.PutObject(context.Background(), input.bucketName, input.objectName, mustGetPutObjReader(t, bytes.NewReader(input.textData), input.contentLength, input.metaData[""], ""), ObjectOptions{UserDefined: input.metaData})
		// if object upload fails stop the test.
		if err != nil {
			t.Fatalf("Put Object case %d:  Error uploading object: <ERROR> %v", i+1, err)
		}
	}

	// test cases with inputs and expected result for HeadObject.
	testCases := []struct {
		bucketName string
		objectName string
		accessKey  string
		secretKey  string
		// expected output.
		expectedRespStatus int // expected response status body.
	}{
		// Test case - 1.
		// Fetching stat info of object and validating it.
		{
			bucketName:         bucketName,
			objectName:         objectName,
			accessKey:          credentials.AccessKey,
			secretKey:          credentials.SecretKey,
			expectedRespStatus: http.StatusOK,
		},
		// Test case - 2.
		// Case with non-existent object name.
		{
			bucketName:         bucketName,
			objectName:         "abcd",
			accessKey:          credentials.AccessKey,
			secretKey:          credentials.SecretKey,
			expectedRespStatus: http.StatusNotFound,
		},
		// Test case - 3.
		// Test case to induce a signature mismatch.
		// Using invalid accessID.
		{
			bucketName:         bucketName,
			objectName:         objectName,
			accessKey:          "Invalid-AccessID",
			secretKey:          credentials.SecretKey,
			expectedRespStatus: http.StatusForbidden,
		},
	}

	// Iterating over the cases, fetching the object validating the response.
	for i, testCase := range testCases {
		// initialize HTTP NewRecorder, this records any mutations to response writer inside the handler.
		rec := httptest.NewRecorder()
		// construct HTTP request for Get Object end point.
		req, err := newTestSignedRequestV4(http.MethodHead, getHeadObjectURL("", testCase.bucketName, testCase.objectName),
			0, nil, testCase.accessKey, testCase.secretKey, nil)
		if err != nil {
			t.Fatalf("Test %d: %s: Failed to create HTTP request for Head Object: <ERROR> %v", i+1, instanceType, err)
		}
		// Since `apiRouter` satisfies `http.Handler` it has a ServeHTTP to execute the logic of the handler.
		// Call the ServeHTTP to execute the handler,`func (api objectAPIHandlers) GetObjectHandler`  handles the request.
		apiRouter.ServeHTTP(rec, req)

		// Assert the response code with the expected status.
		if rec.Code != testCase.expectedRespStatus {
			t.Fatalf("Case %d: Expected the response status to be `%d`, but instead found `%d`", i+1, testCase.expectedRespStatus, rec.Code)
		}

		// Verify response of the V2 signed HTTP request.
		// initialize HTTP NewRecorder, this records any mutations to response writer inside the handler.
		recV2 := httptest.NewRecorder()
		// construct HTTP request for Head Object endpoint.
		reqV2, err := newTestSignedRequestV2(http.MethodHead, getHeadObjectURL("", testCase.bucketName, testCase.objectName),
			0, nil, testCase.accessKey, testCase.secretKey, nil)
		if err != nil {
			t.Fatalf("Test %d: %s: Failed to create HTTP request for Head Object: <ERROR> %v", i+1, instanceType, err)
		}

		// Since `apiRouter` satisfies `http.Handler` it has a ServeHTTP to execute the logic of the handler.
		// Call the ServeHTTP to execute the handler.
		apiRouter.ServeHTTP(recV2, reqV2)
		if recV2.Code != testCase.expectedRespStatus {
			t.Errorf("Test %d: %s: Expected the response status to be `%d`, but instead found `%d`", i+1, instanceType, testCase.expectedRespStatus, recV2.Code)
		}
	}

	// Test for Anonymous/unsigned http request.
	anonReq, err := newTestRequest(http.MethodHead, getHeadObjectURL("", bucketName, objectName), 0, nil)
	if err != nil {
		t.Fatalf("MinIO %s: Failed to create an anonymous request for %s/%s: <ERROR> %v",
			instanceType, bucketName, objectName, err)
	}

	// ExecObjectLayerAPIAnonTest - Calls the HTTP API handler using the anonymous request, validates the ErrAccessDeniedResponse,
	// sets the bucket policy using the policy statement generated from `getWriteOnlyObjectStatement` so that the
	// unsigned request goes through and its validated again.
	ExecObjectLayerAPIAnonTest(t, obj, "TestAPIHeadObjectHandler", bucketName, objectName, instanceType, apiRouter, anonReq, getAnonReadOnlyObjectPolicy(bucketName, objectName))

	// HTTP request for testing when `objectLayer` is set to `nil`.
	// There is no need to use an existing bucket and valid input for creating the request
	// since the `objectLayer==nil`  check is performed before any other checks inside the handlers.
	// The only aim is to generate an HTTP request in a way that the relevant/registered end point is evoked/called.

	nilBucket := "dummy-bucket"
	nilObject := "dummy-object"
	nilReq, err := newTestSignedRequestV4(http.MethodHead, getGetObjectURL("", nilBucket, nilObject),
		0, nil, "", "", nil)
	if err != nil {
		t.Errorf("MinIO %s: Failed to create HTTP request for testing the response when object Layer is set to `nil`.", instanceType)
	}
	// execute the object layer set to `nil` test.
	// `ExecObjectLayerAPINilTest` manages the operation.
	ExecObjectLayerAPINilTest(t, nilBucket, nilObject, instanceType, apiRouter, nilReq)
}

func TestAPIHeadObjectHandlerWithEncryption(t *testing.T) {
	globalPolicySys = NewPolicySys()
	defer func() { globalPolicySys = nil }()

	defer DetectTestLeak(t)()
	ExecObjectLayerAPITest(ExecObjectLayerAPITestArgs{t: t, objAPITest: testAPIHeadObjectHandlerWithEncryption, endpoints: []string{"NewMultipart", "PutObjectPart", "CompleteMultipart", "GetObject", "PutObject", "HeadObject"}})
}

func testAPIHeadObjectHandlerWithEncryption(obj ObjectLayer, instanceType, bucketName string, apiRouter http.Handler,
	credentials auth.Credentials, t *testing.T,
) {
	// Set SSL to on to do encryption tests
	globalIsTLS = true
	defer func() { globalIsTLS = false }()

	var (
		oneMiB        int64 = 1024 * 1024
		key32Bytes          = generateBytesData(32 * humanize.Byte)
		key32BytesMd5       = md5.Sum(key32Bytes)
		metaWithSSEC        = map[string]string{
			xhttp.AmzServerSideEncryptionCustomerAlgorithm: xhttp.AmzEncryptionAES,
			xhttp.AmzServerSideEncryptionCustomerKey:       base64.StdEncoding.EncodeToString(key32Bytes),
			xhttp.AmzServerSideEncryptionCustomerKeyMD5:    base64.StdEncoding.EncodeToString(key32BytesMd5[:]),
		}
		mapCopy = func(m map[string]string) map[string]string {
			r := make(map[string]string, len(m))
			maps.Copy(r, m)
			return r
		}
	)

	type ObjectInput struct {
		objectName  string
		partLengths []int64

		metaData map[string]string
	}

	objectLength := func(oi ObjectInput) (sum int64) {
		for _, l := range oi.partLengths {
			sum += l
		}
		return sum
	}

	// set of inputs for uploading the objects before tests for
	// downloading is done. Data bytes are from DummyDataGen.
	objectInputs := []ObjectInput{
		// Unencrypted objects
		{"nothing", []int64{0}, nil},
		{"small-1", []int64{509}, nil},

		{"mp-1", []int64{5 * oneMiB, 1}, nil},
		{"mp-2", []int64{5487701, 5487799, 3}, nil},

		// Encrypted object
		{"enc-nothing", []int64{0}, mapCopy(metaWithSSEC)},
		{"enc-small-1", []int64{509}, mapCopy(metaWithSSEC)},

		{"enc-mp-1", []int64{5 * oneMiB, 1}, mapCopy(metaWithSSEC)},
		{"enc-mp-2", []int64{5487701, 5487799, 3}, mapCopy(metaWithSSEC)},
	}

	// iterate through the above set of inputs and upload the object.
	for _, input := range objectInputs {
		uploadTestObject(t, apiRouter, credentials, bucketName, input.objectName, input.partLengths, input.metaData, false)
	}

	for i, input := range objectInputs {
		// initialize HTTP NewRecorder, this records any
		// mutations to response writer inside the handler.
		rec := httptest.NewRecorder()
		// construct HTTP request for HEAD object.
		req, err := newTestSignedRequestV4(http.MethodHead, getHeadObjectURL("", bucketName, input.objectName),
			0, nil, credentials.AccessKey, credentials.SecretKey, nil)
		if err != nil {
			t.Fatalf("Test %d: %s: Failed to create HTTP request for Head Object: <ERROR> %v", i+1, instanceType, err)
		}
		// Since `apiRouter` satisfies `http.Handler` it has a
		// ServeHTTP to execute the logic of the handler.
		apiRouter.ServeHTTP(rec, req)

		isEnc := false
		expected := 200
		if strings.HasPrefix(input.objectName, "enc-") {
			isEnc = true
			expected = 400
		}
		if rec.Code != expected {
			t.Errorf("Test %d: expected code %d but got %d for object %s", i+1, expected, rec.Code, input.objectName)
		}

		contentLength := rec.Header().Get("Content-Length")
		if isEnc {
			// initialize HTTP NewRecorder, this records any
			// mutations to response writer inside the handler.
			rec := httptest.NewRecorder()
			// construct HTTP request for HEAD object.
			req, err := newTestSignedRequestV4(http.MethodHead, getHeadObjectURL("", bucketName, input.objectName),
				0, nil, credentials.AccessKey, credentials.SecretKey, input.metaData)
			if err != nil {
				t.Fatalf("Test %d: %s: Failed to create HTTP request for Head Object: <ERROR> %v", i+1, instanceType, err)
			}
			// Since `apiRouter` satisfies `http.Handler` it has a
			// ServeHTTP to execute the logic of the handler.
			apiRouter.ServeHTTP(rec, req)

			if rec.Code != 200 {
				t.Errorf("Test %d: Did not receive a 200 response: %d", i+1, rec.Code)
			}
			contentLength = rec.Header().Get("Content-Length")
		}

		if contentLength != fmt.Sprintf("%d", objectLength(input)) {
			t.Errorf("Test %d: Content length is mismatching: got %s (expected: %d)", i+1, contentLength, objectLength(input))
		}
	}
}

// Wrapper for calling GetObject API handler tests for both Erasure multiple disks and FS single drive setup.
func TestAPIGetObjectHandler(t *testing.T) {
	globalPolicySys = NewPolicySys()
	defer func() { globalPolicySys = nil }()

	defer DetectTestLeak(t)()
	ExecExtendedObjectLayerAPITest(t, testAPIGetObjectHandler, []string{"GetObject"})
}

func testAPIGetObjectHandler(obj ObjectLayer, instanceType, bucketName string, apiRouter http.Handler,
	credentials auth.Credentials, t *testing.T,
) {
	objectName := "test-object"
	// set of byte data for PutObject.
	// object has to be created before running tests for GetObject.
	// this is required even to assert the GetObject data,
	// since dataInserted === dataFetched back is a primary criteria for any object storage this assertion is critical.
	bytesData := []struct {
		byteData []byte
	}{
		{generateBytesData(6 * humanize.MiByte)},
	}
	// set of inputs for uploading the objects before tests for downloading is done.
	putObjectInputs := []struct {
		bucketName    string
		objectName    string
		contentLength int64
		textData      []byte
		metaData      map[string]string
	}{
		// case - 1.
		{bucketName, objectName, int64(len(bytesData[0].byteData)), bytesData[0].byteData, make(map[string]string)},
	}
	// iterate through the above set of inputs and upload the object.
	for i, input := range putObjectInputs {
		// uploading the object.
		_, err := obj.PutObject(context.Background(), input.bucketName, input.objectName, mustGetPutObjReader(t, bytes.NewReader(input.textData), input.contentLength, input.metaData[""], ""), ObjectOptions{UserDefined: input.metaData})
		// if object upload fails stop the test.
		if err != nil {
			t.Fatalf("Put Object case %d:  Error uploading object: <ERROR> %v", i+1, err)
		}
	}

	ctx := context.Background()

	// test cases with inputs and expected result for GetObject.
	testCases := []struct {
		bucketName string
		objectName string
		byteRange  string // range of bytes to be fetched from GetObject.
		accessKey  string
		secretKey  string
		// expected output.
		expectedContent    []byte // expected response body.
		expectedRespStatus int    // expected response status body.
	}{
		// Test case - 1.
		// Fetching the entire object and validating its contents.
		{
			bucketName: bucketName,
			objectName: objectName,
			byteRange:  "",
			accessKey:  credentials.AccessKey,
			secretKey:  credentials.SecretKey,

			expectedContent:    bytesData[0].byteData,
			expectedRespStatus: http.StatusOK,
		},
		// Test case - 2.
		// Case with non-existent object name.
		{
			bucketName: bucketName,
			objectName: "abcd",
			byteRange:  "",
			accessKey:  credentials.AccessKey,
			secretKey:  credentials.SecretKey,

			expectedContent: encodeResponse(getAPIErrorResponse(ctx,
				getAPIError(ErrNoSuchKey),
				getGetObjectURL("", bucketName, "abcd"), "", "")),
			expectedRespStatus: http.StatusNotFound,
		},
		// Test case - 3.
		// Requesting from range 10-100.
		{
			bucketName: bucketName,
			objectName: objectName,
			byteRange:  "bytes=10-100",
			accessKey:  credentials.AccessKey,
			secretKey:  credentials.SecretKey,

			expectedContent:    bytesData[0].byteData[10:101],
			expectedRespStatus: http.StatusPartialContent,
		},
		// Test case - 4.
		// Test case with invalid range.
		{
			bucketName: bucketName,
			objectName: objectName,
			byteRange:  "bytes=-0",
			accessKey:  credentials.AccessKey,
			secretKey:  credentials.SecretKey,

			expectedContent: encodeResponse(getAPIErrorResponse(ctx,
				getAPIError(ErrInvalidRange),
				getGetObjectURL("", bucketName, objectName), "", "")),
			expectedRespStatus: http.StatusRequestedRangeNotSatisfiable,
		},
		// Test case - 5.
		// Test case with byte range exceeding the object size.
		// Expected to read till end of the object.
		{
			bucketName: bucketName,
			objectName: objectName,
			byteRange:  "bytes=10-1000000000000000",
			accessKey:  credentials.AccessKey,
			secretKey:  credentials.SecretKey,

			expectedContent:    bytesData[0].byteData[10:],
			expectedRespStatus: http.StatusPartialContent,
		},
		// Test case - 6.
		// Test case to induce a signature mismatch.
		// Using invalid accessID.
		{
			bucketName: bucketName,
			objectName: objectName,
			byteRange:  "",
			accessKey:  "Invalid-AccessID",
			secretKey:  credentials.SecretKey,

			expectedContent: encodeResponse(getAPIErrorResponse(ctx,
				getAPIError(ErrInvalidAccessKeyID),
				getGetObjectURL("", bucketName, objectName), "", "")),
			expectedRespStatus: http.StatusForbidden,
		},
		// Test case - 7.
		// Case with bad components in object name.
		{
			bucketName: bucketName,
			objectName: "../../etc",
			byteRange:  "",
			accessKey:  credentials.AccessKey,
			secretKey:  credentials.SecretKey,

			expectedContent: encodeResponse(getAPIErrorResponse(ctx,
				getAPIError(ErrInvalidObjectName),
				getGetObjectURL("", bucketName, "../../etc"), "", "")),
			expectedRespStatus: http.StatusBadRequest,
		},
		// Test case - 8.
		// Case with strange components but returning error as not found.
		{
			bucketName: bucketName,
			objectName: ". ./. ./etc",
			byteRange:  "",
			accessKey:  credentials.AccessKey,
			secretKey:  credentials.SecretKey,

			expectedContent: encodeResponse(getAPIErrorResponse(ctx,
				getAPIError(ErrNoSuchKey),
				SlashSeparator+bucketName+SlashSeparator+". ./. ./etc", "", "")),
			expectedRespStatus: http.StatusNotFound,
		},
		// Test case - 9.
		// Case with bad components in object name.
		{
			bucketName: bucketName,
			objectName: ". ./../etc",
			byteRange:  "",
			accessKey:  credentials.AccessKey,
			secretKey:  credentials.SecretKey,

			expectedContent: encodeResponse(getAPIErrorResponse(ctx,
				getAPIError(ErrInvalidObjectName),
				SlashSeparator+bucketName+SlashSeparator+". ./../etc", "", "")),
			expectedRespStatus: http.StatusBadRequest,
		},
		// Test case - 10.
		// Case with proper components
		{
			bucketName: bucketName,
			objectName: "etc/path/proper/.../etc",
			byteRange:  "",
			accessKey:  credentials.AccessKey,
			secretKey:  credentials.SecretKey,

			expectedContent: encodeResponse(getAPIErrorResponse(ctx,
				getAPIError(ErrNoSuchKey),
				getGetObjectURL("", bucketName, "etc/path/proper/.../etc"),
				"", "")),
			expectedRespStatus: http.StatusNotFound,
		},
	}

	// Iterating over the cases, fetching the object validating the response.
	for i, testCase := range testCases {
		// initialize HTTP NewRecorder, this records any mutations to response writer inside the handler.
		rec := httptest.NewRecorder()
		// construct HTTP request for Get Object end point.
		req, err := newTestSignedRequestV4(http.MethodGet, getGetObjectURL("", testCase.bucketName, testCase.objectName),
			0, nil, testCase.accessKey, testCase.secretKey, nil)
		if err != nil {
			t.Fatalf("Test %d: Failed to create HTTP request for Get Object: <ERROR> %v", i+1, err)
		}
		if testCase.byteRange != "" {
			req.Header.Set("Range", testCase.byteRange)
		}
		// Since `apiRouter` satisfies `http.Handler` it has a ServeHTTP to execute the logic of the handler.
		// Call the ServeHTTP to execute the handler,`func (api objectAPIHandlers) GetObjectHandler`  handles the request.
		apiRouter.ServeHTTP(rec, req)
		// Assert the response code with the expected status.
		if rec.Code != testCase.expectedRespStatus {
			t.Fatalf("Case %d: Expected the response status to be `%d`, but instead found `%d`", i+1, testCase.expectedRespStatus, rec.Code)
		}
		// read the response body.
		actualContent, err := io.ReadAll(rec.Body)
		if err != nil {
			t.Fatalf("Test %d: %s: Failed reading response body: <ERROR> %v", i+1, instanceType, err)
		}

		if rec.Code == http.StatusOK || rec.Code == http.StatusPartialContent {
			if !bytes.Equal(testCase.expectedContent, actualContent) {
				t.Errorf("Test %d: %s: Object content differs from expected value %s, got %s", i+1, instanceType, testCase.expectedContent, string(actualContent))
			}
			continue
		}

		// Verify whether the bucket obtained object is same as the one created.
		actualError := &APIErrorResponse{}
		if err = xml.Unmarshal(actualContent, actualError); err != nil {
			t.Fatalf("Test %d: %s: Failed parsing response body: <ERROR> %v", i+1, instanceType, err)
		}

		if path.Clean(actualError.Resource) != pathJoin(SlashSeparator, testCase.bucketName, testCase.objectName) {
			t.Fatalf("Test %d: %s: Unexpected resource, expected %s, got %s", i+1, instanceType, pathJoin(SlashSeparator, testCase.bucketName, testCase.objectName), actualError.Resource)
		}

		// Verify response of the V2 signed HTTP request.
		// initialize HTTP NewRecorder, this records any mutations to response writer inside the handler.
		recV2 := httptest.NewRecorder()
		// construct HTTP request for  GET Object endpoint.
		reqV2, err := newTestSignedRequestV2(http.MethodGet, getGetObjectURL("", testCase.bucketName, testCase.objectName),
			0, nil, testCase.accessKey, testCase.secretKey, nil)
		if err != nil {
			t.Fatalf("Test %d: %s: Failed to create HTTP request for GetObject: <ERROR> %v", i+1, instanceType, err)
		}

		if testCase.byteRange != "" {
			reqV2.Header.Set("Range", testCase.byteRange)
		}

		// Since `apiRouter` satisfies `http.Handler` it has a ServeHTTP to execute the logic of the handler.
		// Call the ServeHTTP to execute the handler.
		apiRouter.ServeHTTP(recV2, reqV2)
		if recV2.Code != testCase.expectedRespStatus {
			t.Errorf("Test %d: %s: Expected the response status to be `%d`, but instead found `%d`", i+1, instanceType, testCase.expectedRespStatus, recV2.Code)
		}

		// read the response body.
		actualContent, err = io.ReadAll(recV2.Body)
		if err != nil {
			t.Fatalf("Test %d: %s: Failed to read response body: <ERROR> %v", i+1, instanceType, err)
		}

		if rec.Code == http.StatusOK || rec.Code == http.StatusPartialContent {
			// Verify whether the bucket obtained object is same as the one created.
			if !bytes.Equal(testCase.expectedContent, actualContent) {
				t.Errorf("Test %d: %s: Object content differs from expected value.", i+1, instanceType)
			}
			continue
		}

		actualError = &APIErrorResponse{}
		if err = xml.Unmarshal(actualContent, actualError); err != nil {
			t.Fatalf("Test %d: %s: Failed parsing response body: <ERROR> %v", i+1, instanceType, err)
		}

		if path.Clean(actualError.Resource) != pathJoin(SlashSeparator, testCase.bucketName, testCase.objectName) {
			t.Fatalf("Test %d: %s: Unexpected resource, expected %s, got %s", i+1, instanceType, pathJoin(SlashSeparator, testCase.bucketName, testCase.objectName), actualError.Resource)
		}
	}

	// Test for Anonymous/unsigned http request.
	anonReq, err := newTestRequest(http.MethodGet, getGetObjectURL("", bucketName, objectName), 0, nil)
	if err != nil {
		t.Fatalf("MinIO %s: Failed to create an anonymous request for %s/%s: <ERROR> %v",
			instanceType, bucketName, objectName, err)
	}

	// ExecObjectLayerAPIAnonTest - Calls the HTTP API handler using the anonymous request, validates the ErrAccessDeniedResponse,
	// sets the bucket policy using the policy statement generated from `getWriteOnlyObjectStatement` so that the
	// unsigned request goes through and its validated again.
	ExecObjectLayerAPIAnonTest(t, obj, "TestAPIGetObjectHandler", bucketName, objectName, instanceType, apiRouter, anonReq, getAnonReadOnlyObjectPolicy(bucketName, objectName))

	// HTTP request for testing when `objectLayer` is set to `nil`.
	// There is no need to use an existing bucket and valid input for creating the request
	// since the `objectLayer==nil`  check is performed before any other checks inside the handlers.
	// The only aim is to generate an HTTP request in a way that the relevant/registered end point is evoked/called.

	nilBucket := "dummy-bucket"
	nilObject := "dummy-object"
	nilReq, err := newTestSignedRequestV4(http.MethodGet, getGetObjectURL("", nilBucket, nilObject),
		0, nil, "", "", nil)
	if err != nil {
		t.Errorf("MinIO %s: Failed to create HTTP request for testing the response when object Layer is set to `nil`.", instanceType)
	}
	// execute the object layer set to `nil` test.
	// `ExecObjectLayerAPINilTest` manages the operation.
	ExecObjectLayerAPINilTest(t, nilBucket, nilObject, instanceType, apiRouter, nilReq)
}

// Wrapper for calling GetObject API handler tests for both Erasure multiple disks and FS single drive setup.
func TestAPIGetObjectWithMPHandler(t *testing.T) {
	globalPolicySys = NewPolicySys()
	defer func() { globalPolicySys = nil }()

	defer DetectTestLeak(t)()
	ExecExtendedObjectLayerAPITest(t, testAPIGetObjectWithMPHandler, []string{"NewMultipart", "PutObjectPart", "CompleteMultipart", "GetObject", "PutObject"})
}

func testAPIGetObjectWithMPHandler(obj ObjectLayer, instanceType, bucketName string, apiRouter http.Handler,
	credentials auth.Credentials, t *testing.T,
) {
	// Set SSL to on to do encryption tests
	globalIsTLS = true
	defer func() { globalIsTLS = false }()

	var (
		oneMiB        int64 = 1024 * 1024
		key32Bytes          = generateBytesData(32 * humanize.Byte)
		key32BytesMd5       = md5.Sum(key32Bytes)
		metaWithSSEC        = map[string]string{
			xhttp.AmzServerSideEncryptionCustomerAlgorithm: xhttp.AmzEncryptionAES,
			xhttp.AmzServerSideEncryptionCustomerKey:       base64.StdEncoding.EncodeToString(key32Bytes),
			xhttp.AmzServerSideEncryptionCustomerKeyMD5:    base64.StdEncoding.EncodeToString(key32BytesMd5[:]),
		}
		mapCopy = func(m map[string]string) map[string]string {
			r := make(map[string]string, len(m))
			maps.Copy(r, m)
			return r
		}
	)

	type ObjectInput struct {
		objectName  string
		partLengths []int64

		metaData map[string]string
	}

	objectLength := func(oi ObjectInput) (sum int64) {
		for _, l := range oi.partLengths {
			sum += l
		}
		return sum
	}

	// set of inputs for uploading the objects before tests for
	// downloading is done. Data bytes are from DummyDataGen.
	objectInputs := []ObjectInput{
		// // cases 0-3: small single part objects
		{"nothing", []int64{0}, make(map[string]string)},
		{"small-0", []int64{11}, make(map[string]string)},
		{"small-1", []int64{509}, make(map[string]string)},
		{"small-2", []int64{5 * oneMiB}, make(map[string]string)},
		// // // cases 4-7: multipart part objects
		{"mp-0", []int64{5 * oneMiB, 10}, make(map[string]string)},
		{"mp-1", []int64{5*oneMiB + 1, 10}, make(map[string]string)},
		{"mp-2", []int64{5487701, 5487799, 3}, make(map[string]string)},
		{"mp-3", []int64{10499807, 10499963, 7}, make(map[string]string)},
		// cases 8-11: small single part objects with encryption
		{"enc-nothing", []int64{0}, mapCopy(metaWithSSEC)},
		{"enc-small-0", []int64{11}, mapCopy(metaWithSSEC)},
		{"enc-small-1", []int64{509}, mapCopy(metaWithSSEC)},
		{"enc-small-2", []int64{5 * oneMiB}, mapCopy(metaWithSSEC)},
		// cases 12-15: multipart part objects with encryption
		{"enc-mp-0", []int64{5 * oneMiB, 10}, mapCopy(metaWithSSEC)},
		{"enc-mp-1", []int64{5*oneMiB + 1, 10}, mapCopy(metaWithSSEC)},
		{"enc-mp-2", []int64{5487701, 5487799, 3}, mapCopy(metaWithSSEC)},
		{"enc-mp-3", []int64{10499807, 10499963, 7}, mapCopy(metaWithSSEC)},
	}
	if testing.Short() {
		objectInputs = append(objectInputs[0:5], objectInputs[8:11]...)
	}
	// iterate through the above set of inputs and upload the object.
	for _, input := range objectInputs {
		uploadTestObject(t, apiRouter, credentials, bucketName, input.objectName, input.partLengths, input.metaData, false)
	}

	// function type for creating signed requests - used to repeat
	// requests with V2 and V4 signing.
	type testSignedReqFn func(method, urlStr string, contentLength int64,
		body io.ReadSeeker, accessKey, secretKey string, metamap map[string]string) (*http.Request,
		error)

	mkGetReq := func(oi ObjectInput, byteRange string, i int, mkSignedReq testSignedReqFn) {
		object := oi.objectName
		rec := httptest.NewRecorder()
		req, err := mkSignedReq(http.MethodGet, getGetObjectURL("", bucketName, object),
			0, nil, credentials.AccessKey, credentials.SecretKey, oi.metaData)
		if err != nil {
			t.Fatalf("Object: %s Case %d ByteRange: %s: Failed to create HTTP request for Get Object: <ERROR> %v",
				object, i+1, byteRange, err)
		}

		if byteRange != "" {
			req.Header.Set("Range", byteRange)
		}

		apiRouter.ServeHTTP(rec, req)

		// Check response code (we make only valid requests in
		// this test)
		if rec.Code != http.StatusPartialContent && rec.Code != http.StatusOK {
			bd, err1 := io.ReadAll(rec.Body)
			t.Fatalf("%s Object: %s Case %d ByteRange: %s: Got response status `%d` and body: %s,%v",
				instanceType, object, i+1, byteRange, rec.Code, string(bd), err1)
		}

		var off, length int64
		var rs *HTTPRangeSpec
		if byteRange != "" {
			rs, err = parseRequestRangeSpec(byteRange)
			if err != nil {
				t.Fatalf("Object: %s Case %d ByteRange: %s: Unexpected err: %v", object, i+1, byteRange, err)
			}
		}
		off, length, err = rs.GetOffsetLength(objectLength(oi))
		if err != nil {
			t.Fatalf("Object: %s Case %d ByteRange: %s: Unexpected err: %v", object, i+1, byteRange, err)
		}

		readers := []io.Reader{}
		cumulativeSum := int64(0)
		for _, p := range oi.partLengths {
			readers = append(readers, NewDummyDataGen(p, cumulativeSum))
			cumulativeSum += p
		}

		refReader := io.LimitReader(ioutilx.NewSkipReader(io.MultiReader(readers...), off), length)
		if ok, msg := cmpReaders(refReader, rec.Body); !ok {
			t.Fatalf("(%s) Object: %s Case %d ByteRange: %s --> data mismatch! (msg: %s)", instanceType, oi.objectName, i+1, byteRange, msg)
		}
	}

	// Iterate over each uploaded object and do a bunch of get
	// requests on them.
	caseNumber := 0
	signFns := []testSignedReqFn{newTestSignedRequestV2, newTestSignedRequestV4}
	for _, oi := range objectInputs {
		objLen := objectLength(oi)
		for _, sf := range signFns {
			// Read whole object
			mkGetReq(oi, "", caseNumber, sf)
			caseNumber++

			// No range requests are possible if the
			// object length is 0
			if objLen == 0 {
				continue
			}

			// Various ranges to query - all are valid!
			rangeHdrs := []string{
				// Read first byte of object
				fmt.Sprintf("bytes=%d-%d", 0, 0),
				// Read second byte of object
				fmt.Sprintf("bytes=%d-%d", 1, 1),
				// Read last byte of object
				fmt.Sprintf("bytes=-%d", 1),
				// Read all but first byte of object
				"bytes=1-",
				// Read first half of object
				fmt.Sprintf("bytes=%d-%d", 0, objLen/2),
				// Read last half of object
				fmt.Sprintf("bytes=-%d", objLen/2),
				// Read middle half of object
				fmt.Sprintf("bytes=%d-%d", objLen/4, objLen*3/4),
				// Read 100MiB of the object from the beginning
				fmt.Sprintf("bytes=%d-%d", 0, 100*humanize.MiByte),
				// Read 100MiB of the object from the end
				fmt.Sprintf("bytes=-%d", 100*humanize.MiByte),
			}
			for _, rangeHdr := range rangeHdrs {
				mkGetReq(oi, rangeHdr, caseNumber, sf)
				caseNumber++
			}
		}
	}

	// HTTP request for testing when `objectLayer` is set to `nil`.
	// There is no need to use an existing bucket and valid input for creating the request
	// since the `objectLayer==nil`  check is performed before any other checks inside the handlers.
	// The only aim is to generate an HTTP request in a way that the relevant/registered end point is evoked/called.

	nilBucket := "dummy-bucket"
	nilObject := "dummy-object"
	nilReq, err := newTestSignedRequestV4(http.MethodGet, getGetObjectURL("", nilBucket, nilObject),
		0, nil, "", "", nil)
	if err != nil {
		t.Errorf("MinIO %s: Failed to create HTTP request for testing the response when object Layer is set to `nil`.", instanceType)
	}
	// execute the object layer set to `nil` test.
	// `ExecObjectLayerAPINilTest` manages the operation.
	ExecObjectLayerAPINilTest(t, nilBucket, nilObject, instanceType, apiRouter, nilReq)
}

// Wrapper for calling GetObject API handler tests for both Erasure multiple disks and FS single drive setup.
func TestAPIGetObjectWithPartNumberHandler(t *testing.T) {
	globalPolicySys = NewPolicySys()
	defer func() { globalPolicySys = nil }()

	defer DetectTestLeak(t)()
	ExecExtendedObjectLayerAPITest(t, testAPIGetObjectWithPartNumberHandler, []string{"NewMultipart", "PutObjectPart", "CompleteMultipart", "GetObject", "PutObject"})
}

func testAPIGetObjectWithPartNumberHandler(obj ObjectLayer, instanceType, bucketName string, apiRouter http.Handler,
	credentials auth.Credentials, t *testing.T,
) {
	// Set SSL to on to do encryption tests
	globalIsTLS = true
	defer func() { globalIsTLS = false }()

	var (
		oneMiB        int64 = 1024 * 1024
		key32Bytes          = generateBytesData(32 * humanize.Byte)
		key32BytesMd5       = md5.Sum(key32Bytes)
		metaWithSSEC        = map[string]string{
			xhttp.AmzServerSideEncryptionCustomerAlgorithm: xhttp.AmzEncryptionAES,
			xhttp.AmzServerSideEncryptionCustomerKey:       base64.StdEncoding.EncodeToString(key32Bytes),
			xhttp.AmzServerSideEncryptionCustomerKeyMD5:    base64.StdEncoding.EncodeToString(key32BytesMd5[:]),
		}
		mapCopy = func(m map[string]string) map[string]string {
			r := make(map[string]string, len(m))
			maps.Copy(r, m)
			return r
		}
	)

	type ObjectInput struct {
		objectName  string
		partLengths []int64

		metaData map[string]string
	}

	// set of inputs for uploading the objects before tests for
	// downloading is done. Data bytes are from DummyDataGen.
	objectInputs := []ObjectInput{
		// // cases 0-4: small single part objects
		{"nothing", []int64{0}, make(map[string]string)},
		{"1byte", []int64{1}, make(map[string]string)},
		{"small-0", []int64{11}, make(map[string]string)},
		{"small-1", []int64{509}, make(map[string]string)},
		{"small-2", []int64{5 * oneMiB}, make(map[string]string)},
		// // // cases 5-8: multipart part objects
		{"mp-0", []int64{5 * oneMiB, 1}, make(map[string]string)},
		{"mp-1", []int64{5*oneMiB + 1, 1}, make(map[string]string)},
		{"mp-2", []int64{5487701, 5487799, 3}, make(map[string]string)},
		{"mp-3", []int64{10499807, 10499963, 7}, make(map[string]string)},
		// cases 9-12: small single part objects with encryption
		{"enc-nothing", []int64{0}, mapCopy(metaWithSSEC)},
		{"enc-small-0", []int64{11}, mapCopy(metaWithSSEC)},
		{"enc-small-1", []int64{509}, mapCopy(metaWithSSEC)},
		{"enc-small-2", []int64{5 * oneMiB}, mapCopy(metaWithSSEC)},
		// cases 13-16: multipart part objects with encryption
		{"enc-mp-0", []int64{5 * oneMiB, 1}, mapCopy(metaWithSSEC)},
		{"enc-mp-1", []int64{5*oneMiB + 1, 1}, mapCopy(metaWithSSEC)},
		{"enc-mp-2", []int64{5487701, 5487799, 3}, mapCopy(metaWithSSEC)},
		{"enc-mp-3", []int64{10499807, 10499963, 7}, mapCopy(metaWithSSEC)},
	}

	// SSEC can't be used with compression
	globalCompressConfigMu.Lock()
	compressEnabled := globalCompressConfig.Enabled
	globalCompressConfigMu.Unlock()
	if compressEnabled {
		objectInputs = objectInputs[0:9]
	}

	// iterate through the above set of inputs and upload the object.
	for _, input := range objectInputs {
		uploadTestObject(t, apiRouter, credentials, bucketName, input.objectName, input.partLengths, input.metaData, false)
	}

	mkGetReqWithPartNumber := func(oindex int, oi ObjectInput, partNumber int) {
		object := oi.objectName

		queries := url.Values{}
		queries.Add("partNumber", strconv.Itoa(partNumber))
		targetURL := makeTestTargetURL("", bucketName, object, queries)
		req, err := newTestSignedRequestV4(http.MethodGet, targetURL,
			0, nil, credentials.AccessKey, credentials.SecretKey, oi.metaData)
		if err != nil {
			t.Fatalf("Object: %s Object Index %d PartNumber: %d: Failed to create HTTP request for Get Object: <ERROR> %v",
				object, oindex, partNumber, err)
		}

		rec := httptest.NewRecorder()
		apiRouter.ServeHTTP(rec, req)

		// Check response code (we make only valid requests in this test)
		if rec.Code != http.StatusPartialContent && rec.Code != http.StatusOK {
			bd, err1 := io.ReadAll(rec.Body)
			t.Fatalf("%s Object: %s ObjectIndex %d PartNumber: %d: Got response status `%d` and body: %s,%v",
				instanceType, object, oindex, partNumber, rec.Code, string(bd), err1)
		}

		oinfo, err := obj.GetObjectInfo(context.Background(), bucketName, object, ObjectOptions{})
		if err != nil {
			t.Fatalf("Object: %s Object Index %d: Unexpected err: %v", object, oindex, err)
		}

		rs := partNumberToRangeSpec(oinfo, partNumber)
		size, err := oinfo.GetActualSize()
		if err != nil {
			t.Fatalf("Object: %s Object Index %d: Unexpected err: %v", object, oindex, err)
		}

		off, length, err := rs.GetOffsetLength(size)
		if err != nil {
			t.Fatalf("Object: %s Object Index %d: Unexpected err: %v", object, oindex, err)
		}

		readers := []io.Reader{}
		cumulativeSum := int64(0)
		for _, p := range oi.partLengths {
			readers = append(readers, NewDummyDataGen(p, cumulativeSum))
			cumulativeSum += p
		}

		refReader := io.LimitReader(ioutilx.NewSkipReader(io.MultiReader(readers...), off), length)
		if ok, msg := cmpReaders(refReader, rec.Body); !ok {
			t.Fatalf("(%s) Object: %s ObjectIndex %d PartNumber: %d --> data mismatch! (msg: %s)", instanceType, oi.objectName, oindex, partNumber, msg)
		}
	}

	for idx, oi := range objectInputs {
		for partNum := 1; partNum <= len(oi.partLengths); partNum++ {
			mkGetReqWithPartNumber(idx, oi, partNum)
		}
	}
}

// Wrapper for calling PutObject API handler tests using streaming signature v4 for both Erasure multiple disks and FS single drive setup.
func TestAPIPutObjectStreamSigV4Handler(t *testing.T) {
	defer DetectTestLeak(t)()
	ExecExtendedObjectLayerAPITest(t, testAPIPutObjectStreamSigV4Handler, []string{"PutObject"})
}

func testAPIPutObjectStreamSigV4Handler(obj ObjectLayer, instanceType, bucketName string, apiRouter http.Handler,
	credentials auth.Credentials, t *testing.T,
) {
	objectName := "test-object"
	bytesDataLen := 65 * humanize.KiByte
	bytesData := bytes.Repeat([]byte{'a'}, bytesDataLen)
	oneKData := bytes.Repeat([]byte("a"), 1*humanize.KiByte)

	var err error

	type streamFault int
	const (
		None streamFault = iota
		malformedEncoding
		unexpectedEOF
		signatureMismatch
		chunkDateMismatch
		tooBigDecodedLength
	)

	// byte data for PutObject.
	// test cases with inputs and expected result for GetObject.
	testCases := []struct {
		bucketName string
		objectName string
		data       []byte
		dataLen    int
		chunkSize  int64
		// expected output.
		expectedContent    []byte // expected response body.
		expectedRespStatus int    // expected response status body.
		// Access keys
		accessKey        string
		secretKey        string
		shouldPass       bool
		removeAuthHeader bool
		fault            streamFault
		// Custom content encoding.
		contentEncoding string
	}{
		// Test case - 1.
		// Fetching the entire object and validating its contents.
		{
			bucketName:         bucketName,
			objectName:         objectName,
			data:               bytesData,
			dataLen:            len(bytesData),
			chunkSize:          64 * humanize.KiByte,
			expectedContent:    []byte{},
			expectedRespStatus: http.StatusOK,
			accessKey:          credentials.AccessKey,
			secretKey:          credentials.SecretKey,
			shouldPass:         true,
			fault:              None,
		},
		// Test case - 2
		// Small chunk size.
		{
			bucketName:         bucketName,
			objectName:         objectName,
			data:               bytesData,
			dataLen:            len(bytesData),
			chunkSize:          1 * humanize.KiByte,
			expectedContent:    []byte{},
			expectedRespStatus: http.StatusOK,
			accessKey:          credentials.AccessKey,
			secretKey:          credentials.SecretKey,
			shouldPass:         true,
			fault:              None,
		},
		// Test case - 3
		// Empty data
		{
			bucketName:         bucketName,
			objectName:         objectName,
			data:               []byte{},
			dataLen:            0,
			chunkSize:          64 * humanize.KiByte,
			expectedContent:    []byte{},
			expectedRespStatus: http.StatusOK,
			accessKey:          credentials.AccessKey,
			secretKey:          credentials.SecretKey,
			shouldPass:         true,
		},
		// Test case - 4
		// Invalid access key id.
		{
			bucketName:         bucketName,
			objectName:         objectName,
			data:               bytesData,
			dataLen:            len(bytesData),
			chunkSize:          64 * humanize.KiByte,
			expectedContent:    []byte{},
			expectedRespStatus: http.StatusForbidden,
			accessKey:          "",
			secretKey:          "",
			shouldPass:         false,
			fault:              None,
		},
		// Test case - 5
		// Wrong auth header returns as bad request.
		{
			bucketName:         bucketName,
			objectName:         objectName,
			data:               bytesData,
			dataLen:            len(bytesData),
			chunkSize:          64 * humanize.KiByte,
			expectedContent:    []byte{},
			expectedRespStatus: http.StatusBadRequest,
			accessKey:          credentials.AccessKey,
			secretKey:          credentials.SecretKey,
			shouldPass:         false,
			removeAuthHeader:   true,
			fault:              None,
		},
		// Test case - 6
		// Large chunk size.. also passes.
		{
			bucketName:         bucketName,
			objectName:         objectName,
			data:               bytesData,
			dataLen:            len(bytesData),
			chunkSize:          100 * humanize.KiByte,
			expectedContent:    []byte{},
			expectedRespStatus: http.StatusOK,
			accessKey:          credentials.AccessKey,
			secretKey:          credentials.SecretKey,
			shouldPass:         true,
			fault:              None,
		},
		// Test case - 7
		// Chunk with malformed encoding.
		// Causes signature mismatch.
		{
			bucketName:         bucketName,
			objectName:         objectName,
			data:               oneKData,
			dataLen:            1024,
			chunkSize:          1024,
			expectedContent:    []byte{},
			expectedRespStatus: http.StatusForbidden,
			accessKey:          credentials.AccessKey,
			secretKey:          credentials.SecretKey,
			shouldPass:         false,
			fault:              malformedEncoding,
		},
		// Test case - 8
		// Chunk with shorter than advertised chunk data.
		{
			bucketName:         bucketName,
			objectName:         objectName,
			data:               oneKData,
			dataLen:            1024,
			chunkSize:          1024,
			expectedContent:    []byte{},
			expectedRespStatus: http.StatusBadRequest,
			accessKey:          credentials.AccessKey,
			secretKey:          credentials.SecretKey,
			shouldPass:         false,
			fault:              unexpectedEOF,
		},
		// Test case - 9
		// Chunk with first chunk data byte tampered.
		{
			bucketName:         bucketName,
			objectName:         objectName,
			data:               oneKData,
			dataLen:            1024,
			chunkSize:          1024,
			expectedContent:    []byte{},
			expectedRespStatus: http.StatusForbidden,
			accessKey:          credentials.AccessKey,
			secretKey:          credentials.SecretKey,
			shouldPass:         false,
			fault:              signatureMismatch,
		},
		// Test case - 10
		// Different date (timestamps) used in seed signature calculation
		// and chunks signature calculation.
		{
			bucketName:         bucketName,
			objectName:         objectName,
			data:               oneKData,
			dataLen:            1024,
			chunkSize:          1024,
			expectedContent:    []byte{},
			expectedRespStatus: http.StatusForbidden,
			accessKey:          credentials.AccessKey,
			secretKey:          credentials.SecretKey,
			shouldPass:         false,
			fault:              chunkDateMismatch,
		},
		// Test case - 11
		// Set x-amz-decoded-content-length to a value too big to hold in int64.
		{
			bucketName:         bucketName,
			objectName:         objectName,
			data:               oneKData,
			dataLen:            1024,
			chunkSize:          1024,
			expectedContent:    []byte{},
			expectedRespStatus: http.StatusBadRequest,
			accessKey:          credentials.AccessKey,
			secretKey:          credentials.SecretKey,
			shouldPass:         false,
			fault:              tooBigDecodedLength,
		},
		// Test case - 12
		// Set custom content encoding should succeed and save the encoding properly.
		{
			bucketName:         bucketName,
			objectName:         objectName,
			data:               bytesData,
			dataLen:            len(bytesData),
			chunkSize:          100 * humanize.KiByte,
			expectedContent:    []byte{},
			expectedRespStatus: http.StatusOK,
			accessKey:          credentials.AccessKey,
			secretKey:          credentials.SecretKey,
			shouldPass:         true,
			contentEncoding:    "aws-chunked,gzip",
			fault:              None,
		},
	}
	// Iterating over the cases, fetching the object validating the response.
	for i, testCase := range testCases {
		// initialize HTTP NewRecorder, this records any mutations to response writer inside the handler.
		rec := httptest.NewRecorder()
		// construct HTTP request for Put Object end point.
		var req *http.Request
		switch {
		case testCase.fault == chunkDateMismatch:
			req, err = newTestStreamingSignedBadChunkDateRequest(http.MethodPut,
				getPutObjectURL("", testCase.bucketName, testCase.objectName),
				int64(testCase.dataLen), testCase.chunkSize, bytes.NewReader(testCase.data),
				testCase.accessKey, testCase.secretKey)
		case testCase.contentEncoding == "":
			req, err = newTestStreamingSignedRequest(http.MethodPut,
				getPutObjectURL("", testCase.bucketName, testCase.objectName),
				int64(testCase.dataLen), testCase.chunkSize, bytes.NewReader(testCase.data),
				testCase.accessKey, testCase.secretKey)
		case testCase.contentEncoding != "":
			req, err = newTestStreamingSignedCustomEncodingRequest(http.MethodPut,
				getPutObjectURL("", testCase.bucketName, testCase.objectName),
				int64(testCase.dataLen), testCase.chunkSize, bytes.NewReader(testCase.data),
				testCase.accessKey, testCase.secretKey, testCase.contentEncoding)
		}
		if err != nil {
			t.Fatalf("Test %d: Failed to create HTTP request for Put Object: <ERROR> %v", i+1, err)
		}
		// Removes auth header if test case requires it.
		if testCase.removeAuthHeader {
			req.Header.Del("Authorization")
		}
		switch testCase.fault {
		case malformedEncoding:
			req, err = malformChunkSizeSigV4(req, testCase.chunkSize-1)
		case signatureMismatch:
			req, err = malformDataSigV4(req, 'z')
		case unexpectedEOF:
			req, err = truncateChunkByHalfSigv4(req)
		case tooBigDecodedLength:
			// Set decoded length to a large value out of int64 range to simulate parse failure.
			req.Header.Set("x-amz-decoded-content-length", "9999999999999999999999")
		}

		if err != nil {
			t.Fatalf("Error injecting faults into the request: <ERROR> %v.", err)
		}

		// Since `apiRouter` satisfies `http.Handler` it has a ServeHTTP to execute the logic of the handler.
		// Call the ServeHTTP to execute the handler,`func (api objectAPIHandlers) GetObjectHandler`  handles the request.
		apiRouter.ServeHTTP(rec, req)
		// Assert the response code with the expected status.
		if rec.Code != testCase.expectedRespStatus {
			t.Errorf("Test %d %s: Expected the response status to be `%d`, but instead found `%d`: fault case %d",
				i+1, instanceType, testCase.expectedRespStatus, rec.Code, testCase.fault)
		}
		// read the response body.
		actualContent, err := io.ReadAll(rec.Body)
		if err != nil {
			t.Fatalf("Test %d: %s: Failed parsing response body: <ERROR> %v", i+1, instanceType, err)
		}
		opts := ObjectOptions{}
		if testCase.shouldPass {
			// Verify whether the bucket obtained object is same as the one created.
			if !bytes.Equal(testCase.expectedContent, actualContent) {
				t.Errorf("Test %d: %s: Object content differs from expected value.: %s", i+1, instanceType, string(actualContent))
				continue
			}
			objInfo, err := obj.GetObjectInfo(context.Background(), testCase.bucketName, testCase.objectName, opts)
			if err != nil {
				t.Fatalf("Test %d: %s: Failed to fetch the copied object: <ERROR> %s", i+1, instanceType, err)
			}
			if objInfo.ContentEncoding == streamingContentEncoding {
				t.Fatalf("Test %d: %s: ContentEncoding is set to \"aws-chunked\" which is unexpected", i+1, instanceType)
			}
			expectedContentEncoding := trimAwsChunkedContentEncoding(testCase.contentEncoding)
			if expectedContentEncoding != objInfo.ContentEncoding {
				t.Fatalf("Test %d: %s: ContentEncoding is set to \"%s\" which is unexpected, expected \"%s\"", i+1, instanceType, objInfo.ContentEncoding, expectedContentEncoding)
			}
			buffer := new(bytes.Buffer)
			r, err := obj.GetObjectNInfo(context.Background(), testCase.bucketName, testCase.objectName, nil, nil, opts)
			if err != nil {
				t.Fatalf("Test %d: %s: Failed to fetch the copied object: <ERROR> %s", i+1, instanceType, err)
			}
			if _, err = io.Copy(buffer, r); err != nil {
				r.Close()
				t.Fatalf("Test %d: %s: Failed to fetch the copied object: <ERROR> %s", i+1, instanceType, err)
			}
			r.Close()
			if !bytes.Equal(testCase.data, buffer.Bytes()) {
				t.Errorf("Test %d: %s: Data Mismatch: Data fetched back from the uploaded object doesn't match the original one.", i+1, instanceType)
			}
		}
	}
}

// Wrapper for calling PutObject API handler tests for both Erasure multiple disks and FS single drive setup.
func TestAPIPutObjectHandler(t *testing.T) {
	defer DetectTestLeak(t)()
	ExecExtendedObjectLayerAPITest(t, testAPIPutObjectHandler, []string{"PutObject"})
}

func testAPIPutObjectHandler(obj ObjectLayer, instanceType, bucketName string, apiRouter http.Handler,
	credentials auth.Credentials, t *testing.T,
) {
	var err error
	objectName := "test-object"
	opts := ObjectOptions{}
	// byte data for PutObject.
	bytesData := generateBytesData(6 * humanize.KiByte)

	copySourceHeader := map[string]string{"X-Amz-Copy-Source": "somewhere"}
	invalidMD5Header := map[string]string{"Content-Md5": "42"}
	invalidStorageClassHeader := map[string]string{xhttp.AmzStorageClass: "INVALID"}

	addCustomHeaders := func(req *http.Request, customHeaders map[string]string) {
		for k, value := range customHeaders {
			req.Header.Set(k, value)
		}
	}

	checksumData := func(b []byte, h hash.Hash) string {
		h.Reset()
		_, err := h.Write(b)
		if err != nil {
			t.Fatal(err)
		}
		return base64.StdEncoding.EncodeToString(h.Sum(nil))
	}
	// test cases with inputs and expected result for GetObject.
	testCases := []struct {
		bucketName string
		objectName string
		headers    map[string]string
		data       []byte
		dataLen    int
		accessKey  string
		secretKey  string
		fault      Fault
		// expected output.
		expectedRespStatus int // expected response status body.
		wantAPICode        string
		wantHeaders        map[string]string
	}{
		// Fetching the entire object and validating its contents.
		0: {
			bucketName: bucketName,
			objectName: objectName,
			data:       bytesData,
			dataLen:    len(bytesData),
			accessKey:  credentials.AccessKey,
			secretKey:  credentials.SecretKey,

			expectedRespStatus: http.StatusOK,
		},
		// Test Case with invalid accessID.
		1: {
			bucketName: bucketName,
			objectName: objectName,
			data:       bytesData,
			dataLen:    len(bytesData),
			accessKey:  "Wrong-AccessID",
			secretKey:  credentials.SecretKey,

			expectedRespStatus: http.StatusForbidden,
			wantAPICode:        "InvalidAccessKeyId",
		},
		// Test Case with invalid header key X-Amz-Copy-Source.
		2: {
			bucketName:         bucketName,
			objectName:         objectName,
			headers:            copySourceHeader,
			data:               bytesData,
			dataLen:            len(bytesData),
			accessKey:          credentials.AccessKey,
			secretKey:          credentials.SecretKey,
			expectedRespStatus: http.StatusBadRequest,
			wantAPICode:        "InvalidArgument",
		},
		// Test Case with invalid Content-Md5 value
		3: {
			bucketName:         bucketName,
			objectName:         objectName,
			headers:            invalidMD5Header,
			data:               bytesData,
			dataLen:            len(bytesData),
			accessKey:          credentials.AccessKey,
			secretKey:          credentials.SecretKey,
			expectedRespStatus: http.StatusBadRequest,
			wantAPICode:        "InvalidDigest",
		},
		// Test Case with object greater than maximum allowed size.
		4: {
			bucketName:         bucketName,
			objectName:         objectName,
			data:               bytesData,
			dataLen:            len(bytesData),
			accessKey:          credentials.AccessKey,
			secretKey:          credentials.SecretKey,
			fault:              TooBigObject,
			expectedRespStatus: http.StatusBadRequest,
			wantAPICode:        "EntityTooLarge",
		},
		// Test Case with missing content length
		5: {
			bucketName:         bucketName,
			objectName:         objectName,
			data:               bytesData,
			dataLen:            len(bytesData),
			accessKey:          credentials.AccessKey,
			secretKey:          credentials.SecretKey,
			fault:              MissingContentLength,
			expectedRespStatus: http.StatusLengthRequired,
			wantAPICode:        "MissingContentLength",
		},
		// Test Case with invalid header key X-Amz-Storage-Class
		6: {
			bucketName:         bucketName,
			objectName:         objectName,
			headers:            invalidStorageClassHeader,
			data:               bytesData,
			dataLen:            len(bytesData),
			accessKey:          credentials.AccessKey,
			secretKey:          credentials.SecretKey,
			expectedRespStatus: http.StatusBadRequest,
			wantAPICode:        "InvalidStorageClass",
		},

		// Invalid crc32
		7: {
			bucketName:         bucketName,
			objectName:         objectName,
			headers:            map[string]string{"x-amz-checksum-crc32": "123"},
			data:               bytesData,
			dataLen:            len(bytesData),
			accessKey:          credentials.AccessKey,
			secretKey:          credentials.SecretKey,
			expectedRespStatus: http.StatusBadRequest,
			wantAPICode:        "InvalidArgument",
		},
		// Wrong crc32
		8: {
			bucketName:         bucketName,
			objectName:         objectName,
			headers:            map[string]string{"x-amz-checksum-crc32": "MTIzNA=="},
			data:               bytesData,
			dataLen:            len(bytesData),
			accessKey:          credentials.AccessKey,
			secretKey:          credentials.SecretKey,
			expectedRespStatus: http.StatusBadRequest,
			wantAPICode:        "XAmzContentChecksumMismatch",
		},
		// Correct crc32
		9: {
			bucketName:         bucketName,
			objectName:         objectName,
			headers:            map[string]string{"x-amz-checksum-crc32": checksumData(bytesData, crc32.New(crc32.IEEETable))},
			data:               bytesData,
			dataLen:            len(bytesData),
			accessKey:          credentials.AccessKey,
			secretKey:          credentials.SecretKey,
			expectedRespStatus: http.StatusOK,
			wantHeaders:        map[string]string{"x-amz-checksum-crc32": checksumData(bytesData, crc32.New(crc32.IEEETable))},
		},
		// Correct crc32c
		10: {
			bucketName:         bucketName,
			objectName:         objectName,
			headers:            map[string]string{"x-amz-checksum-crc32c": checksumData(bytesData, crc32.New(crc32.MakeTable(crc32.Castagnoli)))},
			data:               bytesData,
			dataLen:            len(bytesData),
			accessKey:          credentials.AccessKey,
			secretKey:          credentials.SecretKey,
			expectedRespStatus: http.StatusOK,
			wantHeaders:        map[string]string{"x-amz-checksum-crc32c": checksumData(bytesData, crc32.New(crc32.MakeTable(crc32.Castagnoli)))},
		},
		// CRC32 as CRC32C
		11: {
			bucketName:         bucketName,
			objectName:         objectName,
			headers:            map[string]string{"x-amz-checksum-crc32c": checksumData(bytesData, crc32.New(crc32.IEEETable))},
			data:               bytesData,
			dataLen:            len(bytesData),
			accessKey:          credentials.AccessKey,
			secretKey:          credentials.SecretKey,
			expectedRespStatus: http.StatusBadRequest,
			wantAPICode:        "XAmzContentChecksumMismatch",
		},
		// SHA1
		12: {
			bucketName:         bucketName,
			objectName:         objectName,
			headers:            map[string]string{"x-amz-checksum-sha1": checksumData(bytesData, sha1.New())},
			data:               bytesData,
			dataLen:            len(bytesData),
			accessKey:          credentials.AccessKey,
			secretKey:          credentials.SecretKey,
			expectedRespStatus: http.StatusOK,
			wantHeaders:        map[string]string{"x-amz-checksum-sha1": checksumData(bytesData, sha1.New())},
		},
		// SHA256
		13: {
			bucketName:         bucketName,
			objectName:         objectName,
			headers:            map[string]string{"x-amz-checksum-sha256": checksumData(bytesData, sha256.New())},
			data:               bytesData,
			dataLen:            len(bytesData),
			accessKey:          credentials.AccessKey,
			secretKey:          credentials.SecretKey,
			expectedRespStatus: http.StatusOK,
			wantHeaders:        map[string]string{"x-amz-checksum-sha256": checksumData(bytesData, sha256.New())},
		},
	}
	// Iterating over the cases, fetching the object validating the response.
	for i, testCase := range testCases {
		var req, reqV2 *http.Request
		// initialize HTTP NewRecorder, this records any mutations to response writer inside the handler.
		rec := httptest.NewRecorder()
		// construct HTTP request for Get Object end point.
		req, err = newTestSignedRequestV4(http.MethodPut, getPutObjectURL("", testCase.bucketName, testCase.objectName),
			int64(testCase.dataLen), bytes.NewReader(testCase.data), testCase.accessKey, testCase.secretKey, testCase.headers)
		if err != nil {
			t.Fatalf("Test %d: Failed to create HTTP request for Put Object: <ERROR> %v", i, err)
		}
		// Add test case specific headers to the request.
		addCustomHeaders(req, testCase.headers)

		// Inject faults if specified in testCase.fault
		switch testCase.fault {
		case MissingContentLength:
			req.ContentLength = -1
			req.TransferEncoding = []string{}
		case TooBigObject:
			req.ContentLength = globalMaxObjectSize + 1
		}
		// Since `apiRouter` satisfies `http.Handler` it has a ServeHTTP to execute the logic of the handler.
		// Call the ServeHTTP to execute the handler,`func (api objectAPIHandlers) GetObjectHandler`  handles the request.
		apiRouter.ServeHTTP(rec, req)
		// Assert the response code with the expected status.
		if rec.Code != testCase.expectedRespStatus {
			b, _ := io.ReadAll(rec.Body)
			t.Fatalf("Test %d: Expected the response status to be `%d`, but instead found `%d`: %s", i, testCase.expectedRespStatus, rec.Code, string(b))
		}
		if testCase.expectedRespStatus != http.StatusOK {
			b, err := io.ReadAll(rec.Body)
			if err != nil {
				t.Fatal(err)
			}
			var apiErr APIErrorResponse
			err = xml.Unmarshal(b, &apiErr)
			if err != nil {
				t.Fatal(err)
			}
			gotErr := apiErr.Code
			wantErr := testCase.wantAPICode
			if gotErr != wantErr {
				t.Errorf("test %d: want api error %q, got %q", i, wantErr, gotErr)
			}
			if testCase.wantHeaders != nil {
				for k, v := range testCase.wantHeaders {
					got := rec.Header().Get(k)
					if got != v {
						t.Errorf("Want header %s = %s, got %#v", k, v, rec.Header())
					}
				}
			}
		}

		if testCase.expectedRespStatus == http.StatusOK {
			buffer := new(bytes.Buffer)
			// Fetch the object to check whether the content is same as the one uploaded via PutObject.
			gr, err := obj.GetObjectNInfo(context.Background(), testCase.bucketName, testCase.objectName, nil, nil, opts)
			if err != nil {
				t.Fatalf("Test %d: %s: Failed to fetch the copied object: <ERROR> %s", i, instanceType, err)
			}
			if _, err = io.Copy(buffer, gr); err != nil {
				gr.Close()
				t.Fatalf("Test %d: %s: Failed to fetch the copied object: <ERROR> %s", i, instanceType, err)
			}
			gr.Close()
			if !bytes.Equal(bytesData, buffer.Bytes()) {
				t.Errorf("Test %d: %s: Data Mismatch: Data fetched back from the uploaded object doesn't match the original one.", i, instanceType)
			}
			buffer.Reset()
		}

		// Verify response of the V2 signed HTTP request.
		// initialize HTTP NewRecorder, this records any mutations to response writer inside the handler.
		recV2 := httptest.NewRecorder()
		// construct HTTP request for PUT Object endpoint.
		reqV2, err = newTestSignedRequestV2(http.MethodPut, getPutObjectURL("", testCase.bucketName, testCase.objectName),
			int64(testCase.dataLen), bytes.NewReader(testCase.data), testCase.accessKey, testCase.secretKey, testCase.headers)
		if err != nil {
			t.Fatalf("Test %d: %s: Failed to create HTTP request for PutObject: <ERROR> %v", i, instanceType, err)
		}

		// Add test case specific headers to the request.
		addCustomHeaders(reqV2, testCase.headers)

		// Inject faults if specified in testCase.fault
		switch testCase.fault {
		case MissingContentLength:
			reqV2.ContentLength = -1
			reqV2.TransferEncoding = []string{}
		case TooBigObject:
			reqV2.ContentLength = globalMaxObjectSize + 1
		}

		// Since `apiRouter` satisfies `http.Handler` it has a ServeHTTP to execute the logic of the handler.
		// Call the ServeHTTP to execute the handler.
		apiRouter.ServeHTTP(recV2, reqV2)
		if recV2.Code != testCase.expectedRespStatus {
			b, _ := io.ReadAll(rec.Body)
			t.Errorf("Test %d: %s: Expected the response status to be `%d`, but instead found `%d`: %s", i, instanceType, testCase.expectedRespStatus, recV2.Code, string(b))
		}

		if testCase.expectedRespStatus == http.StatusOK {
			buffer := new(bytes.Buffer)
			// Fetch the object to check whether the content is same as the one uploaded via PutObject.
			gr, err := obj.GetObjectNInfo(context.Background(), testCase.bucketName, testCase.objectName, nil, nil, opts)
			if err != nil {
				t.Fatalf("Test %d: %s: Failed to fetch the copied object: <ERROR> %s", i, instanceType, err)
			}
			if _, err = io.Copy(buffer, gr); err != nil {
				gr.Close()
				t.Fatalf("Test %d: %s: Failed to fetch the copied object: <ERROR> %s", i, instanceType, err)
			}
			gr.Close()
			if !bytes.Equal(bytesData, buffer.Bytes()) {
				t.Errorf("Test %d: %s: Data Mismatch: Data fetched back from the uploaded object doesn't match the original one.", i, instanceType)
			}
			buffer.Reset()

			if testCase.wantHeaders != nil {
				for k, v := range testCase.wantHeaders {
					got := recV2.Header().Get(k)
					if got != v {
						t.Errorf("Want header %s = %s, got %#v", k, v, recV2.Header())
					}
				}
			}
		}
	}

	// Test for Anonymous/unsigned http request.
	anonReq, err := newTestRequest(http.MethodPut, getPutObjectURL("", bucketName, objectName),
		int64(len("hello")), bytes.NewReader([]byte("hello")))
	if err != nil {
		t.Fatalf("MinIO %s: Failed to create an anonymous request for %s/%s: <ERROR> %v",
			instanceType, bucketName, objectName, err)
	}

	// ExecObjectLayerAPIAnonTest - Calls the HTTP API handler using the anonymous request, validates the ErrAccessDeniedResponse,
	// sets the bucket policy using the policy statement generated from `getWriteOnlyObjectStatement` so that the
	// unsigned request goes through and its validated again.
	ExecObjectLayerAPIAnonTest(t, obj, "TestAPIPutObjectHandler", bucketName, objectName, instanceType, apiRouter, anonReq, getAnonWriteOnlyObjectPolicy(bucketName, objectName))

	// HTTP request to test the case of `objectLayer` being set to `nil`.
	// There is no need to use an existing bucket or valid input for creating the request,
	// since the `objectLayer==nil`  check is performed before any other checks inside the handlers.
	// The only aim is to generate an HTTP request in a way that the relevant/registered end point is evoked/called.
	nilBucket := "dummy-bucket"
	nilObject := "dummy-object"

	nilReq, err := newTestSignedRequestV4(http.MethodPut, getPutObjectURL("", nilBucket, nilObject),
		0, nil, "", "", nil)
	if err != nil {
		t.Errorf("MinIO %s: Failed to create HTTP request for testing the response when object Layer is set to `nil`.", instanceType)
	}
	// execute the object layer set to `nil` test.
	// `ExecObjectLayerAPINilTest` manages the operation.
	ExecObjectLayerAPINilTest(t, nilBucket, nilObject, instanceType, apiRouter, nilReq)
}

// Tests sanity of attempting to copying each parts at offsets from an existing
// file and create a new object. Also validates if the written is same as what we
// expected.
func TestAPICopyObjectPartHandlerSanity(t *testing.T) {
	defer DetectTestLeak(t)()
	ExecExtendedObjectLayerAPITest(t, testAPICopyObjectPartHandlerSanity, []string{"NewMultipart", "CompleteMultipart", "CopyObjectPart"})
}

func testAPICopyObjectPartHandlerSanity(obj ObjectLayer, instanceType, bucketName string, apiRouter http.Handler,
	credentials auth.Credentials, t *testing.T,
) {
	objectName := "test-object"
	var err error
	// set of byte data for PutObject.
	// object has to be created before running tests for Copy Object.
	// this is required even to assert the copied object,
	bytesData := []struct {
		byteData []byte
	}{
		{generateBytesData(6 * humanize.MiByte)},
	}

	// set of inputs for uploading the objects before tests for downloading is done.
	putObjectInputs := []struct {
		bucketName    string
		objectName    string
		contentLength int64
		textData      []byte
		metaData      map[string]string
	}{
		// case - 1.
		{bucketName, objectName, int64(len(bytesData[0].byteData)), bytesData[0].byteData, make(map[string]string)},
	}
	// iterate through the above set of inputs and upload the object.
	for i, input := range putObjectInputs {
		// uploading the object.
		_, err = obj.PutObject(context.Background(), input.bucketName, input.objectName,
			mustGetPutObjReader(t, bytes.NewReader(input.textData), input.contentLength, input.metaData[""], ""), ObjectOptions{UserDefined: input.metaData})
		// if object upload fails stop the test.
		if err != nil {
			t.Fatalf("Put Object case %d:  Error uploading object: <ERROR> %v", i+1, err)
		}
	}

	testObject := "testobject"

	// Initiate Multipart upload for testing CopyObjectPartHandler.
	rec := httptest.NewRecorder()
	req, err := newTestSignedRequestV4(http.MethodPost, getNewMultipartURL("", bucketName, testObject),
		0, nil, credentials.AccessKey, credentials.SecretKey, nil)
	if err != nil {
		t.Fatalf("Failed to create HTTP request for NewMultipart Request: <ERROR> %v", err)
	}
	apiRouter.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("%s:  Expected the response status to be `%d`, but instead found `%d`", instanceType, http.StatusOK, rec.Code)
	}
	decoder := xml.NewDecoder(rec.Body)
	multipartResponse := &InitiateMultipartUploadResponse{}
	err = decoder.Decode(multipartResponse)
	if err != nil {
		t.Fatalf("Error decoding the recorded response Body")
	}
	uploadID := multipartResponse.UploadID

	a := 0
	b := globalMinPartSize
	var parts []CompletePart
	for partNumber := 1; partNumber <= 2; partNumber++ {
		// initialize HTTP NewRecorder, this records any mutations to response writer inside the handler.
		rec := httptest.NewRecorder()
		cpPartURL := getCopyObjectPartURL("", bucketName, testObject, uploadID, fmt.Sprintf("%d", partNumber))

		// construct HTTP request for copy object.
		var req *http.Request
		req, err = newTestSignedRequestV4(http.MethodPut, cpPartURL, 0, nil, credentials.AccessKey, credentials.SecretKey, nil)
		if err != nil {
			t.Fatalf("Test failed to create HTTP request for copy object part: <ERROR> %v", err)
		}

		// "X-Amz-Copy-Source" header contains the information about the source bucket and the object to copied.
		req.Header.Set("X-Amz-Copy-Source", url.QueryEscape(pathJoin(bucketName, objectName)))
		req.Header.Set("X-Amz-Copy-Source-Range", fmt.Sprintf("bytes=%d-%d", a, b))

		// Since `apiRouter` satisfies `http.Handler` it has a ServeHTTP to execute the logic of the handler.
		// Call the ServeHTTP to execute the handler, `func (api objectAPIHandlers) CopyObjectHandler` handles the request.
		a = globalMinPartSize + 1
		b = len(bytesData[0].byteData) - 1
		apiRouter.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("Test failed to create HTTP request for copy %d", rec.Code)
		}

		resp := &CopyObjectPartResponse{}
		if err = xmlDecoder(rec.Body, resp, rec.Result().ContentLength); err != nil {
			t.Fatalf("Test failed to decode XML response: <ERROR> %v", err)
		}

		parts = append(parts, CompletePart{
			PartNumber: partNumber,
			ETag:       canonicalizeETag(resp.ETag),
		})
	}

	var completeBytes []byte
	// Complete multipart upload parts.
	completeUploads := &CompleteMultipartUpload{
		Parts: parts,
	}
	completeBytes, err = xml.Marshal(completeUploads)
	if err != nil {
		t.Fatalf("Error XML encoding of parts: <ERROR> %s.", err)
	}
	// Indicating that all parts are uploaded and initiating CompleteMultipartUpload.
	req, err = newTestSignedRequestV4(http.MethodPost, getCompleteMultipartUploadURL("", bucketName, testObject, uploadID),
		int64(len(completeBytes)), bytes.NewReader(completeBytes), credentials.AccessKey, credentials.SecretKey, nil)
	if err != nil {
		t.Fatalf("Failed to create HTTP request for CompleteMultipartUpload: <ERROR> %v", err)
	}

	rec = httptest.NewRecorder()

	apiRouter.ServeHTTP(rec, req)
	// Assert the response code with the expected status.
	if rec.Code != http.StatusOK {
		t.Errorf("Test %s: Expected the response status to be `%d`, but instead found `%d`", instanceType, http.StatusOK, rec.Code)
	}

	var buf bytes.Buffer
	r, err := obj.GetObjectNInfo(context.Background(), bucketName, testObject, nil, nil, ObjectOptions{})
	if err != nil {
		t.Fatalf("Test %s: reading completed file failed: <ERROR> %v", instanceType, err)
	}
	if _, err = io.Copy(&buf, r); err != nil {
		r.Close()
		t.Fatalf("Test %s: Failed to fetch the copied object: <ERROR> %s", instanceType, err)
	}
	r.Close()
	if !bytes.Equal(buf.Bytes(), bytesData[0].byteData) {
		t.Fatalf("Test %s: returned data is not expected corruption detected:", instanceType)
	}

	globalCompressConfigMu.Lock()
	compressEnabled := globalCompressConfig.Enabled
	globalCompressConfigMu.Unlock()
	if compressEnabled && !r.ObjInfo.IsCompressed() {
		t.Errorf("Test %s: object found to be uncompressed though compression was enabled", instanceType)
	}
}

// Wrapper for calling Copy Object Part API handler tests for both Erasure multiple disks and single node setup.
func TestAPICopyObjectPartHandler(t *testing.T) {
	defer DetectTestLeak(t)()
	ExecExtendedObjectLayerAPITest(t, testAPICopyObjectPartHandler, []string{"CopyObjectPart"})
}

func testAPICopyObjectPartHandler(obj ObjectLayer, instanceType, bucketName string, apiRouter http.Handler,
	credentials auth.Credentials, t *testing.T,
) {
	objectName := "test-object"
	var err error
	opts := ObjectOptions{}
	// set of byte data for PutObject.
	// object has to be created before running tests for Copy Object.
	// this is required even to assert the copied object,
	bytesData := []struct {
		byteData []byte
	}{
		{generateBytesData(6 * humanize.KiByte)},
	}

	// set of inputs for uploading the objects before tests for downloading is done.
	putObjectInputs := []struct {
		bucketName    string
		objectName    string
		contentLength int64
		textData      []byte
		metaData      map[string]string
	}{
		// case - 1.
		{bucketName, objectName, int64(len(bytesData[0].byteData)), bytesData[0].byteData, make(map[string]string)},
	}
	// iterate through the above set of inputs and upload the object.
	for i, input := range putObjectInputs {
		// uploading the object.
		_, err = obj.PutObject(context.Background(), input.bucketName, input.objectName, mustGetPutObjReader(t, bytes.NewReader(input.textData), input.contentLength, input.metaData[""], ""), ObjectOptions{UserDefined: input.metaData})
		// if object upload fails stop the test.
		if err != nil {
			t.Fatalf("Put Object case %d:  Error uploading object: <ERROR> %v", i+1, err)
		}
	}

	// Initiate Multipart upload for testing PutObjectPartHandler.
	testObject := "testobject"

	// PutObjectPart API HTTP Handler has to be tested in isolation,
	// that is without any other handler being registered,
	// That's why NewMultipartUpload is initiated using ObjectLayer.
	res, err := obj.NewMultipartUpload(context.Background(), bucketName, testObject, opts)
	if err != nil {
		// Failed to create NewMultipartUpload, abort.
		t.Fatalf("MinIO %s : <ERROR>  %s", instanceType, err)
	}
	uploadID := res.UploadID

	// test cases with inputs and expected result for Copy Object.
	testCases := []struct {
		bucketName        string
		copySourceHeader  string // data for "X-Amz-Copy-Source" header. Contains the object to be copied in the URL.
		copySourceRange   string // data for "X-Amz-Copy-Source-Range" header, contains the byte range offsets of data to be copied.
		uploadID          string // uploadID of the transaction.
		invalidPartNumber bool   // Sets an invalid multipart.
		maximumPartNumber bool   // Sets a maximum parts.
		accessKey         string
		secretKey         string
		// expected output.
		expectedRespStatus int
	}{
		// Test case - 1, copy part 1 from newObject1, ignore request headers.
		{
			bucketName:         bucketName,
			uploadID:           uploadID,
			copySourceHeader:   url.QueryEscape(SlashSeparator + bucketName + SlashSeparator + objectName),
			accessKey:          credentials.AccessKey,
			secretKey:          credentials.SecretKey,
			expectedRespStatus: http.StatusOK,
		},

		// Test case - 2.
		// Test case with invalid source object.
		{
			bucketName:       bucketName,
			uploadID:         uploadID,
			copySourceHeader: url.QueryEscape(SlashSeparator),
			accessKey:        credentials.AccessKey,
			secretKey:        credentials.SecretKey,

			expectedRespStatus: http.StatusBadRequest,
		},

		// Test case - 3.
		// Test case with new object name is same as object to be copied.
		// Fail with file not found.
		{
			bucketName:       bucketName,
			uploadID:         uploadID,
			copySourceHeader: url.QueryEscape(SlashSeparator + bucketName + SlashSeparator + testObject),
			accessKey:        credentials.AccessKey,
			secretKey:        credentials.SecretKey,

			expectedRespStatus: http.StatusNotFound,
		},

		// Test case - 4.
		// Test case with valid byte range.
		{
			bucketName:       bucketName,
			uploadID:         uploadID,
			copySourceHeader: url.QueryEscape(SlashSeparator + bucketName + SlashSeparator + objectName),
			copySourceRange:  "bytes=500-4096",
			accessKey:        credentials.AccessKey,
			secretKey:        credentials.SecretKey,

			expectedRespStatus: http.StatusOK,
		},

		// Test case - 5.
		// Test case with invalid byte range.
		{
			bucketName:       bucketName,
			uploadID:         uploadID,
			copySourceHeader: url.QueryEscape(SlashSeparator + bucketName + SlashSeparator + objectName),
			copySourceRange:  "bytes=6145-",
			accessKey:        credentials.AccessKey,
			secretKey:        credentials.SecretKey,

			expectedRespStatus: http.StatusBadRequest,
		},

		// Test case - 6.
		// Test case with invalid byte range for exceeding source size boundaries.
		{
			bucketName:       bucketName,
			uploadID:         uploadID,
			copySourceHeader: url.QueryEscape(SlashSeparator + bucketName + SlashSeparator + objectName),
			copySourceRange:  "bytes=0-6144",
			accessKey:        credentials.AccessKey,
			secretKey:        credentials.SecretKey,

			expectedRespStatus: http.StatusBadRequest,
		},

		// Test case - 7.
		// Test case with object name missing from source.
		// fail with BadRequest.
		{
			bucketName:       bucketName,
			uploadID:         uploadID,
			copySourceHeader: url.QueryEscape("//123"),
			accessKey:        credentials.AccessKey,
			secretKey:        credentials.SecretKey,

			expectedRespStatus: http.StatusBadRequest,
		},

		// Test case - 8.
		// Test case with non-existent source file.
		// Case for the purpose of failing `api.ObjectAPI.GetObjectInfo`.
		// Expecting the response status code to http.StatusNotFound (404).
		{
			bucketName:       bucketName,
			uploadID:         uploadID,
			copySourceHeader: url.QueryEscape(SlashSeparator + bucketName + SlashSeparator + "non-existent-object"),
			accessKey:        credentials.AccessKey,
			secretKey:        credentials.SecretKey,

			expectedRespStatus: http.StatusNotFound,
		},

		// Test case - 9.
		// Test case with non-existent source file.
		// Case for the purpose of failing `api.ObjectAPI.PutObjectPart`.
		// Expecting the response status code to http.StatusNotFound (404).
		{
			bucketName:       "non-existent-destination-bucket",
			uploadID:         uploadID,
			copySourceHeader: url.QueryEscape(SlashSeparator + bucketName + SlashSeparator + objectName),
			accessKey:        credentials.AccessKey,
			secretKey:        credentials.SecretKey,

			expectedRespStatus: http.StatusNotFound,
		},

		// Test case - 10.
		// Case with invalid AccessKey.
		{
			bucketName:       bucketName,
			uploadID:         uploadID,
			copySourceHeader: url.QueryEscape(SlashSeparator + bucketName + SlashSeparator + objectName),
			accessKey:        "Invalid-AccessID",
			secretKey:        credentials.SecretKey,

			expectedRespStatus: http.StatusForbidden,
		},

		// Test case - 11.
		// Case with non-existent upload id.
		{
			bucketName:       bucketName,
			uploadID:         "-1",
			copySourceHeader: url.QueryEscape(SlashSeparator + bucketName + SlashSeparator + objectName),
			accessKey:        credentials.AccessKey,
			secretKey:        credentials.SecretKey,

			expectedRespStatus: http.StatusNotFound,
		},
		// Test case - 12.
		// invalid part number.
		{
			bucketName:         bucketName,
			uploadID:           uploadID,
			copySourceHeader:   url.QueryEscape(SlashSeparator + bucketName + SlashSeparator + objectName),
			invalidPartNumber:  true,
			accessKey:          credentials.AccessKey,
			secretKey:          credentials.SecretKey,
			expectedRespStatus: http.StatusOK,
		},
		// Test case - 13.
		// maximum part number.
		{
			bucketName:         bucketName,
			uploadID:           uploadID,
			copySourceHeader:   url.QueryEscape(SlashSeparator + bucketName + SlashSeparator + objectName),
			maximumPartNumber:  true,
			accessKey:          credentials.AccessKey,
			secretKey:          credentials.SecretKey,
			expectedRespStatus: http.StatusOK,
		},
		// Test case - 14, copy part 1 from newObject1 with null versionId
		{
			bucketName:         bucketName,
			uploadID:           uploadID,
			copySourceHeader:   url.QueryEscape(SlashSeparator+bucketName+SlashSeparator+objectName) + "?versionId=null",
			accessKey:          credentials.AccessKey,
			secretKey:          credentials.SecretKey,
			expectedRespStatus: http.StatusOK,
		},
		// Test case - 15, copy part 1 from newObject1 with non null versionId
		{
			bucketName:         bucketName,
			uploadID:           uploadID,
			copySourceHeader:   url.QueryEscape(SlashSeparator+bucketName+SlashSeparator+objectName) + "?versionId=17",
			accessKey:          credentials.AccessKey,
			secretKey:          credentials.SecretKey,
			expectedRespStatus: http.StatusNotFound,
		},
		// Test case - 16,  Test case with invalid byte range empty value.
		{
			bucketName:       bucketName,
			uploadID:         uploadID,
			copySourceHeader: url.QueryEscape(SlashSeparator + bucketName + SlashSeparator + objectName),
			copySourceRange:  "empty",
			accessKey:        credentials.AccessKey,
			secretKey:        credentials.SecretKey,

			expectedRespStatus: http.StatusBadRequest,
		},
	}

	for i, testCase := range testCases {
		var req *http.Request
		// initialize HTTP NewRecorder, this records any mutations to response writer inside the handler.
		rec := httptest.NewRecorder()
		switch {
		case !testCase.invalidPartNumber || !testCase.maximumPartNumber:
			// construct HTTP request for copy object.
			req, err = newTestSignedRequestV4(http.MethodPut, getCopyObjectPartURL("", testCase.bucketName, testObject, testCase.uploadID, "1"), 0, nil, testCase.accessKey, testCase.secretKey, nil)
		case testCase.invalidPartNumber:
			req, err = newTestSignedRequestV4(http.MethodPut, getCopyObjectPartURL("", testCase.bucketName, testObject, testCase.uploadID, "abc"), 0, nil, testCase.accessKey, testCase.secretKey, nil)
		case testCase.maximumPartNumber:
			req, err = newTestSignedRequestV4(http.MethodPut, getCopyObjectPartURL("", testCase.bucketName, testObject, testCase.uploadID, "99999"), 0, nil, testCase.accessKey, testCase.secretKey, nil)
		}
		if err != nil {
			t.Fatalf("Test %d: Failed to create HTTP request for copy Object: <ERROR> %v", i+1, err)
		}

		// "X-Amz-Copy-Source" header contains the information about the source bucket and the object to copied.
		if testCase.copySourceHeader != "" {
			req.Header.Set("X-Amz-Copy-Source", testCase.copySourceHeader)
		}
		if testCase.copySourceRange != "" {
			if testCase.copySourceRange == "empty" {
				req.Header.Set("X-Amz-Copy-Source-Range", "") // specifically test for S3 errors in this scenario.
			} else {
				req.Header.Set("X-Amz-Copy-Source-Range", testCase.copySourceRange)
			}
		}

		// Since `apiRouter` satisfies `http.Handler` it has a ServeHTTP to execute the logic of the handler.
		// Call the ServeHTTP to execute the handler, `func (api objectAPIHandlers) CopyObjectHandler` handles the request.
		apiRouter.ServeHTTP(rec, req)
		// Assert the response code with the expected status.
		if rec.Code != testCase.expectedRespStatus {
			t.Fatalf("Test %d: %s:  Expected the response status to be `%d`, but instead found `%d`", i+1, instanceType, testCase.expectedRespStatus, rec.Code)
		}
		if rec.Code == http.StatusOK {
			// See if the new part has been uploaded.
			// testing whether the copy was successful.
			var results ListPartsInfo
			results, err = obj.ListObjectParts(context.Background(), testCase.bucketName, testObject, testCase.uploadID, 0, 1, ObjectOptions{})
			if err != nil {
				t.Fatalf("Test %d: %s: Failed to look for copied object part: <ERROR> %s", i+1, instanceType, err)
			}
			if len(results.Parts) != 1 {
				t.Fatalf("Test %d: %s: Expected only one entry returned %d entries", i+1, instanceType, len(results.Parts))
			}
		}
	}

	// HTTP request for testing when `ObjectLayer` is set to `nil`.
	// There is no need to use an existing bucket and valid input for creating the request
	// since the `objectLayer==nil`  check is performed before any other checks inside the handlers.
	// The only aim is to generate an HTTP request in a way that the relevant/registered end point is evoked/called.
	nilBucket := "dummy-bucket"
	nilObject := "dummy-object"

	nilReq, err := newTestSignedRequestV4(http.MethodPut, getCopyObjectPartURL("", nilBucket, nilObject, "0", "0"),
		0, bytes.NewReader([]byte("testNilObjLayer")), "", "", nil)
	if err != nil {
		t.Errorf("MinIO %s: Failed to create http request for testing the response when object Layer is set to `nil`.", instanceType)
	}

	// Below is how CopyObjectPartHandler is registered.
	// bucket.Methods(http.MethodPut).Path("/{object:.+}").HeadersRegexp("X-Amz-Copy-Source", ".*?(\\/|%2F).*?").HandlerFunc(api.CopyObjectPartHandler).Queries("partNumber", "{partNumber:[0-9]+}", "uploadId", "{uploadId:.*}")
	// Its necessary to set the "X-Amz-Copy-Source" header for the request to be accepted by the handler.
	nilReq.Header.Set("X-Amz-Copy-Source", url.QueryEscape(SlashSeparator+nilBucket+SlashSeparator+nilObject))

	// execute the object layer set to `nil` test.
	// `ExecObjectLayerAPINilTest` manages the operation.
	ExecObjectLayerAPINilTest(t, nilBucket, nilObject, instanceType, apiRouter, nilReq)
}

// Wrapper for calling Copy Object API handler tests for both Erasure multiple disks and single node setup.
func TestAPICopyObjectHandler(t *testing.T) {
	defer DetectTestLeak(t)()
	ExecExtendedObjectLayerAPITest(t, testAPICopyObjectHandler, []string{"CopyObject", "PutObject"})
}

func testAPICopyObjectHandler(obj ObjectLayer, instanceType, bucketName string, apiRouter http.Handler,
	credentials auth.Credentials, t *testing.T,
) {
	objectName := "test?object" // use file with ? to test URL parsing...
	if runtime.GOOS == "windows" {
		objectName = "test-object" // ...except on Windows
	}
	// object used for anonymous HTTP request test.
	anonObject := "anon-object"
	var err error
	opts := ObjectOptions{}
	// set of byte data for PutObject.
	// object has to be created before running tests for Copy Object.
	// this is required even to assert the copied object,
	bytesData := []struct {
		byteData []byte
		md5sum   string
	}{
		{byteData: generateBytesData(6 * humanize.KiByte)},
	}
	h := md5.New()
	h.Write(bytesData[0].byteData)
	bytesData[0].md5sum = hex.EncodeToString(h.Sum(nil))

	buffers := []*bytes.Buffer{
		new(bytes.Buffer),
		new(bytes.Buffer),
	}
	bucketInfo, err := obj.GetBucketInfo(context.Background(), bucketName, BucketOptions{})
	if err != nil {
		t.Fatalf("Test -1: %s: Failed to get bucket info: <ERROR> %s", instanceType, err)
	}
	// set of inputs for uploading the objects before tests for downloading is done.
	putObjectInputs := []struct {
		bucketName    string
		objectName    string
		contentLength int64
		textData      []byte
		md5sum        string
		metaData      map[string]string
	}{
		// case - 1.
		{bucketName, objectName, int64(len(bytesData[0].byteData)), bytesData[0].byteData, bytesData[0].md5sum, make(map[string]string)},

		// case - 2.
		// used for anonymous HTTP request test.
		{bucketName, anonObject, int64(len(bytesData[0].byteData)), bytesData[0].byteData, bytesData[0].md5sum, make(map[string]string)},
	}
	// iterate through the above set of inputs and upload the object.
	for i, input := range putObjectInputs {
		rec := httptest.NewRecorder()
		req, err := newTestSignedRequestV4(http.MethodPut, getPutObjectURL("", input.bucketName, input.objectName),
			input.contentLength, bytes.NewReader(input.textData), credentials.AccessKey, credentials.SecretKey, nil)
		if err != nil {
			t.Fatalf("Test %d: Failed to create HTTP request for Put Object: <ERROR> %v", i, err)
		}
		apiRouter.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			b, err := io.ReadAll(rec.Body)
			if err != nil {
				t.Fatal(err)
			}
			var apiErr APIErrorResponse
			err = xml.Unmarshal(b, &apiErr)
			if err != nil {
				t.Fatal(err)
			}
			gotErr := apiErr.Code
			t.Errorf("test %d: want api got %q", i, gotErr)
		}
	}
	// test cases with inputs and expected result for Copy Object.
	testCases := []struct {
		bucketName           string
		newObjectName        string // name of the newly copied object.
		copySourceHeader     string // data for "X-Amz-Copy-Source" header. Contains the object to be copied in the URL.
		copyModifiedHeader   string // data for "X-Amz-Copy-Source-If-Modified-Since" header
		copyUnmodifiedHeader string // data for "X-Amz-Copy-Source-If-Unmodified-Since" header
		copySourceSame       bool
		metadataGarbage      bool
		metadataReplace      bool
		metadataCopy         bool
		metadata             map[string]string
		accessKey            string
		secretKey            string
		// expected output.
		expectedRespStatus int
	}{
		0: {
			expectedRespStatus: http.StatusMethodNotAllowed,
		},
		// Test case - 1, copy metadata from newObject1, ignore request headers.
		1: {
			bucketName:       bucketName,
			newObjectName:    "newObject1",
			copySourceHeader: url.QueryEscape(SlashSeparator + bucketName + SlashSeparator + objectName),
			accessKey:        credentials.AccessKey,
			secretKey:        credentials.SecretKey,
			metadata: map[string]string{
				"Content-Type": "application/json",
			},
			expectedRespStatus: http.StatusOK,
		},

		// Test case - 2.
		// Test case with invalid source object.
		2: {
			bucketName:       bucketName,
			newObjectName:    "newObject1",
			copySourceHeader: url.QueryEscape(SlashSeparator),
			accessKey:        credentials.AccessKey,
			secretKey:        credentials.SecretKey,

			expectedRespStatus: http.StatusBadRequest,
		},
		// Test case - 3.
		// Test case with invalid source object.
		3: {
			bucketName:         bucketName,
			newObjectName:      "dir//newObject1",
			copySourceHeader:   url.QueryEscape(SlashSeparator + bucketName + SlashSeparator + objectName),
			accessKey:          credentials.AccessKey,
			secretKey:          credentials.SecretKey,
			expectedRespStatus: http.StatusBadRequest,
		},
		// Test case - 4.
		// Test case with new object name is same as object to be copied.
		4: {
			bucketName:       bucketName,
			newObjectName:    objectName,
			copySourceHeader: url.QueryEscape(SlashSeparator + bucketName + SlashSeparator + objectName),
			accessKey:        credentials.AccessKey,
			copySourceSame:   true,
			secretKey:        credentials.SecretKey,

			expectedRespStatus: http.StatusBadRequest,
		},

		// Test case - 5.
		// Test case with new object name is same as object to be copied.
		// But source copy is without leading slash
		5: {
			bucketName:       bucketName,
			newObjectName:    objectName,
			copySourceSame:   true,
			copySourceHeader: url.QueryEscape(bucketName + SlashSeparator + objectName),
			accessKey:        credentials.AccessKey,
			secretKey:        credentials.SecretKey,

			expectedRespStatus: http.StatusBadRequest,
		},

		// Test case - 6.
		// Test case with new object name is same as object to be copied
		// but metadata is updated.
		6: {
			bucketName:       bucketName,
			newObjectName:    objectName,
			copySourceHeader: url.QueryEscape(SlashSeparator + bucketName + SlashSeparator + objectName),
			metadata: map[string]string{
				"Content-Type": "application/json",
			},
			metadataReplace: true,
			accessKey:       credentials.AccessKey,
			secretKey:       credentials.SecretKey,

			expectedRespStatus: http.StatusOK,
		},

		// Test case - 7.
		// Test case with invalid metadata-directive.
		7: {
			bucketName:       bucketName,
			newObjectName:    "newObject1",
			copySourceHeader: url.QueryEscape(SlashSeparator + bucketName + SlashSeparator + objectName),
			metadata: map[string]string{
				"Content-Type": "application/json",
			},
			metadataGarbage: true,
			accessKey:       credentials.AccessKey,
			secretKey:       credentials.SecretKey,

			expectedRespStatus: http.StatusBadRequest,
		},

		// Test case - 8.
		// Test case with new object name is same as object to be copied
		// fail with BadRequest.
		8: {
			bucketName:       bucketName,
			newObjectName:    objectName,
			copySourceHeader: url.QueryEscape(SlashSeparator + bucketName + SlashSeparator + objectName),
			metadata: map[string]string{
				"Content-Type": "application/json",
			},
			copySourceSame: true,
			metadataCopy:   true,
			accessKey:      credentials.AccessKey,
			secretKey:      credentials.SecretKey,

			expectedRespStatus: http.StatusBadRequest,
		},

		// Test case - 9.
		// Test case with non-existent source file.
		// Case for the purpose of failing `api.ObjectAPI.GetObjectInfo`.
		// Expecting the response status code to http.StatusNotFound (404).
		9: {
			bucketName:       bucketName,
			newObjectName:    objectName,
			copySourceHeader: url.QueryEscape(SlashSeparator + bucketName + SlashSeparator + "non-existent-object"),
			accessKey:        credentials.AccessKey,
			secretKey:        credentials.SecretKey,

			expectedRespStatus: http.StatusNotFound,
		},

		// Test case - 10.
		// Test case with non-existent source file.
		// Case for the purpose of failing `api.ObjectAPI.PutObject`.
		// Expecting the response status code to http.StatusNotFound (404).
		10: {
			bucketName:       "non-existent-destination-bucket",
			newObjectName:    objectName,
			copySourceHeader: url.QueryEscape(SlashSeparator + bucketName + SlashSeparator + objectName),
			accessKey:        credentials.AccessKey,
			secretKey:        credentials.SecretKey,

			expectedRespStatus: http.StatusNotFound,
		},

		// Test case - 11.
		// Case with invalid AccessKey.
		11: {
			bucketName:       bucketName,
			newObjectName:    objectName,
			copySourceHeader: url.QueryEscape(SlashSeparator + bucketName + SlashSeparator + objectName),
			accessKey:        "Invalid-AccessID",
			secretKey:        credentials.SecretKey,

			expectedRespStatus: http.StatusForbidden,
		},
		// Test case - 12, copy metadata from newObject1 with satisfying modified header.
		12: {
			bucketName:         bucketName,
			newObjectName:      "newObject1",
			copySourceHeader:   url.QueryEscape(SlashSeparator + bucketName + SlashSeparator + objectName),
			copyModifiedHeader: "Mon, 02 Jan 2006 15:04:05 GMT",
			accessKey:          credentials.AccessKey,
			secretKey:          credentials.SecretKey,
			expectedRespStatus: http.StatusOK,
		},
		// Test case - 13, copy metadata from newObject1 with unsatisfying modified header.
		13: {
			bucketName:         bucketName,
			newObjectName:      "newObject1",
			copySourceHeader:   url.QueryEscape(SlashSeparator + bucketName + SlashSeparator + objectName),
			copyModifiedHeader: "Mon, 02 Jan 2217 15:04:05 GMT",
			accessKey:          credentials.AccessKey,
			secretKey:          credentials.SecretKey,
			expectedRespStatus: http.StatusPreconditionFailed,
		},
		// Test case - 14, copy metadata from newObject1 with wrong modified header format
		14: {
			bucketName:         bucketName,
			newObjectName:      "newObject1",
			copySourceHeader:   url.QueryEscape(SlashSeparator + bucketName + SlashSeparator + objectName),
			copyModifiedHeader: "Mon, 02 Jan 2217 15:04:05 +00:00",
			accessKey:          credentials.AccessKey,
			secretKey:          credentials.SecretKey,
			expectedRespStatus: http.StatusOK,
		},
		// Test case - 15, copy metadata from newObject1 with satisfying unmodified header.
		15: {
			bucketName:           bucketName,
			newObjectName:        "newObject1",
			copySourceHeader:     url.QueryEscape(SlashSeparator + bucketName + SlashSeparator + objectName),
			copyUnmodifiedHeader: "Mon, 02 Jan 2217 15:04:05 GMT",
			accessKey:            credentials.AccessKey,
			secretKey:            credentials.SecretKey,
			expectedRespStatus:   http.StatusOK,
		},
		// Test case - 16, copy metadata from newObject1 with unsatisfying unmodified header.
		16: {
			bucketName:           bucketName,
			newObjectName:        "newObject1",
			copySourceHeader:     url.QueryEscape(SlashSeparator + bucketName + SlashSeparator + objectName),
			copyUnmodifiedHeader: "Mon, 02 Jan 2007 15:04:05 GMT",
			accessKey:            credentials.AccessKey,
			secretKey:            credentials.SecretKey,
			expectedRespStatus:   http.StatusPreconditionFailed,
		},
		// Test case - 17, copy metadata from newObject1 with incorrect unmodified header format.
		17: {
			bucketName:           bucketName,
			newObjectName:        "newObject1",
			copySourceHeader:     url.QueryEscape(SlashSeparator + bucketName + SlashSeparator + objectName),
			copyUnmodifiedHeader: "Mon, 02 Jan 2007 15:04:05 +00:00",
			accessKey:            credentials.AccessKey,
			secretKey:            credentials.SecretKey,
			expectedRespStatus:   http.StatusOK,
		},
		// Test case - 18, copy metadata from newObject1 with null versionId
		18: {
			bucketName:         bucketName,
			newObjectName:      "newObject1",
			copySourceHeader:   url.QueryEscape(SlashSeparator+bucketName+SlashSeparator+objectName) + "?versionId=null",
			accessKey:          credentials.AccessKey,
			secretKey:          credentials.SecretKey,
			expectedRespStatus: http.StatusOK,
		},
		// Test case - 19, copy metadata from newObject1 with non null versionId
		19: {
			bucketName:         bucketName,
			newObjectName:      "newObject1",
			copySourceHeader:   url.QueryEscape(SlashSeparator+bucketName+SlashSeparator+objectName) + "?versionId=17",
			accessKey:          credentials.AccessKey,
			secretKey:          credentials.SecretKey,
			expectedRespStatus: http.StatusNotFound,
		},
	}

	for i, testCase := range testCases {
		if bucketInfo.Versioning {
			if strings.Contains(testCase.copySourceHeader, "versionId=null") {
				testCase.expectedRespStatus = http.StatusNotFound
			}
		}
		values := url.Values{}
		if testCase.expectedRespStatus == http.StatusOK {
			r, err := obj.GetObjectNInfo(context.Background(), testCase.bucketName, objectName, nil, nil, opts)
			if err != nil {
				t.Fatalf("Test %d: %s reading completed file failed: <ERROR> %v", i, instanceType, err)
			}
			r.Close()
			if r.ObjInfo.VersionID != "" {
				values.Set(xhttp.VersionID, r.ObjInfo.VersionID)
			}
		}
		var req *http.Request
		var reqV2 *http.Request
		// initialize HTTP NewRecorder, this records any mutations to response writer inside the handler.
		rec := httptest.NewRecorder()
		// construct HTTP request for copy object.
		req, err = newTestSignedRequestV4(http.MethodPut, getCopyObjectURL("", testCase.bucketName, testCase.newObjectName),
			0, nil, testCase.accessKey, testCase.secretKey, nil)
		if err != nil {
			t.Fatalf("Test %d: Failed to create HTTP request for copy Object: <ERROR> %v", i, err)
		}
		// "X-Amz-Copy-Source" header contains the information about the source bucket and the object to copied.
		if testCase.copySourceHeader != "" {
			if values.Encode() != "" && !strings.Contains(testCase.copySourceHeader, "?") {
				req.Header.Set("X-Amz-Copy-Source", testCase.copySourceHeader+"?"+values.Encode())
			} else {
				req.Header.Set("X-Amz-Copy-Source", testCase.copySourceHeader)
			}
		}
		if testCase.copyModifiedHeader != "" {
			req.Header.Set("X-Amz-Copy-Source-If-Modified-Since", testCase.copyModifiedHeader)
		}
		if testCase.copyUnmodifiedHeader != "" {
			req.Header.Set("X-Amz-Copy-Source-If-Unmodified-Since", testCase.copyUnmodifiedHeader)
		}
		// Add custom metadata.
		for k, v := range testCase.metadata {
			req.Header.Set(k, v)
		}
		if testCase.metadataReplace {
			req.Header.Set("X-Amz-Metadata-Directive", "REPLACE")
		}
		if testCase.metadataCopy {
			req.Header.Set("X-Amz-Metadata-Directive", "COPY")
		}
		if testCase.metadataGarbage {
			req.Header.Set("X-Amz-Metadata-Directive", "Unknown")
		}
		// Since `apiRouter` satisfies `http.Handler` it has a ServeHTTP to execute the logic of the handler.
		// Call the ServeHTTP to execute the handler, `func (api objectAPIHandlers) CopyObjectHandler` handles the request.
		apiRouter.ServeHTTP(rec, req)
		// Assert the response code with the expected status.
		if rec.Code != testCase.expectedRespStatus {
			if testCase.copySourceSame {
				// encryption will rotate creds, so fail only for non-encryption scenario.
				if GlobalKMS == nil {
					t.Errorf("Test %d: %s:  Expected the response status to be `%d`, but instead found `%d`", i, instanceType, testCase.expectedRespStatus, rec.Code)
					continue
				}
			} else {
				t.Errorf("Test %d: %s:  Expected the response status to be `%d`, but instead found `%d`", i, instanceType, testCase.expectedRespStatus, rec.Code)
				continue
			}
		}
		if rec.Code == http.StatusOK {
			var cpObjResp CopyObjectResponse
			if err = xml.Unmarshal(rec.Body.Bytes(), &cpObjResp); err != nil {
				t.Fatalf("Test %d: %s: Failed to parse the CopyObjectResult response: <ERROR> %s", i, instanceType, err)
			}

			// See if the new object is formed.
			// testing whether the copy was successful.
			// Note that this goes directly to the file system,
			// so encryption/compression may interfere at some point.
			buffers[0].Reset()
			r, err := obj.GetObjectNInfo(context.Background(), testCase.bucketName, testCase.newObjectName, nil, nil, opts)
			if err != nil {
				t.Fatalf("Test %d: %s reading completed file failed: <ERROR> %v", i, instanceType, err)
			}
			if _, err = io.Copy(buffers[0], r); err != nil {
				r.Close()
				t.Fatalf("Test %d %s: Failed to fetch the copied object: <ERROR> %s", i, instanceType, err)
			}
			r.Close()
			if !bytes.Equal(bytesData[0].byteData, buffers[0].Bytes()) {
				t.Errorf("Test %d: %s: Data Mismatch: Data fetched back from the copied object doesn't match the original one.", i, instanceType)
			}

			globalCompressConfigMu.Lock()
			compressEnabled := globalCompressConfig.Enabled
			globalCompressConfigMu.Unlock()
			if compressEnabled && !r.ObjInfo.IsCompressed() {
				t.Errorf("Test %d %s: object found to be uncompressed though compression was enabled", i, instanceType)
			}
		}

		// Verify response of the V2 signed HTTP request.
		// initialize HTTP NewRecorder, this records any mutations to response writer inside the handler.
		recV2 := httptest.NewRecorder()

		reqV2, err = newTestRequest(http.MethodPut, getCopyObjectURL("", testCase.bucketName, testCase.newObjectName), 0, nil)
		if err != nil {
			t.Fatalf("Test %d: Failed to create HTTP request for copy Object: <ERROR> %v", i, err)
		}
		// "X-Amz-Copy-Source" header contains the information about the source bucket and the object to copied.
		if testCase.copySourceHeader != "" {
			reqV2.Header.Set("X-Amz-Copy-Source", testCase.copySourceHeader)
		}
		if testCase.copyModifiedHeader != "" {
			reqV2.Header.Set("X-Amz-Copy-Source-If-Modified-Since", testCase.copyModifiedHeader)
		}
		if testCase.copyUnmodifiedHeader != "" {
			reqV2.Header.Set("X-Amz-Copy-Source-If-Unmodified-Since", testCase.copyUnmodifiedHeader)
		}

		// Add custom metadata.
		for k, v := range testCase.metadata {
			reqV2.Header.Set(k, v+"+x")
		}
		if testCase.metadataReplace {
			reqV2.Header.Set("X-Amz-Metadata-Directive", "REPLACE")
		}
		if testCase.metadataCopy {
			reqV2.Header.Set("X-Amz-Metadata-Directive", "COPY")
		}
		if testCase.metadataGarbage {
			reqV2.Header.Set("X-Amz-Metadata-Directive", "Unknown")
		}

		err = signRequestV2(reqV2, testCase.accessKey, testCase.secretKey)
		if err != nil {
			t.Fatalf("Failed to V2 Sign the HTTP request: %v.", err)
		}
		// Since `apiRouter` satisfies `http.Handler` it has a ServeHTTP to execute the logic of the handler.
		// Call the ServeHTTP to execute the handler.
		apiRouter.ServeHTTP(recV2, reqV2)
		if recV2.Code != testCase.expectedRespStatus {
			if testCase.copySourceSame {
				// encryption will rotate creds, so fail only for non-encryption scenario.
				if GlobalKMS == nil {
					t.Errorf("Test %d: %s:  Expected the response status to be `%d`, but instead found `%d`", i, instanceType, testCase.expectedRespStatus, recV2.Code)
				}
			} else {
				t.Errorf("Test %d: %s:  Expected the response status to be `%d`, but instead found `%d`", i+1, instanceType, testCase.expectedRespStatus, recV2.Code)
			}
		}
	}

	// HTTP request to test the case of `objectLayer` being set to `nil`.
	// There is no need to use an existing bucket or valid input for creating the request,
	// since the `objectLayer==nil`  check is performed before any other checks inside the handlers.
	// The only aim is to generate an HTTP request in a way that the relevant/registered end point is evoked/called.
	nilBucket := "dummy-bucket"
	nilObject := "dummy-object"

	nilReq, err := newTestSignedRequestV4(http.MethodPut, getCopyObjectURL("", nilBucket, nilObject),
		0, nil, "", "", nil)

	// Below is how CopyObjectHandler is registered.
	// bucket.Methods(http.MethodPut).Path("/{object:.+}").HeadersRegexp("X-Amz-Copy-Source", ".*?(\\/|%2F).*?")
	// Its necessary to set the "X-Amz-Copy-Source" header for the request to be accepted by the handler.
	nilReq.Header.Set("X-Amz-Copy-Source", url.QueryEscape(SlashSeparator+nilBucket+SlashSeparator+nilObject))
	if err != nil {
		t.Errorf("MinIO %s: Failed to create HTTP request for testing the response when object Layer is set to `nil`.", instanceType)
	}

	// execute the object layer set to `nil` test.
	// `ExecObjectLayerAPINilTest` manages the operation.
	ExecObjectLayerAPINilTest(t, nilBucket, nilObject, instanceType, apiRouter, nilReq)
}

// Wrapper for calling NewMultipartUpload tests for both Erasure multiple disks and single node setup.
// First register the HTTP handler for NewMultipartUpload, then a HTTP request for NewMultipart upload is made.
// The UploadID from the response body is parsed and its existence is asserted with an attempt to ListParts using it.
func TestAPINewMultipartHandler(t *testing.T) {
	defer DetectTestLeak(t)()
	ExecObjectLayerAPITest(ExecObjectLayerAPITestArgs{t: t, objAPITest: testAPINewMultipartHandler, endpoints: []string{"NewMultipart"}})
}

func testAPINewMultipartHandler(obj ObjectLayer, instanceType, bucketName string, apiRouter http.Handler,
	credentials auth.Credentials, t *testing.T,
) {
	objectName := "test-object-new-multipart"
	rec := httptest.NewRecorder()
	// construct HTTP request for NewMultipart upload.
	req, err := newTestSignedRequestV4(http.MethodPost, getNewMultipartURL("", bucketName, objectName),
		0, nil, credentials.AccessKey, credentials.SecretKey, nil)
	if err != nil {
		t.Fatalf("Failed to create HTTP request for NewMultipart Request: <ERROR> %v", err)
	}
	// Since `apiRouter` satisfies `http.Handler` it has a ServeHTTP to execute the logic of the handler.
	// Call the ServeHTTP to executes the registered handler.
	apiRouter.ServeHTTP(rec, req)
	// Assert the response code with the expected status.
	if rec.Code != http.StatusOK {
		t.Fatalf("%s:  Expected the response status to be `%d`, but instead found `%d`", instanceType, http.StatusOK, rec.Code)
	}

	// decode the response body.
	decoder := xml.NewDecoder(rec.Body)
	multipartResponse := &InitiateMultipartUploadResponse{}

	err = decoder.Decode(multipartResponse)
	if err != nil {
		t.Fatalf("Error decoding the recorded response Body")
	}
	// verify the uploadID my making an attempt to list parts.
	_, err = obj.ListObjectParts(context.Background(), bucketName, objectName, multipartResponse.UploadID, 0, 1, ObjectOptions{})
	if err != nil {
		t.Fatalf("Invalid UploadID: <ERROR> %s", err)
	}

	// Testing the response for Invalid AccessID.
	// Forcing the signature check to fail.
	rec = httptest.NewRecorder()
	// construct HTTP request for NewMultipart upload.
	// Setting an invalid accessID.
	req, err = newTestSignedRequestV4(http.MethodPost, getNewMultipartURL("", bucketName, objectName),
		0, nil, "Invalid-AccessID", credentials.SecretKey, nil)
	if err != nil {
		t.Fatalf("Failed to create HTTP request for NewMultipart Request: <ERROR> %v", err)
	}

	// Since `apiRouter` satisfies `http.Handler` it has a ServeHTTP method to execute the logic of the handler.
	// Call the ServeHTTP to executes the registered handler.
	apiRouter.ServeHTTP(rec, req)
	// Assert the response code with the expected status.
	if rec.Code != http.StatusForbidden {
		t.Fatalf("%s:  Expected the response status to be `%d`, but instead found `%d`", instanceType, http.StatusForbidden, rec.Code)
	}

	// Verify response of the V2 signed HTTP request.
	// initialize HTTP NewRecorder, this records any mutations to response writer inside the handler.
	recV2 := httptest.NewRecorder()
	// construct HTTP request for NewMultipartUpload endpoint.
	reqV2, err := newTestSignedRequestV2(http.MethodPost, getNewMultipartURL("", bucketName, objectName),
		0, nil, credentials.AccessKey, credentials.SecretKey, nil)
	if err != nil {
		t.Fatalf("Failed to create HTTP request for NewMultipart Request: <ERROR> %v", err)
	}

	// Since `apiRouter` satisfies `http.Handler` it has a ServeHTTP to execute the logic of the handler.
	// Call the ServeHTTP to execute the handler.
	apiRouter.ServeHTTP(recV2, reqV2)
	// Assert the response code with the expected status.
	if recV2.Code != http.StatusOK {
		t.Fatalf("%s:  Expected the response status to be `%d`, but instead found `%d`", instanceType, http.StatusOK, recV2.Code)
	}
	// decode the response body.
	decoder = xml.NewDecoder(recV2.Body)
	multipartResponse = &InitiateMultipartUploadResponse{}

	err = decoder.Decode(multipartResponse)
	if err != nil {
		t.Fatalf("Error decoding the recorded response Body")
	}
	// verify the uploadID my making an attempt to list parts.
	_, err = obj.ListObjectParts(context.Background(), bucketName, objectName, multipartResponse.UploadID, 0, 1, ObjectOptions{})
	if err != nil {
		t.Fatalf("Invalid UploadID: <ERROR> %s", err)
	}

	// Testing the response for invalid AccessID.
	// Forcing the V2 signature check to fail.
	recV2 = httptest.NewRecorder()
	// construct HTTP request for NewMultipartUpload endpoint.
	// Setting invalid AccessID.
	reqV2, err = newTestSignedRequestV2(http.MethodPost, getNewMultipartURL("", bucketName, objectName),
		0, nil, "Invalid-AccessID", credentials.SecretKey, nil)
	if err != nil {
		t.Fatalf("Failed to create HTTP request for NewMultipart Request: <ERROR> %v", err)
	}

	// Since `apiRouter` satisfies `http.Handler` it has a ServeHTTP to execute the logic of the handler.
	// Call the ServeHTTP to execute the handler.
	apiRouter.ServeHTTP(recV2, reqV2)
	// Assert the response code with the expected status.
	if recV2.Code != http.StatusForbidden {
		t.Fatalf("%s:  Expected the response status to be `%d`, but instead found `%d`", instanceType, http.StatusForbidden, recV2.Code)
	}

	// Test for Anonymous/unsigned http request.
	anonReq, err := newTestRequest(http.MethodPost, getNewMultipartURL("", bucketName, objectName), 0, nil)
	if err != nil {
		t.Fatalf("MinIO %s: Failed to create an anonymous request for %s/%s: <ERROR> %v",
			instanceType, bucketName, objectName, err)
	}

	// ExecObjectLayerAPIAnonTest - Calls the HTTP API handler using the anonymous request, validates the ErrAccessDeniedResponse,
	// sets the bucket policy using the policy statement generated from `getWriteOnlyObjectStatement` so that the
	// unsigned request goes through and its validated again.
	ExecObjectLayerAPIAnonTest(t, obj, "TestAPINewMultipartHandler", bucketName, objectName, instanceType, apiRouter, anonReq, getAnonWriteOnlyObjectPolicy(bucketName, objectName))

	// HTTP request to test the case of `objectLayer` being set to `nil`.
	// There is no need to use an existing bucket or valid input for creating the request,
	// since the `objectLayer==nil`  check is performed before any other checks inside the handlers.
	// The only aim is to generate an HTTP request in a way that the relevant/registered end point is evoked/called.
	nilBucket := "dummy-bucket"
	nilObject := "dummy-object"

	nilReq, err := newTestSignedRequestV4(http.MethodPost, getNewMultipartURL("", nilBucket, nilObject),
		0, nil, "", "", nil)
	if err != nil {
		t.Errorf("MinIO %s: Failed to create HTTP request for testing the response when object Layer is set to `nil`.", instanceType)
	}
	// execute the object layer set to `nil` test.
	// `ExecObjectLayerAPINilTest` manages the operation.
	ExecObjectLayerAPINilTest(t, nilBucket, nilObject, instanceType, apiRouter, nilReq)
}

// Wrapper for calling NewMultipartUploadParallel tests for both Erasure multiple disks and single node setup.
// The objective of the test is to initialte multipart upload on the same object 10 times concurrently,
// The UploadID from the response body is parsed and its existence is asserted with an attempt to ListParts using it.
func TestAPINewMultipartHandlerParallel(t *testing.T) {
	defer DetectTestLeak(t)()
	ExecObjectLayerAPITest(ExecObjectLayerAPITestArgs{t: t, objAPITest: testAPINewMultipartHandlerParallel, endpoints: []string{"NewMultipart"}})
}

func testAPINewMultipartHandlerParallel(obj ObjectLayer, instanceType, bucketName string, apiRouter http.Handler,
	credentials auth.Credentials, t *testing.T,
) {
	// used for storing the uploadID's parsed on concurrent HTTP requests for NewMultipart upload on the same object.
	testUploads := struct {
		sync.Mutex
		uploads []string
	}{}

	objectName := "test-object-new-multipart-parallel"
	var wg sync.WaitGroup
	for range 10 {
		wg.Add(1)
		// Initiate NewMultipart upload on the same object 10 times concurrrently.
		go func() {
			defer wg.Done()
			rec := httptest.NewRecorder()
			// construct HTTP request NewMultipartUpload.
			req, err := newTestSignedRequestV4(http.MethodPost, getNewMultipartURL("", bucketName, objectName), 0, nil, credentials.AccessKey, credentials.SecretKey, nil)
			if err != nil {
				t.Errorf("Failed to create HTTP request for NewMultipart request: <ERROR> %v", err)
				return
			}
			// Since `apiRouter` satisfies `http.Handler` it has a ServeHTTP to execute the logic of the handler.
			// Call the ServeHTTP to executes the registered handler.
			apiRouter.ServeHTTP(rec, req)
			// Assert the response code with the expected status.
			if rec.Code != http.StatusOK {
				t.Errorf("MinIO %s:  Expected the response status to be `%d`, but instead found `%d`", instanceType, http.StatusOK, rec.Code)
				return
			}
			// decode the response body.
			decoder := xml.NewDecoder(rec.Body)
			multipartResponse := &InitiateMultipartUploadResponse{}

			err = decoder.Decode(multipartResponse)
			if err != nil {
				t.Errorf("MinIO %s: Error decoding the recorded response Body", instanceType)
				return
			}
			// push the obtained upload ID from the response into the array.
			testUploads.Lock()
			testUploads.uploads = append(testUploads.uploads, multipartResponse.UploadID)
			testUploads.Unlock()
		}()
	}
	// Wait till all go routines finishes execution.
	wg.Wait()
	// Validate the upload ID by an attempt to list parts using it.
	for _, uploadID := range testUploads.uploads {
		_, err := obj.ListObjectParts(context.Background(), bucketName, objectName, uploadID, 0, 1, ObjectOptions{})
		if err != nil {
			t.Fatalf("Invalid UploadID: <ERROR> %s", err)
		}
	}
}

// The UploadID from the response body is parsed and its existence is asserted with an attempt to ListParts using it.
func TestAPICompleteMultipartHandler(t *testing.T) {
	defer DetectTestLeak(t)()
	ExecObjectLayerAPITest(ExecObjectLayerAPITestArgs{t: t, objAPITest: testAPICompleteMultipartHandler, endpoints: []string{"CompleteMultipart"}})
}

func testAPICompleteMultipartHandler(obj ObjectLayer, instanceType, bucketName string, apiRouter http.Handler,
	credentials auth.Credentials, t *testing.T,
) {
	var err error

	var opts ObjectOptions
	// object used for the test.
	objectName := "test-object-new-multipart"

	// upload IDs collected.
	var uploadIDs []string

	for range 2 {
		// initiate new multipart uploadID.
		res, err := obj.NewMultipartUpload(context.Background(), bucketName, objectName, opts)
		if err != nil {
			// Failed to create NewMultipartUpload, abort.
			t.Fatalf("MinIO %s : <ERROR>  %s", instanceType, err)
		}

		uploadIDs = append(uploadIDs, res.UploadID)
	}

	// Parts with size greater than 5 MiB.
	// Generating a 6 MiB byte array.
	validPart := bytes.Repeat([]byte("abcdef"), 1*humanize.MiByte)
	validPartMD5 := getMD5Hash(validPart)
	// Create multipart parts.
	// Need parts to be uploaded before CompleteMultiPartUpload can be called tested.
	parts := []struct {
		bucketName      string
		objName         string
		uploadID        string
		PartID          int
		inputReaderData string
		inputMd5        string
		inputDataSize   int64
	}{
		// Case 1-4.
		// Creating sequence of parts for same uploadID.
		{bucketName, objectName, uploadIDs[0], 1, "abcd", "e2fc714c4727ee9395f324cd2e7f331f", int64(len("abcd"))},
		{bucketName, objectName, uploadIDs[0], 2, "efgh", "1f7690ebdd9b4caf8fab49ca1757bf27", int64(len("efgh"))},
		{bucketName, objectName, uploadIDs[0], 3, "ijkl", "09a0877d04abf8759f99adec02baf579", int64(len("abcd"))},
		{bucketName, objectName, uploadIDs[0], 4, "mnop", "e132e96a5ddad6da8b07bba6f6131fef", int64(len("abcd"))},
		// Part with size larger than 5 MiB.
		{bucketName, objectName, uploadIDs[0], 5, string(validPart), validPartMD5, int64(len(validPart))},
		{bucketName, objectName, uploadIDs[0], 6, string(validPart), validPartMD5, int64(len(validPart))},

		// Part with size larger than 5 MiB.
		// Parts uploaded for anonymous/unsigned API handler test.
		{bucketName, objectName, uploadIDs[1], 1, string(validPart), validPartMD5, int64(len(validPart))},
		{bucketName, objectName, uploadIDs[1], 2, string(validPart), validPartMD5, int64(len(validPart))},
	}
	// Iterating over creatPartCases to generate multipart chunks.
	for _, part := range parts {
		_, err = obj.PutObjectPart(context.Background(), part.bucketName, part.objName, part.uploadID, part.PartID,
			mustGetPutObjReader(t, strings.NewReader(part.inputReaderData), part.inputDataSize, part.inputMd5, ""), opts)
		if err != nil {
			t.Fatalf("%s : %s", instanceType, err)
		}
	}
	// Parts to be sent as input for CompleteMultipartUpload.
	inputParts := []struct {
		parts []CompletePart
	}{
		// inputParts - 0.
		// Case for replicating ETag mismatch.
		{
			[]CompletePart{
				{ETag: "abcd", PartNumber: 1},
			},
		},
		// inputParts - 1.
		// should error out with part too small.
		{
			[]CompletePart{
				{ETag: "e2fc714c4727ee9395f324cd2e7f331f", PartNumber: 1},
				{ETag: "1f7690ebdd9b4caf8fab49ca1757bf27", PartNumber: 2},
			},
		},
		// inputParts - 2.
		// Case with invalid Part number.
		{
			[]CompletePart{
				{ETag: "e2fc714c4727ee9395f324cd2e7f331f", PartNumber: 10},
			},
		},
		// inputParts - 3.
		// Case with valid parts,but parts are unsorted.
		// Part size greater than 5 MiB.
		{
			[]CompletePart{
				{ETag: validPartMD5, PartNumber: 6},
				{ETag: validPartMD5, PartNumber: 5},
			},
		},
		// inputParts - 4.
		// Case with valid part.
		// Part size greater than 5 MiB.
		{
			[]CompletePart{
				{ETag: validPartMD5, PartNumber: 5},
				{ETag: validPartMD5, PartNumber: 6},
			},
		},

		// inputParts - 5.
		// Used for the case of testing for anonymous API request.
		// Part size greater than 5 MiB.
		{
			[]CompletePart{
				{ETag: validPartMD5, PartNumber: 1},
				{ETag: validPartMD5, PartNumber: 2},
			},
		},
	}

	// on successful complete multipart operation the s3MD5 for the parts uploaded will be returned.
	s3MD5 := getCompleteMultipartMD5(inputParts[3].parts)

	// generating the response body content for the success case.
	successResponse := generateCompleteMultipartUploadResponse(bucketName, objectName, getGetObjectURL("", bucketName, objectName), ObjectInfo{ETag: s3MD5}, nil)
	encodedSuccessResponse := encodeResponse(successResponse)

	ctx := context.Background()

	testCases := []struct {
		bucket    string
		object    string
		uploadID  string
		parts     []CompletePart
		accessKey string
		secretKey string
		// Expected output of CompleteMultipartUpload.
		expectedContent []byte
		// Expected HTTP Response status.
		expectedRespStatus int
	}{
		// Test case - 1.
		// Upload and PartNumber exists, But a deliberate ETag mismatch is introduced.
		{
			bucket:    bucketName,
			object:    objectName,
			uploadID:  uploadIDs[0],
			parts:     inputParts[0].parts,
			accessKey: credentials.AccessKey,
			secretKey: credentials.SecretKey,

			expectedContent: encodeResponse(getAPIErrorResponse(ctx,
				toAPIError(ctx, InvalidPart{}),
				getGetObjectURL("", bucketName, objectName), "", "")),
			expectedRespStatus: http.StatusBadRequest,
		},
		// Test case - 2.
		// No parts specified in CompletePart{}.
		// Should return ErrMalformedXML in the response body.
		{
			bucket:    bucketName,
			object:    objectName,
			uploadID:  uploadIDs[0],
			parts:     []CompletePart{},
			accessKey: credentials.AccessKey,
			secretKey: credentials.SecretKey,

			expectedContent: encodeResponse(getAPIErrorResponse(ctx,
				getAPIError(ErrMalformedXML),
				getGetObjectURL("", bucketName, objectName), "", "")),
			expectedRespStatus: http.StatusBadRequest,
		},
		// Test case - 3.
		// Non-Existent uploadID.
		// 404 Not Found response status expected.
		{
			bucket:    bucketName,
			object:    objectName,
			uploadID:  "abc",
			parts:     inputParts[0].parts,
			accessKey: credentials.AccessKey,
			secretKey: credentials.SecretKey,

			expectedContent: encodeResponse(getAPIErrorResponse(ctx,
				toAPIError(ctx, InvalidUploadID{UploadID: "abc"}),
				getGetObjectURL("", bucketName, objectName), "", "")),
			expectedRespStatus: http.StatusNotFound,
		},
		// Test case - 4.
		// Case with part size being less than minimum allowed size.
		{
			bucket:    bucketName,
			object:    objectName,
			uploadID:  uploadIDs[0],
			parts:     inputParts[1].parts,
			accessKey: credentials.AccessKey,
			secretKey: credentials.SecretKey,

			expectedContent: encodeResponse(getAPIErrorResponse(ctx,
				toAPIError(ctx, PartTooSmall{PartNumber: 1}),
				getGetObjectURL("", bucketName, objectName), "", "")),
			expectedRespStatus: http.StatusBadRequest,
		},
		// Test case - 5.
		// TestCase with invalid Part Number.
		{
			bucket:    bucketName,
			object:    objectName,
			uploadID:  uploadIDs[0],
			parts:     inputParts[2].parts,
			accessKey: credentials.AccessKey,
			secretKey: credentials.SecretKey,

			expectedContent: encodeResponse(getAPIErrorResponse(ctx,
				toAPIError(ctx, InvalidPart{}),
				getGetObjectURL("", bucketName, objectName), "", "")),
			expectedRespStatus: http.StatusBadRequest,
		},
		// Test case - 6.
		// Parts are not sorted according to the part number.
		// This should return ErrInvalidPartOrder in the response body.
		{
			bucket:    bucketName,
			object:    objectName,
			uploadID:  uploadIDs[0],
			parts:     inputParts[3].parts,
			accessKey: credentials.AccessKey,
			secretKey: credentials.SecretKey,

			expectedContent: encodeResponse(getAPIErrorResponse(ctx,
				getAPIError(ErrInvalidPartOrder),
				getGetObjectURL("", bucketName, objectName), "", "")),
			expectedRespStatus: http.StatusBadRequest,
		},
		// Test case - 7.
		// Test case with proper parts.
		// Should succeeded and the content in the response body is asserted.
		{
			bucket:    bucketName,
			object:    objectName,
			uploadID:  uploadIDs[0],
			parts:     inputParts[4].parts,
			accessKey: "Invalid-AccessID",
			secretKey: credentials.SecretKey,

			expectedContent: encodeResponse(getAPIErrorResponse(ctx,
				getAPIError(ErrInvalidAccessKeyID),
				getGetObjectURL("", bucketName, objectName), "", "")),
			expectedRespStatus: http.StatusForbidden,
		},
		// Test case - 8.
		// Test case with proper parts.
		// Should succeeded and the content in the response body is asserted.
		{
			bucket:    bucketName,
			object:    objectName,
			uploadID:  uploadIDs[0],
			parts:     inputParts[4].parts,
			accessKey: credentials.AccessKey,
			secretKey: credentials.SecretKey,

			expectedContent:    encodedSuccessResponse,
			expectedRespStatus: http.StatusOK,
		},
	}

	for i, testCase := range testCases {
		var req *http.Request
		var completeBytes, actualContent []byte
		// Complete multipart upload parts.
		completeUploads := &CompleteMultipartUpload{
			Parts: testCase.parts,
		}
		completeBytes, err = xml.Marshal(completeUploads)
		if err != nil {
			t.Fatalf("Error XML encoding of parts: <ERROR> %s.", err)
		}
		// Indicating that all parts are uploaded and initiating CompleteMultipartUpload.
		req, err = newTestSignedRequestV4(http.MethodPost, getCompleteMultipartUploadURL("", bucketName, objectName, testCase.uploadID),
			int64(len(completeBytes)), bytes.NewReader(completeBytes), testCase.accessKey, testCase.secretKey, nil)
		if err != nil {
			t.Fatalf("Failed to create HTTP request for CompleteMultipartUpload: <ERROR> %v", err)
		}

		rec := httptest.NewRecorder()

		// Since `apiRouter` satisfies `http.Handler` it has a ServeHTTP to execute the logic of the handler.
		// Call the ServeHTTP to executes the registered handler.
		apiRouter.ServeHTTP(rec, req)
		// Assert the response code with the expected status.
		if rec.Code != testCase.expectedRespStatus {
			t.Errorf("Case %d: MinIO %s: Expected the response status to be `%d`, but instead found `%d`", i+1, instanceType, testCase.expectedRespStatus, rec.Code)
		}

		// read the response body.
		actualContent, err = io.ReadAll(rec.Body)
		if err != nil {
			t.Fatalf("Test %d : MinIO %s: Failed parsing response body: <ERROR> %v", i+1, instanceType, err)
		}

		if rec.Code == http.StatusOK {
			// Verify whether the bucket obtained object is same as the one created.
			if !bytes.Equal(testCase.expectedContent, actualContent) {
				t.Errorf("Test %d : MinIO %s: CompleteMultipart response content differs from expected value. got %s, expected %s", i+1, instanceType,
					string(actualContent), string(testCase.expectedContent))
			}
			continue
		}

		actualError := &APIErrorResponse{}
		if err = xml.Unmarshal(actualContent, actualError); err != nil {
			t.Errorf("MinIO %s: error response failed to parse error XML", instanceType)
		}

		if actualError.BucketName != bucketName {
			t.Errorf("MinIO %s: error response bucket name differs from expected value", instanceType)
		}

		if actualError.Key != objectName {
			t.Errorf("MinIO %s: error response object name (%s) differs from expected value (%s)", instanceType, actualError.Key, objectName)
		}
	}

	// Testing for anonymous API request.
	var completeBytes []byte
	// Complete multipart upload parts.
	completeUploads := &CompleteMultipartUpload{
		Parts: inputParts[5].parts,
	}
	completeBytes, err = xml.Marshal(completeUploads)
	if err != nil {
		t.Fatalf("Error XML encoding of parts: <ERROR> %s.", err)
	}

	// create unsigned HTTP request for CompleteMultipart upload.
	anonReq, err := newTestRequest(http.MethodPost, getCompleteMultipartUploadURL("", bucketName, objectName, uploadIDs[1]),
		int64(len(completeBytes)), bytes.NewReader(completeBytes))
	if err != nil {
		t.Fatalf("MinIO %s: Failed to create an anonymous request for %s/%s: <ERROR> %v",
			instanceType, bucketName, objectName, err)
	}

	// ExecObjectLayerAPIAnonTest - Calls the HTTP API handler using the anonymous request, validates the ErrAccessDeniedResponse,
	// sets the bucket policy using the policy statement generated from `getWriteOnlyObjectStatement` so that the
	// unsigned request goes through and its validated again.
	ExecObjectLayerAPIAnonTest(t, obj, "TestAPICompleteMultipartHandler", bucketName, objectName, instanceType,
		apiRouter, anonReq, getAnonWriteOnlyObjectPolicy(bucketName, objectName))

	// HTTP request to test the case of `objectLayer` being set to `nil`.
	// There is no need to use an existing bucket or valid input for creating the request,
	// since the `objectLayer==nil`  check is performed before any other checks inside the handlers.
	// The only aim is to generate an HTTP request in a way that the relevant/registered end point is evoked/called.
	// Indicating that all parts are uploaded and initiating CompleteMultipartUpload.
	nilBucket := "dummy-bucket"
	nilObject := "dummy-object"

	nilReq, err := newTestSignedRequestV4(http.MethodPost, getCompleteMultipartUploadURL("", nilBucket, nilObject, "dummy-uploadID"),
		0, nil, "", "", nil)
	if err != nil {
		t.Errorf("MinIO %s: Failed to create HTTP request for testing the response when object Layer is set to `nil`.", instanceType)
	}
	// execute the object layer set to `nil` test.
	// `ExecObjectLayerAPINilTest` manages the operation.
	ExecObjectLayerAPINilTest(t, nilBucket, nilObject, instanceType, apiRouter, nilReq)
}

// The UploadID from the response body is parsed and its existence is asserted with an attempt to ListParts using it.
func TestAPIAbortMultipartHandler(t *testing.T) {
	defer DetectTestLeak(t)()
	ExecObjectLayerAPITest(ExecObjectLayerAPITestArgs{t: t, objAPITest: testAPIAbortMultipartHandler, endpoints: []string{"AbortMultipart"}})
}

func testAPIAbortMultipartHandler(obj ObjectLayer, instanceType, bucketName string, apiRouter http.Handler,
	credentials auth.Credentials, t *testing.T,
) {
	var err error
	opts := ObjectOptions{}
	// object used for the test.
	objectName := "test-object-new-multipart"

	// upload IDs collected.
	var uploadIDs []string

	for range 2 {
		// initiate new multipart uploadID.
		res, err := obj.NewMultipartUpload(context.Background(), bucketName, objectName, opts)
		if err != nil {
			// Failed to create NewMultipartUpload, abort.
			t.Fatalf("MinIO %s : <ERROR>  %s", instanceType, err)
		}

		uploadIDs = append(uploadIDs, res.UploadID)
	}

	// Parts with size greater than 5 MiB.
	// Generating a 6 MiB byte array.
	validPart := bytes.Repeat([]byte("abcdef"), 1*humanize.MiByte)
	validPartMD5 := getMD5Hash(validPart)
	// Create multipart parts.
	// Need parts to be uploaded before AbortMultiPartUpload can be called tested.
	parts := []struct {
		bucketName      string
		objName         string
		uploadID        string
		PartID          int
		inputReaderData string
		inputMd5        string
		inputDataSize   int64
	}{
		// Case 1-4.
		// Creating sequence of parts for same uploadID.
		{bucketName, objectName, uploadIDs[0], 1, "abcd", "e2fc714c4727ee9395f324cd2e7f331f", int64(len("abcd"))},
		{bucketName, objectName, uploadIDs[0], 2, "efgh", "1f7690ebdd9b4caf8fab49ca1757bf27", int64(len("efgh"))},
		{bucketName, objectName, uploadIDs[0], 3, "ijkl", "09a0877d04abf8759f99adec02baf579", int64(len("abcd"))},
		{bucketName, objectName, uploadIDs[0], 4, "mnop", "e132e96a5ddad6da8b07bba6f6131fef", int64(len("abcd"))},
		// Part with size larger than 5 MiB.
		{bucketName, objectName, uploadIDs[0], 5, string(validPart), validPartMD5, int64(len(validPart))},
		{bucketName, objectName, uploadIDs[0], 6, string(validPart), validPartMD5, int64(len(validPart))},

		// Part with size larger than 5 MiB.
		// Parts uploaded for anonymous/unsigned API handler test.
		{bucketName, objectName, uploadIDs[1], 1, string(validPart), validPartMD5, int64(len(validPart))},
		{bucketName, objectName, uploadIDs[1], 2, string(validPart), validPartMD5, int64(len(validPart))},
	}
	// Iterating over createPartCases to generate multipart chunks.
	for _, part := range parts {
		_, err = obj.PutObjectPart(context.Background(), part.bucketName, part.objName, part.uploadID, part.PartID,
			mustGetPutObjReader(t, strings.NewReader(part.inputReaderData), part.inputDataSize, part.inputMd5, ""), opts)
		if err != nil {
			t.Fatalf("%s : %s", instanceType, err)
		}
	}

	testCases := []struct {
		bucket    string
		object    string
		uploadID  string
		accessKey string
		secretKey string
		// Expected HTTP Response status.
		expectedRespStatus int
	}{
		// Test case - 1.
		// Abort existing upload ID.
		{
			bucket:             bucketName,
			object:             objectName,
			uploadID:           uploadIDs[0],
			accessKey:          credentials.AccessKey,
			secretKey:          credentials.SecretKey,
			expectedRespStatus: http.StatusNoContent,
		},
		// Test case - 2.
		// Abort non-existing upload ID.
		{
			bucket:             bucketName,
			object:             objectName,
			uploadID:           "nonexistent-upload-id",
			accessKey:          credentials.AccessKey,
			secretKey:          credentials.SecretKey,
			expectedRespStatus: http.StatusNotFound,
		},
		// Test case - 3.
		// Abort with unknown Access key.
		{
			bucket:             bucketName,
			object:             objectName,
			uploadID:           uploadIDs[0],
			accessKey:          "Invalid-AccessID",
			secretKey:          credentials.SecretKey,
			expectedRespStatus: http.StatusForbidden,
		},
	}

	for i, testCase := range testCases {
		var req *http.Request
		// Indicating that all parts are uploaded and initiating abortMultipartUpload.
		req, err = newTestSignedRequestV4(http.MethodDelete, getAbortMultipartUploadURL("", testCase.bucket, testCase.object, testCase.uploadID),
			0, nil, testCase.accessKey, testCase.secretKey, nil)
		if err != nil {
			t.Fatalf("Failed to create HTTP request for AbortMultipartUpload: <ERROR> %v", err)
		}

		rec := httptest.NewRecorder()

		// Since `apiRouter` satisfies `http.Handler` it has a ServeHTTP to execute the logic of the handler.
		// Call the ServeHTTP to executes the registered handler.
		apiRouter.ServeHTTP(rec, req)
		// Assert the response code with the expected status.
		if rec.Code != testCase.expectedRespStatus {
			t.Errorf("Case %d: MinIO %s: Expected the response status to be `%d`, but instead found `%d`", i+1, instanceType, testCase.expectedRespStatus, rec.Code)
		}
	}

	// create unsigned HTTP request for Abort multipart upload.
	anonReq, err := newTestRequest(http.MethodDelete, getAbortMultipartUploadURL("", bucketName, objectName, uploadIDs[1]),
		0, nil)
	if err != nil {
		t.Fatalf("MinIO %s: Failed to create an anonymous request for %s/%s: <ERROR> %v",
			instanceType, bucketName, objectName, err)
	}

	// ExecObjectLayerAPIAnonTest - Calls the HTTP API handler using the anonymous request, validates the ErrAccessDeniedResponse,
	// sets the bucket policy using the policy statement generated from `getWriteOnlyObjectStatement` so that the
	// unsigned request goes through and its validated again.
	ExecObjectLayerAPIAnonTest(t, obj, "TestAPIAbortMultipartHandler", bucketName, objectName, instanceType,
		apiRouter, anonReq, getAnonWriteOnlyObjectPolicy(bucketName, objectName))

	// HTTP request to test the case of `objectLayer` being set to `nil`.
	// There is no need to use an existing bucket or valid input for creating the request,
	// since the `objectLayer==nil`  check is performed before any other checks inside the handlers.
	// The only aim is to generate an HTTP request in a way that the relevant/registered end point is evoked/called.
	// Indicating that all parts are uploaded and initiating abortMultipartUpload.
	nilBucket := "dummy-bucket"
	nilObject := "dummy-object"

	nilReq, err := newTestSignedRequestV4(http.MethodDelete, getAbortMultipartUploadURL("", nilBucket, nilObject, "dummy-uploadID"),
		0, nil, "", "", nil)
	if err != nil {
		t.Errorf("MinIO %s: Failed to create HTTP request for testing the response when object Layer is set to `nil`.", instanceType)
	}
	// execute the object layer set to `nil` test.
	// `ExecObjectLayerAPINilTest` manages the operation.
	ExecObjectLayerAPINilTest(t, nilBucket, nilObject, instanceType, apiRouter, nilReq)
}

// Wrapper for calling Delete Object API handler tests for both Erasure multiple disks and FS single drive setup.
func TestAPIDeleteObjectHandler(t *testing.T) {
	defer DetectTestLeak(t)()
	ExecObjectLayerAPITest(ExecObjectLayerAPITestArgs{t: t, objAPITest: testAPIDeleteObjectHandler, endpoints: []string{"DeleteObject"}})
}

func testAPIDeleteObjectHandler(obj ObjectLayer, instanceType, bucketName string, apiRouter http.Handler,
	credentials auth.Credentials, t *testing.T,
) {
	var err error
	objectName := "test-object"
	// Object used for anonymous API request test.
	anonObjectName := "test-anon-obj"
	// set of byte data for PutObject.
	// object has to be created before running tests for Deleting the object.
	bytesData := []struct {
		byteData []byte
	}{
		{generateBytesData(6 * humanize.MiByte)},
	}

	// set of inputs for uploading the objects before tests for deleting them is done.
	putObjectInputs := []struct {
		bucketName    string
		objectName    string
		contentLength int64
		textData      []byte
		metaData      map[string]string
	}{
		// case - 1.
		{bucketName, objectName, int64(len(bytesData[0].byteData)), bytesData[0].byteData, make(map[string]string)},
		// case - 2.
		{bucketName, anonObjectName, int64(len(bytesData[0].byteData)), bytesData[0].byteData, make(map[string]string)},
	}
	// iterate through the above set of inputs and upload the object.
	for i, input := range putObjectInputs {
		// uploading the object.
		_, err = obj.PutObject(context.Background(), input.bucketName, input.objectName, mustGetPutObjReader(t, bytes.NewReader(input.textData), input.contentLength, input.metaData[""], ""), ObjectOptions{UserDefined: input.metaData})
		// if object upload fails stop the test.
		if err != nil {
			t.Fatalf("Put Object case %d:  Error uploading object: <ERROR> %v", i+1, err)
		}
	}

	// test cases with inputs and expected result for DeleteObject.
	testCases := []struct {
		bucketName string
		objectName string
		accessKey  string
		secretKey  string

		expectedRespStatus int // expected response status body.
	}{
		// Test case - 1.
		// Deleting an existing object.
		// Expected to return HTTP response status code 204.
		{
			bucketName: bucketName,
			objectName: objectName,
			accessKey:  credentials.AccessKey,
			secretKey:  credentials.SecretKey,

			expectedRespStatus: http.StatusNoContent,
		},
		// Test case - 2.
		// Attempt to delete an object which is already deleted.
		// Still should return http response status 204.
		{
			bucketName: bucketName,
			objectName: objectName,
			accessKey:  credentials.AccessKey,
			secretKey:  credentials.SecretKey,

			expectedRespStatus: http.StatusNoContent,
		},
		// Test case - 3.
		// Setting Invalid AccessKey to force signature check inside the handler to fail.
		// Should return HTTP response status 403 forbidden.
		{
			bucketName: bucketName,
			objectName: objectName,
			accessKey:  "Invalid-AccessKey",
			secretKey:  credentials.SecretKey,

			expectedRespStatus: http.StatusForbidden,
		},
	}

	// Iterating over the cases, call DeleteObjectHandler and validate the HTTP response.
	for i, testCase := range testCases {
		var req, reqV2 *http.Request
		// initialize HTTP NewRecorder, this records any mutations to response writer inside the handler.
		rec := httptest.NewRecorder()
		// construct HTTP request for Delete Object end point.
		req, err = newTestSignedRequestV4(http.MethodDelete, getDeleteObjectURL("", testCase.bucketName, testCase.objectName),
			0, nil, testCase.accessKey, testCase.secretKey, nil)
		if err != nil {
			t.Fatalf("Test %d: Failed to create HTTP request for Delete Object: <ERROR> %v", i+1, err)
		}
		// Since `apiRouter` satisfies `http.Handler` it has a ServeHTTP to execute the logic of the handler.
		// Call the ServeHTTP to execute the handler,`func (api objectAPIHandlers) DeleteObjectHandler`  handles the request.
		apiRouter.ServeHTTP(rec, req)
		// Assert the response code with the expected status.
		if rec.Code != testCase.expectedRespStatus {
			t.Fatalf("MinIO %s: Case %d: Expected the response status to be `%d`, but instead found `%d`", instanceType, i+1, testCase.expectedRespStatus, rec.Code)
		}

		// Verify response of the V2 signed HTTP request.
		// initialize HTTP NewRecorder, this records any mutations to response writer inside the handler.
		recV2 := httptest.NewRecorder()
		// construct HTTP request for Delete Object endpoint.
		reqV2, err = newTestSignedRequestV2(http.MethodDelete, getDeleteObjectURL("", testCase.bucketName, testCase.objectName),
			0, nil, testCase.accessKey, testCase.secretKey, nil)
		if err != nil {
			t.Fatalf("Failed to create HTTP request for NewMultipart Request: <ERROR> %v", err)
		}

		// Since `apiRouter` satisfies `http.Handler` it has a ServeHTTP to execute the logic of the handler.
		// Call the ServeHTTP to execute the handler.
		apiRouter.ServeHTTP(recV2, reqV2)
		// Assert the response code with the expected status.
		if recV2.Code != testCase.expectedRespStatus {
			t.Errorf("Case %d: MinIO %s: Expected the response status to be `%d`, but instead found `%d`", i+1,
				instanceType, testCase.expectedRespStatus, recV2.Code)
		}
	}

	// Test for Anonymous/unsigned http request.
	anonReq, err := newTestRequest(http.MethodDelete, getDeleteObjectURL("", bucketName, anonObjectName), 0, nil)
	if err != nil {
		t.Fatalf("MinIO %s: Failed to create an anonymous request for %s/%s: <ERROR> %v",
			instanceType, bucketName, anonObjectName, err)
	}

	// ExecObjectLayerAPIAnonTest - Calls the HTTP API handler using the anonymous request, validates the ErrAccessDeniedResponse,
	// sets the bucket policy using the policy statement generated from `getWriteOnlyObjectStatement` so that the
	// unsigned request goes through and its validated again.
	ExecObjectLayerAPIAnonTest(t, obj, "TestAPIDeleteObjectHandler", bucketName, anonObjectName, instanceType, apiRouter, anonReq, getAnonWriteOnlyObjectPolicy(bucketName, anonObjectName))

	// HTTP request to test the case of `objectLayer` being set to `nil`.
	// There is no need to use an existing bucket or valid input for creating the request,
	// since the `objectLayer==nil`  check is performed before any other checks inside the handlers.
	// The only aim is to generate an HTTP request in a way that the relevant/registered end point is evoked/called.
	nilBucket := "dummy-bucket"
	nilObject := "dummy-object"

	nilReq, err := newTestSignedRequestV4(http.MethodDelete, getDeleteObjectURL("", nilBucket, nilObject),
		0, nil, "", "", nil)
	if err != nil {
		t.Errorf("MinIO %s: Failed to create HTTP request for testing the response when object Layer is set to `nil`.", instanceType)
	}
	// execute the object layer set to `nil` test.
	// `ExecObjectLayerAPINilTest` manages the operation.
	ExecObjectLayerAPINilTest(t, nilBucket, nilObject, instanceType, apiRouter, nilReq)
}

// TestAPIPutObjectPartHandlerStreaming - Tests validate the response of PutObjectPart HTTP handler
// when the request signature type is `streaming signature`.
func TestAPIPutObjectPartHandlerStreaming(t *testing.T) {
	defer DetectTestLeak(t)()
	ExecExtendedObjectLayerAPITest(t, testAPIPutObjectPartHandlerStreaming, []string{"NewMultipart", "PutObjectPart"})
}

func testAPIPutObjectPartHandlerStreaming(obj ObjectLayer, instanceType, bucketName string, apiRouter http.Handler,
	credentials auth.Credentials, t *testing.T,
) {
	testObject := "testobject"
	rec := httptest.NewRecorder()
	req, err := newTestSignedRequestV4(http.MethodPost, getNewMultipartURL("", bucketName, "testobject"),
		0, nil, credentials.AccessKey, credentials.SecretKey, nil)
	if err != nil {
		t.Fatalf("[%s] - Failed to create a signed request to initiate multipart upload for %s/%s: <ERROR> %v",
			instanceType, bucketName, testObject, err)
	}
	apiRouter.ServeHTTP(rec, req)

	// Get uploadID of the multipart upload initiated.
	var mpartResp InitiateMultipartUploadResponse
	mpartRespBytes, err := io.ReadAll(rec.Result().Body)
	if err != nil {
		t.Fatalf("[%s] Failed to read NewMultipartUpload response <ERROR> %v", instanceType, err)
	}
	err = xml.Unmarshal(mpartRespBytes, &mpartResp)
	if err != nil {
		t.Fatalf("[%s] Failed to unmarshal NewMultipartUpload response <ERROR> %v", instanceType, err)
	}

	noAPIErr := APIError{}
	missingDateHeaderErr := getAPIError(ErrMissingDateHeader)
	internalErr := getAPIError(ErrBadRequest)
	testCases := []struct {
		fault       Fault
		expectedErr APIError
	}{
		{BadSignature, missingDateHeaderErr},
		{None, noAPIErr},
		{TooBigDecodedLength, internalErr},
	}

	for i, test := range testCases {
		rec = httptest.NewRecorder()
		req, err = newTestStreamingSignedRequest(http.MethodPut,
			getPutObjectPartURL("", bucketName, testObject, mpartResp.UploadID, "1"),
			5, 1, bytes.NewReader([]byte("hello")), credentials.AccessKey, credentials.SecretKey)
		if err != nil {
			t.Fatalf("Failed to create new streaming signed HTTP request: <ERROR> %v.", err)
		}
		switch test.fault {
		case BadSignature:
			// Reset date field in header to make streaming signature fail.
			req.Header.Set("x-amz-date", "")
		case TooBigDecodedLength:
			// Set decoded length to a large value out of int64 range to simulate parse failure.
			req.Header.Set("x-amz-decoded-content-length", "9999999999999999999999")
		}
		apiRouter.ServeHTTP(rec, req)

		if test.expectedErr != noAPIErr {
			errBytes, err := io.ReadAll(rec.Result().Body)
			if err != nil {
				t.Fatalf("Test %d %s Failed to read error response from upload part request %s/%s: <ERROR> %v",
					i+1, instanceType, bucketName, testObject, err)
			}
			var errXML APIErrorResponse
			err = xml.Unmarshal(errBytes, &errXML)
			if err != nil {
				t.Fatalf("Test %d %s Failed to unmarshal error response from upload part request %s/%s: <ERROR> %v",
					i+1, instanceType, bucketName, testObject, err)
			}
			if test.expectedErr.Code != errXML.Code {
				t.Errorf("Test %d %s expected to fail with error %s, but received %s", i+1, instanceType,
					test.expectedErr.Code, errXML.Code)
			}
		} else if rec.Code != http.StatusOK {
			t.Errorf("Test %d %s expected to succeed, but failed with HTTP status code %d",
				i+1, instanceType, rec.Code)
		}
	}
}

// TestAPIPutObjectPartHandler - Tests validate the response of PutObjectPart HTTP handler
//
//	for variety of inputs.
func TestAPIPutObjectPartHandler(t *testing.T) {
	defer DetectTestLeak(t)()
	ExecExtendedObjectLayerAPITest(t, testAPIPutObjectPartHandler, []string{"PutObjectPart"})
}

func testAPIPutObjectPartHandler(obj ObjectLayer, instanceType, bucketName string, apiRouter http.Handler,
	credentials auth.Credentials, t *testing.T,
) {
	// Initiate Multipart upload for testing PutObjectPartHandler.
	testObject := "testobject"
	var opts ObjectOptions
	// PutObjectPart API HTTP Handler has to be tested in isolation,
	// that is without any other handler being registered,
	// That's why NewMultipartUpload is initiated using ObjectLayer.
	res, err := obj.NewMultipartUpload(context.Background(), bucketName, testObject, opts)
	if err != nil {
		// Failed to create NewMultipartUpload, abort.
		t.Fatalf("MinIO %s : <ERROR>  %s", instanceType, err)
	}
	uploadID := res.UploadID

	uploadIDCopy := uploadID

	// SignatureMismatch for various signing types
	testCases := []struct {
		objectName string
		content    string
		partNumber string
		fault      Fault
		accessKey  string
		secretKey  string

		expectedAPIError APIErrorCode
	}{
		// Success case.
		0: {
			objectName: testObject,
			content:    "hello",
			partNumber: "1",
			fault:      None,
			accessKey:  credentials.AccessKey,
			secretKey:  credentials.SecretKey,

			expectedAPIError: -1,
		},
		// Case where part number is invalid.
		1: {
			objectName: testObject,
			content:    "hello",
			partNumber: "9999999999999999999",
			fault:      None,
			accessKey:  credentials.AccessKey,
			secretKey:  credentials.SecretKey,

			expectedAPIError: ErrInvalidPart,
		},
		// Case where the part number has exceeded the max allowed parts in an upload.
		2: {
			objectName: testObject,
			content:    "hello",
			partNumber: strconv.Itoa(globalMaxPartID + 1),
			fault:      None,
			accessKey:  credentials.AccessKey,
			secretKey:  credentials.SecretKey,

			expectedAPIError: ErrInvalidMaxParts,
		},
		// Case where the content length is not set in the HTTP request.
		3: {
			objectName: testObject,
			content:    "hello",
			partNumber: "1",
			fault:      MissingContentLength,
			accessKey:  credentials.AccessKey,
			secretKey:  credentials.SecretKey,

			expectedAPIError: ErrMissingContentLength,
		},
		// case where the object size is set to a value greater than the max allowed size.
		4: {
			objectName: testObject,
			content:    "hello",
			partNumber: "1",
			fault:      TooBigObject,
			accessKey:  credentials.AccessKey,
			secretKey:  credentials.SecretKey,

			expectedAPIError: ErrEntityTooLarge,
		},
		// case where a signature mismatch is introduced and the response is validated.
		5: {
			objectName: testObject,
			content:    "hello",
			partNumber: "1",
			fault:      BadSignature,
			accessKey:  credentials.AccessKey,
			secretKey:  credentials.SecretKey,

			expectedAPIError: ErrSignatureDoesNotMatch,
		},
		// Case where incorrect checksum is set and the error response
		// is asserted with the expected error response.
		6: {
			objectName: testObject,
			content:    "hello",
			partNumber: "1",
			fault:      BadMD5,
			accessKey:  credentials.AccessKey,
			secretKey:  credentials.SecretKey,

			expectedAPIError: ErrInvalidDigest,
		},
		// case where the a non-existent uploadID is set.
		7: {
			objectName: testObject,
			content:    "hello",
			partNumber: "1",
			fault:      MissingUploadID,
			accessKey:  credentials.AccessKey,
			secretKey:  credentials.SecretKey,

			expectedAPIError: ErrNoSuchUpload,
		},
		// case with invalid AccessID.
		// Forcing the signature check inside the handler to fail.
		8: {
			objectName: testObject,
			content:    "hello",
			partNumber: "1",
			fault:      None,
			accessKey:  "Invalid-AccessID",
			secretKey:  credentials.SecretKey,

			expectedAPIError: ErrInvalidAccessKeyID,
		},
		// Case where part number is invalid.
		9: {
			objectName: testObject,
			content:    "hello",
			partNumber: "0",
			fault:      None,
			accessKey:  credentials.AccessKey,
			secretKey:  credentials.SecretKey,

			expectedAPIError: ErrInvalidPart,
		},
		10: {
			objectName: testObject,
			content:    "hello",
			partNumber: "-10",
			fault:      None,
			accessKey:  credentials.AccessKey,
			secretKey:  credentials.SecretKey,

			expectedAPIError: ErrInvalidPart,
		},
	}

	reqV2Str := "V2 Signed HTTP request"
	reqV4Str := "V4 Signed HTTP request"

	type inputReqRec struct {
		req     *http.Request
		rec     *httptest.ResponseRecorder
		reqType string
	}

	for i, test := range testCases {
		// Using sub-tests introduced in Go 1.7.
		t.Run(fmt.Sprintf("MinIO-%s-Test-%d.", instanceType, i), func(t *testing.T) {
			// collection of input HTTP request, ResponseRecorder and request type.
			// Used to make a collection of V4 and V4 HTTP request.
			var reqV4, reqV2 *http.Request
			var recV4, recV2 *httptest.ResponseRecorder

			// initialize HTTP NewRecorder, this records any mutations to response writer inside the handler.
			recV4 = httptest.NewRecorder()
			recV2 = httptest.NewRecorder()
			// setting a non-existent uploadID.
			// deliberately introducing the invalid value to be able to assert the response with the expected error response.
			if test.fault == MissingUploadID {
				uploadID = "upload1"
			}
			// constructing a v4 signed HTTP request.
			reqV4, err = newTestSignedRequestV4(http.MethodPut,
				getPutObjectPartURL("", bucketName, test.objectName, uploadID, test.partNumber),
				int64(len(test.content)), bytes.NewReader([]byte(test.content)), test.accessKey, test.secretKey, nil)
			if err != nil {
				t.Fatalf("Failed to create a signed V4 request to upload part for %s/%s: <ERROR> %v",
					bucketName, test.objectName, err)
			}
			// Verify response of the V2 signed HTTP request.
			// construct HTTP request for PutObject Part Object endpoint.
			reqV2, err = newTestSignedRequestV2(http.MethodPut,
				getPutObjectPartURL("", bucketName, test.objectName, uploadID, test.partNumber),
				int64(len(test.content)), bytes.NewReader([]byte(test.content)), test.accessKey, test.secretKey, nil)
			if err != nil {
				t.Fatalf("Test %d %s Failed to create a V2  signed request to upload part for %s/%s: <ERROR> %v", i, instanceType,
					bucketName, test.objectName, err)
			}

			// collection of input HTTP request, ResponseRecorder and request type.
			reqRecs := []inputReqRec{
				{
					req:     reqV4,
					rec:     recV4,
					reqType: reqV4Str,
				},
				{
					req:     reqV2,
					rec:     recV2,
					reqType: reqV2Str,
				},
			}

			for _, reqRec := range reqRecs {
				// Response recorder to record the response of the handler.
				rec := reqRec.rec
				// HTTP request used to call the handler.
				req := reqRec.req
				// HTTP request type string for V4/V2 requests.
				reqType := reqRec.reqType

				// Clone so we don't retain values we do not want.
				req.Header = req.Header.Clone()

				// introduce faults in the request.
				// deliberately introducing the invalid value to be able to assert the response with the expected error response.
				switch test.fault {
				case MissingContentLength:
					req.ContentLength = -1
					// Setting the content length to a value greater than the max allowed size of a part.
					// Used in test case  4.
				case TooBigObject:
					req.ContentLength = globalMaxObjectSize + 1
					// Malformed signature.
					// Used in test case  6.
				case BadSignature:
					req.Header.Set("authorization", req.Header.Get("authorization")+"a")
					// Setting an invalid Content-MD5 to force a Md5 Mismatch error.
					// Used in tesr case 7.
				case BadMD5:
					req.Header.Set("Content-MD5", "badmd5")
				}

				// invoke the PutObjectPart HTTP handler.
				apiRouter.ServeHTTP(rec, req)

				// validate the error response.
				want := getAPIError(test.expectedAPIError)
				if test.expectedAPIError == -1 {
					want.HTTPStatusCode = 200
					want.Code = "<no error>"
					want.Description = "<no error>"
				}
				if rec.Code != http.StatusOK {
					var errBytes []byte
					// read the response body.
					errBytes, err = io.ReadAll(rec.Result().Body)
					if err != nil {
						t.Fatalf("%s, Failed to read error response from upload part request \"%s\"/\"%s\": <ERROR> %v.",
							reqType, bucketName, test.objectName, err)
					}
					// parse the XML error response.
					var errXML APIErrorResponse
					err = xml.Unmarshal(errBytes, &errXML)
					if err != nil {
						t.Fatalf("%s, Failed to unmarshal error response from upload part request \"%s\"/\"%s\": <ERROR> %v.",
							reqType, bucketName, test.objectName, err)
					}
					// Validate whether the error has occurred for the expected reason.
					if want.Code != errXML.Code {
						t.Errorf("%s, Expected to fail with error \"%s\", but received \"%s\": %q.",
							reqType, want.Code, errXML.Code, errXML.Message)
					}
					// Validate the HTTP response status code  with the expected one.
					if want.HTTPStatusCode != rec.Code {
						t.Errorf("%s, Expected the HTTP response status code to be %d, got %d.", reqType, want.HTTPStatusCode, rec.Code)
					}
				} else if want.HTTPStatusCode != http.StatusOK {
					t.Errorf("got 200 ok, want %d", rec.Code)
				}
			}
		})
	}

	// Test for Anonymous/unsigned http request.
	anonReq, err := newTestRequest(http.MethodPut, getPutObjectPartURL("", bucketName, testObject, uploadIDCopy, "1"),
		int64(len("hello")), bytes.NewReader([]byte("hello")))
	if err != nil {
		t.Fatalf("MinIO %s: Failed to create an anonymous request for %s/%s: <ERROR> %v",
			instanceType, bucketName, testObject, err)
	}

	// ExecObjectLayerAPIAnonTest - Calls the HTTP API handler using the anonymous request, validates the ErrAccessDeniedResponse,
	// sets the bucket policy using the policy statement generated from `getWriteOnlyObjectStatement` so that the
	// unsigned request goes through and its validated again.
	ExecObjectLayerAPIAnonTest(t, obj, "TestAPIPutObjectPartHandler", bucketName, testObject, instanceType, apiRouter, anonReq, getAnonWriteOnlyObjectPolicy(bucketName, testObject))

	// HTTP request for testing when `ObjectLayer` is set to `nil`.
	// There is no need to use an existing bucket and valid input for creating the request
	// since the `objectLayer==nil`  check is performed before any other checks inside the handlers.
	// The only aim is to generate an HTTP request in a way that the relevant/registered end point is evoked/called.
	nilBucket := "dummy-bucket"
	nilObject := "dummy-object"

	nilReq, err := newTestSignedRequestV4(http.MethodPut, getPutObjectPartURL("", nilBucket, nilObject, "0", "0"),
		0, bytes.NewReader([]byte("testNilObjLayer")), "", "", nil)
	if err != nil {
		t.Errorf("MinIO %s: Failed to create http request for testing the response when object Layer is set to `nil`.", instanceType)
	}
	// execute the object layer set to `nil` test.
	// `ExecObjectLayerAPINilTest` manages the operation.
	ExecObjectLayerAPINilTest(t, nilBucket, nilObject, instanceType, apiRouter, nilReq)
}

// TestAPIListObjectPartsHandlerPreSign - Tests validate the response of ListObjectParts HTTP handler
//
//	when signature type of the HTTP request is `Presigned`.
func TestAPIListObjectPartsHandlerPreSign(t *testing.T) {
	defer DetectTestLeak(t)()
	ExecObjectLayerAPITest(ExecObjectLayerAPITestArgs{t: t, objAPITest: testAPIListObjectPartsHandlerPreSign, endpoints: []string{"PutObjectPart", "NewMultipart", "ListObjectParts"}})
}

func testAPIListObjectPartsHandlerPreSign(obj ObjectLayer, instanceType, bucketName string, apiRouter http.Handler,
	credentials auth.Credentials, t *testing.T,
) {
	testObject := "testobject"
	rec := httptest.NewRecorder()
	req, err := newTestSignedRequestV4(http.MethodPost, getNewMultipartURL("", bucketName, testObject),
		0, nil, credentials.AccessKey, credentials.SecretKey, nil)
	if err != nil {
		t.Fatalf("[%s] - Failed to create a signed request to initiate multipart upload for %s/%s: <ERROR> %v",
			instanceType, bucketName, testObject, err)
	}
	apiRouter.ServeHTTP(rec, req)

	// Get uploadID of the multipart upload initiated.
	var mpartResp InitiateMultipartUploadResponse
	mpartRespBytes, err := io.ReadAll(rec.Result().Body)
	if err != nil {
		t.Fatalf("[%s] Failed to read NewMultipartUpload response <ERROR> %v", instanceType, err)
	}
	err = xml.Unmarshal(mpartRespBytes, &mpartResp)
	if err != nil {
		t.Fatalf("[%s] Failed to unmarshal NewMultipartUpload response <ERROR> %v", instanceType, err)
	}

	// Upload a part for listing purposes.
	rec = httptest.NewRecorder()
	req, err = newTestSignedRequestV4(http.MethodPut,
		getPutObjectPartURL("", bucketName, testObject, mpartResp.UploadID, "1"),
		int64(len("hello")), bytes.NewReader([]byte("hello")), credentials.AccessKey, credentials.SecretKey, nil)
	if err != nil {
		t.Fatalf("[%s] - Failed to create a signed request to initiate multipart upload for %s/%s: <ERROR> %v",
			instanceType, bucketName, testObject, err)
	}
	apiRouter.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("[%s] - Failed to PutObjectPart bucket: %s object: %s HTTP status code: %d",
			instanceType, bucketName, testObject, rec.Code)
	}
	rec = httptest.NewRecorder()
	req, err = newTestRequest(http.MethodGet,
		getListMultipartURLWithParams("", bucketName, testObject, mpartResp.UploadID, "", "", ""),
		0, nil)
	if err != nil {
		t.Fatalf("[%s] - Failed to create an unsigned request to list object parts for bucket %s, uploadId %s",
			instanceType, bucketName, mpartResp.UploadID)
	}

	req.Header = http.Header{}
	err = preSignV2(req, credentials.AccessKey, credentials.SecretKey, int64(10*60*60))
	if err != nil {
		t.Fatalf("[%s] - Failed to presignV2 an unsigned request to list object parts for bucket %s, uploadId %s",
			instanceType, bucketName, mpartResp.UploadID)
	}
	apiRouter.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("Test %d %s expected to succeed but failed with HTTP status code %d",
			1, instanceType, rec.Code)
	}

	rec = httptest.NewRecorder()
	req, err = newTestRequest(http.MethodGet,
		getListMultipartURLWithParams("", bucketName, testObject, mpartResp.UploadID, "", "", ""),
		0, nil)
	if err != nil {
		t.Fatalf("[%s] - Failed to create an unsigned request to list object parts for bucket %s, uploadId %s",
			instanceType, bucketName, mpartResp.UploadID)
	}

	err = preSignV4(req, credentials.AccessKey, credentials.SecretKey, int64(10*60*60))
	if err != nil {
		t.Fatalf("[%s] - Failed to presignV2 an unsigned request to list object parts for bucket %s, uploadId %s",
			instanceType, bucketName, mpartResp.UploadID)
	}
	apiRouter.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("Test %d %s expected to succeed but failed with HTTP status code %d",
			1, instanceType, rec.Code)
	}
}

// TestAPIListObjectPartsHandler - Tests validate the response of ListObjectParts HTTP handler
//
//	for variety of success/failure cases.
func TestAPIListObjectPartsHandler(t *testing.T) {
	defer DetectTestLeak(t)()
	ExecExtendedObjectLayerAPITest(t, testAPIListObjectPartsHandler, []string{"ListObjectParts"})
}

func testAPIListObjectPartsHandler(obj ObjectLayer, instanceType, bucketName string, apiRouter http.Handler,
	credentials auth.Credentials, t *testing.T,
) {
	testObject := "testobject"
	var opts ObjectOptions
	// PutObjectPart API HTTP Handler has to be tested in isolation,
	// that is without any other handler being registered,
	// That's why NewMultipartUpload is initiated using ObjectLayer.
	res, err := obj.NewMultipartUpload(context.Background(), bucketName, testObject, opts)
	if err != nil {
		// Failed to create NewMultipartUpload, abort.
		t.Fatalf("MinIO %s : <ERROR>  %s", instanceType, err)
	}
	uploadID := res.UploadID
	uploadIDCopy := uploadID

	// create an object Part, will be used to test list object parts.
	_, err = obj.PutObjectPart(context.Background(), bucketName, testObject, uploadID, 1, mustGetPutObjReader(t, bytes.NewReader([]byte("hello")), int64(len("hello")), "5d41402abc4b2a76b9719d911017c592", ""), opts)
	if err != nil {
		t.Fatalf("MinIO %s : %s.", instanceType, err)
	}

	// expected error types for invalid inputs to ListObjectParts handler.
	noAPIErr := APIError{}
	// expected error when the signature check fails.
	signatureMismatchErr := getAPIError(ErrSignatureDoesNotMatch)
	// expected error the when the uploadID is invalid.
	noSuchUploadErr := getAPIError(ErrNoSuchUpload)
	// expected error the part number marker use in the ListObjectParts request is invalid.
	invalidPartMarkerErr := getAPIError(ErrInvalidPartNumberMarker)
	// expected error when the maximum number of parts requested to listed in invalid.
	invalidMaxPartsErr := getAPIError(ErrInvalidMaxParts)

	testCases := []struct {
		fault            Fault
		partNumberMarker string
		maxParts         string
		expectedErr      APIError
	}{
		// Test case - 1.
		// case where a signature mismatch is introduced and the response is validated.
		{
			fault:            BadSignature,
			partNumberMarker: "",
			maxParts:         "",
			expectedErr:      signatureMismatchErr,
		},

		// Test case - 2.
		// Marker is set to invalid value of -1, error response is asserted.
		{
			fault:            None,
			partNumberMarker: "-1",
			maxParts:         "",
			expectedErr:      invalidPartMarkerErr,
		},
		// Test case - 3.
		// Max Parts is set a negative value, error response is validated.
		{
			fault:            None,
			partNumberMarker: "",
			maxParts:         "-1",
			expectedErr:      invalidMaxPartsErr,
		},
		// Test case - 4.
		// Invalid UploadID is set and the error response is validated.
		{
			fault:            MissingUploadID,
			partNumberMarker: "",
			maxParts:         "",
			expectedErr:      noSuchUploadErr,
		},
	}

	// string to represent V2 signed HTTP request.
	reqV2Str := "V2 Signed HTTP request"
	// string to represent V4 signed HTTP request.
	reqV4Str := "V4 Signed HTTP request"
	// Collection of HTTP request and ResponseRecorder and request type string.
	type inputReqRec struct {
		req     *http.Request
		rec     *httptest.ResponseRecorder
		reqType string
	}

	for i, test := range testCases {
		var reqV4, reqV2 *http.Request
		// Using sub-tests introduced in Go 1.7.
		t.Run(fmt.Sprintf("MinIO %s: Test case %d failed.", instanceType, i+1), func(t *testing.T) {
			recV2 := httptest.NewRecorder()
			recV4 := httptest.NewRecorder()

			// setting a non-existent uploadID.
			// deliberately introducing the invalid value to be able to assert the response with the expected error response.
			if test.fault == MissingUploadID {
				uploadID = "upload1"
			}

			// constructing a v4 signed HTTP request for ListMultipartUploads.
			reqV4, err = newTestSignedRequestV4(http.MethodGet,
				getListMultipartURLWithParams("", bucketName, testObject, uploadID, test.maxParts, test.partNumberMarker, ""),
				0, nil, credentials.AccessKey, credentials.SecretKey, nil)
			if err != nil {
				t.Fatalf("Failed to create a V4 signed request to list object parts for %s/%s: <ERROR> %v.",
					bucketName, testObject, err)
			}
			// Verify response of the V2 signed HTTP request.
			// construct HTTP request for PutObject Part Object endpoint.
			reqV2, err = newTestSignedRequestV2(http.MethodGet,
				getListMultipartURLWithParams("", bucketName, testObject, uploadID, test.maxParts, test.partNumberMarker, ""),
				0, nil, credentials.AccessKey, credentials.SecretKey, nil)
			if err != nil {
				t.Fatalf("Failed to create a V2 signed request to list object parts for %s/%s: <ERROR> %v.",
					bucketName, testObject, err)
			}

			// collection of input HTTP request, ResponseRecorder and request type.
			reqRecs := []inputReqRec{
				{
					req:     reqV4,
					rec:     recV4,
					reqType: reqV4Str,
				},
				{
					req:     reqV2,
					rec:     recV2,
					reqType: reqV2Str,
				},
			}
			for _, reqRec := range reqRecs {
				// Response recorder to record the response of the handler.
				rec := reqRec.rec
				// HTTP request used to call the handler.
				req := reqRec.req
				// HTTP request type string for V4/V2 requests.
				reqType := reqRec.reqType
				// Malformed signature.
				if test.fault == BadSignature {
					req.Header.Set("authorization", req.Header.Get("authorization")+"a")
				}

				// invoke the PutObjectPart HTTP handler with the given HTTP request.
				apiRouter.ServeHTTP(rec, req)

				// validate the error response.
				if test.expectedErr != noAPIErr {
					var errBytes []byte
					// read the response body.
					errBytes, err = io.ReadAll(rec.Result().Body)
					if err != nil {
						t.Fatalf("%s,Failed to read error response list object parts request %s/%s: <ERROR> %v", reqType, bucketName, testObject, err)
					}
					// parse the error response.
					var errXML APIErrorResponse
					err = xml.Unmarshal(errBytes, &errXML)
					if err != nil {
						t.Fatalf("%s, Failed to unmarshal error response from list object partsest %s/%s: <ERROR> %v",
							reqType, bucketName, testObject, err)
					}
					// Validate whether the error has occurred for the expected reason.
					if test.expectedErr.Code != errXML.Code {
						t.Errorf("%s, Expected to fail with %s but received %s",
							reqType, test.expectedErr.Code, errXML.Code)
					}
					// in case error is not expected response status should be 200OK.
				} else if rec.Code != http.StatusOK {
					t.Errorf("%s, Expected to succeed with response HTTP status 200OK, but failed with HTTP status code %d.", reqType, rec.Code)
				}
			}
		})
	}

	// Test for Anonymous/unsigned http request.
	anonReq, err := newTestRequest(http.MethodGet,
		getListMultipartURLWithParams("", bucketName, testObject, uploadIDCopy, "", "", ""), 0, nil)
	if err != nil {
		t.Fatalf("MinIO %s: Failed to create an anonymous request for %s/%s: <ERROR> %v",
			instanceType, bucketName, testObject, err)
	}

	// ExecObjectLayerAPIAnonTest - Calls the HTTP API handler using the anonymous request, validates the ErrAccessDeniedResponse,
	// sets the bucket policy using the policy statement generated from `getWriteOnlyObjectStatement` so that the
	// unsigned request goes through and its validated again.
	ExecObjectLayerAPIAnonTest(t, obj, "TestAPIListObjectPartsHandler", bucketName, testObject, instanceType, apiRouter, anonReq, getAnonWriteOnlyObjectPolicy(bucketName, testObject))

	// HTTP request for testing when `objectLayer` is set to `nil`.
	// There is no need to use an existing bucket and valid input for creating the request
	// since the `objectLayer==nil`  check is performed before any other checks inside the handlers.
	// The only aim is to generate an HTTP request in a way that the relevant/registered end point is evoked/called.
	nilBucket := "dummy-bucket"
	nilObject := "dummy-object"

	nilReq, err := newTestSignedRequestV4(http.MethodGet,
		getListMultipartURLWithParams("", nilBucket, nilObject, "dummy-uploadID", "0", "0", ""),
		0, nil, "", "", nil)
	if err != nil {
		t.Errorf("MinIO %s:Failed to create http request for testing the response when object Layer is set to `nil`.", instanceType)
	}
	// execute the object layer set to `nil` test.
	// `ExecObjectLayerAPINilTest` sets the Object Layer to `nil` and calls the handler.
	ExecObjectLayerAPINilTest(t, nilBucket, nilObject, instanceType, apiRouter, nilReq)
}

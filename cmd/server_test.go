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
	"encoding/xml"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"reflect"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dustin/go-humanize"
	jwtgo "github.com/golang-jwt/jwt/v4"
	"github.com/minio/minio-go/v7/pkg/set"
	"github.com/minio/minio-go/v7/pkg/signer"
	xhttp "github.com/minio/minio/internal/http"
	"github.com/minio/pkg/v3/policy"
)

// API suite container common to both ErasureSD and Erasure.
type TestSuiteCommon struct {
	serverType string
	testServer TestServer
	endPoint   string
	accessKey  string
	secretKey  string
	signer     signerType
	secure     bool
	client     *http.Client
}

type check struct {
	*testing.T
	testType string
}

// Assert - checks if gotValue is same as expectedValue, if not fails the test.
func (c *check) Assert(gotValue any, expectedValue any) {
	c.Helper()
	if !reflect.DeepEqual(gotValue, expectedValue) {
		c.Fatalf("Test %s expected %v, got %v", c.testType, expectedValue, gotValue)
	}
}

func verifyError(c *check, response *http.Response, code, description string, statusCode int) {
	c.Helper()
	data, err := io.ReadAll(response.Body)
	c.Assert(err, nil)
	errorResponse := APIErrorResponse{}
	err = xml.Unmarshal(data, &errorResponse)
	c.Assert(err, nil)
	c.Assert(errorResponse.Code, code)
	c.Assert(errorResponse.Message, description)
	c.Assert(response.StatusCode, statusCode)
}

func runAllTests(suite *TestSuiteCommon, c *check) {
	suite.SetUpSuite(c)
	suite.TestCors(c)
	suite.TestObjectDir(c)
	suite.TestBucketPolicy(c)
	suite.TestDeleteBucket(c)
	suite.TestDeleteBucketNotEmpty(c)
	suite.TestDeleteMultipleObjects(c)
	suite.TestDeleteObject(c)
	suite.TestNonExistentBucket(c)
	suite.TestEmptyObject(c)
	suite.TestBucket(c)
	suite.TestObjectGetAnonymous(c)
	suite.TestMultipleObjects(c)
	suite.TestHeader(c)
	suite.TestPutBucket(c)
	suite.TestCopyObject(c)
	suite.TestPutObject(c)
	suite.TestListBuckets(c)
	suite.TestValidateSignature(c)
	suite.TestSHA256Mismatch(c)
	suite.TestPutObjectLongName(c)
	suite.TestNotBeAbleToCreateObjectInNonexistentBucket(c)
	suite.TestHeadOnObjectLastModified(c)
	suite.TestHeadOnBucket(c)
	suite.TestContentTypePersists(c)
	suite.TestPartialContent(c)
	suite.TestListObjectsHandler(c)
	suite.TestListObjectVersionsOutputOrderHandler(c)
	suite.TestListObjectsHandlerErrors(c)
	suite.TestListObjectsV2HadoopUAHandler(c)
	suite.TestPutBucketErrors(c)
	suite.TestGetObjectLarge10MiB(c)
	suite.TestGetObjectLarge11MiB(c)
	suite.TestGetPartialObjectMisAligned(c)
	suite.TestGetPartialObjectLarge11MiB(c)
	suite.TestGetPartialObjectLarge10MiB(c)
	suite.TestGetObjectErrors(c)
	suite.TestGetObjectRangeErrors(c)
	suite.TestObjectMultipartAbort(c)
	suite.TestBucketMultipartList(c)
	suite.TestValidateObjectMultipartUploadID(c)
	suite.TestObjectMultipartListError(c)
	suite.TestObjectValidMD5(c)
	suite.TestObjectMultipart(c)
	suite.TestMetricsV3Handler(c)
	suite.TestBucketSQSNotificationWebHook(c)
	suite.TestBucketSQSNotificationAMQP(c)
	suite.TestUnsignedCVE(c)
	suite.TearDownSuite(c)
}

func TestServerSuite(t *testing.T) {
	testCases := []*TestSuiteCommon{
		// Init and run test on ErasureSD backend with signature v4.
		{serverType: "ErasureSD", signer: signerV4},
		// Init and run test on ErasureSD backend with signature v2.
		{serverType: "ErasureSD", signer: signerV2},
		// Init and run test on ErasureSD backend, with tls enabled.
		{serverType: "ErasureSD", signer: signerV4, secure: true},
		// Init and run test on Erasure backend.
		{serverType: "Erasure", signer: signerV4},
		// Init and run test on ErasureSet backend.
		{serverType: "ErasureSet", signer: signerV4},
	}
	globalServerCtxt.StrictS3Compat = true
	for i, testCase := range testCases {
		t.Run(fmt.Sprintf("Test: %d, ServerType: %s", i+1, testCase.serverType), func(t *testing.T) {
			runAllTests(testCase, &check{t, testCase.serverType})
		})
	}
}

// Setting up the test suite.
// Starting the Test server with temporary backend.
func (s *TestSuiteCommon) SetUpSuite(c *check) {
	if s.secure {
		cert, key, err := generateTLSCertKey("127.0.0.1")
		c.Assert(err, nil)

		s.testServer = StartTestTLSServer(c, s.serverType, cert, key)
	} else {
		s.testServer = StartTestServer(c, s.serverType)
	}

	s.client = s.testServer.Server.Client()
	s.endPoint = s.testServer.Server.URL
	s.accessKey = s.testServer.AccessKey
	s.secretKey = s.testServer.SecretKey
}

func (s *TestSuiteCommon) RestartTestServer(c *check) {
	// Shutdown.
	s.testServer.cancel()
	s.testServer.Server.Close()
	s.testServer.Obj.Shutdown(context.Background())

	// Restart.
	ctx, cancel := context.WithCancel(context.Background())

	s.testServer.cancel = cancel
	s.testServer = initTestServerWithBackend(ctx, c, s.testServer, s.testServer.Obj, s.testServer.rawDiskPaths)
	if s.secure {
		s.testServer.Server.StartTLS()
	} else {
		s.testServer.Server.Start()
	}

	s.client = s.testServer.Server.Client()
	s.endPoint = s.testServer.Server.URL
}

func (s *TestSuiteCommon) TearDownSuite(c *check) {
	s.testServer.Stop()
}

const (
	defaultPrometheusJWTExpiry = 100 * 365 * 24 * time.Hour
)

func (s *TestSuiteCommon) TestMetricsV3Handler(c *check) {
	jwt := jwtgo.NewWithClaims(jwtgo.SigningMethodHS512, jwtgo.StandardClaims{
		ExpiresAt: time.Now().UTC().Add(defaultPrometheusJWTExpiry).Unix(),
		Subject:   s.accessKey,
		Issuer:    "prometheus",
	})

	token, err := jwt.SignedString([]byte(s.secretKey))
	c.Assert(err, nil)

	for _, cpath := range globalMetricsV3CollectorPaths {
		request, err := newTestSignedRequest(http.MethodGet, s.endPoint+minioReservedBucketPath+metricsV3Path+string(cpath),
			0, nil, s.accessKey, s.secretKey, s.signer)
		c.Assert(err, nil)

		request.Header.Set("Authorization", "Bearer "+token)

		// execute the request.
		response, err := s.client.Do(request)
		c.Assert(err, nil)

		// assert the http response status code.
		c.Assert(response.StatusCode, http.StatusOK)
	}
}

func (s *TestSuiteCommon) TestBucketSQSNotificationWebHook(c *check) {
	// Sample bucket notification.
	bucketNotificationBuf := `<NotificationConfiguration><QueueConfiguration><Event>s3:ObjectCreated:Put</Event><Filter><S3Key><FilterRule><Name>prefix</Name><Value>images/</Value></FilterRule></S3Key></Filter><Id>1</Id><Queue>arn:minio:sqs:us-east-1:444455556666:webhook</Queue></QueueConfiguration></NotificationConfiguration>`
	// generate a random bucket Name.
	bucketName := getRandomBucketName()
	// HTTP request to create the bucket.
	request, err := newTestSignedRequest(http.MethodPut, getMakeBucketURL(s.endPoint, bucketName),
		0, nil, s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)

	// execute the request.
	response, err := s.client.Do(request)
	c.Assert(err, nil)

	// assert the http response status code.
	c.Assert(response.StatusCode, http.StatusOK)

	request, err = newTestSignedRequest(http.MethodPut, getPutNotificationURL(s.endPoint, bucketName),
		int64(len(bucketNotificationBuf)), bytes.NewReader([]byte(bucketNotificationBuf)), s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)

	// execute the HTTP request.
	response, err = s.client.Do(request)

	c.Assert(err, nil)
	verifyError(c, response, "InvalidArgument", "A specified destination ARN does not exist or is not well-formed. Verify the destination ARN.", http.StatusBadRequest)
}

func (s *TestSuiteCommon) TestCors(c *check) {
	expectedMap := http.Header{}
	expectedMap.Set("Access-Control-Allow-Credentials", "true")
	expectedMap.Set("Access-Control-Allow-Origin", "http://foobar.com")
	expectedMap["Access-Control-Expose-Headers"] = []string{
		"Date",
		"Etag",
		"Server",
		"Connection",
		"Accept-Ranges",
		"Content-Range",
		"Content-Encoding",
		"Content-Length",
		"Content-Type",
		"Content-Disposition",
		"Last-Modified",
		"Content-Language",
		"Cache-Control",
		"Retry-After",
		"X-Amz-Bucket-Region",
		"Expires",
		"X-Amz*",
		"X-Amz*",
		"*",
	}
	expectedMap.Set("Vary", "Origin")

	req, _ := http.NewRequest(http.MethodOptions, s.endPoint, nil)
	req.Header.Set("Origin", "http://foobar.com")
	res, err := s.client.Do(req)
	if err != nil {
		c.Fatal(err)
	}

	for k := range expectedMap {
		if v, ok := res.Header[k]; !ok {
			c.Errorf("Expected key %s missing from %v", k, res.Header)
		} else {
			expectedSet := set.CreateStringSet(expectedMap[k]...)
			gotSet := set.CreateStringSet(strings.Split(v[0], ", ")...)
			if !expectedSet.Equals(gotSet) {
				c.Errorf("Expected value %v, got %v", strings.Join(expectedMap[k], ", "), v)
			}
		}
	}
}

func (s *TestSuiteCommon) TestObjectDir(c *check) {
	bucketName := getRandomBucketName()
	// HTTP request to create the bucket.
	request, err := newTestSignedRequest(http.MethodPut, getMakeBucketURL(s.endPoint, bucketName),
		0, nil, s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)

	// execute the request.
	response, err := s.client.Do(request)
	c.Assert(err, nil)

	// assert the http response status code.
	c.Assert(response.StatusCode, http.StatusOK)

	request, err = newTestSignedRequest(http.MethodPut, getPutObjectURL(s.endPoint, bucketName, "my-object-directory/"),
		0, nil, s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)

	// execute the HTTP request.
	response, err = s.client.Do(request)

	c.Assert(err, nil)
	// assert the http response status code.
	c.Assert(response.StatusCode, http.StatusOK)

	request, err = newTestSignedRequest(http.MethodHead, getHeadObjectURL(s.endPoint, bucketName, "my-object-directory/"),
		0, nil, s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)

	// execute the HTTP request.
	response, err = s.client.Do(request)

	c.Assert(err, nil)
	c.Assert(response.StatusCode, http.StatusOK)

	request, err = newTestSignedRequest(http.MethodGet, getGetObjectURL(s.endPoint, bucketName, "my-object-directory/"),
		0, nil, s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)

	// execute the HTTP request.
	response, err = s.client.Do(request)

	c.Assert(err, nil)
	c.Assert(response.StatusCode, http.StatusOK)

	request, err = newTestSignedRequest(http.MethodDelete, getDeleteObjectURL(s.endPoint, bucketName, "my-object-directory/"),
		0, nil, s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)

	// execute the HTTP request.
	response, err = s.client.Do(request)

	c.Assert(err, nil)
	c.Assert(response.StatusCode, http.StatusNoContent)
}

func (s *TestSuiteCommon) TestUnsignedCVE(c *check) {
	c.Helper()

	// generate a random bucket Name.
	bucketName := getRandomBucketName()

	// HTTP request to create the bucket.
	request, err := newTestSignedRequest(http.MethodPut, getMakeBucketURL(s.endPoint, bucketName),
		0, nil, s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)

	// execute the request.
	response, err := s.client.Do(request)
	c.Assert(err, nil)

	// assert the http response status code.
	c.Assert(response.StatusCode, http.StatusOK)

	req, err := http.NewRequest(http.MethodPut, getPutObjectURL(s.endPoint, bucketName, "test-cve-object.txt"), nil)
	c.Assert(err, nil)

	req.Body = io.NopCloser(bytes.NewReader([]byte("foobar!\n")))
	req.Trailer = http.Header{}
	req.Trailer.Set("x-amz-checksum-crc32", "rK0DXg==")

	now := UTCNow()

	req = signer.StreamingUnsignedV4(req, "", 8, now)

	maliciousHeaders := http.Header{
		"Authorization":                []string{fmt.Sprintf("AWS4-HMAC-SHA256 Credential=%s/%s/us-east-1/s3/aws4_request, SignedHeaders=invalidheader, Signature=deadbeefdeadbeefdeadbeeddeadbeeddeadbeefdeadbeefdeadbeefdeadbeef", s.accessKey, now.Format(yyyymmdd))},
		"User-Agent":                   []string{"A malicious request"},
		"X-Amz-Decoded-Content-Length": []string{"8"},
		"Content-Encoding":             []string{"aws-chunked"},
		"X-Amz-Trailer":                []string{"x-amz-checksum-crc32"},
		"x-amz-content-sha256":         []string{unsignedPayloadTrailer},
	}

	for k, v := range maliciousHeaders {
		req.Header.Set(k, v[0])
	}

	// execute the request.
	response, err = s.client.Do(req)
	c.Assert(err, nil)

	// out, err = httputil.DumpResponse(response, true)
	// fmt.Println("RESPONSE ===\n", string(out), err)

	// assert the http response status code.
	c.Assert(response.StatusCode, http.StatusBadRequest)
}

func (s *TestSuiteCommon) TestBucketSQSNotificationAMQP(c *check) {
	// Sample bucket notification.
	bucketNotificationBuf := `<NotificationConfiguration><QueueConfiguration><Event>s3:ObjectCreated:Put</Event><Filter><S3Key><FilterRule><Name>prefix</Name><Value>images/</Value></FilterRule></S3Key></Filter><Id>1</Id><Queue>arn:minio:sqs:us-east-1:444455556666:amqp</Queue></QueueConfiguration></NotificationConfiguration>`
	// generate a random bucket Name.
	bucketName := getRandomBucketName()
	// HTTP request to create the bucket.
	request, err := newTestSignedRequest(http.MethodPut, getMakeBucketURL(s.endPoint, bucketName),
		0, nil, s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)

	// execute the request.
	response, err := s.client.Do(request)
	c.Assert(err, nil)

	// assert the http response status code.
	c.Assert(response.StatusCode, http.StatusOK)

	request, err = newTestSignedRequest(http.MethodPut, getPutNotificationURL(s.endPoint, bucketName),
		int64(len(bucketNotificationBuf)), bytes.NewReader([]byte(bucketNotificationBuf)), s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)

	// execute the HTTP request.
	response, err = s.client.Do(request)

	c.Assert(err, nil)
	verifyError(c, response, "InvalidArgument", "A specified destination ARN does not exist or is not well-formed. Verify the destination ARN.", http.StatusBadRequest)
}

// TestBucketPolicy - Inserts the bucket policy and verifies it by fetching the policy back.
// Deletes the policy and verifies the deletion by fetching it back.
func (s *TestSuiteCommon) TestBucketPolicy(c *check) {
	// Sample bucket policy.
	bucketPolicyBuf := `{"Version":"2012-10-17","Statement":[{"Action":["s3:GetBucketLocation","s3:ListBucket"],"Effect":"Allow","Principal":{"AWS":["*"]},"Resource":["arn:aws:s3:::%s"]},{"Action":["s3:GetObject"],"Effect":"Allow","Principal":{"AWS":["*"]},"Resource":["arn:aws:s3:::%s/this*"]}]}`

	// generate a random bucket Name.
	bucketName := getRandomBucketName()
	// create the policy statement string with the randomly generated bucket name.
	bucketPolicyStr := fmt.Sprintf(bucketPolicyBuf, bucketName, bucketName)
	// HTTP request to create the bucket.
	request, err := newTestSignedRequest(http.MethodPut, getMakeBucketURL(s.endPoint, bucketName),
		0, nil, s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)

	// execute the request.
	response, err := s.client.Do(request)
	c.Assert(err, nil)
	// assert the http response status code.
	c.Assert(response.StatusCode, http.StatusOK)

	// Put a new bucket policy.
	request, err = newTestSignedRequest(http.MethodPut, getPutPolicyURL(s.endPoint, bucketName),
		int64(len(bucketPolicyStr)), bytes.NewReader([]byte(bucketPolicyStr)), s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)

	// execute the HTTP request to create bucket.
	response, err = s.client.Do(request)
	c.Assert(err, nil)
	c.Assert(response.StatusCode, http.StatusNoContent)

	// Fetch the uploaded policy.
	request, err = newTestSignedRequest(http.MethodGet, getGetPolicyURL(s.endPoint, bucketName), 0, nil,
		s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)

	response, err = s.client.Do(request)
	c.Assert(err, nil)
	c.Assert(response.StatusCode, http.StatusOK)

	bucketPolicyReadBuf, err := io.ReadAll(response.Body)
	c.Assert(err, nil)
	// Verify if downloaded policy matches with previously uploaded.
	expectedPolicy, err := policy.ParseBucketPolicyConfig(strings.NewReader(bucketPolicyStr), bucketName)
	c.Assert(err, nil)
	gotPolicy, err := policy.ParseBucketPolicyConfig(bytes.NewReader(bucketPolicyReadBuf), bucketName)
	c.Assert(err, nil)
	c.Assert(reflect.DeepEqual(expectedPolicy, gotPolicy), true)

	// Delete policy.
	request, err = newTestSignedRequest(http.MethodDelete, getDeletePolicyURL(s.endPoint, bucketName), 0, nil,
		s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)

	response, err = s.client.Do(request)
	c.Assert(err, nil)
	c.Assert(response.StatusCode, http.StatusNoContent)

	// Verify if the policy was indeed deleted.
	request, err = newTestSignedRequest(http.MethodGet, getGetPolicyURL(s.endPoint, bucketName),
		0, nil, s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)

	response, err = s.client.Do(request)
	c.Assert(err, nil)
	c.Assert(response.StatusCode, http.StatusNotFound)
}

// TestDeleteBucket - validates DELETE bucket operation.
func (s *TestSuiteCommon) TestDeleteBucket(c *check) {
	bucketName := getRandomBucketName()

	// HTTP request to create the bucket.
	request, err := newTestSignedRequest(http.MethodPut, getMakeBucketURL(s.endPoint, bucketName),
		0, nil, s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)

	response, err := s.client.Do(request)
	c.Assert(err, nil)
	// assert the response status code.
	c.Assert(response.StatusCode, http.StatusOK)

	// construct request to delete the bucket.
	request, err = newTestSignedRequest(http.MethodDelete, getDeleteBucketURL(s.endPoint, bucketName),
		0, nil, s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)

	response, err = s.client.Do(request)
	c.Assert(err, nil)
	// Assert the response status code.
	c.Assert(response.StatusCode, http.StatusNoContent)
}

// TestDeleteBucketNotEmpty - Validates the operation during an attempt to delete a non-empty bucket.
func (s *TestSuiteCommon) TestDeleteBucketNotEmpty(c *check) {
	// generate a random bucket name.
	bucketName := getRandomBucketName()

	// HTTP request to create the bucket.
	request, err := newTestSignedRequest(http.MethodPut, getMakeBucketURL(s.endPoint, bucketName),
		0, nil, s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)

	// execute the request.
	response, err := s.client.Do(request)
	c.Assert(err, nil)
	// assert the response status code.
	c.Assert(response.StatusCode, http.StatusOK)

	// generate http request for an object upload.
	// "test-object" is the object name.
	objectName := "test-object"
	request, err = newTestSignedRequest(http.MethodPut, getPutObjectURL(s.endPoint, bucketName, objectName),
		0, nil, s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)

	// execute the request to complete object upload.
	response, err = s.client.Do(request)
	c.Assert(err, nil)
	// assert the status code of the response.
	c.Assert(response.StatusCode, http.StatusOK)

	// constructing http request to delete the bucket.
	// making an attempt to delete an non-empty bucket.
	// expected to fail.
	request, err = newTestSignedRequest(http.MethodDelete, getDeleteBucketURL(s.endPoint, bucketName),
		0, nil, s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)

	response, err = s.client.Do(request)
	c.Assert(err, nil)
	c.Assert(response.StatusCode, http.StatusConflict)
}

func (s *TestSuiteCommon) TestListenNotificationHandler(c *check) {
	// generate a random bucket name.
	bucketName := getRandomBucketName()
	// HTTP request to create the bucket.
	req, err := newTestSignedRequest(http.MethodPut, getMakeBucketURL(s.endPoint, bucketName),
		0, nil, s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)

	// execute the request.
	response, err := s.client.Do(req)
	c.Assert(err, nil)
	// assert the http response status code.
	c.Assert(response.StatusCode, http.StatusOK)

	invalidBucket := "Invalid\\Bucket"
	tooByte := bytes.Repeat([]byte("a"), 1025)
	tooBigPrefix := string(tooByte)
	validEvents := []string{"s3:ObjectCreated:*", "s3:ObjectRemoved:*"}
	invalidEvents := []string{"invalidEvent"}

	req, err = newTestSignedRequest(http.MethodGet,
		getListenNotificationURL(s.endPoint, invalidBucket, []string{}, []string{}, []string{}),
		0, nil, s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)

	// execute the request.
	response, err = s.client.Do(req)
	c.Assert(err, nil)
	verifyError(c, response, "InvalidBucketName", "The specified bucket is not valid.", http.StatusBadRequest)

	req, err = newTestSignedRequest(http.MethodGet,
		getListenNotificationURL(s.endPoint, bucketName, []string{}, []string{}, invalidEvents),
		0, nil, s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)

	// execute the request.
	response, err = s.client.Do(req)
	c.Assert(err, nil)
	verifyError(c, response, "InvalidArgument", "A specified event is not supported for notifications.", http.StatusBadRequest)

	req, err = newTestSignedRequest(http.MethodGet,
		getListenNotificationURL(s.endPoint, bucketName, []string{tooBigPrefix}, []string{}, validEvents),
		0, nil, s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)

	// execute the request.
	response, err = s.client.Do(req)
	c.Assert(err, nil)
	verifyError(c, response, "InvalidArgument", "Size of filter rule value cannot exceed 1024 bytes in UTF-8 representation", http.StatusBadRequest)

	req, err = newTestSignedBadSHARequest(http.MethodGet,
		getListenNotificationURL(s.endPoint, bucketName, []string{}, []string{}, validEvents),
		0, nil, s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)

	// execute the request.
	response, err = s.client.Do(req)
	c.Assert(err, nil)
	if s.signer == signerV4 {
		verifyError(c, response, "XAmzContentSHA256Mismatch", "The provided 'x-amz-content-sha256' header does not match what was computed.", http.StatusBadRequest)
	}
}

// Test deletes multiple objects and verifies server response.
func (s *TestSuiteCommon) TestDeleteMultipleObjects(c *check) {
	// generate a random bucket name.
	bucketName := getRandomBucketName()
	// HTTP request to create the bucket.
	request, err := newTestSignedRequest(http.MethodPut, getMakeBucketURL(s.endPoint, bucketName),
		0, nil, s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)

	// execute the request.
	response, err := s.client.Do(request)
	c.Assert(err, nil)
	// assert the http response status code.
	c.Assert(response.StatusCode, http.StatusOK)

	objectName := "prefix/myobject"
	delObjReq := DeleteObjectsRequest{
		Quiet: false,
	}
	for i := range 10 {
		// Obtain http request to upload object.
		// object Name contains a prefix.
		objName := fmt.Sprintf("%d/%s", i, objectName)
		request, err = newTestSignedRequest(http.MethodPut, getPutObjectURL(s.endPoint, bucketName, objName),
			0, nil, s.accessKey, s.secretKey, s.signer)
		c.Assert(err, nil)

		// execute the http request.
		response, err = s.client.Do(request)
		c.Assert(err, nil)
		// assert the status of http response.
		c.Assert(response.StatusCode, http.StatusOK)
		// Append all objects.
		delObjReq.Objects = append(delObjReq.Objects, ObjectToDelete{
			ObjectV: ObjectV{
				ObjectName: objName,
			},
		})
	}
	// Marshal delete request.
	deleteReqBytes, err := xml.Marshal(delObjReq)
	c.Assert(err, nil)

	// Delete list of objects.
	request, err = newTestSignedRequest(http.MethodPost, getMultiDeleteObjectURL(s.endPoint, bucketName),
		int64(len(deleteReqBytes)), bytes.NewReader(deleteReqBytes), s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)
	response, err = s.client.Do(request)
	c.Assert(err, nil)
	c.Assert(response.StatusCode, http.StatusOK)

	deleteResp := DeleteObjectsResponse{}
	delRespBytes, err := io.ReadAll(response.Body)
	c.Assert(err, nil)
	err = xml.Unmarshal(delRespBytes, &deleteResp)
	c.Assert(err, nil)
	for i := range 10 {
		// All the objects should be under deleted list (including non-existent object)
		c.Assert(deleteResp.DeletedObjects[i], DeletedObject{
			ObjectName: delObjReq.Objects[i].ObjectName,
			VersionID:  delObjReq.Objects[i].VersionID,
		})
	}
	c.Assert(len(deleteResp.Errors), 0)

	// Attempt second time results should be same, NoSuchKey for objects not found
	// shouldn't be set.
	request, err = newTestSignedRequest(http.MethodPost, getMultiDeleteObjectURL(s.endPoint, bucketName),
		int64(len(deleteReqBytes)), bytes.NewReader(deleteReqBytes), s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)
	response, err = s.client.Do(request)
	c.Assert(err, nil)
	c.Assert(response.StatusCode, http.StatusOK)

	deleteResp = DeleteObjectsResponse{}
	delRespBytes, err = io.ReadAll(response.Body)
	c.Assert(err, nil)
	err = xml.Unmarshal(delRespBytes, &deleteResp)
	c.Assert(err, nil)
	c.Assert(len(deleteResp.DeletedObjects), len(delObjReq.Objects))
	for i := range 10 {
		c.Assert(deleteResp.DeletedObjects[i], DeletedObject{
			ObjectName: delObjReq.Objects[i].ObjectName,
			VersionID:  delObjReq.Objects[i].VersionID,
		})
	}
	c.Assert(len(deleteResp.Errors), 0)
}

// Tests delete object responses and success.
func (s *TestSuiteCommon) TestDeleteObject(c *check) {
	// generate a random bucket name.
	bucketName := getRandomBucketName()
	// HTTP request to create the bucket.
	request, err := newTestSignedRequest(http.MethodPut, getMakeBucketURL(s.endPoint, bucketName),
		0, nil, s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)

	// execute the request.
	response, err := s.client.Do(request)
	c.Assert(err, nil)
	// assert the http response status code.
	c.Assert(response.StatusCode, http.StatusOK)

	objectName := "prefix/myobject"
	// obtain http request to upload object.
	// object Name contains a prefix.
	request, err = newTestSignedRequest(http.MethodPut, getPutObjectURL(s.endPoint, bucketName, objectName),
		0, nil, s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)

	// execute the http request.
	response, err = s.client.Do(request)
	c.Assert(err, nil)
	// assert the status of http response.
	c.Assert(response.StatusCode, http.StatusOK)

	// object name was "prefix/myobject", an attempt to delete "prefix"
	// Should not delete "prefix/myobject"
	request, err = newTestSignedRequest(http.MethodDelete, getDeleteObjectURL(s.endPoint, bucketName, "prefix"),
		0, nil, s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)
	response, err = s.client.Do(request)
	c.Assert(err, nil)
	c.Assert(response.StatusCode, http.StatusNoContent)

	// create http request to HEAD on the object.
	// this helps to validate the existence of the bucket.
	request, err = newTestSignedRequest(http.MethodHead, getHeadObjectURL(s.endPoint, bucketName, objectName),
		0, nil, s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)

	response, err = s.client.Do(request)
	c.Assert(err, nil)
	// Assert the HTTP response status code.
	c.Assert(response.StatusCode, http.StatusOK)

	// create HTTP request to delete the object.
	request, err = newTestSignedRequest(http.MethodDelete, getDeleteObjectURL(s.endPoint, bucketName, objectName),
		0, nil, s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)
	// execute the http request.
	response, err = s.client.Do(request)
	c.Assert(err, nil)
	// assert the http response status code.
	c.Assert(response.StatusCode, http.StatusNoContent)

	// Delete of non-existent data should return success.
	request, err = newTestSignedRequest(http.MethodDelete, getDeleteObjectURL(s.endPoint, bucketName, "prefix/myobject1"),
		0, nil, s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)
	// execute the http request.
	response, err = s.client.Do(request)
	c.Assert(err, nil)
	// assert the http response status.
	c.Assert(response.StatusCode, http.StatusNoContent)
}

// TestNonExistentBucket - Asserts response for HEAD on non-existent bucket.
func (s *TestSuiteCommon) TestNonExistentBucket(c *check) {
	// generate a random bucket name.
	bucketName := getRandomBucketName()
	// create request to HEAD on the bucket.
	// HEAD on an bucket helps validate the existence of the bucket.
	request, err := newTestSignedRequest(http.MethodHead, getHEADBucketURL(s.endPoint, bucketName),
		0, nil, s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)

	// execute the http request.
	response, err := s.client.Do(request)
	c.Assert(err, nil)
	// Assert the response.
	c.Assert(response.StatusCode, http.StatusNotFound)
}

// TestEmptyObject - Asserts the response for operation on a 0 byte object.
func (s *TestSuiteCommon) TestEmptyObject(c *check) {
	// generate a random bucket name.
	bucketName := getRandomBucketName()
	// HTTP request to create the bucket.
	request, err := newTestSignedRequest(http.MethodPut, getMakeBucketURL(s.endPoint, bucketName),
		0, nil, s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)

	// execute the http request.
	response, err := s.client.Do(request)
	c.Assert(err, nil)
	// assert the http response status code.
	c.Assert(response.StatusCode, http.StatusOK)

	objectName := "test-object"
	// construct http request for uploading the object.
	request, err = newTestSignedRequest(http.MethodPut, getPutObjectURL(s.endPoint, bucketName, objectName),
		0, nil, s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)

	// execute the upload request.
	response, err = s.client.Do(request)
	c.Assert(err, nil)
	// assert the http response.
	c.Assert(response.StatusCode, http.StatusOK)

	// make HTTP request to fetch the object.
	request, err = newTestSignedRequest(http.MethodGet, getGetObjectURL(s.endPoint, bucketName, objectName),
		0, nil, s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)

	// execute the http request to fetch object.
	response, err = s.client.Do(request)
	c.Assert(err, nil)
	// assert the http response status code.
	c.Assert(response.StatusCode, http.StatusOK)

	var buffer bytes.Buffer
	// extract the body of the response.
	responseBody, err := io.ReadAll(response.Body)
	c.Assert(err, nil)
	// assert the http response body content.
	c.Assert(true, bytes.Equal(responseBody, buffer.Bytes()))
}

func (s *TestSuiteCommon) TestBucket(c *check) {
	// generate a random bucket name.
	bucketName := getRandomBucketName()

	request, err := newTestSignedRequest(http.MethodPut, getMakeBucketURL(s.endPoint, bucketName),
		0, nil, s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)

	response, err := s.client.Do(request)
	c.Assert(err, nil)
	c.Assert(response.StatusCode, http.StatusOK)

	request, err = newTestSignedRequest(http.MethodHead, getMakeBucketURL(s.endPoint, bucketName),
		0, nil, s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)

	response, err = s.client.Do(request)
	c.Assert(err, nil)
	c.Assert(response.StatusCode, http.StatusOK)
}

// Tests get anonymous object.
func (s *TestSuiteCommon) TestObjectGetAnonymous(c *check) {
	// generate a random bucket name.
	bucketName := getRandomBucketName()
	buffer := bytes.NewReader([]byte("hello world"))
	// HTTP request to create the bucket.
	request, err := newTestSignedRequest(http.MethodPut, getMakeBucketURL(s.endPoint, bucketName),
		0, nil, s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)

	// execute the make bucket http request.
	response, err := s.client.Do(request)
	c.Assert(err, nil)
	// assert the response http status code.
	c.Assert(response.StatusCode, http.StatusOK)

	objectName := "testObject"
	// create HTTP request to upload the object.
	request, err = newTestSignedRequest(http.MethodPut, getPutObjectURL(s.endPoint, bucketName, objectName),
		int64(buffer.Len()), buffer, s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)

	// execute the HTTP request to upload the object.
	response, err = s.client.Do(request)
	c.Assert(err, nil)
	// assert the HTTP response status code.
	c.Assert(response.StatusCode, http.StatusOK)

	// initiate anonymous HTTP request to fetch the object which does not exist. We need to return AccessDenied.
	response, err = s.client.Get(getGetObjectURL(s.endPoint, bucketName, objectName+".1"))
	c.Assert(err, nil)
	// assert the http response status code.
	verifyError(c, response, "AccessDenied", "Access Denied.", http.StatusForbidden)

	// initiate anonymous HTTP request to fetch the object which does exist. We need to return AccessDenied.
	response, err = s.client.Get(getGetObjectURL(s.endPoint, bucketName, objectName))
	c.Assert(err, nil)
	// assert the http response status code.
	verifyError(c, response, "AccessDenied", "Access Denied.", http.StatusForbidden)
}

// TestMultipleObjects - Validates upload and fetching of multiple object into the bucket.
func (s *TestSuiteCommon) TestMultipleObjects(c *check) {
	// generate a random bucket name.
	bucketName := getRandomBucketName()
	// HTTP request to create the bucket.
	request, err := newTestSignedRequest(http.MethodPut, getMakeBucketURL(s.endPoint, bucketName),
		0, nil, s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)

	// execute the HTTP request to create the bucket.
	response, err := s.client.Do(request)
	c.Assert(err, nil)
	c.Assert(response.StatusCode, http.StatusOK)

	// constructing HTTP request to fetch a non-existent object.
	// expected to fail, error response asserted for expected error values later.
	objectName := "testObject"
	request, err = newTestSignedRequest(http.MethodGet, getGetObjectURL(s.endPoint, bucketName, objectName),
		0, nil, s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)

	// execute the HTTP request.
	response, err = s.client.Do(request)
	c.Assert(err, nil)
	// Asserting the error response with the expected values.
	verifyError(c, response, "NoSuchKey", "The specified key does not exist.", http.StatusNotFound)

	objectName = "testObject1"
	// content for the object to be uploaded.
	buffer1 := bytes.NewReader([]byte("hello one"))
	// create HTTP request for the object upload.
	request, err = newTestSignedRequest(http.MethodPut, getPutObjectURL(s.endPoint, bucketName, objectName),
		int64(buffer1.Len()), buffer1, s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)

	// execute the HTTP request for object upload.
	response, err = s.client.Do(request)
	c.Assert(err, nil)
	// assert the returned values.
	c.Assert(response.StatusCode, http.StatusOK)

	// create HTTP request to fetch the object which was uploaded above.
	request, err = newTestSignedRequest(http.MethodGet, getGetObjectURL(s.endPoint, bucketName, objectName),
		0, nil, s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)

	// execute the HTTP request.
	response, err = s.client.Do(request)
	c.Assert(err, nil)
	// assert whether 200 OK response status is obtained.
	c.Assert(response.StatusCode, http.StatusOK)

	// extract the response body.
	responseBody, err := io.ReadAll(response.Body)
	c.Assert(err, nil)
	// assert the content body for the expected object data.
	c.Assert(true, bytes.Equal(responseBody, []byte("hello one")))

	// data for new object to be uploaded.
	buffer2 := bytes.NewReader([]byte("hello two"))
	objectName = "testObject2"
	request, err = newTestSignedRequest(http.MethodPut, getPutObjectURL(s.endPoint, bucketName, objectName),
		int64(buffer2.Len()), buffer2, s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)

	// execute the HTTP request for object upload.
	response, err = s.client.Do(request)
	c.Assert(err, nil)
	// assert the response status code for expected value 200 OK.
	c.Assert(response.StatusCode, http.StatusOK)
	// fetch the object which was uploaded above.
	request, err = newTestSignedRequest(http.MethodGet, getGetObjectURL(s.endPoint, bucketName, objectName),
		0, nil, s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)

	// execute the HTTP request to fetch the object.
	response, err = s.client.Do(request)
	c.Assert(err, nil)
	// assert the response status code for expected value 200 OK.
	c.Assert(response.StatusCode, http.StatusOK)

	// verify response data
	responseBody, err = io.ReadAll(response.Body)
	c.Assert(err, nil)
	c.Assert(true, bytes.Equal(responseBody, []byte("hello two")))

	// data for new object to be uploaded.
	buffer3 := bytes.NewReader([]byte("hello three"))
	objectName = "testObject3"
	request, err = newTestSignedRequest(http.MethodPut, getPutObjectURL(s.endPoint, bucketName, objectName),
		int64(buffer3.Len()), buffer3, s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)

	// execute HTTP request.
	response, err = s.client.Do(request)
	c.Assert(err, nil)
	// verify the response code with the expected value of 200 OK.
	c.Assert(response.StatusCode, http.StatusOK)

	// fetch the object which was uploaded above.
	request, err = newTestSignedRequest(http.MethodGet, getPutObjectURL(s.endPoint, bucketName, objectName),
		0, nil, s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)

	response, err = s.client.Do(request)
	c.Assert(err, nil)
	c.Assert(response.StatusCode, http.StatusOK)

	// verify object.
	responseBody, err = io.ReadAll(response.Body)
	c.Assert(err, nil)
	c.Assert(true, bytes.Equal(responseBody, []byte("hello three")))
}

// TestHeader - Validates the error response for an attempt to fetch non-existent object.
func (s *TestSuiteCommon) TestHeader(c *check) {
	// generate a random bucket name.
	bucketName := getRandomBucketName()
	// obtain HTTP request to fetch an object from non-existent bucket/object.
	request, err := newTestSignedRequest(http.MethodGet, getGetObjectURL(s.endPoint, bucketName, "testObject"),
		0, nil, s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)

	response, err := s.client.Do(request)
	c.Assert(err, nil)
	// asserting for the expected error response.
	verifyError(c, response, "NoSuchBucket", "The specified bucket does not exist", http.StatusNotFound)
}

func (s *TestSuiteCommon) TestPutBucket(c *check) {
	// generate a random bucket name.
	bucketName := getRandomBucketName()
	// Block 1: Testing for racy access
	// The assertion is removed from this block since the purpose of this block is to find races
	// The purpose this block is not to check for correctness of functionality
	// Run the test with -race flag to utilize this
	var wg sync.WaitGroup
	for range testConcurrencyLevel {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// HTTP request to create the bucket.
			request, err := newTestSignedRequest(http.MethodPut, getMakeBucketURL(s.endPoint, bucketName),
				0, nil, s.accessKey, s.secretKey, s.signer)
			c.Assert(err, nil)

			response, err := s.client.Do(request)
			if err != nil {
				c.Errorf("Put bucket Failed: <ERROR> %s", err)
				return
			}
			defer response.Body.Close()
		}()
	}
	wg.Wait()

	bucketName = getRandomBucketName()
	// Block 2: testing for correctness of the functionality
	// HTTP request to create the bucket.
	request, err := newTestSignedRequest(http.MethodPut, getMakeBucketURL(s.endPoint, bucketName),
		0, nil, s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)

	response, err := s.client.Do(request)
	c.Assert(err, nil)
	c.Assert(response.StatusCode, http.StatusOK)
	response.Body.Close()
}

// TestCopyObject - Validates copy object.
// The following is the test flow.
// 1. Create bucket.
// 2. Insert Object.
// 3. Use "X-Amz-Copy-Source" header to copy the previously created object.
// 4. Validate the content of copied object.
func (s *TestSuiteCommon) TestCopyObject(c *check) {
	// generate a random bucket name.
	bucketName := getRandomBucketName()
	// HTTP request to create the bucket.
	request, err := newTestSignedRequest(http.MethodPut, getMakeBucketURL(s.endPoint, bucketName),
		0, nil, s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)

	// execute the HTTP request to create bucket.
	response, err := s.client.Do(request)
	c.Assert(err, nil)
	c.Assert(response.StatusCode, http.StatusOK)

	// content for the object to be created.
	buffer1 := bytes.NewReader([]byte("hello world"))
	objectName := "testObject"
	// create HTTP request for object upload.
	request, err = newTestSignedRequest(http.MethodPut, getPutObjectURL(s.endPoint, bucketName, objectName),
		int64(buffer1.Len()), buffer1, s.accessKey, s.secretKey, s.signer)
	request.Header.Set("Content-Type", "application/json")
	if s.signer == signerV2 {
		c.Assert(err, nil)
		err = signRequestV2(request, s.accessKey, s.secretKey)
	}
	c.Assert(err, nil)
	// execute the HTTP request for object upload.
	response, err = s.client.Do(request)
	c.Assert(err, nil)
	c.Assert(response.StatusCode, http.StatusOK)

	objectName2 := "testObject2"
	// Unlike the actual PUT object request, the request to Copy Object doesn't contain request body,
	// empty body with the "X-Amz-Copy-Source" header pointing to the object to copies it in the backend.
	request, err = newTestRequest(http.MethodPut, getPutObjectURL(s.endPoint, bucketName, objectName2), 0, nil)
	c.Assert(err, nil)
	// setting the "X-Amz-Copy-Source" to allow copying the content of previously uploaded object.
	request.Header.Set("X-Amz-Copy-Source", url.QueryEscape(SlashSeparator+bucketName+SlashSeparator+objectName))
	if s.signer == signerV4 {
		err = signRequestV4(request, s.accessKey, s.secretKey)
	} else {
		err = signRequestV2(request, s.accessKey, s.secretKey)
	}
	c.Assert(err, nil)
	// execute the HTTP request.
	// the content is expected to have the content of previous disk.
	response, err = s.client.Do(request)
	c.Assert(err, nil)
	c.Assert(response.StatusCode, http.StatusOK)

	// creating HTTP request to fetch the previously uploaded object.
	request, err = newTestSignedRequest(http.MethodGet, getGetObjectURL(s.endPoint, bucketName, objectName2),
		0, nil, s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)
	// executing the HTTP request.
	response, err = s.client.Do(request)
	c.Assert(err, nil)
	// validating the response status code.
	c.Assert(response.StatusCode, http.StatusOK)
	// reading the response body.
	// response body is expected to have the copied content of the first uploaded object.
	object, err := io.ReadAll(response.Body)
	c.Assert(err, nil)
	c.Assert(string(object), "hello world")
}

// TestPutObject -  Tests successful put object request.
func (s *TestSuiteCommon) TestPutObject(c *check) {
	// generate a random bucket name.
	bucketName := getRandomBucketName()
	// HTTP request to create the bucket.
	request, err := newTestSignedRequest(http.MethodPut, getMakeBucketURL(s.endPoint, bucketName),
		0, nil, s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)

	// execute the HTTP request to create bucket.
	response, err := s.client.Do(request)
	c.Assert(err, nil)
	c.Assert(response.StatusCode, http.StatusOK)

	// content for new object upload.
	buffer1 := bytes.NewReader([]byte("hello world"))
	objectName := "testObject"
	// creating HTTP request for object upload.
	request, err = newTestSignedRequest(http.MethodPut, getPutObjectURL(s.endPoint, bucketName, objectName),
		int64(buffer1.Len()), buffer1, s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)
	// execute the HTTP request for object upload.
	response, err = s.client.Do(request)
	c.Assert(err, nil)
	c.Assert(response.StatusCode, http.StatusOK)

	// fetch the object back and verify its contents.
	request, err = newTestSignedRequest(http.MethodGet, getGetObjectURL(s.endPoint, bucketName, objectName),
		0, nil, s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)
	// execute the HTTP request to fetch the object.
	response, err = s.client.Do(request)
	c.Assert(err, nil)
	c.Assert(response.StatusCode, http.StatusOK)
	c.Assert(response.ContentLength, int64(len([]byte("hello world"))))
	var buffer2 bytes.Buffer
	// retrieve the contents of response body.
	n, err := io.Copy(&buffer2, response.Body)
	c.Assert(err, nil)
	c.Assert(n, int64(len([]byte("hello world"))))
	// asserted the contents of the fetched object with the expected result.
	c.Assert(true, bytes.Equal(buffer2.Bytes(), []byte("hello world")))

	// Test the response when object name ends with a slash.
	// This is a special case with size as '0' and object ends with
	// a slash separator, we treat it like a valid operation and
	// return success.
	// The response Etag headers should contain Md5Sum of empty string.
	objectName = "objectwith/"
	// create HTTP request for object upload.
	request, err = newTestSignedRequest(http.MethodPut, getPutObjectURL(s.endPoint, bucketName, objectName),
		0, nil, s.accessKey, s.secretKey, s.signer)
	if s.signer == signerV2 {
		c.Assert(err, nil)
		err = signRequestV2(request, s.accessKey, s.secretKey)
	}
	c.Assert(err, nil)
	// execute the HTTP request for object upload.
	response, err = s.client.Do(request)
	c.Assert(err, nil)
	c.Assert(response.StatusCode, http.StatusOK)
	// The response Etag header should contain Md5sum of an empty string.
	c.Assert(response.Header.Get(xhttp.ETag), "\""+emptyETag+"\"")
}

// TestListBuckets - Make request for listing of all buckets.
// XML response is parsed.
// Its success verifies the format of the response.
func (s *TestSuiteCommon) TestListBuckets(c *check) {
	// generate a random bucket name.
	bucketName := getRandomBucketName()
	// HTTP request to create the bucket.
	request, err := newTestSignedRequest(http.MethodPut, getMakeBucketURL(s.endPoint, bucketName),
		0, nil, s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)
	// execute the HTTP request to list buckets.
	response, err := s.client.Do(request)
	c.Assert(err, nil)
	c.Assert(response.StatusCode, http.StatusOK)

	// create HTTP request for listing buckets.
	request, err = newTestSignedRequest(http.MethodGet, getListBucketURL(s.endPoint),
		0, nil, s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)

	// execute the HTTP request to list buckets.
	response, err = s.client.Do(request)
	c.Assert(err, nil)
	c.Assert(response.StatusCode, http.StatusOK)

	var results ListBucketsResponse
	// parse the list bucket response.
	decoder := xml.NewDecoder(response.Body)
	err = decoder.Decode(&results)
	// validating that the xml-decoding/parsing was successful.
	c.Assert(err, nil)

	// Fetch the bucket created above
	var createdBucket Bucket
	for _, b := range results.Buckets.Buckets {
		if b.Name == bucketName {
			createdBucket = b
		}
	}
	c.Assert(createdBucket.Name != "", true)

	// Parse the bucket modtime
	creationTime, err := time.Parse(iso8601TimeFormat, createdBucket.CreationDate)
	c.Assert(err, nil)

	// Check if bucket modtime is consistent (not less than current time and not late more than 5 minutes)
	timeNow := time.Now().UTC()
	c.Assert(creationTime.Before(timeNow), true)
	c.Assert(timeNow.Sub(creationTime) < time.Minute*5, true)
}

// This tests validate if PUT handler can successfully detect signature mismatch.
func (s *TestSuiteCommon) TestValidateSignature(c *check) {
	// generate a random bucket name.
	bucketName := getRandomBucketName()
	// HTTP request to create the bucket.
	request, err := newTestSignedRequest(http.MethodPut, getMakeBucketURL(s.endPoint, bucketName),
		0, nil, s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)

	// Execute the HTTP request to create bucket.
	response, err := s.client.Do(request)
	c.Assert(err, nil)
	c.Assert(response.StatusCode, http.StatusOK)

	objName := "test-object"

	// Body is on purpose set to nil so that we get payload generated for empty bytes.

	// Create new HTTP request with incorrect secretKey to generate an incorrect signature.
	secretKey := s.secretKey + "a"
	request, err = newTestSignedRequest(http.MethodPut, getPutObjectURL(s.endPoint, bucketName, objName), 0, nil, s.accessKey, secretKey, s.signer)
	c.Assert(err, nil)
	response, err = s.client.Do(request)
	c.Assert(err, nil)
	verifyError(c, response, "SignatureDoesNotMatch", "The request signature we calculated does not match the signature you provided. Check your key and signing method.", http.StatusForbidden)
}

// This tests validate if PUT handler can successfully detect SHA256 mismatch.
func (s *TestSuiteCommon) TestSHA256Mismatch(c *check) {
	// generate a random bucket name.
	bucketName := getRandomBucketName()
	// HTTP request to create the bucket.
	request, err := newTestSignedRequest(http.MethodPut, getMakeBucketURL(s.endPoint, bucketName),
		0, nil, s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)

	// Execute the HTTP request to create bucket.
	response, err := s.client.Do(request)
	c.Assert(err, nil)
	c.Assert(response.StatusCode, http.StatusOK)

	objName := "test-object"

	// Body is on purpose set to nil so that we get payload generated for empty bytes.

	// Create new HTTP request with incorrect secretKey to generate an incorrect signature.
	request, err = newTestSignedRequest(http.MethodPut, getPutObjectURL(s.endPoint, bucketName, objName), 0, nil, s.accessKey, s.secretKey, s.signer)
	if s.signer == signerV4 {
		c.Assert(request.Header.Get("x-amz-content-sha256"), "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
	}
	// Set the body to generate signature mismatch.
	helloReader := bytes.NewReader([]byte("Hello, World"))
	request.ContentLength = helloReader.Size()
	request.Body = io.NopCloser(helloReader)
	c.Assert(err, nil)

	// execute the HTTP request.
	response, err = s.client.Do(request)
	c.Assert(err, nil)
	if s.signer == signerV4 {
		verifyError(c, response, "XAmzContentSHA256Mismatch", "The provided 'x-amz-content-sha256' header does not match what was computed.", http.StatusBadRequest)
	}
}

// TestPutObjectLongName - Validates the error response
// on an attempt to upload an object with long name.
func (s *TestSuiteCommon) TestPutObjectLongName(c *check) {
	// generate a random bucket name.
	bucketName := getRandomBucketName()
	// HTTP request to create the bucket.
	request, err := newTestSignedRequest(http.MethodPut, getMakeBucketURL(s.endPoint, bucketName),
		0, nil, s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)

	// Execute the HTTP request to create bucket.
	response, err := s.client.Do(request)
	c.Assert(err, nil)
	c.Assert(response.StatusCode, http.StatusOK)
	// Content for the object to be uploaded.
	buffer := bytes.NewReader([]byte("hello world"))
	// make long object name.
	longObjName := fmt.Sprintf("%0255d/%0255d/%0255d", 1, 1, 1)
	if IsDocker() || IsKubernetes() {
		longObjName = fmt.Sprintf("%0242d/%0242d/%0242d", 1, 1, 1)
	}
	// create new HTTP request to insert the object.
	request, err = newTestSignedRequest(http.MethodPut, getPutObjectURL(s.endPoint, bucketName, longObjName),
		int64(buffer.Len()), buffer, s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)
	// execute the HTTP request.
	response, err = s.client.Do(request)
	c.Assert(err, nil)
	c.Assert(response.StatusCode, http.StatusOK)

	// make long object name.
	longObjName = fmt.Sprintf("%0255d/%0255d/%0255d/%0255d/%0255d", 1, 1, 1, 1, 1)
	if IsDocker() || IsKubernetes() {
		longObjName = fmt.Sprintf("%0242d/%0242d/%0242d/%0242d/%0242d", 1, 1, 1, 1, 1)
	}
	// create new HTTP request to insert the object.
	buffer = bytes.NewReader([]byte("hello world"))
	request, err = newTestSignedRequest(http.MethodPut, getPutObjectURL(s.endPoint, bucketName, longObjName),
		int64(buffer.Len()), buffer, s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)
	// execute the HTTP request.
	response, err = s.client.Do(request)
	c.Assert(err, nil)
	c.Assert(response.StatusCode, http.StatusBadRequest)
	verifyError(c, response, "KeyTooLongError", "Your key is too long", http.StatusBadRequest)

	// make object name as unsupported
	longObjName = fmt.Sprintf("%0256d", 1)
	buffer = bytes.NewReader([]byte("hello world"))
	request, err = newTestSignedRequest(http.MethodPut, getPutObjectURL(s.endPoint, bucketName, longObjName),
		int64(buffer.Len()), buffer, s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)

	response, err = s.client.Do(request)
	c.Assert(err, nil)
	verifyError(c, response, "XMinioInvalidObjectName", "Object name contains unsupported characters.", http.StatusBadRequest)
}

// TestNotBeAbleToCreateObjectInNonexistentBucket - Validates the error response
// on an attempt to upload an object into a non-existent bucket.
func (s *TestSuiteCommon) TestNotBeAbleToCreateObjectInNonexistentBucket(c *check) {
	// generate a random bucket name.
	bucketName := getRandomBucketName()
	// content of the object to be uploaded.
	buffer1 := bytes.NewReader([]byte("hello world"))

	// preparing for upload by generating the upload URL.
	objectName := "test-object"
	request, err := newTestSignedRequest(http.MethodPut, getPutObjectURL(s.endPoint, bucketName, objectName),
		int64(buffer1.Len()), buffer1, s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)

	// Execute the HTTP request.
	response, err := s.client.Do(request)
	c.Assert(err, nil)
	// Assert the response error message.
	verifyError(c, response, "NoSuchBucket", "The specified bucket does not exist", http.StatusNotFound)
}

// TestHeadOnObjectLastModified - Asserts response for HEAD on an object.
// HEAD requests on an object validates the existence of the object.
// The responses for fetching the object when If-Modified-Since
// and If-Unmodified-Since headers set are validated.
// If-Modified-Since - Return the object only if it has been modified since the specified time, else return a 304 (not modified).
// If-Unmodified-Since - Return the object only if it has not been modified since the specified time, else return a 412 (precondition failed).
func (s *TestSuiteCommon) TestHeadOnObjectLastModified(c *check) {
	// generate a random bucket name.
	bucketName := getRandomBucketName()
	// HTTP request to create the bucket.
	request, err := newTestSignedRequest(http.MethodPut, getMakeBucketURL(s.endPoint, bucketName),
		0, nil, s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)

	// execute the HTTP request to create bucket.
	response, err := s.client.Do(request)
	c.Assert(err, nil)
	c.Assert(response.StatusCode, http.StatusOK)

	// preparing for object upload.
	objectName := "test-object"
	// content for the object to be uploaded.
	buffer1 := bytes.NewReader([]byte("hello world"))
	// obtaining URL for uploading the object.
	request, err = newTestSignedRequest(http.MethodPut, getPutObjectURL(s.endPoint, bucketName, objectName),
		int64(buffer1.Len()), buffer1, s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)

	// executing the HTTP request to download the object.
	response, err = s.client.Do(request)
	c.Assert(err, nil)
	c.Assert(response.StatusCode, http.StatusOK)
	// make HTTP request to obtain object info.
	request, err = newTestSignedRequest(http.MethodHead, getHeadObjectURL(s.endPoint, bucketName, objectName),
		0, nil, s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)
	// execute the HTTP request.
	response, err = s.client.Do(request)
	c.Assert(err, nil)
	// verify the status of the HTTP response.
	c.Assert(response.StatusCode, http.StatusOK)

	// retrieve the info of last modification time of the object from the response header.
	lastModified := response.Header.Get("Last-Modified")
	// Parse it into time.Time structure.
	t, err := time.Parse(http.TimeFormat, lastModified)
	c.Assert(err, nil)

	// make HTTP request to obtain object info.
	// But this time set the "If-Modified-Since" header to be 10 minute more than the actual
	// last modified time of the object.
	request, err = newTestSignedRequest(http.MethodHead, getHeadObjectURL(s.endPoint, bucketName, objectName),
		0, nil, s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)
	request.Header.Set("If-Modified-Since", t.Add(10*time.Minute).UTC().Format(http.TimeFormat))
	response, err = s.client.Do(request)
	c.Assert(err, nil)
	// Since the "If-Modified-Since" header was ahead in time compared to the actual
	// modified time of the object expecting the response status to be http.StatusNotModified.
	c.Assert(response.StatusCode, http.StatusNotModified)

	// Again, obtain the object info.
	// This time setting "If-Unmodified-Since" to a time after the object is modified.
	// As documented above, expecting http.StatusPreconditionFailed.
	request, err = newTestSignedRequest(http.MethodHead, getHeadObjectURL(s.endPoint, bucketName, objectName),
		0, nil, s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)
	request.Header.Set("If-Unmodified-Since", t.Add(-10*time.Minute).UTC().Format(http.TimeFormat))
	response, err = s.client.Do(request)
	c.Assert(err, nil)
	c.Assert(response.StatusCode, http.StatusPreconditionFailed)

	// make HTTP request to obtain object info.
	// But this time set a date with unrecognized format to the "If-Modified-Since" header
	request, err = newTestSignedRequest(http.MethodHead, getHeadObjectURL(s.endPoint, bucketName, objectName),
		0, nil, s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)
	request.Header.Set("If-Unmodified-Since", "Mon, 02 Jan 2006 15:04:05 +00:00")
	response, err = s.client.Do(request)
	c.Assert(err, nil)
	// Since the "If-Modified-Since" header was ahead in time compared to the actual
	// modified time of the object expecting the response status to be http.StatusNotModified.
	c.Assert(response.StatusCode, http.StatusOK)
}

// TestHeadOnBucket - Validates response for HEAD on the bucket.
// HEAD request on the bucket validates the existence of the bucket.
func (s *TestSuiteCommon) TestHeadOnBucket(c *check) {
	// generate a random bucket name.
	bucketName := getRandomBucketName()
	// HTTP request to create the bucket.
	request, err := newTestSignedRequest(http.MethodPut, getHEADBucketURL(s.endPoint, bucketName),
		0, nil, s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)

	// execute the HTTP request to create bucket.
	response, err := s.client.Do(request)
	c.Assert(err, nil)
	c.Assert(response.StatusCode, http.StatusOK)
	// make HEAD request on the bucket.
	request, err = newTestSignedRequest(http.MethodHead, getHEADBucketURL(s.endPoint, bucketName),
		0, nil, s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)
	// execute the HTTP request.
	response, err = s.client.Do(request)
	c.Assert(err, nil)
	// Asserting the response status for expected value of http.StatusOK.
	c.Assert(response.StatusCode, http.StatusOK)
}

// TestContentTypePersists - Object upload with different Content-type is first done.
// And then a HEAD and GET request on these objects are done to validate if the same Content-Type set during upload persists.
func (s *TestSuiteCommon) TestContentTypePersists(c *check) {
	// generate a random bucket name.
	bucketName := getRandomBucketName()
	// HTTP request to create the bucket.
	request, err := newTestSignedRequest(http.MethodPut, getMakeBucketURL(s.endPoint, bucketName),
		0, nil, s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)

	// execute the HTTP request to create bucket.
	response, err := s.client.Do(request)
	c.Assert(err, nil)
	c.Assert(response.StatusCode, http.StatusOK)

	// Uploading a new object with Content-Type "image/png".
	// content for the object to be uploaded.
	buffer1 := bytes.NewReader([]byte("hello world"))
	objectName := "test-object.png"
	// constructing HTTP request for object upload.
	request, err = newTestSignedRequest(http.MethodPut, getPutObjectURL(s.endPoint, bucketName, objectName),
		int64(buffer1.Len()), buffer1, s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)
	request.Header.Set("Content-Type", "image/png")
	if s.signer == signerV2 {
		err = signRequestV2(request, s.accessKey, s.secretKey)
		c.Assert(err, nil)
	}

	// execute the HTTP request for object upload.
	response, err = s.client.Do(request)
	c.Assert(err, nil)
	c.Assert(response.StatusCode, http.StatusOK)

	// Fetching the object info using HEAD request for the object which was uploaded above.
	request, err = newTestSignedRequest(http.MethodHead, getHeadObjectURL(s.endPoint, bucketName, objectName),
		0, nil, s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)

	// Execute the HTTP request.
	response, err = s.client.Do(request)
	c.Assert(err, nil)
	// Verify if the Content-Type header is set during the object persists.
	c.Assert(response.Header.Get("Content-Type"), "image/png")

	// Fetching the object itself and then verify the Content-Type header.
	request, err = newTestSignedRequest(http.MethodGet, getGetObjectURL(s.endPoint, bucketName, objectName),
		0, nil, s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)

	// Execute the HTTP to fetch the object.
	response, err = s.client.Do(request)
	c.Assert(err, nil)
	c.Assert(response.StatusCode, http.StatusOK)
	// Verify if the Content-Type header is set during the object persists.
	c.Assert(response.Header.Get("Content-Type"), "image/png")

	// Uploading a new object with Content-Type  "application/json".
	objectName = "test-object.json"
	buffer2 := bytes.NewReader([]byte("hello world"))
	request, err = newTestSignedRequest(http.MethodPut, getPutObjectURL(s.endPoint, bucketName, objectName),
		int64(buffer2.Len()), buffer2, s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)
	// setting the request header to be application/json.
	request.Header.Set("Content-Type", "application/json")
	if s.signer == signerV2 {
		err = signRequestV2(request, s.accessKey, s.secretKey)
		c.Assert(err, nil)
	}

	// Execute the HTTP request to upload the object.
	response, err = s.client.Do(request)
	c.Assert(err, nil)
	c.Assert(response.StatusCode, http.StatusOK)

	// Obtain the info of the object which was uploaded above using HEAD request.
	request, err = newTestSignedRequest(http.MethodHead, getHeadObjectURL(s.endPoint, bucketName, objectName),
		0, nil, s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)
	// Execute the HTTP request.
	response, err = s.client.Do(request)
	c.Assert(err, nil)
	// Assert if the content-type header set during the object upload persists.
	c.Assert(response.Header.Get("Content-Type"), "application/json")

	// Fetch the object and assert whether the Content-Type header persists.
	request, err = newTestSignedRequest(http.MethodGet, getGetObjectURL(s.endPoint, bucketName, objectName),
		0, nil, s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)

	// Execute the HTTP request.
	response, err = s.client.Do(request)
	c.Assert(err, nil)
	// Assert if the content-type header set during the object upload persists.
	c.Assert(response.Header.Get("Content-Type"), "application/json")
}

// TestPartialContent - Validating for GetObject with partial content request.
// By setting the Range header, A request to send specific bytes range of data from an
// already uploaded object can be done.
func (s *TestSuiteCommon) TestPartialContent(c *check) {
	bucketName := getRandomBucketName()

	request, err := newTestSignedRequest(http.MethodPut, getMakeBucketURL(s.endPoint, bucketName),
		0, nil, s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)

	response, err := s.client.Do(request)
	c.Assert(err, nil)
	c.Assert(response.StatusCode, http.StatusOK)

	buffer1 := bytes.NewReader([]byte("Hello World"))
	request, err = newTestSignedRequest(http.MethodPut, getPutObjectURL(s.endPoint, bucketName, "bar"),
		int64(buffer1.Len()), buffer1, s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)

	response, err = s.client.Do(request)
	c.Assert(err, nil)
	c.Assert(response.StatusCode, http.StatusOK)

	// Prepare request
	request, err = newTestSignedRequest(http.MethodGet, getGetObjectURL(s.endPoint, bucketName, "bar"),
		0, nil, s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)
	request.Header.Set("Range", "bytes=6-7")

	response, err = s.client.Do(request)
	c.Assert(err, nil)
	c.Assert(response.StatusCode, http.StatusPartialContent)
	partialObject, err := io.ReadAll(response.Body)
	c.Assert(err, nil)

	c.Assert(string(partialObject), "Wo")
}

// TestListObjectsHandler - Setting valid parameters to List Objects
// and then asserting the response with the expected one.
func (s *TestSuiteCommon) TestListObjectsHandler(c *check) {
	// generate a random bucket name.
	bucketName := getRandomBucketName()
	// HTTP request to create the bucket.
	request, err := newTestSignedRequest(http.MethodPut, getMakeBucketURL(s.endPoint, bucketName),
		0, nil, s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)

	// execute the HTTP request to create bucket.
	response, err := s.client.Do(request)
	c.Assert(err, nil)
	c.Assert(response.StatusCode, http.StatusOK)

	for _, objectName := range []string{"foo bar 1", "foo bar 2", "obj2", "obj2/"} {
		buffer := bytes.NewReader([]byte("Hello World"))
		request, err = newTestSignedRequest(http.MethodPut, getPutObjectURL(s.endPoint, bucketName, objectName),
			int64(buffer.Len()), buffer, s.accessKey, s.secretKey, s.signer)
		c.Assert(err, nil)

		response, err = s.client.Do(request)
		c.Assert(err, nil)
		c.Assert(response.StatusCode, http.StatusOK)
	}

	testCases := []struct {
		getURL          string
		expectedStrings []string
	}{
		{getListObjectsV1URL(s.endPoint, bucketName, "", "1000", ""), []string{"<Key>foo bar 1</Key>", "<Key>foo bar 2</Key>"}},
		{getListObjectsV1URL(s.endPoint, bucketName, "", "1000", "url"), []string{"<Key>foo+bar+1</Key>", "<Key>foo+bar+2</Key>"}},
		{
			getListObjectsV2URL(s.endPoint, bucketName, "", "1000", "", "", ""),
			[]string{
				"<Key>foo bar 1</Key>",
				"<Key>foo bar 2</Key>",
			},
		},
		{
			getListObjectsV2URL(s.endPoint, bucketName, "", "1000", "true", "", ""),
			[]string{
				"<Key>foo bar 1</Key>",
				"<Key>foo bar 2</Key>",
				fmt.Sprintf("<Owner><ID>%s</ID><DisplayName>minio</DisplayName></Owner>", globalMinioDefaultOwnerID),
			},
		},
		{getListObjectsV2URL(s.endPoint, bucketName, "", "1000", "", "url", ""), []string{"<Key>foo+bar+1</Key>", "<Key>foo+bar+2</Key>"}},
		{
			getListObjectsV2URL(s.endPoint, bucketName, "", "1000", "", "", ""),
			[]string{
				"<Key>obj2</Key>",
				"<Key>obj2/</Key>",
			},
		},
	}

	for _, testCase := range testCases {
		// create listObjectsV1 request with valid parameters
		request, err = newTestSignedRequest(http.MethodGet, testCase.getURL, 0, nil, s.accessKey, s.secretKey, s.signer)
		c.Assert(err, nil)
		// execute the HTTP request.
		response, err = s.client.Do(request)
		c.Assert(err, nil)
		c.Assert(response.StatusCode, http.StatusOK)

		getContent, err := io.ReadAll(response.Body)
		c.Assert(err, nil)

		for _, expectedStr := range testCase.expectedStrings {
			c.Assert(strings.Contains(string(getContent), expectedStr), true)
		}
	}
}

// TestListObjectsV2HadoopUAHandler - Test ListObjectsV2 call with max-keys=2 and Hadoop User-Agent
func (s *TestSuiteCommon) TestListObjectsV2HadoopUAHandler(c *check) {
	// generate a random bucket name.
	bucketName := getRandomBucketName()
	// HTTP request to create the bucket.
	request, err := newTestSignedRequest(http.MethodPut, getMakeBucketURL(s.endPoint, bucketName),
		0, nil, s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)

	// execute the HTTP request to create bucket.
	response, err := s.client.Do(request)
	c.Assert(err, nil)
	c.Assert(response.StatusCode, http.StatusOK)

	// enable versioning on the bucket.
	enableVersioningBody := []byte("<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>")
	enableVersioningBucketRequest, err := newTestSignedRequest(http.MethodPut, getBucketVersioningConfigURL(s.endPoint, bucketName),
		int64(len(enableVersioningBody)), bytes.NewReader(enableVersioningBody), s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)
	// execute the HTTP request to create bucket.
	response, err = s.client.Do(enableVersioningBucketRequest)
	c.Assert(err, nil)
	c.Assert(response.StatusCode, http.StatusOK)

	for _, objectName := range []string{"pfx/a/1.txt", "pfx/b/2.txt", "pfx/", "pfx2/c/3.txt", "pfx2/d/3.txt", "pfx1/1.txt", "pfx2/", "pfx3/", "pfx4/"} {
		buffer := bytes.NewReader([]byte(""))
		request, err = newTestSignedRequest(http.MethodPut, getPutObjectURL(s.endPoint, bucketName, objectName),
			int64(buffer.Len()), buffer, s.accessKey, s.secretKey, s.signer)
		c.Assert(err, nil)
		response, err = s.client.Do(request)
		c.Assert(err, nil)
		c.Assert(response.StatusCode, http.StatusOK)
	}
	for _, objectName := range []string{"pfx2/c/3.txt", "pfx2/d/3.txt", "pfx2/", "pfx3/"} {
		delRequest, err := newTestSignedRequest(http.MethodDelete, getDeleteObjectURL(s.endPoint, bucketName, objectName),
			0, nil, s.accessKey, s.secretKey, s.signer)
		c.Assert(err, nil)
		response, err = s.client.Do(delRequest)
		c.Assert(err, nil)
		c.Assert(response.StatusCode, http.StatusNoContent)
	}
	testCases := []struct {
		getURL          string
		expectedStrings []string
		userAgent       string
	}{
		{
			getListObjectsV2URL(s.endPoint, bucketName, "pfx/a/", "2", "", "", "/"),
			[]string{
				"<Contents><Key>pfx/a/1.txt</Key>",
			},
			"Hadoop 3.3.2, aws-sdk-java/1.12.262 Linux/5.14.0-362.24.1.el9_3.x86_64 OpenJDK_64-Bit_Server_VM/11.0.22+7 java/11.0.22 scala/2.12.15 vendor/Eclipse_Adoptium cfg/retry-mode/legacy",
		},
		{
			getListObjectsV2URL(s.endPoint, bucketName, "pfx/a/", "2", "", "", "/"),
			[]string{
				"<Prefix>pfx/a/</Prefix>",
				"<Contents><Key>pfx/a/1.txt</Key>",
			},
			"",
		},
		{
			getListObjectsV2URL(s.endPoint, bucketName, "pfx2/c", "2", "true", "", "/"),
			[]string{
				"<Prefix>pfx2/c</Prefix><KeyCount>1</KeyCount><MaxKeys>2</MaxKeys><Delimiter>/</Delimiter><IsTruncated>false</IsTruncated><CommonPrefixes><Prefix>pfx2/c/</Prefix></CommonPrefixes>",
			},
			"",
		},

		{
			getListObjectsV2URL(s.endPoint, bucketName, "pfx2/c", "2", "true", "", "/"),
			[]string{
				"<Prefix>pfx2/c</Prefix><KeyCount>1</KeyCount><MaxKeys>2</MaxKeys><Delimiter>/</Delimiter><IsTruncated>false</IsTruncated>",
				"<CommonPrefixes><Prefix>pfx2/c/</Prefix>",
			},
			"Hadoop 3.3.2, aws-sdk-java/1.12.262 Linux/5.14.0-362.24.1.el9_3.x86_64 OpenJDK_64-Bit_Server_VM/11.0.22+7 java/11.0.22 scala/2.12.15 vendor/Eclipse_Adoptium cfg/retry-mode/legacy",
		},
		{
			getListObjectsV2URL(s.endPoint, bucketName, "pfx2/c", "2", "true", "", "/"),
			[]string{
				"<Prefix>pfx2/c</Prefix><KeyCount>1</KeyCount><MaxKeys>2</MaxKeys><Delimiter>/</Delimiter><IsTruncated>false</IsTruncated>",
				"<CommonPrefixes><Prefix>pfx2/c/</Prefix></CommonPrefixes>",
			},
			"Hadoop 3.3.2, aws-sdk-java/1.12.262 Linux/5.14.0-362.24.1.el9_3.x86_64 OpenJDK_64-Bit_Server_VM/11.0.22+7 java/11.0.22 scala/2.12.15 vendor/Eclipse_Adoptium cfg/retry-mode/legacy",
		},
		{
			getListObjectsV2URL(s.endPoint, bucketName, "pfx2/", "2", "false", "", "/"),
			[]string{
				"<Prefix>pfx2/</Prefix><KeyCount>2</KeyCount><MaxKeys>2</MaxKeys><Delimiter>/</Delimiter><IsTruncated>false</IsTruncated>",
				"<CommonPrefixes><Prefix>pfx2/c/</Prefix></CommonPrefixes><CommonPrefixes><Prefix>pfx2/d/</Prefix></CommonPrefixes>",
			},
			"Hadoop 3.3.2, aws-sdk-java/1.12.262 Linux/5.14.0-362.24.1.el9_3.x86_64 OpenJDK_64-Bit_Server_VM/11.0.22+7 java/11.0.22 scala/2.12.15 vendor/Eclipse_Adoptium cfg/retry-mode/legacy",
		},
		{
			getListObjectsV2URL(s.endPoint, bucketName, "pfx2/", "2", "false", "", "/"),
			[]string{
				"<CommonPrefixes><Prefix>pfx2/c/</Prefix></CommonPrefixes>",
			},
			"",
		},
		{
			getListObjectsV2URL(s.endPoint, bucketName, "pfx2/", "2", "false", "", "/"),
			[]string{
				"<Prefix>pfx2/</Prefix><KeyCount>2</KeyCount><MaxKeys>2</MaxKeys><Delimiter>/</Delimiter><IsTruncated>false</IsTruncated>",
				"<CommonPrefixes><Prefix>pfx2/c/</Prefix></CommonPrefixes><CommonPrefixes><Prefix>pfx2/d/</Prefix></CommonPrefixes>",
			},
			"Hadoop 3.3.2, aws-sdk-java/1.12.262 Linux/5.14.0-362.24.1.el9_3.x86_64 OpenJDK_64-Bit_Server_VM/11.0.22+7 java/11.0.22 scala/2.12.15 vendor/Eclipse_Adoptium cfg/retry-mode/legacy",
		},
		{
			getListObjectsV2URL(s.endPoint, bucketName, "pfx3/", "2", "false", "", "/"),
			[]string{
				"<Prefix>pfx3/</Prefix><KeyCount>0</KeyCount><MaxKeys>2</MaxKeys><Delimiter>/</Delimiter><IsTruncated>false</IsTruncated>",
			},
			"Hadoop 3.3.2, aws-sdk-java/1.12.262 Linux/5.14.0-362.24.1.el9_3.x86_64 OpenJDK_64-Bit_Server_VM/11.0.22+7 java/11.0.22 scala/2.12.15 vendor/Eclipse_Adoptium cfg/retry-mode/legacy",
		},
		{
			getListObjectsV2URL(s.endPoint, bucketName, "pfx4/", "2", "false", "", "/"),
			[]string{
				"<Prefix>pfx4/</Prefix><KeyCount>1</KeyCount><MaxKeys>2</MaxKeys><Delimiter>/</Delimiter><IsTruncated>false</IsTruncated><Contents><Key>pfx4/</Key>",
			},
			"Hadoop 3.3.2, aws-sdk-java/1.12.262 Linux/5.14.0-362.24.1.el9_3.x86_64 OpenJDK_64-Bit_Server_VM/11.0.22+7 java/11.0.22 scala/2.12.15 vendor/Eclipse_Adoptium cfg/retry-mode/legacy",
		},
		{
			getListObjectsV2URL(s.endPoint, bucketName, "pfx3/", "2", "false", "", "/"),
			[]string{
				"<Prefix>pfx3/</Prefix><KeyCount>0</KeyCount><MaxKeys>2</MaxKeys><Delimiter>/</Delimiter><IsTruncated>false</IsTruncated>",
			},
			"",
		},
		{
			getListObjectsV2URL(s.endPoint, bucketName, "pfx4/", "2", "false", "", "/"),
			[]string{
				"<Prefix>pfx4/</Prefix><KeyCount>1</KeyCount><MaxKeys>2</MaxKeys><Delimiter>/</Delimiter><IsTruncated>false</IsTruncated><Contents><Key>pfx4/</Key>",
			},
			"",
		},
	}
	for _, testCase := range testCases {
		// create listObjectsV1 request with valid parameters
		request, err = newTestSignedRequest(http.MethodGet, testCase.getURL, 0, nil, s.accessKey, s.secretKey, s.signer)
		request.Header.Set("User-Agent", testCase.userAgent)
		c.Assert(err, nil)
		// execute the HTTP request.
		response, err = s.client.Do(request)
		c.Assert(err, nil)
		c.Assert(response.StatusCode, http.StatusOK)

		getContent, err := io.ReadAll(response.Body)
		c.Assert(err, nil)

		for _, expectedStr := range testCase.expectedStrings {
			c.Assert(strings.Contains(string(getContent), expectedStr), true)
		}
	}
}

// TestListObjectVersionsHandler - checks the order of <Version>
// and <DeleteMarker> XML tags in a version listing
func (s *TestSuiteCommon) TestListObjectVersionsOutputOrderHandler(c *check) {
	// generate a random bucket name.
	bucketName := getRandomBucketName()
	// HTTP request to create the bucket.
	makeBucketRequest, err := newTestSignedRequest(http.MethodPut, getMakeBucketURL(s.endPoint, bucketName),
		0, nil, s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)
	// execute the HTTP request to create bucket.
	response, err := s.client.Do(makeBucketRequest)
	c.Assert(err, nil)
	c.Assert(response.StatusCode, http.StatusOK)

	// HTTP request to create the bucket.
	enableVersioningBody := []byte("<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>")
	enableVersioningBucketRequest, err := newTestSignedRequest(http.MethodPut, getBucketVersioningConfigURL(s.endPoint, bucketName),
		int64(len(enableVersioningBody)), bytes.NewReader(enableVersioningBody), s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)
	// execute the HTTP request to create bucket.
	response, err = s.client.Do(enableVersioningBucketRequest)
	c.Assert(err, nil)
	c.Assert(response.StatusCode, http.StatusOK)

	for _, objectName := range []string{"file.1", "file.2"} {
		buffer := bytes.NewReader([]byte("testcontent"))
		putRequest, err := newTestSignedRequest(http.MethodPut, getPutObjectURL(s.endPoint, bucketName, objectName),
			int64(buffer.Len()), buffer, s.accessKey, s.secretKey, s.signer)
		c.Assert(err, nil)
		response, err = s.client.Do(putRequest)
		c.Assert(err, nil)
		c.Assert(response.StatusCode, http.StatusOK)

		delRequest, err := newTestSignedRequest(http.MethodDelete, getDeleteObjectURL(s.endPoint, bucketName, objectName),
			0, nil, s.accessKey, s.secretKey, s.signer)
		c.Assert(err, nil)
		response, err = s.client.Do(delRequest)
		c.Assert(err, nil)
		c.Assert(response.StatusCode, http.StatusNoContent)
	}

	// create listObjectsV1 request with valid parameters
	request, err := newTestSignedRequest(http.MethodGet, getListObjectVersionsURL(s.endPoint, bucketName, "", "1000", ""),
		0, nil, s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)
	// execute the HTTP request.
	response, err = s.client.Do(request)
	c.Assert(err, nil)
	c.Assert(response.StatusCode, http.StatusOK)

	getContent, err := io.ReadAll(response.Body)
	c.Assert(err, nil)

	r := regexp.MustCompile(
		`<ListVersionsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">.*` +
			`<DeleteMarker><Key>file.1</Key>.*<IsLatest>true</IsLatest>.*</DeleteMarker>` +
			`<Version><Key>file.1</Key>.*<IsLatest>false</IsLatest>.*</Version>` +
			`<DeleteMarker><Key>file.2</Key>.*<IsLatest>true</IsLatest>.*</DeleteMarker>` +
			`<Version><Key>file.2</Key>.*<IsLatest>false</IsLatest>.*</Version>` +
			`</ListVersionsResult>`)

	c.Assert(r.MatchString(string(getContent)), true)
}

// TestListObjectsSpecialCharactersHandler - Setting valid parameters to List Objects
// and then asserting the response with the expected one.
func (s *TestSuiteCommon) TestListObjectsSpecialCharactersHandler(c *check) {
	if runtime.GOOS == globalWindowsOSName {
		c.Skip("skip special character test for windows")
	}

	// generate a random bucket name.
	bucketName := getRandomBucketName()
	// HTTP request to create the bucket.
	request, err := newTestSignedRequest(http.MethodPut, getMakeBucketURL(s.endPoint, bucketName),
		0, nil, s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)

	// execute the HTTP request to create bucket.
	response, err := s.client.Do(request)
	c.Assert(err, nil)
	c.Assert(response.StatusCode, http.StatusOK)

	for _, objectName := range []string{"foo bar 1", "foo bar 2", "foo \x01 bar"} {
		buffer := bytes.NewReader([]byte("Hello World"))
		request, err = newTestSignedRequest(http.MethodPut, getPutObjectURL(s.endPoint, bucketName, objectName),
			int64(buffer.Len()), buffer, s.accessKey, s.secretKey, s.signer)
		c.Assert(err, nil)

		response, err = s.client.Do(request)
		c.Assert(err, nil)
		c.Assert(response.StatusCode, http.StatusOK)
	}

	testCases := []struct {
		getURL          string
		expectedStrings []string
	}{
		{getListObjectsV1URL(s.endPoint, bucketName, "", "1000", ""), []string{"<Key>foo bar 1</Key>", "<Key>foo bar 2</Key>", "<Key>foo &#x1; bar</Key>"}},
		{getListObjectsV1URL(s.endPoint, bucketName, "", "1000", "url"), []string{"<Key>foo+bar+1</Key>", "<Key>foo+bar+2</Key>", "<Key>foo+%01+bar</Key>"}},
		{
			getListObjectsV2URL(s.endPoint, bucketName, "", "1000", "", "", ""),
			[]string{
				"<Key>foo bar 1</Key>",
				"<Key>foo bar 2</Key>",
				"<Key>foo &#x1; bar</Key>",
				fmt.Sprintf("<Owner><ID>%s</ID><DisplayName>minio</DisplayName></Owner>", globalMinioDefaultOwnerID),
			},
		},
		{
			getListObjectsV2URL(s.endPoint, bucketName, "", "1000", "true", "", ""),
			[]string{
				"<Key>foo bar 1</Key>",
				"<Key>foo bar 2</Key>",
				"<Key>foo &#x1; bar</Key>",
				fmt.Sprintf("<Owner><ID>%s</ID><DisplayName>minio</DisplayName></Owner>", globalMinioDefaultOwnerID),
			},
		},
		{getListObjectsV2URL(s.endPoint, bucketName, "", "1000", "", "url", ""), []string{"<Key>foo+bar+1</Key>", "<Key>foo+bar+2</Key>", "<Key>foo+%01+bar</Key>"}},
	}

	for _, testCase := range testCases {
		// create listObjectsV1 request with valid parameters
		request, err = newTestSignedRequest(http.MethodGet, testCase.getURL, 0, nil, s.accessKey, s.secretKey, s.signer)
		c.Assert(err, nil)
		// execute the HTTP request.
		response, err = s.client.Do(request)
		c.Assert(err, nil)
		c.Assert(response.StatusCode, http.StatusOK)

		getContent, err := io.ReadAll(response.Body)
		c.Assert(err, nil)

		for _, expectedStr := range testCase.expectedStrings {
			c.Assert(strings.Contains(string(getContent), expectedStr), true)
		}
	}
}

// TestListObjectsHandlerErrors - Setting invalid parameters to List Objects
// and then asserting the error response with the expected one.
func (s *TestSuiteCommon) TestListObjectsHandlerErrors(c *check) {
	// generate a random bucket name.
	bucketName := getRandomBucketName()
	// HTTP request to create the bucket.
	request, err := newTestSignedRequest(http.MethodPut, getMakeBucketURL(s.endPoint, bucketName),
		0, nil, s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)

	// execute the HTTP request to create bucket.
	response, err := s.client.Do(request)
	c.Assert(err, nil)
	c.Assert(response.StatusCode, http.StatusOK)

	// create listObjectsV1 request with invalid value of max-keys parameter. max-keys is set to -2.
	request, err = newTestSignedRequest(http.MethodGet, getListObjectsV1URL(s.endPoint, bucketName, "", "-2", ""),
		0, nil, s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)
	// execute the HTTP request.
	response, err = s.client.Do(request)
	c.Assert(err, nil)
	// validating the error response.
	verifyError(c, response, "InvalidArgument", "Argument maxKeys must be an integer between 0 and 2147483647", http.StatusBadRequest)

	// create listObjectsV2 request with invalid value of max-keys parameter. max-keys is set to -2.
	request, err = newTestSignedRequest(http.MethodGet, getListObjectsV2URL(s.endPoint, bucketName, "", "-2", "", "", ""),
		0, nil, s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)
	// execute the HTTP request.
	response, err = s.client.Do(request)
	c.Assert(err, nil)
	// validating the error response.
	verifyError(c, response, "InvalidArgument", "Argument maxKeys must be an integer between 0 and 2147483647", http.StatusBadRequest)
}

// TestPutBucketErrors - request for non valid bucket operation
// and validate it with expected error result.
func (s *TestSuiteCommon) TestPutBucketErrors(c *check) {
	// generate a random bucket name.
	bucketName := getRandomBucketName()
	// generating a HTTP request to create bucket.
	// using invalid bucket name.
	request, err := newTestSignedRequest(http.MethodPut, s.endPoint+"/putbucket-.",
		0, nil, s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)

	response, err := s.client.Do(request)
	c.Assert(err, nil)
	// expected to fail with error message "InvalidBucketName".
	verifyError(c, response, "InvalidBucketName", "The specified bucket is not valid.", http.StatusBadRequest)
	// HTTP request to create the bucket.
	request, err = newTestSignedRequest(http.MethodPut, getMakeBucketURL(s.endPoint, bucketName),
		0, nil, s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)

	// execute the HTTP request to create bucket.
	response, err = s.client.Do(request)
	c.Assert(err, nil)
	c.Assert(response.StatusCode, http.StatusOK)
	// make HTTP request to create the same bucket again.
	// expected to fail with error message "BucketAlreadyOwnedByYou".
	request, err = newTestSignedRequest(http.MethodPut, getMakeBucketURL(s.endPoint, bucketName),
		0, nil, s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)

	response, err = s.client.Do(request)
	c.Assert(err, nil)
	verifyError(c, response, "BucketAlreadyOwnedByYou", "Your previous request to create the named bucket succeeded and you already own it.",
		http.StatusConflict)
}

func (s *TestSuiteCommon) TestGetObjectLarge10MiB(c *check) {
	// generate a random bucket name.
	bucketName := getRandomBucketName()
	// form HTTP request to create the bucket.
	request, err := newTestSignedRequest(http.MethodPut, getMakeBucketURL(s.endPoint, bucketName),
		0, nil, s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)

	// execute the HTTP request to create the bucket.
	response, err := s.client.Do(request)
	c.Assert(err, nil)
	c.Assert(response.StatusCode, http.StatusOK)

	var buffer bytes.Buffer
	line := `1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,
	1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,
	1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,
	1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,
	1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,
	1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,
	1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,
	1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,
	1234567890,1234567890,1234567890,1234567890,1234567890,123"`
	// Create 10MiB content where each line contains 1024 characters.
	for i := range 10 * 1024 {
		buffer.WriteString(fmt.Sprintf("[%05d] %s\n", i, line))
	}
	putContent := buffer.String()

	buf := bytes.NewReader([]byte(putContent))

	objectName := "test-big-object"
	// create HTTP request for object upload.
	request, err = newTestSignedRequest(http.MethodPut, getPutObjectURL(s.endPoint, bucketName, objectName),
		int64(buf.Len()), buf, s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)

	// execute the HTTP request.
	response, err = s.client.Do(request)
	c.Assert(err, nil)
	// Assert the status code to verify successful upload.
	c.Assert(response.StatusCode, http.StatusOK)

	// prepare HTTP requests to download the object.
	request, err = newTestSignedRequest(http.MethodGet, getPutObjectURL(s.endPoint, bucketName, objectName),
		0, nil, s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)

	// execute the HTTP request to download the object.
	response, err = s.client.Do(request)
	c.Assert(err, nil)
	c.Assert(response.StatusCode, http.StatusOK)
	// extract the content from response body.
	getContent, err := io.ReadAll(response.Body)
	c.Assert(err, nil)

	// Compare putContent and getContent.
	c.Assert(string(getContent), putContent)
}

// TestGetObjectLarge11MiB - Tests validate fetching of an object of size 11MB.
func (s *TestSuiteCommon) TestGetObjectLarge11MiB(c *check) {
	// generate a random bucket name.
	bucketName := getRandomBucketName()
	// HTTP request to create the bucket.
	request, err := newTestSignedRequest(http.MethodPut, getMakeBucketURL(s.endPoint, bucketName),
		0, nil, s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)

	// execute the HTTP request.
	response, err := s.client.Do(request)
	c.Assert(err, nil)
	c.Assert(response.StatusCode, http.StatusOK)

	var buffer bytes.Buffer
	line := `1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,
	1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,
	1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,
	1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,
	1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,
	1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,
	1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,
	1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,
	1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,
	1234567890,1234567890,1234567890,123`
	// Create 11MiB content where each line contains 1024 characters.
	for i := range 11 * 1024 {
		buffer.WriteString(fmt.Sprintf("[%05d] %s\n", i, line))
	}
	putMD5 := getMD5Hash(buffer.Bytes())

	objectName := "test-11Mb-object"
	// Put object
	buf := bytes.NewReader(buffer.Bytes())
	// create HTTP request foe object upload.
	request, err = newTestSignedRequest(http.MethodPut, getPutObjectURL(s.endPoint, bucketName, objectName),
		int64(buf.Len()), buf, s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)

	// execute the HTTP request for object upload.
	response, err = s.client.Do(request)
	c.Assert(err, nil)
	c.Assert(response.StatusCode, http.StatusOK)

	// create HTTP request to download the object.
	request, err = newTestSignedRequest(http.MethodGet, getGetObjectURL(s.endPoint, bucketName, objectName),
		0, nil, s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)

	// execute the HTTP request.
	response, err = s.client.Do(request)
	c.Assert(err, nil)
	c.Assert(response.StatusCode, http.StatusOK)
	// fetch the content from response body.
	getContent, err := io.ReadAll(response.Body)
	c.Assert(err, nil)

	// Get etag of the response content.
	getMD5 := getMD5Hash(getContent)

	// Compare putContent and getContent.
	c.Assert(putMD5, getMD5)
}

// TestGetPartialObjectMisAligned - tests get object partially miss-aligned.
// create a large buffer of miss-aligned data and upload it.
// then make partial range requests to while fetching it back and assert the response content.
func (s *TestSuiteCommon) TestGetPartialObjectMisAligned(c *check) {
	// generate a random bucket name.
	bucketName := getRandomBucketName()
	// HTTP request to create the bucket.
	request, err := newTestSignedRequest(http.MethodPut, getMakeBucketURL(s.endPoint, bucketName),
		0, nil, s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)

	// execute the HTTP request to create the bucket.
	response, err := s.client.Do(request)
	c.Assert(err, nil)
	c.Assert(response.StatusCode, http.StatusOK)

	var buffer bytes.Buffer
	// data to be written into buffer.
	data := "1234567890"
	// seed the random number generator once.
	rand.Seed(3)
	// generate a random number between 13 and 200.
	randInt := getRandomRange(13, 200, -1)
	// write into buffer till length of the buffer is greater than the generated random number.
	for i := 0; i <= randInt; i += 10 {
		buffer.WriteString(data)
	}
	// String content which is used for put object range test.
	putBytes := buffer.Bytes()
	putBytes = putBytes[:randInt]
	// randomize the order of bytes in the byte array and create a reader.
	putBytes = randomizeBytes(putBytes, -1)
	buf := bytes.NewReader(putBytes)
	putContent := string(putBytes)
	objectName := "test-big-file"
	// HTTP request to upload the object.
	request, err = newTestSignedRequest(http.MethodPut, getPutObjectURL(s.endPoint, bucketName, objectName),
		int64(buf.Len()), buf, s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)

	// execute the HTTP request to upload the object.
	response, err = s.client.Do(request)
	c.Assert(err, nil)
	c.Assert(response.StatusCode, http.StatusOK)

	// test Cases containing data to make partial range requests.
	// also has expected response data.
	testCases := []struct {
		byteRange      string
		expectedString string
	}{
		// request for byte range 10-11.
		// expecting the result to contain only putContent[10:12] bytes.
		{"10-11", putContent[10:12]},
		// request for object data after the first byte.
		{"1-", putContent[1:]},
		// request for object data after the first byte.
		{"6-", putContent[6:]},
		// request for last 2 bytes of the object.
		{"-2", putContent[len(putContent)-2:]},
		// request for last 7 bytes of the object.
		{"-7", putContent[len(putContent)-7:]},
	}

	for _, t := range testCases {
		// HTTP request to download the object.
		request, err = newTestSignedRequest(http.MethodGet, getGetObjectURL(s.endPoint, bucketName, objectName),
			0, nil, s.accessKey, s.secretKey, s.signer)
		c.Assert(err, nil)
		// Get partial content based on the byte range set.
		request.Header.Set("Range", "bytes="+t.byteRange)

		// execute the HTTP request.
		response, err = s.client.Do(request)
		c.Assert(err, nil)
		// Since only part of the object is requested, expecting response status to be http.StatusPartialContent .
		c.Assert(response.StatusCode, http.StatusPartialContent)
		// parse the HTTP response body.
		getContent, err := io.ReadAll(response.Body)
		c.Assert(err, nil)

		// Compare putContent and getContent.
		c.Assert(string(getContent), t.expectedString)
	}
}

// TestGetPartialObjectLarge11MiB - Test validates partial content request for a 11MiB object.
func (s *TestSuiteCommon) TestGetPartialObjectLarge11MiB(c *check) {
	// generate a random bucket name.
	bucketName := getRandomBucketName()
	// HTTP request to create the bucket.
	request, err := newTestSignedRequest(http.MethodPut, getMakeBucketURL(s.endPoint, bucketName),
		0, nil, s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)

	// execute the HTTP request to create the bucket.
	response, err := s.client.Do(request)
	c.Assert(err, nil)
	c.Assert(response.StatusCode, http.StatusOK)

	var buffer bytes.Buffer
	line := `234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,
	1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,
	1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,
	1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,
	1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,
	1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,
	1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,
	1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,
	1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,
	1234567890,1234567890,1234567890,123`
	// Create 11MiB content where each line contains 1024
	// characters.
	for i := range 11 * 1024 {
		buffer.WriteString(fmt.Sprintf("[%05d] %s\n", i, line))
	}
	putContent := buffer.String()

	objectName := "test-large-11Mb-object"

	buf := bytes.NewReader([]byte(putContent))
	// HTTP request to upload the object.
	request, err = newTestSignedRequest(http.MethodPut, getPutObjectURL(s.endPoint, bucketName, objectName),
		int64(buf.Len()), buf, s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)

	// execute the HTTP request to upload the object.
	response, err = s.client.Do(request)
	c.Assert(err, nil)
	c.Assert(response.StatusCode, http.StatusOK)

	// HTTP request to download the object.
	request, err = newTestSignedRequest(http.MethodGet, getGetObjectURL(s.endPoint, bucketName, objectName),
		0, nil, s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)
	// This range spans into first two blocks.
	request.Header.Set("Range", "bytes=10485750-10485769")

	// execute the HTTP request.
	response, err = s.client.Do(request)
	c.Assert(err, nil)
	// Since only part of the object is requested, expecting response status to be http.StatusPartialContent .
	c.Assert(response.StatusCode, http.StatusPartialContent)
	// read the downloaded content from the response body.
	getContent, err := io.ReadAll(response.Body)
	c.Assert(err, nil)

	// Compare putContent and getContent.
	c.Assert(string(getContent), putContent[10485750:10485770])
}

// TestGetPartialObjectLarge11MiB - Test validates partial content request for a 10MiB object.
func (s *TestSuiteCommon) TestGetPartialObjectLarge10MiB(c *check) {
	// generate a random bucket name.
	bucketName := getRandomBucketName()
	// HTTP request to create the bucket.
	request, err := newTestSignedRequest(http.MethodPut, getMakeBucketURL(s.endPoint, bucketName),
		0, nil, s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)

	// execute the HTTP request to create bucket.
	response, err := s.client.Do(request)
	// expecting the error to be nil.
	c.Assert(err, nil)
	// expecting the HTTP response status code to 200 OK.
	c.Assert(response.StatusCode, http.StatusOK)

	var buffer bytes.Buffer
	line := `1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,
	1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,
	1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,
	1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,
	1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,
	1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,
	1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,
	1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,
	1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,1234567890,
	1234567890,1234567890,1234567890,123`
	// Create 10MiB content where each line contains 1024 characters.
	for i := range 10 * 1024 {
		buffer.WriteString(fmt.Sprintf("[%05d] %s\n", i, line))
	}

	putContent := buffer.String()
	buf := bytes.NewReader([]byte(putContent))

	objectName := "test-big-10Mb-file"
	// HTTP request to upload the object.
	request, err = newTestSignedRequest(http.MethodPut, getPutObjectURL(s.endPoint, bucketName, objectName),
		int64(buf.Len()), buf, s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)

	// execute the HTTP request to upload the object.
	response, err = s.client.Do(request)
	c.Assert(err, nil)
	// verify whether upload was successful.
	c.Assert(response.StatusCode, http.StatusOK)

	// HTTP request to download the object.
	request, err = newTestSignedRequest(http.MethodGet, getGetObjectURL(s.endPoint, bucketName, objectName),
		0, nil, s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)
	// Get partial content based on the byte range set.
	request.Header.Set("Range", "bytes=2048-2058")

	// execute the HTTP request to download the partial content.
	response, err = s.client.Do(request)
	c.Assert(err, nil)
	// Since only part of the object is requested, expecting response status to be http.StatusPartialContent .
	c.Assert(response.StatusCode, http.StatusPartialContent)
	// read the downloaded content from the response body.
	getContent, err := io.ReadAll(response.Body)
	c.Assert(err, nil)

	// Compare putContent and getContent.
	c.Assert(string(getContent), putContent[2048:2059])
}

// TestGetObjectErrors - Tests validate error response for invalid object operations.
func (s *TestSuiteCommon) TestGetObjectErrors(c *check) {
	// generate a random bucket name.
	bucketName := getRandomBucketName()

	// HTTP request to create the bucket.
	request, err := newTestSignedRequest(http.MethodPut, getMakeBucketURL(s.endPoint, bucketName),
		0, nil, s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)

	// execute the HTTP request to create bucket.
	response, err := s.client.Do(request)
	c.Assert(err, nil)
	c.Assert(response.StatusCode, http.StatusOK)

	objectName := "test-non-exitent-object"
	// HTTP request to download the object.
	// Since the specified object doesn't exist in the given bucket,
	// expected to fail with error message "NoSuchKey"
	request, err = newTestSignedRequest(http.MethodGet, getGetObjectURL(s.endPoint, bucketName, objectName),
		0, nil, s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)

	response, err = s.client.Do(request)
	c.Assert(err, nil)
	verifyError(c, response, "NoSuchKey", "The specified key does not exist.", http.StatusNotFound)

	// request to download an object, but an invalid bucket name is set.
	request, err = newTestSignedRequest(http.MethodGet, getGetObjectURL(s.endPoint, "getobjecterrors-.", objectName),
		0, nil, s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)

	// execute the HTTP request.
	response, err = s.client.Do(request)
	c.Assert(err, nil)
	// expected to fail with "InvalidBucketName".
	verifyError(c, response, "InvalidBucketName", "The specified bucket is not valid.", http.StatusBadRequest)
}

// TestGetObjectRangeErrors - Validate error response when object is fetched with incorrect byte range value.
func (s *TestSuiteCommon) TestGetObjectRangeErrors(c *check) {
	// generate a random bucket name.
	bucketName := getRandomBucketName()
	// HTTP request to create the bucket.
	request, err := newTestSignedRequest(http.MethodPut, getMakeBucketURL(s.endPoint, bucketName),
		0, nil, s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)

	// execute the HTTP request to create bucket.
	response, err := s.client.Do(request)
	c.Assert(err, nil)
	c.Assert(response.StatusCode, http.StatusOK)

	// content for the object to be uploaded.
	buffer1 := bytes.NewReader([]byte("Hello World"))

	objectName := "test-object"
	// HTTP request to upload the object.
	request, err = newTestSignedRequest(http.MethodPut, getPutObjectURL(s.endPoint, bucketName, objectName),
		int64(buffer1.Len()), buffer1, s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)

	// execute the HTTP request to upload the object.
	response, err = s.client.Do(request)
	c.Assert(err, nil)
	// verify whether upload was successful.
	c.Assert(response.StatusCode, http.StatusOK)

	// HTTP request to download the object.
	request, err = newTestSignedRequest(http.MethodGet, getGetObjectURL(s.endPoint, bucketName, objectName),
		0, nil, s.accessKey, s.secretKey, s.signer)
	// Invalid byte range set.
	request.Header.Set("Range", "bytes=-0")
	c.Assert(err, nil)

	// execute the HTTP request.
	response, err = s.client.Do(request)
	c.Assert(err, nil)
	// expected to fail with "InvalidRange" error message.
	verifyError(c, response, "InvalidRange", "The requested range is not satisfiable", http.StatusRequestedRangeNotSatisfiable)
}

// TestObjectMultipartAbort - Test validates abortion of a multipart upload after uploading 2 parts.
func (s *TestSuiteCommon) TestObjectMultipartAbort(c *check) {
	// generate a random bucket name.
	bucketName := getRandomBucketName()
	// HTTP request to create the bucket.
	request, err := newTestSignedRequest(http.MethodPut, getMakeBucketURL(s.endPoint, bucketName),
		0, nil, s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)

	// execute the HTTP request to create bucket.
	response, err := s.client.Do(request)
	c.Assert(err, nil)
	c.Assert(response.StatusCode, http.StatusOK)

	objectName := "test-multipart-object"

	// 1. Initiate 2 uploads for the same object
	// 2. Upload 2 parts for the second upload
	// 3. Abort the second upload.
	// 4. Abort the first upload.
	// This will test abort upload when there are more than one upload IDs
	// and the case where there is only one upload ID.

	// construct HTTP request to initiate a NewMultipart upload.
	request, err = newTestSignedRequest(http.MethodPost, getNewMultipartURL(s.endPoint, bucketName, objectName),
		0, nil, s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)

	// execute the HTTP request initiating the new multipart upload.
	response, err = s.client.Do(request)
	c.Assert(err, nil)
	c.Assert(response.StatusCode, http.StatusOK)

	// parse the response body and obtain the new upload ID.
	decoder := xml.NewDecoder(response.Body)
	newResponse := &InitiateMultipartUploadResponse{}

	err = decoder.Decode(newResponse)
	c.Assert(err, nil)
	c.Assert(len(newResponse.UploadID) > 0, true)

	// construct HTTP request to initiate a NewMultipart upload.
	request, err = newTestSignedRequest(http.MethodPost, getNewMultipartURL(s.endPoint, bucketName, objectName),
		0, nil, s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)

	// execute the HTTP request initiating the new multipart upload.
	response, err = s.client.Do(request)
	c.Assert(err, nil)
	c.Assert(response.StatusCode, http.StatusOK)

	// parse the response body and obtain the new upload ID.
	decoder = xml.NewDecoder(response.Body)
	newResponse = &InitiateMultipartUploadResponse{}

	err = decoder.Decode(newResponse)
	c.Assert(err, nil)
	c.Assert(len(newResponse.UploadID) > 0, true)
	// uploadID to be used for rest of the multipart operations on the object.
	uploadID := newResponse.UploadID

	// content for the part to be uploaded.
	buffer1 := bytes.NewReader([]byte("hello world"))
	// HTTP request for the part to be uploaded.
	request, err = newTestSignedRequest(http.MethodPut, getPartUploadURL(s.endPoint, bucketName, objectName, uploadID, "1"),
		int64(buffer1.Len()), buffer1, s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)
	// execute the HTTP request to upload the first part.
	response1, err := s.client.Do(request)
	c.Assert(err, nil)
	c.Assert(response1.StatusCode, http.StatusOK)

	// content for the second part to be uploaded.
	buffer2 := bytes.NewReader([]byte("hello world"))
	// HTTP request for the second part to be uploaded.
	request, err = newTestSignedRequest(http.MethodPut, getPartUploadURL(s.endPoint, bucketName, objectName, uploadID, "2"),
		int64(buffer2.Len()), buffer2, s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)
	// execute the HTTP request to upload the second part.
	response2, err := s.client.Do(request)
	c.Assert(err, nil)
	c.Assert(response2.StatusCode, http.StatusOK)
	// HTTP request for aborting the multipart upload.
	request, err = newTestSignedRequest(http.MethodDelete, getAbortMultipartUploadURL(s.endPoint, bucketName, objectName, uploadID),
		0, nil, s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)
	// execute the HTTP request to abort the multipart upload.
	response3, err := s.client.Do(request)
	c.Assert(err, nil)
	// expecting the response status code to be http.StatusNoContent.
	// The assertion validates the success of Abort Multipart operation.
	c.Assert(response3.StatusCode, http.StatusNoContent)
}

// TestBucketMultipartList - Initiates a NewMultipart upload, uploads parts and validates listing of the parts.
func (s *TestSuiteCommon) TestBucketMultipartList(c *check) {
	// generate a random bucket name.
	bucketName := getRandomBucketName()
	// HTTP request to create the bucket.
	request, err := newTestSignedRequest(http.MethodPut, getMakeBucketURL(s.endPoint, bucketName), 0,
		nil, s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)

	// execute the HTTP request to create bucket.
	response, err := s.client.Do(request)
	c.Assert(err, nil)
	c.Assert(response.StatusCode, 200)

	objectName := "test-multipart-object"
	// construct HTTP request to initiate a NewMultipart upload.
	request, err = newTestSignedRequest(http.MethodPost, getNewMultipartURL(s.endPoint, bucketName, objectName),
		0, nil, s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)
	// execute the HTTP request initiating the new multipart upload.
	response, err = s.client.Do(request)
	c.Assert(err, nil)
	// expecting the response status code to be http.StatusOK(200 OK) .
	c.Assert(response.StatusCode, http.StatusOK)

	// parse the response body and obtain the new upload ID.
	decoder := xml.NewDecoder(response.Body)
	newResponse := &InitiateMultipartUploadResponse{}

	err = decoder.Decode(newResponse)
	c.Assert(err, nil)
	c.Assert(len(newResponse.UploadID) > 0, true)
	// uploadID to be used for rest of the multipart operations on the object.
	uploadID := newResponse.UploadID

	// content for the part to be uploaded.
	buffer1 := bytes.NewReader([]byte("hello world"))
	// HTTP request for the part to be uploaded.
	request, err = newTestSignedRequest(http.MethodPut, getPartUploadURL(s.endPoint, bucketName, objectName, uploadID, "1"),
		int64(buffer1.Len()), buffer1, s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)
	// execute the HTTP request to upload the first part.
	response1, err := s.client.Do(request)
	c.Assert(err, nil)
	c.Assert(response1.StatusCode, http.StatusOK)

	// content for the second part to be uploaded.
	buffer2 := bytes.NewReader([]byte("hello world"))
	// HTTP request for the second part to be uploaded.
	request, err = newTestSignedRequest(http.MethodPut, getPartUploadURL(s.endPoint, bucketName, objectName, uploadID, "2"),
		int64(buffer2.Len()), buffer2, s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)
	// execute the HTTP request to upload the second part.
	response2, err := s.client.Do(request)
	c.Assert(err, nil)
	c.Assert(response2.StatusCode, http.StatusOK)

	// HTTP request to ListMultipart Uploads.
	request, err = newTestSignedRequest(http.MethodGet, getListMultipartURL(s.endPoint, bucketName),
		0, nil, s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)
	// execute the HTTP request.
	response3, err := s.client.Do(request)
	c.Assert(err, nil)
	c.Assert(response3.StatusCode, http.StatusOK)

	// The reason to duplicate this structure here is to verify if the
	// unmarshalling works from a client perspective, specifically
	// while unmarshalling time.Time type for 'Initiated' field.
	// time.Time does not honor xml marshaller, it means that we need
	// to encode/format it before giving it to xml marshaling.

	// This below check adds client side verification to see if its
	// truly parsable.

	// listMultipartUploadsResponse - format for list multipart uploads response.
	type listMultipartUploadsResponse struct {
		XMLName xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ ListMultipartUploadsResult" json:"-"`

		Bucket             string
		KeyMarker          string
		UploadIDMarker     string `xml:"UploadIdMarker"`
		NextKeyMarker      string
		NextUploadIDMarker string `xml:"NextUploadIdMarker"`
		EncodingType       string
		MaxUploads         int
		IsTruncated        bool
		// All the in progress multipart uploads.
		Uploads []struct {
			Key          string
			UploadID     string `xml:"UploadId"`
			Initiator    Initiator
			Owner        Owner
			StorageClass string
			Initiated    time.Time // Keep this native to be able to parse properly.
		}
		Prefix         string
		Delimiter      string
		CommonPrefixes []CommonPrefix
	}

	// parse the response body.
	decoder = xml.NewDecoder(response3.Body)
	newResponse3 := &listMultipartUploadsResponse{}
	err = decoder.Decode(newResponse3)
	c.Assert(err, nil)
	// Assert the bucket name in the response with the expected bucketName.
	c.Assert(newResponse3.Bucket, bucketName)
	// Assert the bucket name in the response with the expected bucketName.
	c.Assert(newResponse3.IsTruncated, false)
}

// TestValidateObjectMultipartUploadID - Test Initiates a new multipart upload and validates the uploadID.
func (s *TestSuiteCommon) TestValidateObjectMultipartUploadID(c *check) {
	// generate a random bucket name.
	bucketName := getRandomBucketName()
	// HTTP request to create the bucket.
	request, err := newTestSignedRequest(http.MethodPut, getMakeBucketURL(s.endPoint, bucketName),
		0, nil, s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)

	// execute the HTTP request to create bucket.
	response, err := s.client.Do(request)
	c.Assert(err, nil)
	c.Assert(response.StatusCode, 200)

	objectName := "directory1/directory2/object"
	// construct HTTP request to initiate a NewMultipart upload.
	request, err = newTestSignedRequest(http.MethodPost, getNewMultipartURL(s.endPoint, bucketName, objectName),
		0, nil, s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)
	// execute the HTTP request initiating the new multipart upload.
	response, err = s.client.Do(request)
	c.Assert(err, nil)
	c.Assert(response.StatusCode, http.StatusOK)

	// parse the response body and obtain the new upload ID.
	decoder := xml.NewDecoder(response.Body)
	newResponse := &InitiateMultipartUploadResponse{}
	err = decoder.Decode(newResponse)
	// expecting the decoding error to be nil.
	c.Assert(err, nil)
	// Verifying for Upload ID value to be greater than 0.
	c.Assert(len(newResponse.UploadID) > 0, true)
}

// TestObjectMultipartListError - Initiates a NewMultipart upload, uploads parts and validates
// error response for an incorrect max-parts parameter .
func (s *TestSuiteCommon) TestObjectMultipartListError(c *check) {
	// generate a random bucket name.
	bucketName := getRandomBucketName()
	// HTTP request to create the bucket.
	request, err := newTestSignedRequest(http.MethodPut, getMakeBucketURL(s.endPoint, bucketName),
		0, nil, s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)

	// execute the HTTP request to create bucket.
	response, err := s.client.Do(request)
	c.Assert(err, nil)
	c.Assert(response.StatusCode, 200)

	objectName := "test-multipart-object"
	// construct HTTP request to initiate a NewMultipart upload.
	request, err = newTestSignedRequest(http.MethodPost, getNewMultipartURL(s.endPoint, bucketName, objectName),
		0, nil, s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)
	// execute the HTTP request initiating the new multipart upload.
	response, err = s.client.Do(request)
	c.Assert(err, nil)
	c.Assert(response.StatusCode, http.StatusOK)
	// parse the response body and obtain the new upload ID.
	decoder := xml.NewDecoder(response.Body)
	newResponse := &InitiateMultipartUploadResponse{}

	err = decoder.Decode(newResponse)
	c.Assert(err, nil)
	c.Assert(len(newResponse.UploadID) > 0, true)
	// uploadID to be used for rest of the multipart operations on the object.
	uploadID := newResponse.UploadID

	// content for the part to be uploaded.
	buffer1 := bytes.NewReader([]byte("hello world"))
	// HTTP request for the part to be uploaded.
	request, err = newTestSignedRequest(http.MethodPut, getPartUploadURL(s.endPoint, bucketName, objectName, uploadID, "1"),
		int64(buffer1.Len()), buffer1, s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)
	// execute the HTTP request to upload the first part.
	response1, err := s.client.Do(request)
	c.Assert(err, nil)
	c.Assert(response1.StatusCode, http.StatusOK)

	// content for the second part to be uploaded.
	buffer2 := bytes.NewReader([]byte("hello world"))
	// HTTP request for the second part to be uploaded.
	request, err = newTestSignedRequest(http.MethodPut, getPartUploadURL(s.endPoint, bucketName, objectName, uploadID, "2"),
		int64(buffer2.Len()), buffer2, s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)

	// execute the HTTP request to upload the second part.
	response2, err := s.client.Do(request)
	c.Assert(err, nil)
	c.Assert(response2.StatusCode, http.StatusOK)

	// HTTP request to ListMultipart Uploads.
	// max-keys is set to valid value of 1
	request, err = newTestSignedRequest(http.MethodGet, getListMultipartURLWithParams(s.endPoint, bucketName, objectName, uploadID, "1", "", ""),
		0, nil, s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)
	// execute the HTTP request.
	response3, err := s.client.Do(request)
	c.Assert(err, nil)
	c.Assert(response3.StatusCode, http.StatusOK)

	// HTTP request to ListMultipart Uploads.
	// max-keys is set to invalid value of -2.
	request, err = newTestSignedRequest(http.MethodGet, getListMultipartURLWithParams(s.endPoint, bucketName, objectName, uploadID, "-2", "", ""),
		0, nil, s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)
	// execute the HTTP request.
	response4, err := s.client.Do(request)
	c.Assert(err, nil)
	// Since max-keys parameter in the ListMultipart request set to invalid value of -2,
	// its expected to fail with error message "InvalidArgument".
	verifyError(c, response4, "InvalidArgument", "Part number must be an integer between 1 and 10000, inclusive", http.StatusBadRequest)
}

// TestObjectValidMD5 - First uploads an object with a valid Content-Md5 header and verifies the status,
// then upload an object in a wrong Content-Md5 and validate the error response.
func (s *TestSuiteCommon) TestObjectValidMD5(c *check) {
	// generate a random bucket name.
	bucketName := getRandomBucketName()
	// HTTP request to create the bucket.
	request, err := newTestSignedRequest(http.MethodPut, getMakeBucketURL(s.endPoint, bucketName),
		0, nil, s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)

	// execute the HTTP request to create bucket.
	response, err := s.client.Do(request)
	c.Assert(err, nil)
	c.Assert(response.StatusCode, 200)

	// Create a byte array of 5MB.
	// content for the object to be uploaded.
	data := bytes.Repeat([]byte("0123456789abcdef"), 5*humanize.MiByte/16)
	// calculate etag of the data.
	etagBase64 := getMD5HashBase64(data)

	buffer1 := bytes.NewReader(data)
	objectName := "test-1-object"
	// HTTP request for the object to be uploaded.
	request, err = newTestSignedRequest(http.MethodPut, getPutObjectURL(s.endPoint, bucketName, objectName),
		int64(buffer1.Len()), buffer1, s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)
	// set the Content-Md5 to be the hash to content.
	request.Header.Set("Content-Md5", etagBase64)
	response, err = s.client.Do(request)
	c.Assert(err, nil)
	// expecting a successful upload.
	c.Assert(response.StatusCode, http.StatusOK)
	objectName = "test-2-object"
	buffer1 = bytes.NewReader(data)
	// HTTP request for the object to be uploaded.
	request, err = newTestSignedRequest(http.MethodPut, getPutObjectURL(s.endPoint, bucketName, objectName),
		int64(buffer1.Len()), buffer1, s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)
	// set Content-Md5 to invalid value.
	request.Header.Set("Content-Md5", "kvLTlMrX9NpYDQlEIFlnDA==")
	// expecting a failure during upload.
	response, err = s.client.Do(request)
	c.Assert(err, nil)
	// Since Content-Md5 header was wrong, expecting to fail with "SignatureDoesNotMatch" error.
	verifyError(c, response, "SignatureDoesNotMatch", "The request signature we calculated does not match the signature you provided. Check your key and signing method.", http.StatusForbidden)
}

// TestObjectMultipart - Initiates a NewMultipart upload, uploads 2 parts,
// completes the multipart upload and validates the status of the operation.
func (s *TestSuiteCommon) TestObjectMultipart(c *check) {
	// generate a random bucket name.
	bucketName := getRandomBucketName()
	// HTTP request to create the bucket.
	request, err := newTestSignedRequest(http.MethodPut, getMakeBucketURL(s.endPoint, bucketName),
		0, nil, s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)

	// execute the HTTP request to create bucket.
	response, err := s.client.Do(request)
	c.Assert(err, nil)
	c.Assert(response.StatusCode, 200)

	objectName := "test-multipart-object"
	// construct HTTP request to initiate a NewMultipart upload.
	request, err = newTestSignedRequest(http.MethodPost, getNewMultipartURL(s.endPoint, bucketName, objectName),
		0, nil, s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)

	// execute the HTTP request initiating the new multipart upload.
	response, err = s.client.Do(request)
	c.Assert(err, nil)
	// expecting the response status code to be http.StatusOK(200 OK).
	c.Assert(response.StatusCode, http.StatusOK)
	// parse the response body and obtain the new upload ID.
	decoder := xml.NewDecoder(response.Body)
	newResponse := &InitiateMultipartUploadResponse{}

	err = decoder.Decode(newResponse)
	c.Assert(err, nil)
	c.Assert(len(newResponse.UploadID) > 0, true)
	// uploadID to be used for rest of the multipart operations on the object.
	uploadID := newResponse.UploadID

	// content for the part to be uploaded.
	// Create a byte array of 5MB.
	data := bytes.Repeat([]byte("0123456789abcdef"), 5*humanize.MiByte/16)
	// calculate etag of the data.
	md5SumBase64 := getMD5HashBase64(data)

	buffer1 := bytes.NewReader(data)
	// HTTP request for the part to be uploaded.
	request, err = newTestSignedRequest(http.MethodPut, getPartUploadURL(s.endPoint, bucketName, objectName, uploadID, "1"),
		int64(buffer1.Len()), buffer1, s.accessKey, s.secretKey, s.signer)
	// set the Content-Md5 header to the base64 encoding the etag of the content.
	request.Header.Set("Content-Md5", md5SumBase64)
	c.Assert(err, nil)

	// execute the HTTP request to upload the first part.
	response1, err := s.client.Do(request)
	c.Assert(err, nil)
	c.Assert(response1.StatusCode, http.StatusOK)

	// content for the second part to be uploaded.
	// Create a byte array of 1 byte.
	data = []byte("0")

	// calculate etag of the data.
	md5SumBase64 = getMD5HashBase64(data)

	buffer2 := bytes.NewReader(data)
	// HTTP request for the second part to be uploaded.
	request, err = newTestSignedRequest(http.MethodPut, getPartUploadURL(s.endPoint, bucketName, objectName, uploadID, "2"),
		int64(buffer2.Len()), buffer2, s.accessKey, s.secretKey, s.signer)
	// set the Content-Md5 header to the base64 encoding the etag of the content.
	request.Header.Set("Content-Md5", md5SumBase64)
	c.Assert(err, nil)

	// execute the HTTP request to upload the second part.
	response2, err := s.client.Do(request)
	c.Assert(err, nil)
	c.Assert(response2.StatusCode, http.StatusOK)

	// Complete multipart upload
	completeUploads := &CompleteMultipartUpload{
		Parts: []CompletePart{
			{
				PartNumber: 1,
				ETag:       response1.Header.Get("ETag"),
			},
			{
				PartNumber: 2,
				ETag:       response2.Header.Get("ETag"),
			},
		},
	}

	completeBytes, err := xml.Marshal(completeUploads)
	c.Assert(err, nil)
	// Indicating that all parts are uploaded and initiating CompleteMultipartUpload.
	request, err = newTestSignedRequest(http.MethodPost, getCompleteMultipartUploadURL(s.endPoint, bucketName, objectName, uploadID),
		int64(len(completeBytes)), bytes.NewReader(completeBytes), s.accessKey, s.secretKey, s.signer)
	c.Assert(err, nil)
	// Execute the complete multipart request.
	response, err = s.client.Do(request)
	c.Assert(err, nil)
	// verify whether complete multipart was successful.
	c.Assert(response.StatusCode, http.StatusOK)
	var parts []CompletePart
	for _, part := range completeUploads.Parts {
		part.ETag = canonicalizeETag(part.ETag)
		parts = append(parts, part)
	}
	etag := getCompleteMultipartMD5(parts)
	c.Assert(canonicalizeETag(response.Header.Get(xhttp.ETag)), etag)
}

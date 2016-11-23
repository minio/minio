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
	"encoding/base64"
	"fmt"
	"io/ioutil"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	humanize "github.com/dustin/go-humanize"
)

const (
	expirationDateFormat = "2006-01-02T15:04:05.999Z"
	iso8601DateFormat    = "20060102T150405Z"
)

func newPostPolicyBytesV4WithContentRange(credential, bucketName, objectKey string, expiration time.Time) []byte {
	t := time.Now().UTC()
	// Add the expiration date.
	expirationStr := fmt.Sprintf(`"expiration": "%s"`, expiration.Format(expirationDateFormat))
	// Add the bucket condition, only accept buckets equal to the one passed.
	bucketConditionStr := fmt.Sprintf(`["eq", "$bucket", "%s"]`, bucketName)
	// Add the key condition, only accept keys equal to the one passed.
	keyConditionStr := fmt.Sprintf(`["eq", "$key", "%s"]`, objectKey)
	// Add content length condition, only accept content sizes of a given length.
	contentLengthCondStr := `["content-length-range", 1024, 1048576]`
	// Add the algorithm condition, only accept AWS SignV4 Sha256.
	algorithmConditionStr := `["eq", "$x-amz-algorithm", "AWS4-HMAC-SHA256"]`
	// Add the date condition, only accept the current date.
	dateConditionStr := fmt.Sprintf(`["eq", "$x-amz-date", "%s"]`, t.Format(iso8601DateFormat))
	// Add the credential string, only accept the credential passed.
	credentialConditionStr := fmt.Sprintf(`["eq", "$x-amz-credential", "%s"]`, credential)

	// Combine all conditions into one string.
	conditionStr := fmt.Sprintf(`"conditions":[%s, %s, %s, %s, %s, %s]`, bucketConditionStr,
		keyConditionStr, contentLengthCondStr, algorithmConditionStr, dateConditionStr, credentialConditionStr)
	retStr := "{"
	retStr = retStr + expirationStr + ","
	retStr = retStr + conditionStr
	retStr = retStr + "}"

	return []byte(retStr)
}

// newPostPolicyBytesV4 - creates a bare bones postpolicy string with key and bucket matches.
func newPostPolicyBytesV4(credential, bucketName, objectKey string, expiration time.Time) []byte {
	t := time.Now().UTC()
	// Add the expiration date.
	expirationStr := fmt.Sprintf(`"expiration": "%s"`, expiration.Format(expirationDateFormat))
	// Add the bucket condition, only accept buckets equal to the one passed.
	bucketConditionStr := fmt.Sprintf(`["eq", "$bucket", "%s"]`, bucketName)
	// Add the key condition, only accept keys equal to the one passed.
	keyConditionStr := fmt.Sprintf(`["eq", "$key", "%s"]`, objectKey)
	// Add the algorithm condition, only accept AWS SignV4 Sha256.
	algorithmConditionStr := `["eq", "$x-amz-algorithm", "AWS4-HMAC-SHA256"]`
	// Add the date condition, only accept the current date.
	dateConditionStr := fmt.Sprintf(`["eq", "$x-amz-date", "%s"]`, t.Format(iso8601DateFormat))
	// Add the credential string, only accept the credential passed.
	credentialConditionStr := fmt.Sprintf(`["eq", "$x-amz-credential", "%s"]`, credential)

	// Combine all conditions into one string.
	conditionStr := fmt.Sprintf(`"conditions":[%s, %s, %s, %s, %s]`, bucketConditionStr, keyConditionStr, algorithmConditionStr, dateConditionStr, credentialConditionStr)
	retStr := "{"
	retStr = retStr + expirationStr + ","
	retStr = retStr + conditionStr
	retStr = retStr + "}"

	return []byte(retStr)
}

// newPostPolicyBytesV2 - creates a bare bones postpolicy string with key and bucket matches.
func newPostPolicyBytesV2(bucketName, objectKey string, expiration time.Time) []byte {
	// Add the expiration date.
	expirationStr := fmt.Sprintf(`"expiration": "%s"`, expiration.Format(expirationDateFormat))
	// Add the bucket condition, only accept buckets equal to the one passed.
	bucketConditionStr := fmt.Sprintf(`["eq", "$bucket", "%s"]`, bucketName)
	// Add the key condition, only accept keys equal to the one passed.
	keyConditionStr := fmt.Sprintf(`["eq", "$key", "%s"]`, objectKey)

	// Combine all conditions into one string.
	conditionStr := fmt.Sprintf(`"conditions":[%s, %s]`, bucketConditionStr, keyConditionStr)
	retStr := "{"
	retStr = retStr + expirationStr + ","
	retStr = retStr + conditionStr
	retStr = retStr + "}"

	return []byte(retStr)
}

// Wrapper for calling TestPostPolicyHandlerHandler tests for both XL multiple disks and single node setup.
func TestPostPolicyHandler(t *testing.T) {
	ExecObjectLayerTest(t, testPostPolicyHandler)
}

// testPostPolicyHandler - Tests validate post policy handler uploading objects.
func testPostPolicyHandler(obj ObjectLayer, instanceType string, t TestErrHandler) {
	root, err := newTestConfig("us-east-1")
	if err != nil {
		t.Fatalf("Initializing config.json failed")
	}
	defer removeAll(root)

	// Register event notifier.
	err = initEventNotifier(obj)
	if err != nil {
		t.Fatalf("Initializing event notifiers failed")
	}

	// get random bucket name.
	bucketName := getRandomBucketName()

	// Register the API end points with XL/FS object layer.
	apiRouter := initTestAPIEndPoints(obj, []string{"PostPolicy"})

	// initialize the server and obtain the credentials and root.
	// credentials are necessary to sign the HTTP request.
	rootPath, err := newTestConfig("us-east-1")
	if err != nil {
		t.Fatalf("Init Test config failed")
	}
	// remove the root directory after the test ends.
	defer removeAll(rootPath)

	credentials := serverConfig.GetCredential()

	// bucketnames[0].
	// objectNames[0].
	// uploadIds [0].
	// Create bucket before initiating NewMultipartUpload.
	err = obj.MakeBucket(bucketName)
	if err != nil {
		// Failed to create newbucket, abort.
		t.Fatalf("%s : %s", instanceType, err.Error())
	}

	// Test cases for signature-V2.
	testCasesV2 := []struct {
		expectedStatus int
		accessKey      string
		secretKey      string
	}{
		{http.StatusForbidden, "invalidaccesskey", credentials.SecretAccessKey},
		{http.StatusForbidden, credentials.AccessKeyID, "invalidsecretkey"},
		{http.StatusNoContent, credentials.AccessKeyID, credentials.SecretAccessKey},
	}

	for i, test := range testCasesV2 {
		// initialize HTTP NewRecorder, this records any mutations to response writer inside the handler.
		rec := httptest.NewRecorder()
		req, perr := newPostRequestV2("", bucketName, "testobject", test.accessKey, test.secretKey)
		if perr != nil {
			t.Fatalf("Test %d: %s: Failed to create HTTP request for PostPolicyHandler: <ERROR> %v", i+1, instanceType, perr)
		}
		// Since `apiRouter` satisfies `http.Handler` it has a ServeHTTP to execute the logic ofthe handler.
		// Call the ServeHTTP to execute the handler.
		apiRouter.ServeHTTP(rec, req)
		if rec.Code != test.expectedStatus {
			t.Fatalf("Test %d: %s: Expected the response status to be `%d`, but instead found `%d`", i+1, instanceType, test.expectedStatus, rec.Code)
		}
	}

	// Test cases for signature-V4.
	testCasesV4 := []struct {
		objectName         string
		data               []byte
		expectedRespStatus int
		accessKey          string
		secretKey          string
		malformedBody      bool
	}{
		// Success case.
		{
			objectName:         "test",
			data:               []byte("Hello, World"),
			expectedRespStatus: http.StatusNoContent,
			accessKey:          credentials.AccessKeyID,
			secretKey:          credentials.SecretAccessKey,
			malformedBody:      false,
		},
		// Bad case invalid request.
		{
			objectName:         "test",
			data:               []byte("Hello, World"),
			expectedRespStatus: http.StatusBadRequest,
			accessKey:          "",
			secretKey:          "",
			malformedBody:      false,
		},
		// Bad case malformed input.
		{
			objectName:         "test",
			data:               []byte("Hello, World"),
			expectedRespStatus: http.StatusBadRequest,
			accessKey:          credentials.AccessKeyID,
			secretKey:          credentials.SecretAccessKey,
			malformedBody:      true,
		},
	}

	for i, testCase := range testCasesV4 {
		// initialize HTTP NewRecorder, this records any mutations to response writer inside the handler.
		rec := httptest.NewRecorder()
		req, perr := newPostRequestV4("", bucketName, testCase.objectName, testCase.data, testCase.accessKey, testCase.secretKey)
		if perr != nil {
			t.Fatalf("Test %d: %s: Failed to create HTTP request for PostPolicyHandler: <ERROR> %v", i+1, instanceType, perr)
		}
		if testCase.malformedBody {
			// Change the request body.
			req.Body = ioutil.NopCloser(bytes.NewReader([]byte("Hello,")))
		}
		// Since `apiRouter` satisfies `http.Handler` it has a ServeHTTP to execute the logic ofthe handler.
		// Call the ServeHTTP to execute the handler.
		apiRouter.ServeHTTP(rec, req)
		if rec.Code != testCase.expectedRespStatus {
			t.Errorf("Test %d: %s: Expected the response status to be `%d`, but instead found `%d`", i+1, instanceType, testCase.expectedRespStatus, rec.Code)
		}
	}

	testCases2 := []struct {
		objectName         string
		data               []byte
		expectedRespStatus int
		accessKey          string
		secretKey          string
		malformedBody      bool
	}{
		// Success case.
		{
			objectName:         "test",
			data:               bytes.Repeat([]byte("a"), 1025),
			expectedRespStatus: http.StatusNoContent,
			accessKey:          credentials.AccessKeyID,
			secretKey:          credentials.SecretAccessKey,
			malformedBody:      false,
		},
		// Failed with entity too small.
		{
			objectName:         "test",
			data:               bytes.Repeat([]byte("a"), 1023),
			expectedRespStatus: http.StatusBadRequest,
			accessKey:          credentials.AccessKeyID,
			secretKey:          credentials.SecretAccessKey,
			malformedBody:      false,
		},
		// Failed with entity too large.
		{
			objectName:         "test",
			data:               bytes.Repeat([]byte("a"), (1*humanize.MiByte)+1),
			expectedRespStatus: http.StatusBadRequest,
			accessKey:          credentials.AccessKeyID,
			secretKey:          credentials.SecretAccessKey,
			malformedBody:      false,
		},
	}

	for i, testCase := range testCases2 {
		// initialize HTTP NewRecorder, this records any mutations to response writer inside the handler.
		rec := httptest.NewRecorder()
		req, perr := newPostRequestV4WithContentLength("", bucketName, testCase.objectName, testCase.data, testCase.accessKey, testCase.secretKey)
		if perr != nil {
			t.Fatalf("Test %d: %s: Failed to create HTTP request for PostPolicyHandler: <ERROR> %v", i+1, instanceType, perr)
		}
		// Since `apiRouter` satisfies `http.Handler` it has a ServeHTTP to execute the logic ofthe handler.
		// Call the ServeHTTP to execute the handler.
		apiRouter.ServeHTTP(rec, req)
		if rec.Code != testCase.expectedRespStatus {
			t.Errorf("Test %d: %s: Expected the response status to be `%d`, but instead found `%d`", i+1, instanceType, testCase.expectedRespStatus, rec.Code)
		}
	}

}

// postPresignSignatureV4 - presigned signature for PostPolicy requests.
func postPresignSignatureV4(policyBase64 string, t time.Time, secretAccessKey, location string) string {
	// Get signining key.
	signingkey := getSigningKey(secretAccessKey, t, location)
	// Calculate signature.
	signature := getSignature(signingkey, policyBase64)
	return signature
}

func newPostRequestV2(endPoint, bucketName, objectName string, accessKey, secretKey string) (*http.Request, error) {
	// Expire the request five minutes from now.
	expirationTime := time.Now().UTC().Add(time.Minute * 5)
	// Create a new post policy.
	policy := newPostPolicyBytesV2(bucketName, objectName, expirationTime)
	// Only need the encoding.
	encodedPolicy := base64.StdEncoding.EncodeToString(policy)

	// Presign with V4 signature based on the policy.
	signature := calculateSignatureV2(encodedPolicy, secretKey)

	formData := map[string]string{
		"AWSAccessKeyId": accessKey,
		"bucket":         bucketName,
		"key":            objectName,
		"policy":         encodedPolicy,
		"signature":      signature,
	}

	// Create the multipart form.
	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)

	// Set the normal formData
	for k, v := range formData {
		w.WriteField(k, v)
	}
	// Set the File formData
	writer, err := w.CreateFormFile("file", "s3verify/post/object")
	if err != nil {
		// return nil, err
		return nil, err
	}
	writer.Write([]byte("hello world"))
	// Close before creating the new request.
	w.Close()

	// Set the body equal to the created policy.
	reader := bytes.NewReader(buf.Bytes())

	req, err := http.NewRequest("POST", makeTestTargetURL(endPoint, bucketName, objectName, nil), reader)
	if err != nil {
		return nil, err
	}

	// Set form content-type.
	req.Header.Set("Content-Type", w.FormDataContentType())
	return req, nil
}

func newPostRequestV4Generic(endPoint, bucketName, objectName string, objData []byte, accessKey, secretKey string, contentLengthRange bool) (*http.Request, error) {
	// Keep time.
	t := time.Now().UTC()
	// Expire the request five minutes from now.
	expirationTime := t.Add(time.Minute * 5)
	// Get the user credential.
	credStr := getCredential(accessKey, serverConfig.GetRegion(), t)
	// Create a new post policy.
	policy := newPostPolicyBytesV4(credStr, bucketName, objectName, expirationTime)
	if contentLengthRange {
		policy = newPostPolicyBytesV4WithContentRange(credStr, bucketName, objectName, expirationTime)
	}
	// Only need the encoding.
	encodedPolicy := base64.StdEncoding.EncodeToString(policy)

	// Presign with V4 signature based on the policy.
	signature := postPresignSignatureV4(encodedPolicy, t, secretKey, serverConfig.GetRegion())

	formData := map[string]string{
		"bucket":           bucketName,
		"key":              objectName,
		"x-amz-credential": credStr,
		"policy":           encodedPolicy,
		"x-amz-signature":  signature,
		"x-amz-date":       t.Format(iso8601DateFormat),
		"x-amz-algorithm":  "AWS4-HMAC-SHA256",
	}

	// Create the multipart form.
	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)

	// Set the normal formData
	for k, v := range formData {
		w.WriteField(k, v)
	}
	// Set the File formData
	writer, err := w.CreateFormFile("file", "s3verify/post/object")
	if err != nil {
		// return nil, err
		return nil, err
	}
	writer.Write(objData)
	// Close before creating the new request.
	w.Close()

	// Set the body equal to the created policy.
	reader := bytes.NewReader(buf.Bytes())

	req, err := http.NewRequest("POST", makeTestTargetURL(endPoint, bucketName, objectName, nil), reader)
	if err != nil {
		return nil, err
	}

	// Set form content-type.
	req.Header.Set("Content-Type", w.FormDataContentType())
	return req, nil
}

func newPostRequestV4WithContentLength(endPoint, bucketName, objectName string, objData []byte, accessKey, secretKey string) (*http.Request, error) {
	return newPostRequestV4Generic(endPoint, bucketName, objectName, objData, accessKey, secretKey, true)
}

func newPostRequestV4(endPoint, bucketName, objectName string, objData []byte, accessKey, secretKey string) (*http.Request, error) {
	return newPostRequestV4Generic(endPoint, bucketName, objectName, objData, accessKey, secretKey, false)
}

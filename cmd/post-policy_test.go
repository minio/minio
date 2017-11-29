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
	"bytes"
	"encoding/base64"
	"fmt"
	"io/ioutil"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"testing"
	"time"

	humanize "github.com/dustin/go-humanize"
)

const (
	expirationDateFormat = "2006-01-02T15:04:05.999Z"
	iso8601DateFormat    = "20060102T150405Z"
)

func newPostPolicyBytesV4WithContentRange(credential, bucketName, objectKey string, expiration time.Time) []byte {
	t := UTCNow()
	// Add the expiration date.
	expirationStr := fmt.Sprintf(`"expiration": "%s"`, expiration.Format(expirationDateFormat))
	// Add the bucket condition, only accept buckets equal to the one passed.
	bucketConditionStr := fmt.Sprintf(`["eq", "$bucket", "%s"]`, bucketName)
	// Add the key condition, only accept keys equal to the one passed.
	keyConditionStr := fmt.Sprintf(`["eq", "$key", "%s/upload.txt"]`, objectKey)
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
	t := UTCNow()
	// Add the expiration date.
	expirationStr := fmt.Sprintf(`"expiration": "%s"`, expiration.Format(expirationDateFormat))
	// Add the bucket condition, only accept buckets equal to the one passed.
	bucketConditionStr := fmt.Sprintf(`["eq", "$bucket", "%s"]`, bucketName)
	// Add the key condition, only accept keys equal to the one passed.
	keyConditionStr := fmt.Sprintf(`["eq", "$key", "%s/upload.txt"]`, objectKey)
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
	keyConditionStr := fmt.Sprintf(`["starts-with", "$key", "%s/upload.txt"]`, objectKey)

	// Combine all conditions into one string.
	conditionStr := fmt.Sprintf(`"conditions":[%s, %s]`, bucketConditionStr, keyConditionStr)
	retStr := "{"
	retStr = retStr + expirationStr + ","
	retStr = retStr + conditionStr
	retStr = retStr + "}"

	return []byte(retStr)
}

// Wrapper for calling TestPostPolicyBucketHandler tests for both XL multiple disks and single node setup.
func TestPostPolicyBucketHandler(t *testing.T) {
	ExecObjectLayerTest(t, testPostPolicyBucketHandler)
}

// testPostPolicyBucketHandler - Tests validate post policy handler uploading objects.
func testPostPolicyBucketHandler(obj ObjectLayer, instanceType string, t TestErrHandler) {
	root, err := newTestConfig(globalMinioDefaultRegion)
	if err != nil {
		t.Fatalf("Initializing config.json failed")
	}
	defer os.RemoveAll(root)

	// Register event notifier.
	err = initEventNotifier(obj)
	if err != nil {
		t.Fatalf("Initializing event notifiers failed")
	}

	// get random bucket name.
	bucketName := getRandomBucketName()

	// Register the API end points with XL/FS object layer.
	apiRouter := initTestAPIEndPoints(obj, []string{"PostPolicy"})

	credentials := globalServerConfig.GetCredential()

	curTime := UTCNow()
	curTimePlus5Min := curTime.Add(time.Minute * 5)

	// bucketnames[0].
	// objectNames[0].
	// uploadIds [0].
	// Create bucket before initiating NewMultipartUpload.
	err = obj.MakeBucketWithLocation(bucketName, "")
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
		{http.StatusForbidden, "invalidaccesskey", credentials.SecretKey},
		{http.StatusForbidden, credentials.AccessKey, "invalidsecretkey"},
		{http.StatusNoContent, credentials.AccessKey, credentials.SecretKey},
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
		expectedHeaders    map[string]string
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
			expectedHeaders:    map[string]string{"X-Amz-Meta-Uuid": "1234"},
			accessKey:          credentials.AccessKey,
			secretKey:          credentials.SecretKey,
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
			accessKey:          credentials.AccessKey,
			secretKey:          credentials.SecretKey,
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
		// When the operation is successful, check if sending metadata is successful too
		if rec.Code == http.StatusNoContent {
			objInfo, err := obj.GetObjectInfo(bucketName, testCase.objectName+"/upload.txt")
			if err != nil {
				t.Error("Unexpected error: ", err)
			}
			for k, v := range testCase.expectedHeaders {
				if objInfo.UserDefined[k] != v {
					t.Errorf("Expected to have header %s with value %s, but found value `%s` instead", k, v, objInfo.UserDefined[k])
				}
			}
		}
	}

	region := "us-east-1"
	// Test cases for signature-V4.
	testCasesV4BadData := []struct {
		objectName         string
		data               []byte
		expectedRespStatus int
		accessKey          string
		secretKey          string
		dates              []interface{}
		policy             string
		corruptedBase64    bool
		corruptedMultipart bool
	}{
		// Success case.
		{
			objectName:         "test",
			data:               []byte("Hello, World"),
			expectedRespStatus: http.StatusNoContent,
			accessKey:          credentials.AccessKey,
			secretKey:          credentials.SecretKey,
			dates:              []interface{}{curTimePlus5Min.Format(expirationDateFormat), curTime.Format(iso8601DateFormat), curTime.Format(yyyymmdd)},
			policy:             `{"expiration": "%s","conditions":[["eq", "$bucket", "` + bucketName + `"], ["starts-with", "$key", "test/"], ["eq", "$x-amz-algorithm", "AWS4-HMAC-SHA256"], ["eq", "$x-amz-date", "%s"], ["eq", "$x-amz-credential", "` + credentials.AccessKey + `/%s/us-east-1/s3/aws4_request"]]}`,
		},
		// Corrupted Base 64 result
		{
			objectName:         "test",
			data:               []byte("Hello, World"),
			expectedRespStatus: http.StatusBadRequest,
			accessKey:          credentials.AccessKey,
			secretKey:          credentials.SecretKey,
			dates:              []interface{}{curTimePlus5Min.Format(expirationDateFormat), curTime.Format(iso8601DateFormat), curTime.Format(yyyymmdd)},
			policy:             `{"expiration": "%s","conditions":[["eq", "$bucket", "` + bucketName + `"], ["starts-with", "$key", "test/"], ["eq", "$x-amz-algorithm", "AWS4-HMAC-SHA256"], ["eq", "$x-amz-date", "%s"], ["eq", "$x-amz-credential", "` + credentials.AccessKey + `/%s/us-east-1/s3/aws4_request"]]}`,
			corruptedBase64:    true,
		},
		// Corrupted Multipart body
		{
			objectName:         "test",
			data:               []byte("Hello, World"),
			expectedRespStatus: http.StatusBadRequest,
			accessKey:          credentials.AccessKey,
			secretKey:          credentials.SecretKey,
			dates:              []interface{}{curTimePlus5Min.Format(expirationDateFormat), curTime.Format(iso8601DateFormat), curTime.Format(yyyymmdd)},
			policy:             `{"expiration": "%s","conditions":[["eq", "$bucket", "` + bucketName + `"], ["starts-with", "$key", "test/"], ["eq", "$x-amz-algorithm", "AWS4-HMAC-SHA256"], ["eq", "$x-amz-date", "%s"], ["eq", "$x-amz-credential", "` + credentials.AccessKey + `/%s/us-east-1/s3/aws4_request"]]}`,
			corruptedMultipart: true,
		},

		// Bad case invalid request.
		{
			objectName:         "test",
			data:               []byte("Hello, World"),
			expectedRespStatus: http.StatusBadRequest,
			accessKey:          "",
			secretKey:          "",
			dates:              []interface{}{},
			policy:             ``,
		},
		// Expired document
		{
			objectName:         "test",
			data:               []byte("Hello, World"),
			expectedRespStatus: http.StatusBadRequest,
			accessKey:          credentials.AccessKey,
			secretKey:          credentials.SecretKey,
			dates:              []interface{}{curTime.Add(-1 * time.Minute * 5).Format(expirationDateFormat), curTime.Format(iso8601DateFormat), curTime.Format(yyyymmdd)},
			policy:             `{"expiration": "%s","conditions":[["eq", "$bucket", "` + bucketName + `"], ["starts-with", "$key", "test/"], ["eq", "$x-amz-algorithm", "AWS4-HMAC-SHA256"], ["eq", "$x-amz-date", "%s"], ["eq", "$x-amz-credential", "` + credentials.AccessKey + `/%s/us-east-1/s3/aws4_request"]]}`,
		},
		// Corrupted policy document
		{
			objectName:         "test",
			data:               []byte("Hello, World"),
			expectedRespStatus: http.StatusBadRequest,
			accessKey:          credentials.AccessKey,
			secretKey:          credentials.SecretKey,
			dates:              []interface{}{curTimePlus5Min.Format(expirationDateFormat), curTime.Format(iso8601DateFormat), curTime.Format(yyyymmdd)},
			policy:             `{"3/aws4_request"]]}`,
		},
	}

	for i, testCase := range testCasesV4BadData {
		// initialize HTTP NewRecorder, this records any mutations to response writer inside the handler.
		rec := httptest.NewRecorder()

		testCase.policy = fmt.Sprintf(testCase.policy, testCase.dates...)

		req, perr := newPostRequestV4Generic("", bucketName, testCase.objectName, testCase.data, testCase.accessKey,
			testCase.secretKey, region, curTime, []byte(testCase.policy), nil, testCase.corruptedBase64, testCase.corruptedMultipart)
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

	testCases2 := []struct {
		objectName          string
		data                []byte
		expectedRespStatus  int
		accessKey           string
		secretKey           string
		malformedBody       bool
		ignoreContentLength bool
	}{
		// Success case.
		{
			objectName:          "test",
			data:                bytes.Repeat([]byte("a"), 1025),
			expectedRespStatus:  http.StatusNoContent,
			accessKey:           credentials.AccessKey,
			secretKey:           credentials.SecretKey,
			malformedBody:       false,
			ignoreContentLength: false,
		},
		// Failed with Content-Length not specified.
		{
			objectName:          "test",
			data:                bytes.Repeat([]byte("a"), 1025),
			expectedRespStatus:  http.StatusNoContent,
			accessKey:           credentials.AccessKey,
			secretKey:           credentials.SecretKey,
			malformedBody:       false,
			ignoreContentLength: true,
		},
		// Failed with entity too small.
		{
			objectName:          "test",
			data:                bytes.Repeat([]byte("a"), 1023),
			expectedRespStatus:  http.StatusBadRequest,
			accessKey:           credentials.AccessKey,
			secretKey:           credentials.SecretKey,
			malformedBody:       false,
			ignoreContentLength: false,
		},
		// Failed with entity too large.
		{
			objectName:          "test",
			data:                bytes.Repeat([]byte("a"), (1*humanize.MiByte)+1),
			expectedRespStatus:  http.StatusBadRequest,
			accessKey:           credentials.AccessKey,
			secretKey:           credentials.SecretKey,
			malformedBody:       false,
			ignoreContentLength: false,
		},
	}

	for i, testCase := range testCases2 {
		// initialize HTTP NewRecorder, this records any mutations to response writer inside the handler.
		rec := httptest.NewRecorder()
		var req *http.Request
		var perr error
		if testCase.ignoreContentLength {
			req, perr = newPostRequestV4("", bucketName, testCase.objectName, testCase.data, testCase.accessKey, testCase.secretKey)
		} else {
			req, perr = newPostRequestV4WithContentLength("", bucketName, testCase.objectName, testCase.data, testCase.accessKey, testCase.secretKey)
		}
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

// Wrapper for calling TestPostPolicyBucketHandlerRedirect tests for both XL multiple disks and single node setup.
func TestPostPolicyBucketHandlerRedirect(t *testing.T) {
	ExecObjectLayerTest(t, testPostPolicyBucketHandlerRedirect)
}

// testPostPolicyBucketHandlerRedirect tests POST Object when success_action_redirect is specified
func testPostPolicyBucketHandlerRedirect(obj ObjectLayer, instanceType string, t TestErrHandler) {
	root, err := newTestConfig(globalMinioDefaultRegion)
	if err != nil {
		t.Fatalf("Initializing config.json failed")
	}
	defer os.RemoveAll(root)

	// Register event notifier.
	err = initEventNotifier(obj)
	if err != nil {
		t.Fatalf("Initializing event notifiers failed")
	}

	// get random bucket name.
	bucketName := getRandomBucketName()

	// Key specified in Form data
	keyName := "test/object"

	// The final name of the upload object
	targetObj := keyName + "/upload.txt"

	// The url of success_action_redirect field
	redirectURL, err := url.Parse("http://www.google.com")
	if err != nil {
		t.Fatal(err)
	}

	// Register the API end points with XL/FS object layer.
	apiRouter := initTestAPIEndPoints(obj, []string{"PostPolicy"})

	credentials := globalServerConfig.GetCredential()

	curTime := UTCNow()
	curTimePlus5Min := curTime.Add(time.Minute * 5)

	err = obj.MakeBucketWithLocation(bucketName, "")
	if err != nil {
		// Failed to create newbucket, abort.
		t.Fatalf("%s : %s", instanceType, err.Error())
	}

	// initialize HTTP NewRecorder, this records any mutations to response writer inside the handler.
	rec := httptest.NewRecorder()

	dates := []interface{}{curTimePlus5Min.Format(expirationDateFormat), curTime.Format(iso8601DateFormat), curTime.Format(yyyymmdd)}
	policy := `{"expiration": "%s","conditions":[["eq", "$bucket", "` + bucketName + `"], {"success_action_redirect":"` + redirectURL.String() + `"},["starts-with", "$key", "test/"], ["eq", "$x-amz-algorithm", "AWS4-HMAC-SHA256"], ["eq", "$x-amz-date", "%s"], ["eq", "$x-amz-credential", "` + credentials.AccessKey + `/%s/us-east-1/s3/aws4_request"]]}`

	// Generate the final policy document
	policy = fmt.Sprintf(policy, dates...)

	region := "us-east-1"
	// Create a new POST request with success_action_redirect field specified
	req, perr := newPostRequestV4Generic("", bucketName, keyName, []byte("objData"),
		credentials.AccessKey, credentials.SecretKey, region, curTime,
		[]byte(policy), map[string]string{"success_action_redirect": redirectURL.String()}, false, false)

	if perr != nil {
		t.Fatalf("%s: Failed to create HTTP request for PostPolicyHandler: <ERROR> %v", instanceType, perr)
	}
	// Since `apiRouter` satisfies `http.Handler` it has a ServeHTTP to execute the logic ofthe handler.
	// Call the ServeHTTP to execute the handler.
	apiRouter.ServeHTTP(rec, req)

	// Check the status code, which must be 303 because success_action_redirect is specified
	if rec.Code != http.StatusSeeOther {
		t.Errorf("%s: Expected the response status to be `%d`, but instead found `%d`", instanceType, http.StatusSeeOther, rec.Code)
	}

	// Get the uploaded object info
	info, err := obj.GetObjectInfo(bucketName, targetObj)
	if err != nil {
		t.Error("Unexpected error: ", err)
	}

	redirectURL.RawQuery = getRedirectPostRawQuery(info)
	expectedLocation := redirectURL.String()

	// Check the new location url
	if rec.HeaderMap.Get("Location") != expectedLocation {
		t.Errorf("Unexpected location, expected = %s, found = `%s`", rec.HeaderMap.Get("Location"), expectedLocation)
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
	expirationTime := UTCNow().Add(time.Minute * 5)
	// Create a new post policy.
	policy := newPostPolicyBytesV2(bucketName, objectName, expirationTime)
	// Only need the encoding.
	encodedPolicy := base64.StdEncoding.EncodeToString(policy)

	// Presign with V4 signature based on the policy.
	signature := calculateSignatureV2(encodedPolicy, secretKey)

	formData := map[string]string{
		"AWSAccessKeyId": accessKey,
		"bucket":         bucketName,
		"key":            objectName + "/${filename}",
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
	writer, err := w.CreateFormFile("file", "upload.txt")
	if err != nil {
		// return nil, err
		return nil, err
	}
	writer.Write([]byte("hello world"))
	// Close before creating the new request.
	w.Close()

	// Set the body equal to the created policy.
	reader := bytes.NewReader(buf.Bytes())

	req, err := http.NewRequest("POST", makeTestTargetURL(endPoint, bucketName, "", nil), reader)
	if err != nil {
		return nil, err
	}

	// Set form content-type.
	req.Header.Set("Content-Type", w.FormDataContentType())
	return req, nil
}

func buildGenericPolicy(t time.Time, accessKey, region, bucketName, objectName string, contentLengthRange bool) []byte {
	// Expire the request five minutes from now.
	expirationTime := t.Add(time.Minute * 5)

	credStr := getCredentialString(accessKey, region, t)
	// Create a new post policy.
	policy := newPostPolicyBytesV4(credStr, bucketName, objectName, expirationTime)
	if contentLengthRange {
		policy = newPostPolicyBytesV4WithContentRange(credStr, bucketName, objectName, expirationTime)
	}
	return policy
}

func newPostRequestV4Generic(endPoint, bucketName, objectName string, objData []byte, accessKey, secretKey string, region string,
	t time.Time, policy []byte, addFormData map[string]string, corruptedB64 bool, corruptedMultipart bool) (*http.Request, error) {
	// Get the user credential.
	credStr := getCredentialString(accessKey, region, t)

	// Only need the encoding.
	encodedPolicy := base64.StdEncoding.EncodeToString(policy)

	if corruptedB64 {
		encodedPolicy = "%!~&" + encodedPolicy
	}

	// Presign with V4 signature based on the policy.
	signature := postPresignSignatureV4(encodedPolicy, t, secretKey, region)

	formData := map[string]string{
		"bucket":           bucketName,
		"key":              objectName + "/${filename}",
		"x-amz-credential": credStr,
		"policy":           encodedPolicy,
		"x-amz-signature":  signature,
		"x-amz-date":       t.Format(iso8601DateFormat),
		"x-amz-algorithm":  "AWS4-HMAC-SHA256",
		"x-amz-meta-uuid":  "1234",
		"Content-Encoding": "gzip",
	}

	// Add form data
	for k, v := range addFormData {
		formData[k] = v
	}

	// Create the multipart form.
	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)

	// Set the normal formData
	for k, v := range formData {
		w.WriteField(k, v)
	}
	// Set the File formData but don't if we want send an incomplete multipart request
	if !corruptedMultipart {
		writer, err := w.CreateFormFile("file", "upload.txt")
		if err != nil {
			// return nil, err
			return nil, err
		}
		writer.Write(objData)
		// Close before creating the new request.
		w.Close()
	}

	// Set the body equal to the created policy.
	reader := bytes.NewReader(buf.Bytes())

	req, err := http.NewRequest("POST", makeTestTargetURL(endPoint, bucketName, "", nil), reader)
	if err != nil {
		return nil, err
	}

	// Set form content-type.
	req.Header.Set("Content-Type", w.FormDataContentType())
	return req, nil
}

func newPostRequestV4WithContentLength(endPoint, bucketName, objectName string, objData []byte, accessKey, secretKey string) (*http.Request, error) {
	t := UTCNow()
	region := "us-east-1"
	policy := buildGenericPolicy(t, accessKey, region, bucketName, objectName, true)
	return newPostRequestV4Generic(endPoint, bucketName, objectName, objData, accessKey, secretKey, region, t, policy, nil, false, false)
}

func newPostRequestV4(endPoint, bucketName, objectName string, objData []byte, accessKey, secretKey string) (*http.Request, error) {
	t := UTCNow()
	region := "us-east-1"
	policy := buildGenericPolicy(t, accessKey, region, bucketName, objectName, false)
	return newPostRequestV4Generic(endPoint, bucketName, objectName, objData, accessKey, secretKey, region, t, policy, nil, false, false)
}

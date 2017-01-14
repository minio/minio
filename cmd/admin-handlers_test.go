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
	"encoding/json"
	"encoding/xml"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	router "github.com/gorilla/mux"
)

// cmdType - Represents different service subcomands like status, stop
// and restart.
type cmdType int

const (
	statusCmd cmdType = iota
	restartCmd
	setCreds
)

// String - String representation for cmdType
func (c cmdType) String() string {
	switch c {
	case statusCmd:
		return "status"
	case restartCmd:
		return "restart"
	case setCreds:
		return "set-credentials"
	}
	return ""
}

// apiMethod - Returns the HTTP method corresponding to the admin REST
// API for a given cmdType value.
func (c cmdType) apiMethod() string {
	switch c {
	case statusCmd:
		return "GET"
	case restartCmd:
		return "POST"
	case setCreds:
		return "POST"
	}
	return "GET"
}

// toServiceSignal - Helper function that translates a given cmdType
// value to its corresponding serviceSignal value.
func (c cmdType) toServiceSignal() serviceSignal {
	switch c {
	case statusCmd:
		return serviceStatus
	case restartCmd:
		return serviceRestart
	}
	return serviceStatus
}

// testServiceSignalReceiver - Helper function that simulates a
// go-routine waiting on service signal.
func testServiceSignalReceiver(cmd cmdType, t *testing.T) {
	expectedCmd := cmd.toServiceSignal()
	serviceCmd := <-globalServiceSignalCh
	if serviceCmd != expectedCmd {
		t.Errorf("Expected service command %v but received %v", expectedCmd, serviceCmd)
	}
}

// getServiceCmdRequest - Constructs a management REST API request for service
// subcommands for a given cmdType value.
func getServiceCmdRequest(cmd cmdType, cred credential, body []byte) (*http.Request, error) {
	req, err := newTestRequest(cmd.apiMethod(), "/?service", 0, nil)
	if err != nil {
		return nil, err
	}

	// Set body
	req.Body = ioutil.NopCloser(bytes.NewReader(body))

	// minioAdminOpHeader is to identify the request as a
	// management REST API request.
	req.Header.Set(minioAdminOpHeader, cmd.String())
	req.Header.Set("X-Amz-Content-Sha256", getSHA256Hash(body))

	// management REST API uses signature V4 for authentication.
	err = signRequestV4(req, cred.AccessKey, cred.SecretKey)
	if err != nil {
		return nil, err
	}
	return req, nil
}

// testServicesCmdHandler - parametrizes service subcommand tests on
// cmdType value.
func testServicesCmdHandler(cmd cmdType, args map[string]interface{}, t *testing.T) {
	// reset globals.
	// this is to make sure that the tests are not affected by modified value.
	resetTestGlobals()
	// initialize NSLock.
	initNSLock(false)
	// Initialize configuration for access/secret credentials.
	rootPath, err := newTestConfig("us-east-1")
	if err != nil {
		t.Fatalf("Unable to initialize server config. %s", err)
	}
	defer removeAll(rootPath)

	// Initialize admin peers to make admin RPC calls. Note: In a
	// single node setup, this degenerates to a simple function
	// call under the hood.
	eps, err := parseStorageEndpoints([]string{"http://localhost"})
	if err != nil {
		t.Fatalf("Failed to parse storage end point - %v", err)
	}

	// Set globalMinioAddr to be able to distinguish local endpoints from remote.
	globalMinioAddr = eps[0].Host
	initGlobalAdminPeers(eps)

	if cmd == statusCmd {
		// Initializing objectLayer and corresponding
		// []StorageAPI since DiskInfo() method requires it.
		objLayer, xlDirs, xlErr := prepareXL()
		if xlErr != nil {
			t.Fatalf("failed to initialize XL based object layer - %v.", xlErr)
		}
		defer removeRoots(xlDirs)
		// Make objLayer available to all internal services via globalObjectAPI.
		globalObjLayerMutex.Lock()
		globalObjectAPI = objLayer
		globalObjLayerMutex.Unlock()
	}

	// Setting up a go routine to simulate ServerMux's
	// handleServiceSignals for stop and restart commands.
	if cmd == restartCmd {
		go testServiceSignalReceiver(cmd, t)
	}
	credentials := serverConfig.GetCredential()
	adminRouter := router.NewRouter()
	registerAdminRouter(adminRouter)

	var body []byte

	if cmd == setCreds {
		body, _ = xml.Marshal(setCredsReq{Username: args["username"].(string), Password: args["password"].(string)})
	}

	req, err := getServiceCmdRequest(cmd, credentials, body)
	if err != nil {
		t.Fatalf("Failed to build service status request %v", err)
	}

	rec := httptest.NewRecorder()
	adminRouter.ServeHTTP(rec, req)

	if cmd == statusCmd {
		expectedInfo := newObjectLayerFn().StorageInfo()
		receivedInfo := StorageInfo{}
		if jsonErr := json.Unmarshal(rec.Body.Bytes(), &receivedInfo); jsonErr != nil {
			t.Errorf("Failed to unmarshal StorageInfo - %v", jsonErr)
		}
		if expectedInfo != receivedInfo {
			t.Errorf("Expected storage info and received storage info differ, %v %v", expectedInfo, receivedInfo)
		}
	}

	if cmd == setCreds {
		// Check if new credentials are set
		cred := serverConfig.GetCredential()
		if cred.AccessKey != args["username"].(string) {
			t.Errorf("Wrong access key, expected = %s, found = %s", args["username"].(string), cred.AccessKey)
		}
		if cred.SecretKey != args["password"].(string) {
			t.Errorf("Wrong secret key, expected = %s, found = %s", args["password"].(string), cred.SecretKey)
		}

	}

	if rec.Code != http.StatusOK {
		resp, _ := ioutil.ReadAll(rec.Body)
		t.Errorf("Expected to receive %d status code but received %d. Body (%s)",
			http.StatusOK, rec.Code, string(resp))
	}
}

// Test for service status management REST API.
func TestServiceStatusHandler(t *testing.T) {
	testServicesCmdHandler(statusCmd, nil, t)
}

// Test for service restart management REST API.
func TestServiceRestartHandler(t *testing.T) {
	testServicesCmdHandler(restartCmd, nil, t)
}

func TestServiceSetCreds(t *testing.T) {
	testServicesCmdHandler(setCreds, map[string]interface{}{"username": "minio", "password": "minio123"}, t)
}

// mkLockQueryVal - helper function to build lock query param.
func mkLockQueryVal(bucket, prefix, relTimeStr string) url.Values {
	qVal := url.Values{}
	qVal.Set("lock", "")
	qVal.Set(string(mgmtBucket), bucket)
	qVal.Set(string(mgmtPrefix), prefix)
	qVal.Set(string(mgmtOlderThan), relTimeStr)
	return qVal
}

// Test for locks list management REST API.
func TestListLocksHandler(t *testing.T) {
	// reset globals.
	// this is to make sure that the tests are not affected by modified globals.
	resetTestGlobals()
	// initialize NSLock.
	initNSLock(false)

	rootPath, err := newTestConfig("us-east-1")
	if err != nil {
		t.Fatalf("Unable to initialize server config. %s", err)
	}
	defer removeAll(rootPath)

	// Initialize admin peers to make admin RPC calls.
	eps, err := parseStorageEndpoints([]string{"http://localhost"})
	if err != nil {
		t.Fatalf("Failed to parse storage end point - %v", err)
	}

	// Set globalMinioAddr to be able to distinguish local endpoints from remote.
	globalMinioAddr = eps[0].Host
	initGlobalAdminPeers(eps)

	// Setup admin mgmt REST API handlers.
	adminRouter := router.NewRouter()
	registerAdminRouter(adminRouter)

	testCases := []struct {
		bucket         string
		prefix         string
		relTime        string
		expectedStatus int
	}{
		// Test 1 - valid testcase
		{
			bucket:         "mybucket",
			prefix:         "myobject",
			relTime:        "1s",
			expectedStatus: http.StatusOK,
		},
		// Test 2 - invalid duration
		{
			bucket:         "mybucket",
			prefix:         "myprefix",
			relTime:        "invalidDuration",
			expectedStatus: http.StatusBadRequest,
		},
		// Test 3 - invalid bucket name
		{
			bucket:         `invalid\\Bucket`,
			prefix:         "myprefix",
			relTime:        "1h",
			expectedStatus: http.StatusBadRequest,
		},
		// Test 4 - invalid prefix
		{
			bucket:         "mybucket",
			prefix:         `invalid\\Prefix`,
			relTime:        "1h",
			expectedStatus: http.StatusBadRequest,
		},
	}

	for i, test := range testCases {
		queryVal := mkLockQueryVal(test.bucket, test.prefix, test.relTime)
		req, err := newTestRequest("GET", "/?"+queryVal.Encode(), 0, nil)
		if err != nil {
			t.Fatalf("Test %d - Failed to construct list locks request - %v", i+1, err)
		}
		req.Header.Set(minioAdminOpHeader, "list")

		cred := serverConfig.GetCredential()
		err = signRequestV4(req, cred.AccessKey, cred.SecretKey)
		if err != nil {
			t.Fatalf("Test %d - Failed to sign list locks request - %v", i+1, err)
		}
		rec := httptest.NewRecorder()
		adminRouter.ServeHTTP(rec, req)
		if test.expectedStatus != rec.Code {
			t.Errorf("Test %d - Expected HTTP status code %d but received %d", i+1, test.expectedStatus, rec.Code)
		}
	}
}

// Test for locks clear management REST API.
func TestClearLocksHandler(t *testing.T) {
	// reset globals.
	// this is to make sure that the tests are not affected by modified globals.
	resetTestGlobals()
	// initialize NSLock.
	initNSLock(false)

	rootPath, err := newTestConfig("us-east-1")
	if err != nil {
		t.Fatalf("Unable to initialize server config. %s", err)
	}
	defer removeAll(rootPath)

	// Initialize admin peers to make admin RPC calls.
	eps, err := parseStorageEndpoints([]string{"http://localhost"})
	if err != nil {
		t.Fatalf("Failed to parse storage end point - %v", err)
	}
	initGlobalAdminPeers(eps)

	// Setup admin mgmt REST API handlers.
	adminRouter := router.NewRouter()
	registerAdminRouter(adminRouter)

	testCases := []struct {
		bucket         string
		prefix         string
		relTime        string
		expectedStatus int
	}{
		// Test 1 - valid testcase
		{
			bucket:         "mybucket",
			prefix:         "myobject",
			relTime:        "1s",
			expectedStatus: http.StatusOK,
		},
		// Test 2 - invalid duration
		{
			bucket:         "mybucket",
			prefix:         "myprefix",
			relTime:        "invalidDuration",
			expectedStatus: http.StatusBadRequest,
		},
		// Test 3 - invalid bucket name
		{
			bucket:         `invalid\\Bucket`,
			prefix:         "myprefix",
			relTime:        "1h",
			expectedStatus: http.StatusBadRequest,
		},
		// Test 4 - invalid prefix
		{
			bucket:         "mybucket",
			prefix:         `invalid\\Prefix`,
			relTime:        "1h",
			expectedStatus: http.StatusBadRequest,
		},
	}

	for i, test := range testCases {
		queryVal := mkLockQueryVal(test.bucket, test.prefix, test.relTime)
		req, err := newTestRequest("POST", "/?"+queryVal.Encode(), 0, nil)
		if err != nil {
			t.Fatalf("Test %d - Failed to construct clear locks request - %v", i+1, err)
		}
		req.Header.Set(minioAdminOpHeader, "clear")

		cred := serverConfig.GetCredential()
		err = signRequestV4(req, cred.AccessKey, cred.SecretKey)
		if err != nil {
			t.Fatalf("Test %d - Failed to sign clear locks request - %v", i+1, err)
		}
		rec := httptest.NewRecorder()
		adminRouter.ServeHTTP(rec, req)
		if test.expectedStatus != rec.Code {
			t.Errorf("Test %d - Expected HTTP status code %d but received %d", i+1, test.expectedStatus, rec.Code)
		}
	}
}

// Test for lock query param validation helper function.
func TestValidateLockQueryParams(t *testing.T) {
	// reset globals.
	// this is to make sure that the tests are not affected by modified globals.
	resetTestGlobals()
	// initialize NSLock.
	initNSLock(false)
	// Sample query values for test cases.
	allValidVal := mkLockQueryVal("bucket", "prefix", "1s")
	invalidBucketVal := mkLockQueryVal(`invalid\\Bucket`, "prefix", "1s")
	invalidPrefixVal := mkLockQueryVal("bucket", `invalid\\Prefix`, "1s")
	invalidOlderThanVal := mkLockQueryVal("bucket", "prefix", "invalidDuration")

	testCases := []struct {
		qVals  url.Values
		apiErr APIErrorCode
	}{
		{
			qVals:  invalidBucketVal,
			apiErr: ErrInvalidBucketName,
		},
		{
			qVals:  invalidPrefixVal,
			apiErr: ErrInvalidObjectName,
		},
		{
			qVals:  invalidOlderThanVal,
			apiErr: ErrInvalidDuration,
		},
		{
			qVals:  allValidVal,
			apiErr: ErrNone,
		},
	}

	for i, test := range testCases {
		_, _, _, apiErr := validateLockQueryParams(test.qVals)
		if apiErr != test.apiErr {
			t.Errorf("Test %d - Expected error %v but received %v", i+1, test.apiErr, apiErr)
		}
	}
}

// mkListObjectsQueryStr - helper to build ListObjectsHeal query string.
func mkListObjectsQueryVal(bucket, prefix, marker, delimiter, maxKeyStr string) url.Values {
	qVal := url.Values{}
	qVal.Set("heal", "")
	qVal.Set(string(mgmtBucket), bucket)
	qVal.Set(string(mgmtPrefix), prefix)
	qVal.Set(string(mgmtMarker), marker)
	qVal.Set(string(mgmtDelimiter), delimiter)
	qVal.Set(string(mgmtMaxKey), maxKeyStr)
	return qVal
}

// TestValidateHealQueryParams - Test for query param validation helper function for heal APIs.
func TestValidateHealQueryParams(t *testing.T) {
	testCases := []struct {
		bucket    string
		prefix    string
		marker    string
		delimiter string
		maxKeys   string
		apiErr    APIErrorCode
	}{
		// 1. Valid params.
		{
			bucket:    "mybucket",
			prefix:    "prefix",
			marker:    "prefix11",
			delimiter: "/",
			maxKeys:   "10",
			apiErr:    ErrNone,
		},
		// 2. Valid params with meta bucket.
		{
			bucket:    minioMetaBucket,
			prefix:    "prefix",
			marker:    "prefix11",
			delimiter: "/",
			maxKeys:   "10",
			apiErr:    ErrNone,
		},
		// 3. Valid params with empty prefix.
		{
			bucket:    "mybucket",
			prefix:    "",
			marker:    "",
			delimiter: "/",
			maxKeys:   "10",
			apiErr:    ErrNone,
		},
		// 4. Invalid params with invalid bucket.
		{
			bucket:    `invalid\\Bucket`,
			prefix:    "prefix",
			marker:    "prefix11",
			delimiter: "/",
			maxKeys:   "10",
			apiErr:    ErrInvalidBucketName,
		},
		// 5. Invalid params with invalid prefix.
		{
			bucket:    "mybucket",
			prefix:    `invalid\\Prefix`,
			marker:    "prefix11",
			delimiter: "/",
			maxKeys:   "10",
			apiErr:    ErrInvalidObjectName,
		},
		// 6. Invalid params with invalid maxKeys.
		{
			bucket:    "mybucket",
			prefix:    "prefix",
			marker:    "prefix11",
			delimiter: "/",
			maxKeys:   "-1",
			apiErr:    ErrInvalidMaxKeys,
		},
		// 7. Invalid params with unsupported prefix marker combination.
		{
			bucket:    "mybucket",
			prefix:    "prefix",
			marker:    "notmatchingmarker",
			delimiter: "/",
			maxKeys:   "10",
			apiErr:    ErrNotImplemented,
		},
		// 8. Invalid params with unsupported delimiter.
		{
			bucket:    "mybucket",
			prefix:    "prefix",
			marker:    "notmatchingmarker",
			delimiter: "unsupported",
			maxKeys:   "10",
			apiErr:    ErrNotImplemented,
		},
		// 9. Invalid params with invalid max Keys
		{
			bucket:    "mybucket",
			prefix:    "prefix",
			marker:    "prefix11",
			delimiter: "/",
			maxKeys:   "999999999999999999999999999",
			apiErr:    ErrInvalidMaxKeys,
		},
	}
	for i, test := range testCases {
		vars := mkListObjectsQueryVal(test.bucket, test.prefix, test.marker, test.delimiter, test.maxKeys)
		_, _, _, _, _, actualErr := validateHealQueryParams(vars)
		if actualErr != test.apiErr {
			t.Errorf("Test %d - Expected %v but received %v",
				i+1, getAPIError(test.apiErr), getAPIError(actualErr))
		}
	}
}

// TestListObjectsHeal - Test for ListObjectsHealHandler.
func TestListObjectsHealHandler(t *testing.T) {
	rootPath, err := newTestConfig("us-east-1")
	if err != nil {
		t.Fatalf("Unable to initialize server config. %s", err)
	}
	defer removeAll(rootPath)

	// Initializing objectLayer and corresponding []StorageAPI
	// since ListObjectsHeal() method requires it.
	objLayer, xlDirs, xlErr := prepareXL()
	if xlErr != nil {
		t.Fatalf("failed to initialize XL based object layer - %v.", xlErr)
	}
	defer removeRoots(xlDirs)

	err = objLayer.MakeBucket("mybucket")
	if err != nil {
		t.Fatalf("Failed to make bucket - %v", err)
	}

	// Delete bucket after running all test cases.
	defer objLayer.DeleteBucket("mybucket")

	// Make objLayer available to all internal services via globalObjectAPI.
	globalObjLayerMutex.Lock()
	globalObjectAPI = objLayer
	globalObjLayerMutex.Unlock()

	// Setup admin mgmt REST API handlers.
	adminRouter := router.NewRouter()
	registerAdminRouter(adminRouter)

	testCases := []struct {
		bucket     string
		prefix     string
		marker     string
		delimiter  string
		maxKeys    string
		statusCode int
	}{
		// 1. Valid params.
		{
			bucket:     "mybucket",
			prefix:     "prefix",
			marker:     "prefix11",
			delimiter:  "/",
			maxKeys:    "10",
			statusCode: http.StatusOK,
		},
		// 2. Valid params with meta bucket.
		{
			bucket:     minioMetaBucket,
			prefix:     "prefix",
			marker:     "prefix11",
			delimiter:  "/",
			maxKeys:    "10",
			statusCode: http.StatusOK,
		},
		// 3. Valid params with empty prefix.
		{
			bucket:     "mybucket",
			prefix:     "",
			marker:     "",
			delimiter:  "/",
			maxKeys:    "10",
			statusCode: http.StatusOK,
		},
		// 4. Invalid params with invalid bucket.
		{
			bucket:     `invalid\\Bucket`,
			prefix:     "prefix",
			marker:     "prefix11",
			delimiter:  "/",
			maxKeys:    "10",
			statusCode: getAPIError(ErrInvalidBucketName).HTTPStatusCode,
		},
		// 5. Invalid params with invalid prefix.
		{
			bucket:     "mybucket",
			prefix:     `invalid\\Prefix`,
			marker:     "prefix11",
			delimiter:  "/",
			maxKeys:    "10",
			statusCode: getAPIError(ErrInvalidObjectName).HTTPStatusCode,
		},
		// 6. Invalid params with invalid maxKeys.
		{
			bucket:     "mybucket",
			prefix:     "prefix",
			marker:     "prefix11",
			delimiter:  "/",
			maxKeys:    "-1",
			statusCode: getAPIError(ErrInvalidMaxKeys).HTTPStatusCode,
		},
		// 7. Invalid params with unsupported prefix marker combination.
		{
			bucket:     "mybucket",
			prefix:     "prefix",
			marker:     "notmatchingmarker",
			delimiter:  "/",
			maxKeys:    "10",
			statusCode: getAPIError(ErrNotImplemented).HTTPStatusCode,
		},
		// 8. Invalid params with unsupported delimiter.
		{
			bucket:     "mybucket",
			prefix:     "prefix",
			marker:     "notmatchingmarker",
			delimiter:  "unsupported",
			maxKeys:    "10",
			statusCode: getAPIError(ErrNotImplemented).HTTPStatusCode,
		},
		// 9. Invalid params with invalid max Keys
		{
			bucket:     "mybucket",
			prefix:     "prefix",
			marker:     "prefix11",
			delimiter:  "/",
			maxKeys:    "999999999999999999999999999",
			statusCode: getAPIError(ErrInvalidMaxKeys).HTTPStatusCode,
		},
	}

	for i, test := range testCases {
		if i != 0 {
			continue
		}
		queryVal := mkListObjectsQueryVal(test.bucket, test.prefix, test.marker, test.delimiter, test.maxKeys)
		req, err := newTestRequest("GET", "/?"+queryVal.Encode(), 0, nil)
		if err != nil {
			t.Fatalf("Test %d - Failed to construct list objects needing heal request - %v", i+1, err)
		}
		req.Header.Set(minioAdminOpHeader, "list")

		cred := serverConfig.GetCredential()
		err = signRequestV4(req, cred.AccessKey, cred.SecretKey)
		if err != nil {
			t.Fatalf("Test %d - Failed to sign list objects needing heal request - %v", i+1, err)
		}
		rec := httptest.NewRecorder()
		adminRouter.ServeHTTP(rec, req)
		if test.statusCode != rec.Code {
			t.Errorf("Test %d - Expected HTTP status code %d but received %d", i+1, test.statusCode, rec.Code)
		}
	}
}

// TestHealBucketHandler - Test for HealBucketHandler.
func TestHealBucketHandler(t *testing.T) {
	rootPath, err := newTestConfig("us-east-1")
	if err != nil {
		t.Fatalf("Unable to initialize server config. %s", err)
	}
	defer removeAll(rootPath)

	// Initializing objectLayer and corresponding []StorageAPI
	// since MakeBucket() and DeleteBucket() methods requires it.
	objLayer, xlDirs, xlErr := prepareXL()
	if xlErr != nil {
		t.Fatalf("failed to initialize XL based object layer - %v.", xlErr)
	}
	defer removeRoots(xlDirs)

	err = objLayer.MakeBucket("mybucket")
	if err != nil {
		t.Fatalf("Failed to make bucket - %v", err)
	}

	// Delete bucket after running all test cases.
	defer objLayer.DeleteBucket("mybucket")

	// Make objLayer available to all internal services via globalObjectAPI.
	globalObjLayerMutex.Lock()
	globalObjectAPI = objLayer
	globalObjLayerMutex.Unlock()

	// Setup admin mgmt REST API handlers.
	adminRouter := router.NewRouter()
	registerAdminRouter(adminRouter)

	testCases := []struct {
		bucket     string
		statusCode int
		dryrun     string
	}{
		// 1. Valid test case.
		{
			bucket:     "mybucket",
			statusCode: http.StatusOK,
		},
		// 2. Invalid bucket name.
		{
			bucket:     `invalid\\Bucket`,
			statusCode: http.StatusBadRequest,
		},
		// 3. Bucket not found.
		{
			bucket:     "bucketnotfound",
			statusCode: http.StatusNotFound,
		},
		// 4. Valid test case with dry-run.
		{
			bucket:     "mybucket",
			statusCode: http.StatusOK,
			dryrun:     "yes",
		},
	}
	for i, test := range testCases {
		// Prepare query params.
		queryVal := url.Values{}
		queryVal.Set(string(mgmtBucket), test.bucket)
		queryVal.Set("heal", "")
		queryVal.Set(string(mgmtDryRun), test.dryrun)

		req, err := newTestRequest("POST", "/?"+queryVal.Encode(), 0, nil)
		if err != nil {
			t.Fatalf("Test %d - Failed to construct heal bucket request - %v", i+1, err)
		}

		req.Header.Set(minioAdminOpHeader, "bucket")

		cred := serverConfig.GetCredential()
		err = signRequestV4(req, cred.AccessKey, cred.SecretKey)
		if err != nil {
			t.Fatalf("Test %d - Failed to sign heal bucket request - %v", i+1, err)
		}
		rec := httptest.NewRecorder()
		adminRouter.ServeHTTP(rec, req)
		if test.statusCode != rec.Code {
			t.Errorf("Test %d - Expected HTTP status code %d but received %d", i+1, test.statusCode, rec.Code)
		}

	}
}

// TestHealObjectHandler - Test for HealObjectHandler.
func TestHealObjectHandler(t *testing.T) {
	rootPath, err := newTestConfig("us-east-1")
	if err != nil {
		t.Fatalf("Unable to initialize server config. %s", err)
	}
	defer removeAll(rootPath)

	// Initializing objectLayer and corresponding []StorageAPI
	// since MakeBucket(), PutObject() and DeleteBucket() method requires it.
	objLayer, xlDirs, xlErr := prepareXL()
	if xlErr != nil {
		t.Fatalf("failed to initialize XL based object layer - %v.", xlErr)
	}
	defer removeRoots(xlDirs)

	// Create an object myobject under bucket mybucket.
	bucketName := "mybucket"
	objName := "myobject"
	err = objLayer.MakeBucket(bucketName)
	if err != nil {
		t.Fatalf("Failed to make bucket %s - %v", bucketName, err)
	}

	_, err = objLayer.PutObject(bucketName, objName, int64(len("hello")), bytes.NewReader([]byte("hello")), nil, "")
	if err != nil {
		t.Fatalf("Failed to create %s - %v", objName, err)
	}

	// Delete bucket and object after running all test cases.
	defer func(objLayer ObjectLayer, bucketName, objName string) {
		objLayer.DeleteObject(bucketName, objName)
		objLayer.DeleteBucket(bucketName)
	}(objLayer, bucketName, objName)

	// Make objLayer available to all internal services via globalObjectAPI.
	globalObjLayerMutex.Lock()
	globalObjectAPI = objLayer
	globalObjLayerMutex.Unlock()

	// Setup admin mgmt REST API handlers.
	adminRouter := router.NewRouter()
	registerAdminRouter(adminRouter)

	testCases := []struct {
		bucket     string
		object     string
		dryrun     string
		statusCode int
	}{
		// 1. Valid test case.
		{
			bucket:     bucketName,
			object:     objName,
			statusCode: http.StatusOK,
		},
		// 2. Invalid bucket name.
		{
			bucket:     `invalid\\Bucket`,
			object:     "myobject",
			statusCode: http.StatusBadRequest,
		},
		// 3. Bucket not found.
		{
			bucket:     "bucketnotfound",
			object:     "myobject",
			statusCode: http.StatusNotFound,
		},
		// 4. Invalid object name.
		{
			bucket:     bucketName,
			object:     `invalid\\Object`,
			statusCode: http.StatusBadRequest,
		},
		// 5. Object not found.
		{
			bucket:     bucketName,
			object:     "objectnotfound",
			statusCode: http.StatusNotFound,
		},
		// 6. Valid test case with dry-run.
		{
			bucket:     bucketName,
			object:     objName,
			dryrun:     "yes",
			statusCode: http.StatusOK,
		},
	}
	for i, test := range testCases {
		// Prepare query params.
		queryVal := url.Values{}
		queryVal.Set(string(mgmtBucket), test.bucket)
		queryVal.Set(string(mgmtObject), test.object)
		queryVal.Set("heal", "")
		queryVal.Set(string(mgmtDryRun), test.dryrun)

		req, err := newTestRequest("POST", "/?"+queryVal.Encode(), 0, nil)
		if err != nil {
			t.Fatalf("Test %d - Failed to construct heal object request - %v", i+1, err)
		}

		req.Header.Set(minioAdminOpHeader, "object")

		cred := serverConfig.GetCredential()
		err = signRequestV4(req, cred.AccessKey, cred.SecretKey)
		if err != nil {
			t.Fatalf("Test %d - Failed to sign heal object request - %v", i+1, err)
		}
		rec := httptest.NewRecorder()
		adminRouter.ServeHTTP(rec, req)
		if test.statusCode != rec.Code {
			t.Errorf("Test %d - Expected HTTP status code %d but received %d", i+1, test.statusCode, rec.Code)
		}
	}
}

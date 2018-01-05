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
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"reflect"
	"testing"
	"time"

	router "github.com/gorilla/mux"
	"github.com/minio/minio/pkg/auth"
	"github.com/minio/minio/pkg/errors"
)

var configJSON = []byte(`{
	"version": "13",
	"credential": {
		"accessKey": "minio",
		"secretKey": "minio123"
	},
	"region": "us-west-1",
	"logger": {
		"console": {
			"enable": true,
			"level": "fatal"
		},
		"file": {
			"enable": false,
			"fileName": "",
			"level": ""
		}
	},
	"notify": {
		"amqp": {
			"1": {
				"enable": false,
				"url": "",
				"exchange": "",
				"routingKey": "",
				"exchangeType": "",
				"mandatory": false,
				"immediate": false,
				"durable": false,
				"internal": false,
				"noWait": false,
				"autoDeleted": false
			}
		},
		"nats": {
			"1": {
				"enable": false,
				"address": "",
				"subject": "",
				"username": "",
				"password": "",
				"token": "",
				"secure": false,
				"pingInterval": 0,
				"streaming": {
					"enable": false,
					"clusterID": "",
					"clientID": "",
					"async": false,
					"maxPubAcksInflight": 0
				}
			}
		},
		"elasticsearch": {
			"1": {
				"enable": false,
				"url": "",
				"index": ""
			}
		},
		"redis": {
			"1": {
				"enable": false,
				"address": "",
				"password": "",
				"key": ""
			}
		},
		"postgresql": {
			"1": {
				"enable": false,
				"connectionString": "",
				"table": "",
				"host": "",
				"port": "",
				"user": "",
				"password": "",
				"database": ""
			}
		},
		"kafka": {
			"1": {
				"enable": false,
				"brokers": null,
				"topic": ""
			}
		},
		"webhook": {
			"1": {
				"enable": false,
				"endpoint": ""
			}
		}
	}
}`)

// adminXLTestBed - encapsulates subsystems that need to be setup for
// admin-handler unit tests.
type adminXLTestBed struct {
	configPath string
	xlDirs     []string
	objLayer   ObjectLayer
	mux        *router.Router
}

// prepareAdminXLTestBed - helper function that setups a single-node
// XL backend for admin-handler tests.
func prepareAdminXLTestBed() (*adminXLTestBed, error) {
	// reset global variables to start afresh.
	resetTestGlobals()

	// Initialize minio server config.
	rootPath, err := newTestConfig(globalMinioDefaultRegion)
	if err != nil {
		return nil, err
	}
	// Initializing objectLayer for HealFormatHandler.
	objLayer, xlDirs, xlErr := initTestXLObjLayer()
	if xlErr != nil {
		return nil, xlErr
	}

	// Initialize boot time
	globalBootTime = UTCNow()

	globalEndpoints = mustGetNewEndpointList(xlDirs...)

	// Set globalIsXL to indicate that the setup uses an erasure code backend.
	globalIsXL = true

	// initialize NSLock.
	isDistXL := false
	initNSLock(isDistXL)

	// Setup admin mgmt REST API handlers.
	adminRouter := router.NewRouter()
	registerAdminRouter(adminRouter)

	return &adminXLTestBed{
		configPath: rootPath,
		xlDirs:     xlDirs,
		objLayer:   objLayer,
		mux:        adminRouter,
	}, nil
}

// TearDown - method that resets the test bed for subsequent unit
// tests to start afresh.
func (atb *adminXLTestBed) TearDown() {
	os.RemoveAll(atb.configPath)
	removeRoots(atb.xlDirs)
	resetTestGlobals()
}

// initTestObjLayer - Helper function to initialize an XL-based object
// layer and set globalObjectAPI.
func initTestXLObjLayer() (ObjectLayer, []string, error) {
	objLayer, xlDirs, xlErr := prepareXL16()
	if xlErr != nil {
		return nil, nil, xlErr
	}
	// Make objLayer available to all internal services via globalObjectAPI.
	globalObjLayerMutex.Lock()
	globalObjectAPI = objLayer
	globalObjLayerMutex.Unlock()
	return objLayer, xlDirs, nil
}

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
func getServiceCmdRequest(cmd cmdType, cred auth.Credentials, body []byte) (*http.Request, error) {
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
func testServicesCmdHandler(cmd cmdType, t *testing.T) {
	adminTestBed, err := prepareAdminXLTestBed()
	if err != nil {
		t.Fatal("Failed to initialize a single node XL backend for admin handler tests.")
	}
	defer adminTestBed.TearDown()

	// Initialize admin peers to make admin RPC calls. Note: In a
	// single node setup, this degenerates to a simple function
	// call under the hood.
	globalMinioAddr = "127.0.0.1:9000"
	initGlobalAdminPeers(mustGetNewEndpointList("http://127.0.0.1:9000/d1"))

	// Setting up a go routine to simulate ServerMux's
	// handleServiceSignals for stop and restart commands.
	if cmd == restartCmd {
		go testServiceSignalReceiver(cmd, t)
	}
	credentials := globalServerConfig.GetCredential()
	var body []byte

	req, err := getServiceCmdRequest(cmd, credentials, body)
	if err != nil {
		t.Fatalf("Failed to build service status request %v", err)
	}

	rec := httptest.NewRecorder()
	adminTestBed.mux.ServeHTTP(rec, req)

	if cmd == statusCmd {
		expectedInfo := ServerStatus{
			ServerVersion: ServerVersion{Version: Version, CommitID: CommitID},
		}
		receivedInfo := ServerStatus{}
		if jsonErr := json.Unmarshal(rec.Body.Bytes(), &receivedInfo); jsonErr != nil {
			t.Errorf("Failed to unmarshal StorageInfo - %v", jsonErr)
		}
		if expectedInfo.ServerVersion != receivedInfo.ServerVersion {
			t.Errorf("Expected storage info and received storage info differ, %v %v", expectedInfo, receivedInfo)
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
	testServicesCmdHandler(statusCmd, t)
}

// Test for service restart management REST API.
func TestServiceRestartHandler(t *testing.T) {
	testServicesCmdHandler(restartCmd, t)
}

// Test for service set creds management REST API.
func TestServiceSetCreds(t *testing.T) {
	adminTestBed, err := prepareAdminXLTestBed()
	if err != nil {
		t.Fatal("Failed to initialize a single node XL backend for admin handler tests.")
	}
	defer adminTestBed.TearDown()

	// Initialize admin peers to make admin RPC calls. Note: In a
	// single node setup, this degenerates to a simple function
	// call under the hood.
	globalMinioAddr = "127.0.0.1:9000"
	initGlobalAdminPeers(mustGetNewEndpointList("http://127.0.0.1:9000/d1"))

	credentials := globalServerConfig.GetCredential()
	var body []byte

	testCases := []struct {
		Username           string
		Password           string
		EnvKeysSet         bool
		ExpectedStatusCode int
	}{
		// Bad secret key
		{"minio", "minio", false, http.StatusBadRequest},
		// Bad  secret key set from the env
		{"minio", "minio", true, http.StatusMethodNotAllowed},
		// Good keys set from the env
		{"minio", "minio123", true, http.StatusMethodNotAllowed},
		// Successful operation should be the last one to do not change server credentials during tests.
		{"minio", "minio123", false, http.StatusOK},
	}
	for i, testCase := range testCases {
		// Set or unset environement keys
		if !testCase.EnvKeysSet {
			globalIsEnvCreds = false
		} else {
			globalIsEnvCreds = true
		}

		// Construct setCreds request body
		body, _ = xml.Marshal(setCredsReq{Username: testCase.Username, Password: testCase.Password})
		// Construct setCreds request
		req, err := getServiceCmdRequest(setCreds, credentials, body)
		if err != nil {
			t.Fatalf("Failed to build service status request %v", err)
		}

		rec := httptest.NewRecorder()

		// Execute request
		adminTestBed.mux.ServeHTTP(rec, req)

		// Check if the http code response is expected
		if rec.Code != testCase.ExpectedStatusCode {
			t.Errorf("Test %d: Wrong status code, expected = %d, found = %d", i+1, testCase.ExpectedStatusCode, rec.Code)
			resp, _ := ioutil.ReadAll(rec.Body)
			t.Errorf("Expected to receive %d status code but received %d. Body (%s)",
				http.StatusOK, rec.Code, string(resp))
		}

		// If we got 200 OK, check if new credentials are really set
		if rec.Code == http.StatusOK {
			cred := globalServerConfig.GetCredential()
			if cred.AccessKey != testCase.Username {
				t.Errorf("Test %d: Wrong access key, expected = %s, found = %s", i+1, testCase.Username, cred.AccessKey)
			}
			if cred.SecretKey != testCase.Password {
				t.Errorf("Test %d: Wrong secret key, expected = %s, found = %s", i+1, testCase.Password, cred.SecretKey)
			}
		}
	}
}

// mkLockQueryVal - helper function to build lock query param.
func mkLockQueryVal(bucket, prefix, durationStr string) url.Values {
	qVal := url.Values{}
	qVal.Set("lock", "")
	qVal.Set(string(mgmtBucket), bucket)
	qVal.Set(string(mgmtPrefix), prefix)
	qVal.Set(string(mgmtLockDuration), durationStr)
	return qVal
}

// Test for locks list management REST API.
func TestListLocksHandler(t *testing.T) {
	adminTestBed, err := prepareAdminXLTestBed()
	if err != nil {
		t.Fatal("Failed to initialize a single node XL backend for admin handler tests.")
	}
	defer adminTestBed.TearDown()

	// Initialize admin peers to make admin RPC calls.
	globalMinioAddr = "127.0.0.1:9000"
	initGlobalAdminPeers(mustGetNewEndpointList("http://127.0.0.1:9000/d1"))

	testCases := []struct {
		bucket         string
		prefix         string
		duration       string
		expectedStatus int
	}{
		// Test 1 - valid testcase
		{
			bucket:         "mybucket",
			prefix:         "myobject",
			duration:       "1s",
			expectedStatus: http.StatusOK,
		},
		// Test 2 - invalid duration
		{
			bucket:         "mybucket",
			prefix:         "myprefix",
			duration:       "invalidDuration",
			expectedStatus: http.StatusBadRequest,
		},
		// Test 3 - invalid bucket name
		{
			bucket:         `invalid\\Bucket`,
			prefix:         "myprefix",
			duration:       "1h",
			expectedStatus: http.StatusBadRequest,
		},
		// Test 4 - invalid prefix
		{
			bucket:         "mybucket",
			prefix:         `invalid\\Prefix`,
			duration:       "1h",
			expectedStatus: http.StatusBadRequest,
		},
	}

	for i, test := range testCases {
		queryVal := mkLockQueryVal(test.bucket, test.prefix, test.duration)
		req, err := newTestRequest("GET", "/?"+queryVal.Encode(), 0, nil)
		if err != nil {
			t.Fatalf("Test %d - Failed to construct list locks request - %v", i+1, err)
		}
		req.Header.Set(minioAdminOpHeader, "list")

		cred := globalServerConfig.GetCredential()
		err = signRequestV4(req, cred.AccessKey, cred.SecretKey)
		if err != nil {
			t.Fatalf("Test %d - Failed to sign list locks request - %v", i+1, err)
		}
		rec := httptest.NewRecorder()
		adminTestBed.mux.ServeHTTP(rec, req)
		if test.expectedStatus != rec.Code {
			t.Errorf("Test %d - Expected HTTP status code %d but received %d", i+1, test.expectedStatus, rec.Code)
		}
	}
}

// Test for locks clear management REST API.
func TestClearLocksHandler(t *testing.T) {
	adminTestBed, err := prepareAdminXLTestBed()
	if err != nil {
		t.Fatal("Failed to initialize a single node XL backend for admin handler tests.")
	}
	defer adminTestBed.TearDown()

	// Initialize admin peers to make admin RPC calls.
	initGlobalAdminPeers(mustGetNewEndpointList("http://127.0.0.1:9000/d1"))

	testCases := []struct {
		bucket         string
		prefix         string
		duration       string
		expectedStatus int
	}{
		// Test 1 - valid testcase
		{
			bucket:         "mybucket",
			prefix:         "myobject",
			duration:       "1s",
			expectedStatus: http.StatusOK,
		},
		// Test 2 - invalid duration
		{
			bucket:         "mybucket",
			prefix:         "myprefix",
			duration:       "invalidDuration",
			expectedStatus: http.StatusBadRequest,
		},
		// Test 3 - invalid bucket name
		{
			bucket:         `invalid\\Bucket`,
			prefix:         "myprefix",
			duration:       "1h",
			expectedStatus: http.StatusBadRequest,
		},
		// Test 4 - invalid prefix
		{
			bucket:         "mybucket",
			prefix:         `invalid\\Prefix`,
			duration:       "1h",
			expectedStatus: http.StatusBadRequest,
		},
	}

	for i, test := range testCases {
		queryVal := mkLockQueryVal(test.bucket, test.prefix, test.duration)
		req, err := newTestRequest("POST", "/?"+queryVal.Encode(), 0, nil)
		if err != nil {
			t.Fatalf("Test %d - Failed to construct clear locks request - %v", i+1, err)
		}
		req.Header.Set(minioAdminOpHeader, "clear")

		cred := globalServerConfig.GetCredential()
		err = signRequestV4(req, cred.AccessKey, cred.SecretKey)
		if err != nil {
			t.Fatalf("Test %d - Failed to sign clear locks request - %v", i+1, err)
		}
		rec := httptest.NewRecorder()
		adminTestBed.mux.ServeHTTP(rec, req)
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
		_, _, _, _, _, actualErr := extractListObjectsHealQuery(vars)
		if actualErr != test.apiErr {
			t.Errorf("Test %d - Expected %v but received %v",
				i+1, getAPIError(test.apiErr), getAPIError(actualErr))
		}
	}
}

// TestListObjectsHeal - Test for ListObjectsHealHandler.
func TestListObjectsHealHandler(t *testing.T) {
	adminTestBed, err := prepareAdminXLTestBed()
	if err != nil {
		t.Fatal("Failed to initialize a single node XL backend for admin handler tests.")
	}
	defer adminTestBed.TearDown()

	err = adminTestBed.objLayer.MakeBucketWithLocation("mybucket", "")
	if err != nil {
		t.Fatalf("Failed to make bucket - %v", err)
	}

	// Delete bucket after running all test cases.
	defer adminTestBed.objLayer.DeleteBucket("mybucket")

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
		queryVal := mkListObjectsQueryVal(test.bucket, test.prefix, test.marker, test.delimiter, test.maxKeys)
		req, err := newTestRequest("GET", "/?"+queryVal.Encode(), 0, nil)
		if err != nil {
			t.Fatalf("Test %d - Failed to construct list objects needing heal request - %v", i+1, err)
		}
		req.Header.Set(minioAdminOpHeader, "list-objects")

		cred := globalServerConfig.GetCredential()
		err = signRequestV4(req, cred.AccessKey, cred.SecretKey)
		if err != nil {
			t.Fatalf("Test %d - Failed to sign list objects needing heal request - %v", i+1, err)
		}
		rec := httptest.NewRecorder()
		adminTestBed.mux.ServeHTTP(rec, req)
		if test.statusCode != rec.Code {
			t.Errorf("Test %d - Expected HTTP status code %d but received %d", i+1, test.statusCode, rec.Code)
		}
	}
}

// TestHealBucketHandler - Test for HealBucketHandler.
func TestHealBucketHandler(t *testing.T) {
	adminTestBed, err := prepareAdminXLTestBed()
	if err != nil {
		t.Fatal("Failed to initialize a single node XL backend for admin handler tests.")
	}
	defer adminTestBed.TearDown()

	err = adminTestBed.objLayer.MakeBucketWithLocation("mybucket", "")
	if err != nil {
		t.Fatalf("Failed to make bucket - %v", err)
	}

	// Delete bucket after running all test cases.
	defer adminTestBed.objLayer.DeleteBucket("mybucket")

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
			t.Fatalf("Test %d - Failed to construct heal bucket request - %v",
				i+1, err)
		}

		req.Header.Set(minioAdminOpHeader, "bucket")

		cred := globalServerConfig.GetCredential()
		err = signRequestV4(req, cred.AccessKey, cred.SecretKey)
		if err != nil {
			t.Fatalf("Test %d - Failed to sign heal bucket request - %v",
				i+1, err)
		}
		rec := httptest.NewRecorder()
		adminTestBed.mux.ServeHTTP(rec, req)
		if test.statusCode != rec.Code {
			t.Errorf("Test %d - Expected HTTP status code %d but received %d",
				i+1, test.statusCode, rec.Code)
		}

	}
}

// TestHealObjectHandler - Test for HealObjectHandler.
func TestHealObjectHandler(t *testing.T) {
	adminTestBed, err := prepareAdminXLTestBed()
	if err != nil {
		t.Fatal("Failed to initialize a single node XL backend for admin handler tests.")
	}
	defer adminTestBed.TearDown()

	// Create an object myobject under bucket mybucket.
	bucketName := "mybucket"
	objName := "myobject"
	err = adminTestBed.objLayer.MakeBucketWithLocation(bucketName, "")
	if err != nil {
		t.Fatalf("Failed to make bucket %s - %v", bucketName, err)
	}

	_, err = adminTestBed.objLayer.PutObject(bucketName, objName,
		mustGetHashReader(t, bytes.NewReader([]byte("hello")), int64(len("hello")), "", ""), nil)
	if err != nil {
		t.Fatalf("Failed to create %s - %v", objName, err)
	}

	// Delete bucket and object after running all test cases.
	defer func(objLayer ObjectLayer, bucketName, objName string) {
		objLayer.DeleteObject(bucketName, objName)
		objLayer.DeleteBucket(bucketName)
	}(adminTestBed.objLayer, bucketName, objName)

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

		cred := globalServerConfig.GetCredential()
		err = signRequestV4(req, cred.AccessKey, cred.SecretKey)
		if err != nil {
			t.Fatalf("Test %d - Failed to sign heal object request - %v", i+1, err)
		}
		rec := httptest.NewRecorder()
		adminTestBed.mux.ServeHTTP(rec, req)
		if test.statusCode != rec.Code {
			t.Errorf("Test %d - Expected HTTP status code %d but received %d", i+1, test.statusCode, rec.Code)
		}
	}

}

// buildAdminRequest - helper function to build an admin API request.
func buildAdminRequest(queryVal url.Values, opHdr, method string,
	contentLength int64, bodySeeker io.ReadSeeker) (*http.Request, error) {
	req, err := newTestRequest(method, "/?"+queryVal.Encode(), contentLength, bodySeeker)
	if err != nil {
		return nil, errors.Trace(err)
	}

	req.Header.Set(minioAdminOpHeader, opHdr)

	cred := globalServerConfig.GetCredential()
	err = signRequestV4(req, cred.AccessKey, cred.SecretKey)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return req, nil
}

// mkHealUploadQuery - helper to build HealUploadHandler query string.
func mkHealUploadQuery(bucket, object, uploadID, dryRun string) url.Values {
	queryVal := url.Values{}
	queryVal.Set(string(mgmtBucket), bucket)
	queryVal.Set(string(mgmtObject), object)
	queryVal.Set(string(mgmtUploadID), uploadID)
	queryVal.Set("heal", "")
	queryVal.Set(string(mgmtDryRun), dryRun)
	return queryVal
}

// TestHealUploadHandler - test for HealUploadHandler.
func TestHealUploadHandler(t *testing.T) {
	adminTestBed, err := prepareAdminXLTestBed()
	if err != nil {
		t.Fatal("Failed to initialize a single node XL backend for admin handler tests.")
	}
	defer adminTestBed.TearDown()

	// Create an object myobject under bucket mybucket.
	bucketName := "mybucket"
	objName := "myobject"
	err = adminTestBed.objLayer.MakeBucketWithLocation(bucketName, "")
	if err != nil {
		t.Fatalf("Failed to make bucket %s - %v", bucketName, err)
	}

	// Create a new multipart upload.
	uploadID, err := adminTestBed.objLayer.NewMultipartUpload(bucketName, objName, nil)
	if err != nil {
		t.Fatalf("Failed to create a new multipart upload %s/%s - %v",
			bucketName, objName, err)
	}

	// Upload a part.
	partID := 1
	_, err = adminTestBed.objLayer.PutObjectPart(bucketName, objName, uploadID,
		partID, mustGetHashReader(t, bytes.NewReader([]byte("hello")), int64(len("hello")), "", ""))
	if err != nil {
		t.Fatalf("Failed to upload part %d of %s/%s - %v", partID,
			bucketName, objName, err)
	}

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
		queryVal := mkHealUploadQuery(test.bucket, test.object, uploadID, test.dryrun)
		req, err1 := buildAdminRequest(queryVal, "upload", http.MethodPost, 0, nil)
		if err1 != nil {
			t.Fatalf("Test %d - Failed to construct heal object request - %v", i+1, err1)
		}
		rec := httptest.NewRecorder()
		adminTestBed.mux.ServeHTTP(rec, req)
		if test.statusCode != rec.Code {
			t.Errorf("Test %d - Expected HTTP status code %d but received %d", i+1, test.statusCode, rec.Code)
		}
	}

	sample := testCases[0]
	// Modify authorization header after signing to test signature
	// mismatch handling.
	queryVal := mkHealUploadQuery(sample.bucket, sample.object, uploadID, sample.dryrun)
	req, err := buildAdminRequest(queryVal, "upload", "POST", 0, nil)
	if err != nil {
		t.Fatalf("Failed to construct heal object request - %v", err)
	}

	// Set x-amz-date to a date different than time of signing.
	req.Header.Set("x-amz-date", time.Time{}.Format(iso8601Format))

	rec := httptest.NewRecorder()
	adminTestBed.mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Errorf("Expected %d but received %d", http.StatusBadRequest, rec.Code)
	}

	// Set objectAPI to nil to test Server not initialized case.
	resetGlobalObjectAPI()
	queryVal = mkHealUploadQuery(sample.bucket, sample.object, uploadID, sample.dryrun)
	req, err = buildAdminRequest(queryVal, "upload", "POST", 0, nil)
	if err != nil {
		t.Fatalf("Failed to construct heal object request - %v", err)
	}

	rec = httptest.NewRecorder()
	adminTestBed.mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("Expected %d but received %d", http.StatusServiceUnavailable, rec.Code)
	}
}

// TestHealFormatHandler - test for HealFormatHandler.
func TestHealFormatHandler(t *testing.T) {
	adminTestBed, err := prepareAdminXLTestBed()
	if err != nil {
		t.Fatal("Failed to initialize a single node XL backend for admin handler tests.")
	}
	defer adminTestBed.TearDown()

	// Prepare query params for heal-format mgmt REST API.
	queryVal := url.Values{}
	queryVal.Set("heal", "")
	req, err := buildAdminRequest(queryVal, "format", "POST", 0, nil)
	if err != nil {
		t.Fatalf("Failed to construct heal object request - %v", err)
	}

	rec := httptest.NewRecorder()
	adminTestBed.mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("Expected to succeed but failed with %d", rec.Code)
	}
}

// TestGetConfigHandler - test for GetConfigHandler.
func TestGetConfigHandler(t *testing.T) {
	adminTestBed, err := prepareAdminXLTestBed()
	if err != nil {
		t.Fatal("Failed to initialize a single node XL backend for admin handler tests.")
	}
	defer adminTestBed.TearDown()

	// Initialize admin peers to make admin RPC calls.
	globalMinioAddr = "127.0.0.1:9000"
	initGlobalAdminPeers(mustGetNewEndpointList("http://127.0.0.1:9000/d1"))

	// Prepare query params for get-config mgmt REST API.
	queryVal := url.Values{}
	queryVal.Set("config", "")

	req, err := buildAdminRequest(queryVal, "get", http.MethodGet, 0, nil)
	if err != nil {
		t.Fatalf("Failed to construct get-config object request - %v", err)
	}

	rec := httptest.NewRecorder()
	adminTestBed.mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("Expected to succeed but failed with %d", rec.Code)
	}

}

// TestSetConfigHandler - test for SetConfigHandler.
func TestSetConfigHandler(t *testing.T) {
	adminTestBed, err := prepareAdminXLTestBed()
	if err != nil {
		t.Fatal("Failed to initialize a single node XL backend for admin handler tests.")
	}
	defer adminTestBed.TearDown()

	// Initialize admin peers to make admin RPC calls.
	globalMinioAddr = "127.0.0.1:9000"
	initGlobalAdminPeers(mustGetNewEndpointList("http://127.0.0.1:9000/d1"))

	// SetConfigHandler restarts minio setup - need to start a
	// signal receiver to receive on globalServiceSignalCh.
	go testServiceSignalReceiver(restartCmd, t)

	// Prepare query params for set-config mgmt REST API.
	queryVal := url.Values{}
	queryVal.Set("config", "")

	req, err := buildAdminRequest(queryVal, "set", http.MethodPut, int64(len(configJSON)),
		bytes.NewReader(configJSON))
	if err != nil {
		t.Fatalf("Failed to construct get-config object request - %v", err)
	}

	rec := httptest.NewRecorder()
	adminTestBed.mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("Expected to succeed but failed with %d", rec.Code)
	}

	result := setConfigResult{}
	err = json.NewDecoder(rec.Body).Decode(&result)
	if err != nil {
		t.Fatalf("Failed to decode set config result json %v", err)
	}

	if !result.Status {
		t.Error("Expected set-config to succeed, but failed")
	}
}

func TestAdminServerInfo(t *testing.T) {
	adminTestBed, err := prepareAdminXLTestBed()
	if err != nil {
		t.Fatal("Failed to initialize a single node XL backend for admin handler tests.")
	}
	defer adminTestBed.TearDown()

	// Initialize admin peers to make admin RPC calls.
	globalMinioAddr = "127.0.0.1:9000"
	initGlobalAdminPeers(mustGetNewEndpointList("http://127.0.0.1:9000/d1"))

	// Prepare query params for set-config mgmt REST API.
	queryVal := url.Values{}
	queryVal.Set("info", "")

	req, err := buildAdminRequest(queryVal, "", http.MethodGet, 0, nil)
	if err != nil {
		t.Fatalf("Failed to construct get-config object request - %v", err)
	}

	rec := httptest.NewRecorder()
	adminTestBed.mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("Expected to succeed but failed with %d", rec.Code)
	}

	results := []ServerInfo{}
	err = json.NewDecoder(rec.Body).Decode(&results)
	if err != nil {
		t.Fatalf("Failed to decode set config result json %v", err)
	}

	if len(results) == 0 {
		t.Error("Expected at least one server info result")
	}

	for _, serverInfo := range results {
		if len(serverInfo.Addr) == 0 {
			t.Error("Expected server address to be non empty")
		}
		if serverInfo.Error != "" {
			t.Errorf("Unexpected error = %v\n", serverInfo.Error)
		}
		if serverInfo.Data.StorageInfo.Free == 0 {
			t.Error("Expected StorageInfo.Free to be non empty")
		}
		if serverInfo.Data.Properties.Region != globalMinioDefaultRegion {
			t.Errorf("Expected %s, got %s", globalMinioDefaultRegion, serverInfo.Data.Properties.Region)
		}
	}
}

// TestToAdminAPIErr - test for toAdminAPIErr helper function.
func TestToAdminAPIErr(t *testing.T) {
	testCases := []struct {
		err            error
		expectedAPIErr APIErrorCode
	}{
		// 1. Server not in quorum.
		{
			err:            errXLWriteQuorum,
			expectedAPIErr: ErrAdminConfigNoQuorum,
		},
		// 2. No error.
		{
			err:            nil,
			expectedAPIErr: ErrNone,
		},
		// 3. Non-admin API specific error.
		{
			err:            errDiskNotFound,
			expectedAPIErr: toAPIErrorCode(errDiskNotFound),
		},
	}

	for i, test := range testCases {
		actualErr := toAdminAPIErrCode(test.err)
		if actualErr != test.expectedAPIErr {
			t.Errorf("Test %d: Expected %v but received %v",
				i+1, test.expectedAPIErr, actualErr)
		}
	}
}

func TestWriteSetConfigResponse(t *testing.T) {
	rootPath, err := newTestConfig(globalMinioDefaultRegion)
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(rootPath)
	testCases := []struct {
		status bool
		errs   []error
	}{
		// 1. all nodes returned success.
		{
			status: true,
			errs:   []error{nil, nil, nil, nil},
		},
		// 2. some nodes returned errors.
		{
			status: false,
			errs:   []error{errDiskNotFound, nil, errDiskAccessDenied, errFaultyDisk},
		},
	}

	testPeers := []adminPeer{
		{
			addr: "localhost:9001",
		},
		{
			addr: "localhost:9002",
		},
		{
			addr: "localhost:9003",
		},
		{
			addr: "localhost:9004",
		},
	}

	testURL, err := url.Parse("http://dummy.com")
	if err != nil {
		t.Fatalf("Failed to parse a place-holder url")
	}

	var actualResult setConfigResult
	for i, test := range testCases {
		rec := httptest.NewRecorder()
		writeSetConfigResponse(rec, testPeers, test.errs, test.status, testURL)
		resp := rec.Result()
		jsonBytes, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			t.Fatalf("Test %d: Failed to read response %v", i+1, err)
		}

		err = json.Unmarshal(jsonBytes, &actualResult)
		if err != nil {
			t.Fatalf("Test %d: Failed to unmarshal json %v", i+1, err)
		}
		if actualResult.Status != test.status {
			t.Errorf("Test %d: Expected status %v but received %v", i+1, test.status, actualResult.Status)
		}
		for p, res := range actualResult.NodeResults {
			if res.Name != testPeers[p].addr {
				t.Errorf("Test %d: Expected node name %s but received %s", i+1, testPeers[p].addr, res.Name)
			}
			expectedErrMsg := fmt.Sprintf("%v", test.errs[p])
			if res.ErrMsg != expectedErrMsg {
				t.Errorf("Test %d: Expected error %s but received %s", i+1, expectedErrMsg, res.ErrMsg)
			}
			expectedErrSet := test.errs[p] != nil
			if res.ErrSet != expectedErrSet {
				t.Errorf("Test %d: Expected ErrSet %v but received %v", i+1, expectedErrSet, res.ErrSet)
			}
		}
	}
}

// mkListUploadsHealQuery - helper function to construct query values for
// listUploadsHeal.
func mkListUploadsHealQuery(bucket, prefix, keyMarker, uploadIDMarker, delimiter, maxUploadsStr string) url.Values {

	queryVal := make(url.Values)
	queryVal.Set("heal", "")
	queryVal.Set(string(mgmtBucket), bucket)
	queryVal.Set(string(mgmtPrefix), prefix)
	queryVal.Set(string(mgmtKeyMarker), keyMarker)
	queryVal.Set(string(mgmtUploadIDMarker), uploadIDMarker)
	queryVal.Set(string(mgmtDelimiter), delimiter)
	queryVal.Set(string(mgmtMaxUploads), maxUploadsStr)
	return queryVal
}

func TestListHealUploadsHandler(t *testing.T) {
	adminTestBed, err := prepareAdminXLTestBed()
	if err != nil {
		t.Fatal("Failed to initialize a single node XL backend for admin handler tests.")
	}
	defer adminTestBed.TearDown()

	err = adminTestBed.objLayer.MakeBucketWithLocation("mybucket", "")
	if err != nil {
		t.Fatalf("Failed to make bucket - %v", err)
	}

	// Delete bucket after running all test cases.
	defer adminTestBed.objLayer.DeleteBucket("mybucket")

	testCases := []struct {
		bucket       string
		prefix       string
		keyMarker    string
		delimiter    string
		maxKeys      string
		statusCode   int
		expectedResp ListMultipartUploadsResponse
	}{
		// 1. Valid params.
		{
			bucket:     "mybucket",
			prefix:     "prefix",
			keyMarker:  "prefix11",
			delimiter:  "/",
			maxKeys:    "10",
			statusCode: http.StatusOK,
			expectedResp: ListMultipartUploadsResponse{
				XMLName:    xml.Name{Space: "http://s3.amazonaws.com/doc/2006-03-01/", Local: "ListMultipartUploadsResult"},
				Bucket:     "mybucket",
				KeyMarker:  "prefix11",
				Delimiter:  "/",
				Prefix:     "prefix",
				MaxUploads: 10,
			},
		},
		// 2. Valid params with empty prefix.
		{
			bucket:     "mybucket",
			prefix:     "",
			keyMarker:  "",
			delimiter:  "/",
			maxKeys:    "10",
			statusCode: http.StatusOK,
			expectedResp: ListMultipartUploadsResponse{
				XMLName:    xml.Name{Space: "http://s3.amazonaws.com/doc/2006-03-01/", Local: "ListMultipartUploadsResult"},
				Bucket:     "mybucket",
				KeyMarker:  "",
				Delimiter:  "/",
				Prefix:     "",
				MaxUploads: 10,
			},
		},
		// 3. Invalid params with invalid bucket.
		{
			bucket:     `invalid\\Bucket`,
			prefix:     "prefix",
			keyMarker:  "prefix11",
			delimiter:  "/",
			maxKeys:    "10",
			statusCode: getAPIError(ErrInvalidBucketName).HTTPStatusCode,
		},
		// 4. Invalid params with invalid prefix.
		{
			bucket:     "mybucket",
			prefix:     `invalid\\Prefix`,
			keyMarker:  "prefix11",
			delimiter:  "/",
			maxKeys:    "10",
			statusCode: getAPIError(ErrInvalidObjectName).HTTPStatusCode,
		},
		// 5. Invalid params with invalid maxKeys.
		{
			bucket:     "mybucket",
			prefix:     "prefix",
			keyMarker:  "prefix11",
			delimiter:  "/",
			maxKeys:    "-1",
			statusCode: getAPIError(ErrInvalidMaxUploads).HTTPStatusCode,
		},
		// 6. Invalid params with unsupported prefix marker combination.
		{
			bucket:     "mybucket",
			prefix:     "prefix",
			keyMarker:  "notmatchingmarker",
			delimiter:  "/",
			maxKeys:    "10",
			statusCode: getAPIError(ErrNotImplemented).HTTPStatusCode,
		},
		// 7. Invalid params with unsupported delimiter.
		{
			bucket:     "mybucket",
			prefix:     "prefix",
			keyMarker:  "notmatchingmarker",
			delimiter:  "unsupported",
			maxKeys:    "10",
			statusCode: getAPIError(ErrNotImplemented).HTTPStatusCode,
		},
		// 8. Invalid params with invalid max Keys
		{
			bucket:     "mybucket",
			prefix:     "prefix",
			keyMarker:  "prefix11",
			delimiter:  "/",
			maxKeys:    "999999999999999999999999999",
			statusCode: getAPIError(ErrInvalidMaxUploads).HTTPStatusCode,
		},
	}

	for i, test := range testCases {
		queryVal := mkListUploadsHealQuery(test.bucket, test.prefix, test.keyMarker, "", test.delimiter, test.maxKeys)

		req, err := buildAdminRequest(queryVal, "list-uploads", http.MethodGet, 0, nil)
		if err != nil {
			t.Fatalf("Test %d - Failed to construct list uploads needing heal request - %v", i+1, err)
		}

		rec := httptest.NewRecorder()
		adminTestBed.mux.ServeHTTP(rec, req)

		if test.statusCode != rec.Code {
			t.Errorf("Test %d - Expected HTTP status code %d but received %d", i+1, test.statusCode, rec.Code)
		}

		// Compare result with the expected one only when we receive 200 OK
		if rec.Code == http.StatusOK {
			resp := rec.Result()
			xmlBytes, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				t.Errorf("Test %d: Failed to read response %v", i+1, err)
			}

			var actualResult ListMultipartUploadsResponse
			err = xml.Unmarshal(xmlBytes, &actualResult)
			if err != nil {
				t.Errorf("Test %d: Failed to unmarshal xml %v", i+1, err)
			}

			if !reflect.DeepEqual(test.expectedResp, actualResult) {
				t.Fatalf("Test %d: Unexpected response `%+v`, expected: `%+v`", i+1, test.expectedResp, actualResult)
			}
		}

	}
}

// Test for newHealResult helper function.
func TestNewHealResult(t *testing.T) {
	testCases := []struct {
		healedDisks  int
		offlineDisks int
		state        healState
	}{
		// 1. No disks healed, no disks offline.
		{0, 0, healNone},
		// 2. No disks healed, non-zero disks offline.
		{0, 1, healNone},
		// 3. Non-zero disks healed, no disks offline.
		{1, 0, healOK},
		// 4. Non-zero disks healed, non-zero disks offline.
		{1, 1, healPartial},
	}

	for i, test := range testCases {
		actual := newHealResult(test.healedDisks, test.offlineDisks)
		if actual.State != test.state {
			t.Errorf("Test %d: Expected %v but received %v", i+1,
				test.state, actual.State)
		}
	}
}

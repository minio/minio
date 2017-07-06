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
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	router "github.com/gorilla/mux"
	"github.com/minio/minio/pkg/auth"
	"github.com/minio/minio/pkg/errors"
	"github.com/minio/minio/pkg/madmin"
)

var (
	configJSON = []byte(`{
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
)

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

	// Set globalIsXL to indicate that the setup uses an erasure
	// code backend.
	globalIsXL = true

	// initialize NSLock.
	isDistXL := false
	initNSLock(isDistXL)

	// Init global heal state
	initAllHealState(globalIsXL)

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

func (atb *adminXLTestBed) GenerateHealTestData(t *testing.T) {
	// Create an object myobject under bucket mybucket.
	bucketName := "mybucket"
	err := atb.objLayer.MakeBucketWithLocation(bucketName, "")
	if err != nil {
		t.Fatalf("Failed to make bucket %s - %v", bucketName,
			err)
	}

	// create some objects
	{
		objName := "myobject"
		for i := 0; i < 10; i++ {
			objectName := fmt.Sprintf("%s-%d", objName, i)
			_, err = atb.objLayer.PutObject(bucketName, objectName,
				mustGetHashReader(t, bytes.NewReader([]byte("hello")),
					int64(len("hello")), "", ""), nil)
			if err != nil {
				t.Fatalf("Failed to create %s - %v", objectName,
					err)
			}
		}
	}

	// create a multipart upload (incomplete)
	{
		objName := "mpObject"
		uploadID, err := atb.objLayer.NewMultipartUpload(bucketName,
			objName, nil)
		if err != nil {
			t.Fatalf("mp new error: %v", err)
		}

		_, err = atb.objLayer.PutObjectPart(bucketName, objName,
			uploadID, 3, mustGetHashReader(t, bytes.NewReader(
				[]byte("hello")), int64(len("hello")), "", ""))
		if err != nil {
			t.Fatalf("mp put error: %v", err)
		}

	}
}

func (atb *adminXLTestBed) CleanupHealTestData(t *testing.T) {
	bucketName := "mybucket"
	objName := "myobject"
	for i := 0; i < 10; i++ {
		atb.objLayer.DeleteObject(bucketName,
			fmt.Sprintf("%s-%d", objName, i))
	}

	atb.objLayer.DeleteBucket(bucketName)
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

func TestAdminVersionHandler(t *testing.T) {
	adminTestBed, err := prepareAdminXLTestBed()
	if err != nil {
		t.Fatal("Failed to initialize a single node XL backend for admin handler tests.")
	}
	defer adminTestBed.TearDown()

	req, err := newTestRequest("GET", "/minio/admin/version", 0, nil)
	if err != nil {
		t.Fatalf("Failed to construct request - %v", err)
	}
	cred := globalServerConfig.GetCredential()
	err = signRequestV4(req, cred.AccessKey, cred.SecretKey)
	if err != nil {
		t.Fatalf("Failed to sign request - %v", err)
	}

	rec := httptest.NewRecorder()
	adminTestBed.mux.ServeHTTP(rec, req)
	if http.StatusOK != rec.Code {
		t.Errorf("Unexpected status code - got %d but expected %d",
			rec.Code, http.StatusOK)
	}

	var result madmin.AdminAPIVersionInfo
	err = json.NewDecoder(rec.Body).Decode(&result)
	if err != nil {
		t.Errorf("json parse err: %v", err)
	}

	if result != adminAPIVersionInfo {
		t.Errorf("unexpected version: %v", result)
	}
}

// cmdType - Represents different service subcomands like status, stop
// and restart.
type cmdType int

const (
	statusCmd cmdType = iota
	restartCmd
	stopCmd
	setCreds
)

// String - String representation for cmdType
func (c cmdType) String() string {
	switch c {
	case statusCmd:
		return "status"
	case restartCmd:
		return "restart"
	case stopCmd:
		return "stop"
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
	case stopCmd:
		return "POST"
	case setCreds:
		return "PUT"
	}
	return "GET"
}

// apiEndpoint - Return endpoint for each admin REST API mapped to a
// command here.
func (c cmdType) apiEndpoint() string {
	switch c {
	case statusCmd, restartCmd, stopCmd:
		return "/minio/admin/v1/service"
	case setCreds:
		return "/minio/admin/v1/config/credential"
	}
	return ""
}

// toServiceSignal - Helper function that translates a given cmdType
// value to its corresponding serviceSignal value.
func (c cmdType) toServiceSignal() serviceSignal {
	switch c {
	case statusCmd:
		return serviceStatus
	case restartCmd:
		return serviceRestart
	case stopCmd:
		return serviceStop
	}
	return serviceStatus
}

func (c cmdType) toServiceActionValue() madmin.ServiceActionValue {
	switch c {
	case restartCmd:
		return madmin.ServiceActionValueRestart
	case stopCmd:
		return madmin.ServiceActionValueStop
	}
	return madmin.ServiceActionValueStop
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
	req, err := newTestRequest(cmd.apiMethod(), cmd.apiEndpoint(), 0, nil)
	if err != nil {
		return nil, err
	}

	// Set body
	req.Body = ioutil.NopCloser(bytes.NewReader(body))
	// Set sha-sum header
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

	body, err := json.Marshal(madmin.ServiceAction{
		cmd.toServiceActionValue()})
	if err != nil {
		t.Fatalf("JSONify error: %v", err)
	}

	req, err := getServiceCmdRequest(cmd, credentials, body)
	if err != nil {
		t.Fatalf("Failed to build service status request %v", err)
	}

	rec := httptest.NewRecorder()
	adminTestBed.mux.ServeHTTP(rec, req)

	if cmd == statusCmd {
		expectedInfo := madmin.ServiceStatus{
			ServerVersion: madmin.ServerVersion{Version: Version, CommitID: CommitID},
		}
		receivedInfo := madmin.ServiceStatus{}
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

	testCases := []struct {
		AccessKey          string
		SecretKey          string
		EnvKeysSet         bool
		ExpectedStatusCode int
	}{
		// Bad secret key
		{"minio", "minio", false, http.StatusBadRequest},
		// Bad  secret key set from the env
		{"minio", "minio", true, http.StatusMethodNotAllowed},
		// Good keys set from the env
		{"minio", "minio123", true, http.StatusMethodNotAllowed},
		// Successful operation should be the last one to
		// not change server credentials during tests.
		{"minio", "minio123", false, http.StatusOK},
	}
	for i, testCase := range testCases {
		// Set or unset environement keys
		globalIsEnvCreds = testCase.EnvKeysSet

		// Construct setCreds request body
		body, err := json.Marshal(madmin.SetCredsReq{
			AccessKey: testCase.AccessKey,
			SecretKey: testCase.SecretKey})
		if err != nil {
			t.Fatalf("JSONify err: %v", err)
		}
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
			if cred.AccessKey != testCase.AccessKey {
				t.Errorf("Test %d: Wrong access key, expected = %s, found = %s", i+1, testCase.AccessKey, cred.AccessKey)
			}
			if cred.SecretKey != testCase.SecretKey {
				t.Errorf("Test %d: Wrong secret key, expected = %s, found = %s", i+1, testCase.SecretKey, cred.SecretKey)
			}
		}
	}
}

// mkLockQueryVal - helper function to build lock query param.
func mkLockQueryVal(bucket, prefix, durationStr string) url.Values {
	qVal := url.Values{}
	qVal.Set(string(mgmtBucket), bucket)
	qVal.Set(string(mgmtPrefix), prefix)
	qVal.Set(string(mgmtLockOlderThan), durationStr)
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
		req, err := newTestRequest("GET", "/minio/admin/v1/locks?"+queryVal.Encode(), 0, nil)
		if err != nil {
			t.Fatalf("Test %d - Failed to construct list locks request - %v", i+1, err)
		}

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
		req, err := newTestRequest("DELETE", "/minio/admin/v1/locks?"+queryVal.Encode(), 0, nil)
		if err != nil {
			t.Fatalf("Test %d - Failed to construct clear locks request - %v", i+1, err)
		}

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

// buildAdminRequest - helper function to build an admin API request.
func buildAdminRequest(queryVal url.Values, method, path string,
	contentLength int64, bodySeeker io.ReadSeeker) (*http.Request, error) {

	req, err := newTestRequest(method,
		"/minio/admin/v1"+path+"?"+queryVal.Encode(),
		contentLength, bodySeeker)
	if err != nil {
		return nil, errors.Trace(err)
	}

	cred := globalServerConfig.GetCredential()
	err = signRequestV4(req, cred.AccessKey, cred.SecretKey)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return req, nil
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

	req, err := buildAdminRequest(queryVal, http.MethodGet, "/config", 0, nil)
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

	req, err := buildAdminRequest(queryVal, http.MethodPut, "/config",
		int64(len(configJSON)), bytes.NewReader(configJSON))
	if err != nil {
		t.Fatalf("Failed to construct set-config object request - %v", err)
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

	// Check that a very large config file returns an error.
	{
		// Make a large enough config string
		invalidCfg := []byte(strings.Repeat("A", maxConfigJSONSize+1))
		req, err := buildAdminRequest(queryVal, http.MethodPut, "/config",
			int64(len(invalidCfg)), bytes.NewReader(invalidCfg))
		if err != nil {
			t.Fatalf("Failed to construct set-config object request - %v", err)
		}

		rec := httptest.NewRecorder()
		adminTestBed.mux.ServeHTTP(rec, req)
		respBody := string(rec.Body.Bytes())
		if rec.Code != http.StatusBadRequest ||
			!strings.Contains(respBody, "Configuration data provided exceeds the allowed maximum of") {
			t.Errorf("Got unexpected response code or body %d - %s", rec.Code, respBody)
		}
	}

	// Check that a config with duplicate keys in an object return
	// error.
	{
		invalidCfg := append(configJSON[:len(configJSON)-1], []byte(`, "version": "15"}`)...)
		req, err := buildAdminRequest(queryVal, http.MethodPut, "/config",
			int64(len(invalidCfg)), bytes.NewReader(invalidCfg))
		if err != nil {
			t.Fatalf("Failed to construct set-config object request - %v", err)
		}

		rec := httptest.NewRecorder()
		adminTestBed.mux.ServeHTTP(rec, req)
		respBody := string(rec.Body.Bytes())
		if rec.Code != http.StatusBadRequest ||
			!strings.Contains(respBody, "JSON configuration provided has objects with duplicate keys") {
			t.Errorf("Got unexpected response code or body %d - %s", rec.Code, respBody)
		}
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

	req, err := buildAdminRequest(queryVal, http.MethodGet, "/info", 0, nil)
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

func mkHealStartReq(t *testing.T, bucket, prefix string,
	opts madmin.HealOpts) *http.Request {

	body, err := json.Marshal(opts)
	if err != nil {
		t.Fatalf("Unable marshal heal opts")
	}

	path := fmt.Sprintf("/minio/admin/v1/heal/%s", bucket)
	if bucket != "" && prefix != "" {
		path += "/" + prefix
	}

	req, err := newTestRequest("POST", path,
		int64(len(body)), bytes.NewReader(body))
	if err != nil {
		t.Fatalf("Failed to construct request - %v", err)
	}
	cred := globalServerConfig.GetCredential()
	err = signRequestV4(req, cred.AccessKey, cred.SecretKey)
	if err != nil {
		t.Fatalf("Failed to sign request - %v", err)
	}

	return req
}

func mkHealStatusReq(t *testing.T, bucket, prefix,
	clientToken string) *http.Request {

	path := fmt.Sprintf("/minio/admin/v1/heal/%s", bucket)
	if bucket != "" && prefix != "" {
		path += "/" + prefix
	}
	path += fmt.Sprintf("?clientToken=%s", clientToken)

	req, err := newTestRequest("POST", path, 0, nil)
	if err != nil {
		t.Fatalf("Failed to construct request - %v", err)
	}
	cred := globalServerConfig.GetCredential()
	err = signRequestV4(req, cred.AccessKey, cred.SecretKey)
	if err != nil {
		t.Fatalf("Failed to sign request - %v", err)
	}

	return req
}

func collectHealResults(t *testing.T, adminTestBed *adminXLTestBed, bucket,
	prefix, clientToken string, timeLimitSecs int) madmin.HealTaskStatus {

	var res, cur madmin.HealTaskStatus

	// loop and fetch heal status. have a time-limit to loop over
	// all statuses.
	timeLimit := UTCNow().Add(time.Second * time.Duration(timeLimitSecs))
	for cur.Summary != healStoppedStatus && cur.Summary != healFinishedStatus {
		if UTCNow().After(timeLimit) {
			t.Fatalf("heal-status loop took too long - clientToken: %s", clientToken)
		}
		req := mkHealStatusReq(t, bucket, prefix, clientToken)
		rec := httptest.NewRecorder()
		adminTestBed.mux.ServeHTTP(rec, req)
		if http.StatusOK != rec.Code {
			t.Errorf("Unexpected status code - got %d but expected %d",
				rec.Code, http.StatusOK)
			break
		}
		err := json.NewDecoder(rec.Body).Decode(&cur)
		if err != nil {
			t.Errorf("unable to unmarshal resp: %v", err)
			break
		}

		// all results are accumulated into a slice
		// and returned to caller in the end
		allItems := append(res.Items, cur.Items...)
		res = cur
		res.Items = allItems

		time.Sleep(time.Millisecond * 200)
	}

	return res
}

func TestHealStartNStatusHandler(t *testing.T) {
	adminTestBed, err := prepareAdminXLTestBed()
	if err != nil {
		t.Fatal("Failed to initialize a single node XL backend for admin handler tests.")
	}
	defer adminTestBed.TearDown()

	// gen. test data
	adminTestBed.GenerateHealTestData(t)
	defer adminTestBed.CleanupHealTestData(t)

	// Prepare heal-start request to send to the server.
	healOpts := madmin.HealOpts{
		Recursive: true,
		DryRun:    false,
	}
	bucketName, objName := "mybucket", "myobject-0"
	var hss madmin.HealStartSuccess

	{
		req := mkHealStartReq(t, bucketName, objName, healOpts)
		rec := httptest.NewRecorder()
		adminTestBed.mux.ServeHTTP(rec, req)
		if http.StatusOK != rec.Code {
			t.Errorf("Unexpected status code - got %d but expected %d",
				rec.Code, http.StatusOK)
		}

		err = json.Unmarshal(rec.Body.Bytes(), &hss)
		if err != nil {
			t.Fatal("unable to unmarshal response")
		}

		if hss.ClientToken == "" {
			t.Errorf("unexpected result")
		}
	}

	{
		// test with an invalid client token
		req := mkHealStatusReq(t, bucketName, objName, hss.ClientToken+hss.ClientToken)
		rec := httptest.NewRecorder()
		adminTestBed.mux.ServeHTTP(rec, req)
		if rec.Code != http.StatusBadRequest {
			t.Errorf("Unexpected status code")
		}
	}

	{
		// fetch heal status
		results := collectHealResults(t, adminTestBed, bucketName,
			objName, hss.ClientToken, 5)

		// check if we got back an expected record
		foundIt := false
		for _, item := range results.Items {
			if item.Type == madmin.HealItemObject &&
				item.Bucket == bucketName && item.Object == objName {
				foundIt = true
			}
		}
		if !foundIt {
			t.Error("did not find expected heal record in heal results")
		}

		// check that the heal settings in the results is the
		// same as what we started the heal seq. with.
		if results.HealSettings != healOpts {
			t.Errorf("unexpected heal settings: %v",
				results.HealSettings)
		}

		if results.Summary == healStoppedStatus {
			t.Errorf("heal sequence stopped unexpectedly")
		}
	}
}

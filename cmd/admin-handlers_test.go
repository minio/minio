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
	"encoding/json"
	"fmt"
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
	stopCmd
	restartCmd
)

// String - String representation for cmdType
func (c cmdType) String() string {
	switch c {
	case statusCmd:
		return "status"
	case stopCmd:
		return "stop"
	case restartCmd:
		return "restart"
	}
	return ""
}

// apiMethod - Returns the HTTP method corresponding to the admin REST
// API for a given cmdType value.
func (c cmdType) apiMethod() string {
	switch c {
	case statusCmd:
		return "GET"
	case stopCmd:
		return "POST"
	case restartCmd:
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
	case stopCmd:
		return serviceStop
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
func getServiceCmdRequest(cmd cmdType, cred credential) (*http.Request, error) {
	req, err := newTestRequest(cmd.apiMethod(), "/?service", 0, nil)
	if err != nil {
		return nil, err
	}

	// minioAdminOpHeader is to identify the request as a
	// management REST API request.
	req.Header.Set(minioAdminOpHeader, cmd.String())

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
		objLayer, fsDir, fsErr := prepareFS()
		if fsErr != nil {
			t.Fatalf("failed to initialize XL based object layer - %v.", fsErr)
		}
		defer removeRoots([]string{fsDir})
		globalObjLayerMutex.Lock()
		globalObjectAPI = objLayer
		globalObjLayerMutex.Unlock()
	}

	// Setting up a go routine to simulate ServerMux's
	// handleServiceSignals for stop and restart commands.
	switch cmd {
	case stopCmd, restartCmd:
		go testServiceSignalReceiver(cmd, t)
	}
	credentials := serverConfig.GetCredential()
	adminRouter := router.NewRouter()
	registerAdminRouter(adminRouter)

	rec := httptest.NewRecorder()
	req, err := getServiceCmdRequest(cmd, credentials)
	if err != nil {
		t.Fatalf("Failed to build service status request %v", err)
	}
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

	if rec.Code != http.StatusOK {
		t.Errorf("Expected to receive %d status code but received %d",
			http.StatusOK, rec.Code)
	}
}

// Test for service status management REST API.
func TestServiceStatusHandler(t *testing.T) {
	testServicesCmdHandler(statusCmd, t)
}

// Test for service stop management REST API.
func TestServiceStopHandler(t *testing.T) {
	testServicesCmdHandler(stopCmd, t)
}

// Test for service restart management REST API.
func TestServiceRestartHandler(t *testing.T) {
	testServicesCmdHandler(restartCmd, t)
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
			expectedStatus: 200,
		},
		// Test 2 - invalid duration
		{
			bucket:         "mybucket",
			prefix:         "myprefix",
			relTime:        "invalidDuration",
			expectedStatus: 400,
		},
		// Test 3 - invalid bucket name
		{
			bucket:         `invalid\\Bucket`,
			prefix:         "myprefix",
			relTime:        "1h",
			expectedStatus: 400,
		},
		// Test 4 - invalid prefix
		{
			bucket:         "mybucket",
			prefix:         `invalid\\Prefix`,
			relTime:        "1h",
			expectedStatus: 400,
		},
	}

	adminRouter := router.NewRouter()
	registerAdminRouter(adminRouter)

	for i, test := range testCases {
		queryStr := fmt.Sprintf("&bucket=%s&prefix=%s&older-than=%s", test.bucket, test.prefix, test.relTime)
		req, err := newTestRequest("GET", "/?lock"+queryStr, 0, nil)
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
			expectedStatus: 200,
		},
		// Test 2 - invalid duration
		{
			bucket:         "mybucket",
			prefix:         "myprefix",
			relTime:        "invalidDuration",
			expectedStatus: 400,
		},
		// Test 3 - invalid bucket name
		{
			bucket:         `invalid\\Bucket`,
			prefix:         "myprefix",
			relTime:        "1h",
			expectedStatus: 400,
		},
		// Test 4 - invalid prefix
		{
			bucket:         "mybucket",
			prefix:         `invalid\\Prefix`,
			relTime:        "1h",
			expectedStatus: 400,
		},
	}

	adminRouter := router.NewRouter()
	registerAdminRouter(adminRouter)

	for i, test := range testCases {
		queryStr := fmt.Sprintf("&bucket=%s&prefix=%s&older-than=%s", test.bucket, test.prefix, test.relTime)
		req, err := newTestRequest("POST", "/?lock"+queryStr, 0, nil)
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
	allValidVal := url.Values{}
	allValidVal.Set(string(lockBucket), "bucket")
	allValidVal.Set(string(lockPrefix), "prefix")
	allValidVal.Set(string(lockOlderThan), "1s")

	invalidBucketVal := url.Values{}
	invalidBucketVal.Set(string(lockBucket), `invalid\\Bucket`)
	invalidBucketVal.Set(string(lockPrefix), "prefix")
	invalidBucketVal.Set(string(lockOlderThan), "invalidDuration")

	invalidPrefixVal := url.Values{}
	invalidPrefixVal.Set(string(lockBucket), "bucket")
	invalidPrefixVal.Set(string(lockPrefix), `invalid\\PRefix`)
	invalidPrefixVal.Set(string(lockOlderThan), "invalidDuration")

	invalidOlderThanVal := url.Values{}
	invalidOlderThanVal.Set(string(lockBucket), "bucket")
	invalidOlderThanVal.Set(string(lockPrefix), "prefix")
	invalidOlderThanVal.Set(string(lockOlderThan), "invalidDuration")

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

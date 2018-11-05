/*
 * Minio Cloud Storage, (C) 2016, 2017, 2018 Minio, Inc.
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
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"github.com/minio/minio/pkg/auth"
	"github.com/minio/minio/pkg/madmin"
)

var (
	configJSON = []byte(`{
  "version": "31",
  "credential": {
    "accessKey": "minio",
    "secretKey": "minio123"
  },
  "region": "us-east-1",
  "worm": "off",
  "storageclass": {
    "standard": "",
    "rrs": ""
  },
  "cache": {
    "drives": [],
    "expiry": 90,
    "maxuse": 80,
    "exclude": []
  },
  "kms": {
    "vault": {
      "endpoint": "",
      "auth": {
        "type": "",
        "approle": {
          "id": "",
          "secret": ""
        }
      },
      "key-id": {
        "name": "",
        "version": 0
      }
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
        "deliveryMode": 0,
        "mandatory": false,
        "immediate": false,
        "durable": false,
        "internal": false,
        "noWait": false,
        "autoDeleted": false
      }
    },
    "elasticsearch": {
      "1": {
        "enable": false,
        "format": "namespace",
        "url": "",
        "index": ""
      }
    },
    "kafka": {
      "1": {
        "enable": false,
        "brokers": null,
        "topic": "",
        "tls": {
          "enable": false,
          "skipVerify": false,
          "clientAuth": 0
        },
        "sasl": {
          "enable": false,
          "username": "",
          "password": ""
        }
      }
    },
    "mqtt": {
      "1": {
        "enable": false,
        "broker": "",
        "topic": "",
        "qos": 0,
        "clientId": "",
        "username": "",
        "password": "",
        "reconnectInterval": 0,
        "keepAliveInterval": 0
      }
    },
    "mysql": {
      "1": {
        "enable": false,
        "format": "namespace",
        "dsnString": "",
        "table": "",
        "host": "",
        "port": "",
        "user": "",
        "password": "",
        "database": ""
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
    "postgresql": {
      "1": {
        "enable": false,
        "format": "namespace",
        "connectionString": "",
        "table": "",
        "host": "",
        "port": "",
        "user": "",
        "password": "",
        "database": ""
      }
    },
    "redis": {
      "1": {
        "enable": false,
        "format": "namespace",
        "address": "",
        "password": "",
        "key": ""
      }
    },
    "webhook": {
      "1": {
        "enable": false,
        "endpoint": ""
      }
    }
  },
  "logger": {
    "console": {
      "enabled": true
    },
    "http": {
      "1": {
        "enabled": false,
        "endpoint": "https://username:password@example.com/api"
      }
    }
  },
  "compress": {
    "enabled": false,
    "extensions":[".txt",".log",".csv",".json"],
    "mime-types":["text/csv","text/plain","application/json"]
  },
  "openid": {
    "jwks": {
      "url": ""
    }
  },
  "policy": {
    "opa": {
      "url": "",
      "authToken": ""
    }
  }
}
`)
)

// adminXLTestBed - encapsulates subsystems that need to be setup for
// admin-handler unit tests.
type adminXLTestBed struct {
	configPath string
	xlDirs     []string
	objLayer   ObjectLayer
	router     *mux.Router
}

// prepareAdminXLTestBed - helper function that setups a single-node
// XL backend for admin-handler tests.
func prepareAdminXLTestBed() (*adminXLTestBed, error) {
	// reset global variables to start afresh.
	resetTestGlobals()

	// Initializing objectLayer for HealFormatHandler.
	objLayer, xlDirs, xlErr := initTestXLObjLayer()
	if xlErr != nil {
		return nil, xlErr
	}

	// Initialize minio server config.
	if err := newTestConfig(globalMinioDefaultRegion, objLayer); err != nil {
		return nil, err
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

	globalNotificationSys = NewNotificationSys(globalServerConfig, globalEndpoints)

	// Create new policy system.
	globalPolicySys = NewPolicySys()

	// Setup admin mgmt REST API handlers.
	adminRouter := mux.NewRouter()
	registerAdminRouter(adminRouter)

	return &adminXLTestBed{
		xlDirs:   xlDirs,
		objLayer: objLayer,
		router:   adminRouter,
	}, nil
}

// TearDown - method that resets the test bed for subsequent unit
// tests to start afresh.
func (atb *adminXLTestBed) TearDown() {
	removeRoots(atb.xlDirs)
	resetTestGlobals()
}

func (atb *adminXLTestBed) GenerateHealTestData(t *testing.T) {
	// Create an object myobject under bucket mybucket.
	bucketName := "mybucket"
	err := atb.objLayer.MakeBucketWithLocation(context.Background(), bucketName, "")
	if err != nil {
		t.Fatalf("Failed to make bucket %s - %v", bucketName,
			err)
	}

	// create some objects
	{
		objName := "myobject"
		for i := 0; i < 10; i++ {
			objectName := fmt.Sprintf("%s-%d", objName, i)
			_, err = atb.objLayer.PutObject(context.Background(), bucketName, objectName,
				mustGetHashReader(t, bytes.NewReader([]byte("hello")),
					int64(len("hello")), "", ""), nil, ObjectOptions{})
			if err != nil {
				t.Fatalf("Failed to create %s - %v", objectName,
					err)
			}
		}
	}

	// create a multipart upload (incomplete)
	{
		objName := "mpObject"
		uploadID, err := atb.objLayer.NewMultipartUpload(context.Background(), bucketName,
			objName, nil, ObjectOptions{})
		if err != nil {
			t.Fatalf("mp new error: %v", err)
		}

		_, err = atb.objLayer.PutObjectPart(context.Background(), bucketName, objName,
			uploadID, 3, mustGetHashReader(t, bytes.NewReader(
				[]byte("hello")), int64(len("hello")), "", ""), ObjectOptions{})
		if err != nil {
			t.Fatalf("mp put error: %v", err)
		}

	}
}

func (atb *adminXLTestBed) CleanupHealTestData(t *testing.T) {
	bucketName := "mybucket"
	objName := "myobject"
	for i := 0; i < 10; i++ {
		atb.objLayer.DeleteObject(context.Background(), bucketName,
			fmt.Sprintf("%s-%d", objName, i))
	}

	atb.objLayer.DeleteBucket(context.Background(), bucketName)
}

// initTestObjLayer - Helper function to initialize an XL-based object
// layer and set globalObjectAPI.
func initTestXLObjLayer() (ObjectLayer, []string, error) {
	xlDirs, err := getRandomDisks(16)
	if err != nil {
		return nil, nil, err
	}
	endpoints := mustGetNewEndpointList(xlDirs...)
	format, err := waitForFormatXL(context.Background(), true, endpoints, 1, 16)
	if err != nil {
		removeRoots(xlDirs)
		return nil, nil, err
	}

	globalPolicySys = NewPolicySys()
	objLayer, err := newXLSets(endpoints, format, 1, 16)
	if err != nil {
		return nil, nil, err
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
	adminTestBed.router.ServeHTTP(rec, req)
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
	req.ContentLength = int64(len(body))

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

	var wg sync.WaitGroup

	// Setting up a go routine to simulate ServerRouter's
	// handleServiceSignals for stop and restart commands.
	if cmd == restartCmd {
		wg.Add(1)
		go func() {
			defer wg.Done()
			testServiceSignalReceiver(cmd, t)
		}()
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
	adminTestBed.router.ServeHTTP(rec, req)

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

	// Wait until testServiceSignalReceiver() called in a goroutine quits.
	wg.Wait()
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

		ebody, err := madmin.EncryptData(credentials.SecretKey, body)
		if err != nil {
			t.Fatal(err)
		}

		// Construct setCreds request
		req, err := getServiceCmdRequest(setCreds, credentials, ebody)
		if err != nil {
			t.Fatalf("Failed to build service status request %v", err)
		}

		rec := httptest.NewRecorder()

		// Execute request
		adminTestBed.router.ServeHTTP(rec, req)

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

// buildAdminRequest - helper function to build an admin API request.
func buildAdminRequest(queryVal url.Values, method, path string,
	contentLength int64, bodySeeker io.ReadSeeker) (*http.Request, error) {

	req, err := newTestRequest(method,
		"/minio/admin/v1"+path+"?"+queryVal.Encode(),
		contentLength, bodySeeker)
	if err != nil {
		return nil, err
	}

	cred := globalServerConfig.GetCredential()
	err = signRequestV4(req, cred.AccessKey, cred.SecretKey)
	if err != nil {
		return nil, err
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
	adminTestBed.router.ServeHTTP(rec, req)
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

	// Prepare query params for set-config mgmt REST API.
	queryVal := url.Values{}
	queryVal.Set("config", "")

	password := globalServerConfig.GetCredential().SecretKey
	econfigJSON, err := madmin.EncryptData(password, configJSON)
	if err != nil {
		t.Fatal(err)
	}

	req, err := buildAdminRequest(queryVal, http.MethodPut, "/config",
		int64(len(econfigJSON)), bytes.NewReader(econfigJSON))
	if err != nil {
		t.Fatalf("Failed to construct set-config object request - %v", err)
	}

	rec := httptest.NewRecorder()
	adminTestBed.router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("Expected to succeed but failed with %d", rec.Code)
	}

	// Check that a very large config file returns an error.
	{
		// Make a large enough config string
		invalidCfg := []byte(strings.Repeat("A", maxEConfigJSONSize+1))
		req, err := buildAdminRequest(queryVal, http.MethodPut, "/config",
			int64(len(invalidCfg)), bytes.NewReader(invalidCfg))
		if err != nil {
			t.Fatalf("Failed to construct set-config object request - %v", err)
		}

		rec := httptest.NewRecorder()
		adminTestBed.router.ServeHTTP(rec, req)
		respBody := string(rec.Body.Bytes())
		if rec.Code != http.StatusBadRequest ||
			!strings.Contains(respBody, "Configuration data provided exceeds the allowed maximum of") {
			t.Errorf("Got unexpected response code or body %d - %s", rec.Code, respBody)
		}
	}

	// Check that a config with duplicate keys in an object return
	// error.
	{
		invalidCfg := append(econfigJSON[:len(econfigJSON)-1], []byte(`, "version": "15"}`)...)
		req, err := buildAdminRequest(queryVal, http.MethodPut, "/config",
			int64(len(invalidCfg)), bytes.NewReader(invalidCfg))
		if err != nil {
			t.Fatalf("Failed to construct set-config object request - %v", err)
		}

		rec := httptest.NewRecorder()
		adminTestBed.router.ServeHTTP(rec, req)
		respBody := string(rec.Body.Bytes())
		if rec.Code != http.StatusBadRequest ||
			!strings.Contains(respBody, "JSON configuration provided is of incorrect format") {
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
	adminTestBed.router.ServeHTTP(rec, req)
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
		adminTestBed.router.ServeHTTP(rec, req)
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
		adminTestBed.router.ServeHTTP(rec, req)
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
		adminTestBed.router.ServeHTTP(rec, req)
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

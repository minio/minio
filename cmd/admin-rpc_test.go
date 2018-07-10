/*
 * Minio Cloud Storage, (C) 2018 Minio, Inc.
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
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	xnet "github.com/minio/minio/pkg/net"
)

///////////////////////////////////////////////////////////////////////////////
//
// localAdminClient and AdminRPCClient are adminCmdRunner interface compatible,
// hence below test functions are available for both clients.
//
///////////////////////////////////////////////////////////////////////////////

///////////////////////////////////////////////////////////////////////////////
//
// Admin RPC server, adminRPCReceiver and AdminRPCClient are
// inter-dependent, below test functions are sufficient to test all of them.
//
///////////////////////////////////////////////////////////////////////////////

func testAdminCmdRunnerSignalService(t *testing.T, client adminCmdRunner) {
	tmpGlobalServiceSignalCh := globalServiceSignalCh
	globalServiceSignalCh = make(chan serviceSignal, 10)
	defer func() {
		globalServiceSignalCh = tmpGlobalServiceSignalCh
	}()

	testCases := []struct {
		signal    serviceSignal
		expectErr bool
	}{
		{serviceRestart, false},
		{serviceStop, false},
		{serviceStatus, true},
		{serviceSignal(100), true},
	}

	for i, testCase := range testCases {
		err := client.SignalService(testCase.signal)
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			t.Fatalf("case %v: expected: %v, got: %v", i+1, testCase.expectErr, expectErr)
		}
	}
}

func testAdminCmdRunnerReInitFormat(t *testing.T, client adminCmdRunner) {
	tmpGlobalObjectAPI := globalObjectAPI
	defer func() {
		globalObjectAPI = tmpGlobalObjectAPI
	}()

	testCases := []struct {
		objectAPI ObjectLayer
		dryRun    bool
		expectErr bool
	}{
		{&DummyObjectLayer{}, true, false},
		{&DummyObjectLayer{}, false, false},
		{nil, true, true},
		{nil, false, true},
	}

	for i, testCase := range testCases {
		globalObjectAPI = testCase.objectAPI
		err := client.ReInitFormat(testCase.dryRun)
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			t.Fatalf("case %v: expected: %v, got: %v", i+1, testCase.expectErr, expectErr)
		}
	}
}

func testAdminCmdRunnerListLocks(t *testing.T, client adminCmdRunner) {
	tmpGlobalObjectAPI := globalObjectAPI
	defer func() {
		globalObjectAPI = tmpGlobalObjectAPI
	}()

	testCases := []struct {
		objectAPI ObjectLayer
		expectErr bool
	}{
		{&DummyObjectLayer{}, false},
		{nil, true},
	}

	for i, testCase := range testCases {
		globalObjectAPI = testCase.objectAPI
		_, err := client.ListLocks("", "", time.Duration(0))
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			t.Fatalf("case %v: expected: %v, got: %v", i+1, testCase.expectErr, expectErr)
		}
	}
}

func testAdminCmdRunnerServerInfo(t *testing.T, client adminCmdRunner) {
	tmpGlobalBootTime := globalBootTime
	tmpGlobalObjectAPI := globalObjectAPI
	tmpGlobalConnStats := globalConnStats
	tmpGlobalHTTPStats := globalHTTPStats
	tmpGlobalNotificationSys := globalNotificationSys
	defer func() {
		globalBootTime = tmpGlobalBootTime
		globalObjectAPI = tmpGlobalObjectAPI
		globalConnStats = tmpGlobalConnStats
		globalHTTPStats = tmpGlobalHTTPStats
		globalNotificationSys = tmpGlobalNotificationSys
	}()

	endpoints := new(EndpointList)

	notificationSys := NewNotificationSys(globalServerConfig, *endpoints)

	testCases := []struct {
		bootTime        time.Time
		objectAPI       ObjectLayer
		connStats       *ConnStats
		httpStats       *HTTPStats
		notificationSys *NotificationSys
		expectErr       bool
	}{
		{UTCNow(), &DummyObjectLayer{}, newConnStats(), newHTTPStats(), notificationSys, false},
		{time.Time{}, nil, nil, nil, nil, true},
		{UTCNow(), nil, nil, nil, nil, true},
	}

	for i, testCase := range testCases {
		globalBootTime = testCase.bootTime
		globalObjectAPI = testCase.objectAPI
		globalConnStats = testCase.connStats
		globalHTTPStats = testCase.httpStats
		globalNotificationSys = testCase.notificationSys
		_, err := client.ServerInfo()
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			t.Fatalf("case %v: expected: %v, got: %v", i+1, testCase.expectErr, expectErr)
		}
	}
}

func testAdminCmdRunnerGetConfig(t *testing.T, client adminCmdRunner) {
	tmpGlobalServerConfig := globalServerConfig
	defer func() {
		globalServerConfig = tmpGlobalServerConfig
	}()

	config := newServerConfig()

	testCases := []struct {
		config    *serverConfig
		expectErr bool
	}{
		{globalServerConfig, false},
		{config, false},
	}

	for i, testCase := range testCases {
		globalServerConfig = testCase.config
		_, err := client.GetConfig()
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			t.Fatalf("case %v: expected: %v, got: %v", i+1, testCase.expectErr, expectErr)
		}
	}
}

func testAdminCmdRunnerWriteTmpConfig(t *testing.T, client adminCmdRunner) {
	tmpConfigDir := configDir
	defer func() {
		configDir = tmpConfigDir
	}()

	tempDir, err := ioutil.TempDir("", ".AdminCmdRunnerWriteTmpConfig.")
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	defer os.RemoveAll(tempDir)
	configDir = &ConfigDir{dir: tempDir}

	testCases := []struct {
		tmpFilename string
		configBytes []byte
		expectErr   bool
	}{
		{"config1.json", []byte(`{"version":"23","region":"us-west-1a"}`), false},
		// Overwrite test.
		{"config1.json", []byte(`{"version":"23","region":"us-west-1a","browser":"on"}`), false},
		{"config2.json", []byte{}, false},
		{"config3.json", nil, false},
	}

	for i, testCase := range testCases {
		err := client.WriteTmpConfig(testCase.tmpFilename, testCase.configBytes)
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			t.Fatalf("case %v: expected: %v, got: %v", i+1, testCase.expectErr, expectErr)
		}
	}
}

func testAdminCmdRunnerCommitConfig(t *testing.T, client adminCmdRunner) {
	tmpConfigDir := configDir
	defer func() {
		configDir = tmpConfigDir
	}()

	tempDir, err := ioutil.TempDir("", ".AdminCmdRunnerCommitConfig.")
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	defer os.RemoveAll(tempDir)
	configDir = &ConfigDir{dir: tempDir}
	err = ioutil.WriteFile(filepath.Join(tempDir, "config.json"), []byte{}, os.ModePerm)
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}

	err = client.WriteTmpConfig("config1.json", []byte(`{"version":"23","region":"us-west-1a"}`))
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}

	testCases := []struct {
		tmpFilename string
		expectErr   bool
	}{
		{"config1.json", false},
		{"config2.json", true},
	}

	for i, testCase := range testCases {
		err := client.CommitConfig(testCase.tmpFilename)
		expectErr := (err != nil)

		if expectErr != testCase.expectErr {
			t.Fatalf("case %v: expected: %v, got: %v", i+1, testCase.expectErr, expectErr)
		}
	}
}

func newAdminRPCHTTPServerClient(t *testing.T) (*httptest.Server, *AdminRPCClient, *serverConfig) {
	rpcServer, err := NewAdminRPCServer()
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}

	httpServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rpcServer.ServeHTTP(w, r)
	}))

	url, err := xnet.ParseURL(httpServer.URL)
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}

	host, err := xnet.ParseHost(url.Host)
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}

	prevGlobalServerConfig := globalServerConfig
	globalServerConfig = newServerConfig()

	rpcClient, err := NewAdminRPCClient(host)
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}

	return httpServer, rpcClient, prevGlobalServerConfig
}

func TestAdminRPCClientSignalService(t *testing.T) {
	httpServer, rpcClient, prevGlobalServerConfig := newAdminRPCHTTPServerClient(t)
	defer httpServer.Close()
	defer func() {
		globalServerConfig = prevGlobalServerConfig
	}()

	testAdminCmdRunnerSignalService(t, rpcClient)
}

func TestAdminRPCClientReInitFormat(t *testing.T) {
	httpServer, rpcClient, prevGlobalServerConfig := newAdminRPCHTTPServerClient(t)
	defer httpServer.Close()
	defer func() {
		globalServerConfig = prevGlobalServerConfig
	}()

	testAdminCmdRunnerReInitFormat(t, rpcClient)
}

func TestAdminRPCClientListLocks(t *testing.T) {
	httpServer, rpcClient, prevGlobalServerConfig := newAdminRPCHTTPServerClient(t)
	defer httpServer.Close()
	defer func() {
		globalServerConfig = prevGlobalServerConfig
	}()

	testAdminCmdRunnerListLocks(t, rpcClient)
}

func TestAdminRPCClientServerInfo(t *testing.T) {
	httpServer, rpcClient, prevGlobalServerConfig := newAdminRPCHTTPServerClient(t)
	defer httpServer.Close()
	defer func() {
		globalServerConfig = prevGlobalServerConfig
	}()

	testAdminCmdRunnerServerInfo(t, rpcClient)
}

func TestAdminRPCClientGetConfig(t *testing.T) {
	httpServer, rpcClient, prevGlobalServerConfig := newAdminRPCHTTPServerClient(t)
	defer httpServer.Close()
	defer func() {
		globalServerConfig = prevGlobalServerConfig
	}()

	testAdminCmdRunnerGetConfig(t, rpcClient)
}

func TestAdminRPCClientWriteTmpConfig(t *testing.T) {
	httpServer, rpcClient, prevGlobalServerConfig := newAdminRPCHTTPServerClient(t)
	defer httpServer.Close()
	defer func() {
		globalServerConfig = prevGlobalServerConfig
	}()

	testAdminCmdRunnerWriteTmpConfig(t, rpcClient)
}

func TestAdminRPCClientCommitConfig(t *testing.T) {
	httpServer, rpcClient, prevGlobalServerConfig := newAdminRPCHTTPServerClient(t)
	defer httpServer.Close()
	defer func() {
		globalServerConfig = prevGlobalServerConfig
	}()

	testAdminCmdRunnerCommitConfig(t, rpcClient)
}

var (
	config1 = []byte(`{
	"version": "13",
	"credential": {
		"accessKey": "minio",
		"secretKey": "minio123"
	},
	"region": "us-east-1",
	"logger": {
		"console": {
			"enable": true,
			"level": "debug"
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
}
`)
	// diff from config1 - amqp.Enable is True
	config2 = []byte(`{
	"version": "13",
	"credential": {
		"accessKey": "minio",
		"secretKey": "minio123"
	},
	"region": "us-east-1",
	"logger": {
		"console": {
			"enable": true,
			"level": "debug"
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
				"enable": true,
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
}
`)
)

// TestGetValidServerConfig - test for getValidServerConfig.
func TestGetValidServerConfig(t *testing.T) {
	var c1, c2 serverConfig
	err := json.Unmarshal(config1, &c1)
	if err != nil {
		t.Fatalf("json unmarshal of %s failed: %v", string(config1), err)
	}

	err = json.Unmarshal(config2, &c2)
	if err != nil {
		t.Fatalf("json unmarshal of %s failed: %v", string(config2), err)
	}

	// Valid config.
	noErrs := []error{nil, nil, nil, nil}
	serverConfigs := []serverConfig{c1, c2, c1, c1}
	validConfig, err := getValidServerConfig(serverConfigs, noErrs)
	if err != nil {
		t.Errorf("Expected a valid config but received %v instead", err)
	}

	if !reflect.DeepEqual(validConfig, c1) {
		t.Errorf("Expected valid config to be %v but received %v", config1, validConfig)
	}

	// Invalid config - no quorum.
	serverConfigs = []serverConfig{c1, c2, c2, c1}
	_, err = getValidServerConfig(serverConfigs, noErrs)
	if err != errXLWriteQuorum {
		t.Errorf("Expected to fail due to lack of quorum but received %v", err)
	}

	// All errors
	allErrs := []error{errDiskNotFound, errDiskNotFound, errDiskNotFound, errDiskNotFound}
	serverConfigs = []serverConfig{{}, {}, {}, {}}
	_, err = getValidServerConfig(serverConfigs, allErrs)
	if err != errXLWriteQuorum {
		t.Errorf("Expected to fail due to lack of quorum but received %v", err)
	}
}

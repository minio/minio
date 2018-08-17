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
	"net/http"
	"net/http/httptest"
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

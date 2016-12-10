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
	"net/http"
	"net/http/httptest"
	"testing"

	router "github.com/gorilla/mux"
)

type cmdType int

const (
	statusCmd cmdType = iota
	stopCmd
	restartCmd
)

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

func serviceSignalReceiver(cmd cmdType, t *testing.T) {
	expectedCmd := cmd.toServiceSignal()
	serviceCmd := <-globalServiceSignalCh
	if serviceCmd != expectedCmd {
		t.Errorf("Expected service command %v but received %v", expectedCmd, serviceCmd)
	}
}

func getAdminCmdRequest(cmd cmdType, cred credential) (*http.Request, error) {
	req, err := newTestRequest(cmd.apiMethod(), "/?service", 0, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set(minioAdminOpHeader, cmd.String())
	err = signRequestV4(req, cred.AccessKeyID, cred.SecretAccessKey)
	if err != nil {
		return nil, err
	}
	return req, nil
}

func testServicesCmdHandler(cmd cmdType, t *testing.T) {
	rootPath, err := newTestConfig("us-east-1")
	if err != nil {
		t.Fatalf("Unable to initialize server config. %s", err)
	}
	defer removeAll(rootPath)

	// Setting up a go routine to simulate ServerMux's handleServiceSignals
	go serviceSignalReceiver(cmd, t)
	credentials := serverConfig.GetCredential()
	adminRouter := router.NewRouter()
	registerAdminRouter(adminRouter)

	rec := httptest.NewRecorder()
	req, err := getAdminCmdRequest(cmd, credentials)
	if err != nil {
		t.Fatalf("Failed to build service status request %v", err)
	}
	adminRouter.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("Expected to receive %d status code but received %d", http.StatusOK, rec.Code)
	}
}

func TestServiceStatusHandler(t *testing.T) {
	testServicesCmdHandler(statusCmd, t)
}

func TestServiceStopHandler(t *testing.T) {
	testServicesCmdHandler(stopCmd, t)
}

func TestServiceRestartHandler(t *testing.T) {
	testServicesCmdHandler(restartCmd, t)
}

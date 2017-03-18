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
	"encoding/json"
	"net/url"
	"testing"
)

func testAdminCmd(cmd cmdType, t *testing.T) {
	// reset globals.
	// this is to make sure that the tests are not affected by modified globals.
	resetTestGlobals()

	rootPath, err := newTestConfig(globalMinioDefaultRegion)
	if err != nil {
		t.Fatalf("Failed to create test config - %v", err)
	}
	defer removeAll(rootPath)

	adminServer := adminCmd{}
	creds := serverConfig.GetCredential()
	args := LoginRPCArgs{
		Username:    creds.AccessKey,
		Password:    creds.SecretKey,
		Version:     Version,
		RequestTime: UTCNow(),
	}
	reply := LoginRPCReply{}
	err = adminServer.Login(&args, &reply)
	if err != nil {
		t.Fatalf("Failed to login to admin server - %v", err)
	}

	go func() {
		// A test signal receiver
		<-globalServiceSignalCh
	}()

	ga := AuthRPCArgs{AuthToken: reply.AuthToken}
	genReply := AuthRPCReply{}
	switch cmd {
	case restartCmd:
		if err = adminServer.Restart(&ga, &genReply); err != nil {
			t.Errorf("restartCmd: Expected: <nil>, got: %v", err)
		}
	}
}

// TestAdminRestart - test for Admin.Restart RPC service.
func TestAdminRestart(t *testing.T) {
	testAdminCmd(restartCmd, t)
}

// TestReInitDisks - test for Admin.ReInitDisks RPC service.
func TestReInitDisks(t *testing.T) {
	// Reset global variables to start afresh.
	resetTestGlobals()

	rootPath, err := newTestConfig("us-east-1")
	if err != nil {
		t.Fatalf("Unable to initialize server config. %s", err)
	}
	defer removeAll(rootPath)

	// Initializing objectLayer for HealFormatHandler.
	_, xlDirs, xlErr := initTestXLObjLayer()
	if xlErr != nil {
		t.Fatalf("failed to initialize XL based object layer - %v.", xlErr)
	}
	defer removeRoots(xlDirs)

	// Set globalEndpoints for a single node XL setup.
	for _, xlDir := range xlDirs {
		globalEndpoints = append(globalEndpoints, &url.URL{Path: xlDir})
	}

	// Setup admin rpc server for an XL backend.
	globalIsXL = true
	adminServer := adminCmd{}
	creds := serverConfig.GetCredential()
	args := LoginRPCArgs{
		Username:    creds.AccessKey,
		Password:    creds.SecretKey,
		Version:     Version,
		RequestTime: UTCNow(),
	}
	reply := LoginRPCReply{}
	err = adminServer.Login(&args, &reply)
	if err != nil {
		t.Fatalf("Failed to login to admin server - %v", err)
	}

	authArgs := AuthRPCArgs{
		AuthToken: reply.AuthToken,
	}
	authReply := AuthRPCReply{}

	err = adminServer.ReInitDisks(&authArgs, &authReply)
	if err != nil {
		t.Errorf("Expected to pass, but failed with %v", err)
	}

	// Negative test case with admin rpc server setup for FS.
	globalIsXL = false
	fsAdminServer := adminCmd{}
	fsArgs := LoginRPCArgs{
		Username:    creds.AccessKey,
		Password:    creds.SecretKey,
		Version:     Version,
		RequestTime: UTCNow(),
	}
	fsReply := LoginRPCReply{}
	err = fsAdminServer.Login(&fsArgs, &fsReply)
	if err != nil {
		t.Fatalf("Failed to login to fs admin server - %v", err)
	}

	authArgs = AuthRPCArgs{
		AuthToken: fsReply.AuthToken,
	}
	authReply = AuthRPCReply{}
	// Attempt ReInitDisks service on a FS backend.
	err = fsAdminServer.ReInitDisks(&authArgs, &authReply)
	if err != errUnsupportedBackend {
		t.Errorf("Expected to fail with %v, but received %v",
			errUnsupportedBackend, err)
	}
}

// TestGetConfig - Test for GetConfig admin RPC.
func TestGetConfig(t *testing.T) {
	// Reset global variables to start afresh.
	resetTestGlobals()

	rootPath, err := newTestConfig("us-east-1")
	if err != nil {
		t.Fatalf("Unable to initialize server config. %s", err)
	}
	defer removeAll(rootPath)

	adminServer := adminCmd{}
	creds := serverConfig.GetCredential()
	args := LoginRPCArgs{
		Username:    creds.AccessKey,
		Password:    creds.SecretKey,
		Version:     Version,
		RequestTime: UTCNow(),
	}
	reply := LoginRPCReply{}
	err = adminServer.Login(&args, &reply)
	if err != nil {
		t.Fatalf("Failed to login to admin server - %v", err)
	}

	authArgs := AuthRPCArgs{
		AuthToken: reply.AuthToken,
	}

	configReply := ConfigReply{}

	err = adminServer.GetConfig(&authArgs, &configReply)
	if err != nil {
		t.Errorf("Expected GetConfig to pass but failed with %v", err)
	}

	var config serverConfigV13
	err = json.Unmarshal(configReply.Config, &config)
	if err != nil {
		t.Errorf("Expected json unmarshal to pass but failed with %v", err)
	}
}

// TestWriteAndCommitConfig - test for WriteTmpConfig and CommitConfig
// RPC handler.
func TestWriteAndCommitConfig(t *testing.T) {
	// Reset global variables to start afresh.
	resetTestGlobals()

	rootPath, err := newTestConfig("us-east-1")
	if err != nil {
		t.Fatalf("Unable to initialize server config. %s", err)
	}
	defer removeAll(rootPath)

	adminServer := adminCmd{}
	creds := serverConfig.GetCredential()
	args := LoginRPCArgs{
		Username:    creds.AccessKey,
		Password:    creds.SecretKey,
		Version:     Version,
		RequestTime: UTCNow(),
	}
	reply := LoginRPCReply{}
	err = adminServer.Login(&args, &reply)
	if err != nil {
		t.Fatalf("Failed to login to admin server - %v", err)
	}

	// Write temporary config.
	buf := []byte("hello")
	tmpFileName := mustGetUUID()
	wArgs := WriteConfigArgs{
		AuthRPCArgs: AuthRPCArgs{
			AuthToken: reply.AuthToken,
		},
		TmpFileName: tmpFileName,
		Buf:         buf,
	}

	err = adminServer.WriteTmpConfig(&wArgs, &WriteConfigReply{})
	if err != nil {
		t.Fatalf("Failed to write temporary config %v", err)
	}

	if err != nil {
		t.Errorf("Expected to succeed but failed %v", err)
	}

	cArgs := CommitConfigArgs{
		AuthRPCArgs: AuthRPCArgs{
			AuthToken: reply.AuthToken,
		},
		FileName: tmpFileName,
	}
	cReply := CommitConfigReply{}

	err = adminServer.CommitConfig(&cArgs, &cReply)
	if err != nil {
		t.Fatalf("Failed to commit config file %v", err)
	}
}

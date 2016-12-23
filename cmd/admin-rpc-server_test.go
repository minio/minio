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
	"testing"
	"time"
)

func testAdminCmd(cmd cmdType, t *testing.T) {
	rootPath, err := newTestConfig("us-east-1")
	if err != nil {
		t.Fatalf("Failed to create test config - %v", err)
	}
	defer removeAll(rootPath)

	adminServer := serviceCmd{}
	creds := serverConfig.GetCredential()
	args := LoginRPCArgs{
		Username:    creds.AccessKey,
		Password:    creds.SecretKey,
		Version:     Version,
		RequestTime: time.Now().UTC(),
	}
	reply := LoginRPCReply{}
	err = adminServer.Login(&args, &reply)
	if err != nil {
		t.Fatalf("Failed to login to admin server - %v", err)
	}

	go func() {
		// mocking signal receiver
		<-globalServiceSignalCh
	}()

	ga := AuthRPCArgs{AuthToken: reply.AuthToken, RequestTime: time.Now().UTC()}
	genReply := AuthRPCReply{}
	switch cmd {
	case stopCmd:
		if err = adminServer.Shutdown(&ga, &genReply); err != nil {
			t.Errorf("stopCmd: Expected: <nil>, got: %v", err)
		}
	case restartCmd:
		if err = adminServer.Restart(&ga, &genReply); err != nil {
			t.Errorf("restartCmd: Expected: <nil>, got: %v", err)
		}
	}
}

func TestAdminShutdown(t *testing.T) {
	testAdminCmd(stopCmd, t)
}

func TestAdminRestart(t *testing.T) {
	testAdminCmd(restartCmd, t)
}

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
	// reset globals.
	// this is to make sure that the tests are not affected by modified globals.
	resetTestGlobals()

	rootPath, err := newTestConfig("us-east-1")
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
		RequestTime: time.Now().UTC(),
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

	ga := AuthRPCArgs{AuthToken: reply.AuthToken, RequestTime: time.Now().UTC()}
	genReply := AuthRPCReply{}
	switch cmd {
	case restartCmd:
		if err = adminServer.Restart(&ga, &genReply); err != nil {
			t.Errorf("restartCmd: Expected: <nil>, got: %v", err)
		}
	}
}

func TestAdminRestart(t *testing.T) {
	testAdminCmd(restartCmd, t)
}

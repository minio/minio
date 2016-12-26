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
	reply := RPCLoginReply{}
	args := RPCLoginArgs{Username: creds.AccessKey, Password: creds.SecretKey}
	err = adminServer.LoginHandler(&args, &reply)
	if err != nil {
		t.Fatalf("Failed to login to admin server - %v", err)
	}

	go func() {
		// mocking signal receiver
		<-globalServiceSignalCh
	}()

	validToken := reply.Token
	timeNow := time.Now().UTC()
	testCases := []struct {
		ga          GenericArgs
		expectedErr error
	}{
		// Valid case
		{
			ga:          GenericArgs{Token: validToken, Timestamp: timeNow},
			expectedErr: nil,
		},
		// Invalid token
		{
			ga:          GenericArgs{Token: "invalidToken", Timestamp: timeNow},
			expectedErr: errInvalidToken,
		},
	}

	genReply := GenericReply{}
	for i, test := range testCases {
		switch cmd {
		case stopCmd:
			err = adminServer.Shutdown(&test.ga, &genReply)
			if err != test.expectedErr {
				t.Errorf("Test %d: Expected error %v but received %v", i+1, test.expectedErr, err)
			}
		case restartCmd:
			err = adminServer.Restart(&test.ga, &genReply)
			if err != test.expectedErr {
				t.Errorf("Test %d: Expected error %v but received %v", i+1, test.expectedErr, err)
			}
		}
	}
}

func TestAdminShutdown(t *testing.T) {
	testAdminCmd(stopCmd, t)
}

func TestAdminRestart(t *testing.T) {
	testAdminCmd(restartCmd, t)
}

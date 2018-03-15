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
	"os"
	"testing"
	"time"
)

func TestLogin(t *testing.T) {
	rootPath, err := newTestConfig(globalMinioDefaultRegion)
	if err != nil {
		t.Fatalf("Failed to create test config - %v", err)
	}
	defer os.RemoveAll(rootPath)
	creds := globalServerConfig.GetCredential()
	token, err := authenticateNode(creds.AccessKey, creds.SecretKey)
	if err != nil {
		t.Fatal(err)
	}
	ls := AuthRPCServer{}
	testCases := []struct {
		args        LoginRPCArgs
		skewTime    time.Duration
		expectedErr error
	}{
		// Valid case.
		{
			args: LoginRPCArgs{
				AuthToken: token,
				Version:   globalRPCAPIVersion,
			},
			skewTime:    0,
			expectedErr: nil,
		},
		// Valid username, password and request time, not version.
		{
			args: LoginRPCArgs{
				AuthToken: token,
				Version:   semVersion{1, 0, 0},
			},
			skewTime:    0,
			expectedErr: errRPCAPIVersionUnsupported,
		},
		// Valid username, password and version, not request time
		{
			args: LoginRPCArgs{
				AuthToken: token,
				Version:   globalRPCAPIVersion,
			},
			skewTime:    20 * time.Minute,
			expectedErr: errServerTimeMismatch,
		},
		// Invalid token, fails with authentication error
		{
			args: LoginRPCArgs{
				AuthToken: "",
				Version:   globalRPCAPIVersion,
			},
			skewTime:    0,
			expectedErr: errAuthentication,
		},
	}
	for i, test := range testCases {
		reply := LoginRPCReply{}
		test.args.RequestTime = UTCNow().Add(test.skewTime)
		err := ls.Login(&test.args, &reply)
		if err != test.expectedErr {
			t.Errorf("Test %d: Expected error %v but received %v",
				i+1, test.expectedErr, err)
		}
	}
}

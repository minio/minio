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
	creds := serverConfig.GetCredential()
	ls := AuthRPCServer{}
	testCases := []struct {
		args        LoginRPCArgs
		skewTime    time.Duration
		expectedErr error
	}{
		// Valid case.
		{
			args: LoginRPCArgs{
				Username: creds.AccessKey,
				Password: creds.SecretKey,
				Version:  Version,
			},
			skewTime:    0,
			expectedErr: nil,
		},
		// Valid username, password and request time, not version.
		{
			args: LoginRPCArgs{
				Username: creds.AccessKey,
				Password: creds.SecretKey,
				Version:  "INVALID-" + Version,
			},
			skewTime:    0,
			expectedErr: errServerVersionMismatch,
		},
		// Valid username, password and version, not request time
		{
			args: LoginRPCArgs{
				Username: creds.AccessKey,
				Password: creds.SecretKey,
				Version:  Version,
			},
			skewTime:    20 * time.Minute,
			expectedErr: errServerTimeMismatch,
		},
		// Invalid username length
		{
			args: LoginRPCArgs{
				Username: "aaa",
				Password: "minio123",
				Version:  Version,
			},
			skewTime:    0,
			expectedErr: errInvalidAccessKeyLength,
		},
		// Invalid password length
		{
			args: LoginRPCArgs{
				Username: "minio",
				Password: "aaa",
				Version:  Version,
			},
			skewTime:    0,
			expectedErr: errInvalidSecretKeyLength,
		},
		// Invalid username
		{
			args: LoginRPCArgs{
				Username: "aaaaa",
				Password: creds.SecretKey,
				Version:  Version,
			},
			skewTime:    0,
			expectedErr: errInvalidAccessKeyID,
		},
		// Invalid password
		{
			args: LoginRPCArgs{
				Username: creds.AccessKey,
				Password: "aaaaaaaa",
				Version:  Version,
			},
			skewTime:    0,
			expectedErr: errAuthentication,
		},
	}
	for i, test := range testCases {
		reply := LoginRPCReply{}
		test.args.RequestTime = time.Now().Add(test.skewTime).UTC()
		err := ls.Login(&test.args, &reply)
		if err != test.expectedErr {
			t.Errorf("Test %d: Expected error %v but received %v",
				i+1, test.expectedErr, err)
		}
	}
}

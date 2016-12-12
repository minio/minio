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

import "testing"

func TestLoginHandler(t *testing.T) {
	rootPath, err := newTestConfig("us-east-1")
	if err != nil {
		t.Fatalf("Failed to create test config - %v", err)
	}
	defer removeAll(rootPath)
	creds := serverConfig.GetCredential()
	ls := loginServer{}
	testCases := []struct {
		args        RPCLoginArgs
		expectedErr error
	}{
		// Valid username and password
		{
			args:        RPCLoginArgs{Username: creds.AccessKeyID, Password: creds.SecretAccessKey},
			expectedErr: nil,
		},
		// Invalid username length
		{
			args:        RPCLoginArgs{Username: "aaa", Password: "minio123"},
			expectedErr: errInvalidAccessKeyLength,
		},
		// Invalid password length
		{
			args:        RPCLoginArgs{Username: "minio", Password: "aaa"},
			expectedErr: errInvalidSecretKeyLength,
		},
		// Invalid username
		{
			args:        RPCLoginArgs{Username: "aaaaa", Password: creds.SecretAccessKey},
			expectedErr: errInvalidAccessKeyID,
		},
		// Invalid password
		{
			args:        RPCLoginArgs{Username: creds.AccessKeyID, Password: "aaaaaaaa"},
			expectedErr: errAuthentication,
		},
	}
	for i, test := range testCases {
		reply := RPCLoginReply{}
		err := ls.LoginHandler(&test.args, &reply)
		if err != test.expectedErr {
			t.Errorf("Test %d: Expected error %v but received %v",
				i+1, test.expectedErr, err)
		}
	}
}

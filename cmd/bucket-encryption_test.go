/*
 * MinIO Cloud Storage, (C) 2020 MinIO, Inc.
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
	"bytes"
	"errors"
	"testing"
)

func TestValidateBucketSSEConfig(t *testing.T) {
	testCases := []struct {
		inputXML    string
		expectedErr error
		shouldPass  bool
	}{
		// MinIO supported XML
		{
			inputXML: `<ServerSideEncryptionConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
			<Rule>
			<ApplyServerSideEncryptionByDefault>
			<SSEAlgorithm>AES256</SSEAlgorithm>
			</ApplyServerSideEncryptionByDefault>
			</Rule>
			</ServerSideEncryptionConfiguration>`,
			expectedErr: nil,
			shouldPass:  true,
		},
		// Unsupported XML
		{
			inputXML: `<ServerSideEncryptionConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
			<Rule>
			<ApplyServerSideEncryptionByDefault>
                        <SSEAlgorithm>aws:kms</SSEAlgorithm>
                        <KMSMasterKeyID>arn:aws:kms:us-east-1:1234/5678example</KMSMasterKeyID>
			</ApplyServerSideEncryptionByDefault>
			</Rule>
			</ServerSideEncryptionConfiguration>`,
			expectedErr: errors.New("Unsupported bucket encryption configuration"),
			shouldPass:  false,
		},
	}

	for i, tc := range testCases {
		_, err := validateBucketSSEConfig(bytes.NewReader([]byte(tc.inputXML)))
		if tc.shouldPass && err != nil {
			t.Fatalf("Test case %d: Expected to succeed but got %s", i+1, err)
		}

		if !tc.shouldPass {
			if err == nil || err != nil && err.Error() != tc.expectedErr.Error() {
				t.Fatalf("Test case %d: Expected %s but got %s", i+1, tc.expectedErr, err)
			}
		}
	}
}

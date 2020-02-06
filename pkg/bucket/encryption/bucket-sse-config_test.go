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
	"encoding/xml"
	"errors"
	"testing"
)

// TestParseBucketSSEConfig performs basic sanity tests on ParseBucketSSEConfig
func TestParseBucketSSEConfig(t *testing.T) {
	testCases := []struct {
		inputXML    string
		expectedErr error
		shouldPass  bool
	}{
		// 1. Valid XML SSE-S3
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
		// 2. Valid XML SSE-KMS
		{
			inputXML: `<ServerSideEncryptionConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
			<Rule>
			<ApplyServerSideEncryptionByDefault>
                        <SSEAlgorithm>aws:kms</SSEAlgorithm>
                        <KMSMasterKeyID>arn:aws:kms:us-east-1:1234/5678example</KMSMasterKeyID>
			</ApplyServerSideEncryptionByDefault>
			</Rule>
			</ServerSideEncryptionConfiguration>`,
			expectedErr: nil,
			shouldPass:  true,
		},
		// 3. Invalid - more than one rule
		{
			inputXML: `<ServerSideEncryptionConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
			<Rule>
			<ApplyServerSideEncryptionByDefault>
			<SSEAlgorithm>AES256</SSEAlgorithm>
			</ApplyServerSideEncryptionByDefault>
			</Rule>
			<Rule>
			<ApplyServerSideEncryptionByDefault>
			<SSEAlgorithm>AES256</SSEAlgorithm>
			</ApplyServerSideEncryptionByDefault>
			</Rule>
			</ServerSideEncryptionConfiguration>`,
			expectedErr: errors.New("Only one server-side encryption rule is allowed"),
			shouldPass:  false,
		},
		// 4. Invalid XML - master key ID present in AES256
		{
			inputXML: `<ServerSideEncryptionConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
			<Rule>
			<ApplyServerSideEncryptionByDefault>
			<SSEAlgorithm>AES256</SSEAlgorithm>
                        <KMSMasterKeyID>arn:aws:kms:us-east-1:1234/5678example</KMSMasterKeyID>
			</ApplyServerSideEncryptionByDefault>
			</Rule>
			</ServerSideEncryptionConfiguration>`,
			expectedErr: errors.New("MasterKeyID is allowed with aws:kms only"),
			shouldPass:  false,
		},
		// 5. Invalid XML - master key ID not found in aws:kms algorithm
		{
			inputXML: `<ServerSideEncryptionConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
			<Rule>
			<ApplyServerSideEncryptionByDefault>
			<SSEAlgorithm>aws:kms</SSEAlgorithm>
			</ApplyServerSideEncryptionByDefault>
			</Rule>
			</ServerSideEncryptionConfiguration>`,
			expectedErr: errors.New("MasterKeyID is missing"),
			shouldPass:  false,
		},
		// 6. Invalid Algorithm
		{
			inputXML: `<ServerSideEncryptionConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
			<Rule>
			<ApplyServerSideEncryptionByDefault>
			<SSEAlgorithm>InvalidAlgorithm</SSEAlgorithm>
			</ApplyServerSideEncryptionByDefault>
			</Rule>
			</ServerSideEncryptionConfiguration>`,
			expectedErr: errors.New("Unknown SSE algorithm"),
			shouldPass:  false,
		},
		// 7. Allow missing namespace
		{
			inputXML: `<ServerSideEncryptionConfiguration>
			<Rule>
			<ApplyServerSideEncryptionByDefault>
			<SSEAlgorithm>AES256</SSEAlgorithm>
			</ApplyServerSideEncryptionByDefault>
			</Rule>
			</ServerSideEncryptionConfiguration>`,
			expectedErr: nil,
			shouldPass:  true,
		},
	}

	actualConfig := &BucketSSEConfig{
		XMLName: xml.Name{
			Local: "ServerSideEncryptionConfiguration",
		},
		Rules: []SSERule{
			{
				DefaultEncryptionAction: EncryptionAction{
					Algorithm: AES256,
				},
			},
		},
	}

	for i, tc := range testCases {
		_, err := ParseBucketSSEConfig(bytes.NewReader([]byte(tc.inputXML)))
		if tc.shouldPass && err != nil {
			t.Fatalf("Test case %d: Expected to succeed but got %s", i+1, err)
		}

		if !tc.shouldPass {
			if err == nil || err != nil && err.Error() != tc.expectedErr.Error() {
				t.Fatalf("Test case %d: Expected %s but got %s", i+1, tc.expectedErr, err)
			}
		}

		if !tc.shouldPass {
			continue
		}

		if actualXML, err := xml.Marshal(actualConfig); err != nil && bytes.Equal(actualXML, []byte(tc.inputXML)) {
			t.Fatalf("Test case %d: Expected config %s but got %s", i+1, string(actualXML), tc.inputXML)
		}
	}
}

// Copyright (c) 2015-2021 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package cmd

import (
	"bytes"
	"encoding/xml"
	"errors"
	"testing"
)

// TestParseBucketSSEConfig performs basic sanity tests on ParseBucketSSEConfig
func TestParseBucketSSEConfig(t *testing.T) {
	actualAES256NoNSConfig := &BucketSSEConfig{
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

	actualAES256Config := &BucketSSEConfig{
		XMLNS: xmlNS,
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

	actualKMSConfig := &BucketSSEConfig{
		XMLNS: xmlNS,
		XMLName: xml.Name{
			Local: "ServerSideEncryptionConfiguration",
		},
		Rules: []SSERule{
			{
				DefaultEncryptionAction: EncryptionAction{
					Algorithm:   AWSKms,
					MasterKeyID: "arn:aws:kms:us-east-1:1234/5678example",
				},
			},
		},
	}

	testCases := []struct {
		inputXML       string
		expectedErr    error
		shouldPass     bool
		expectedConfig *BucketSSEConfig
	}{
		// 1. Valid XML SSE-S3
		{
			inputXML:       `<ServerSideEncryptionConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Rule><ApplyServerSideEncryptionByDefault><SSEAlgorithm>AES256</SSEAlgorithm></ApplyServerSideEncryptionByDefault></Rule></ServerSideEncryptionConfiguration>`,
			expectedErr:    nil,
			shouldPass:     true,
			expectedConfig: actualAES256Config,
		},
		// 2. Valid XML SSE-KMS
		{
			inputXML:       `<ServerSideEncryptionConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Rule><ApplyServerSideEncryptionByDefault><SSEAlgorithm>aws:kms</SSEAlgorithm><KMSMasterKeyID>arn:aws:kms:us-east-1:1234/5678example</KMSMasterKeyID></ApplyServerSideEncryptionByDefault></Rule></ServerSideEncryptionConfiguration>`,
			expectedErr:    nil,
			shouldPass:     true,
			expectedConfig: actualKMSConfig,
		},
		// 3. Invalid - more than one rule
		{
			inputXML:    `<ServerSideEncryptionConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Rule><ApplyServerSideEncryptionByDefault><SSEAlgorithm>AES256</SSEAlgorithm></ApplyServerSideEncryptionByDefault></Rule><Rule><ApplyServerSideEncryptionByDefault><SSEAlgorithm>AES256</SSEAlgorithm></ApplyServerSideEncryptionByDefault></Rule></ServerSideEncryptionConfiguration>`,
			expectedErr: errors.New("only one server-side encryption rule is allowed at a time"),
			shouldPass:  false,
		},
		// 4. Invalid XML - master key ID present along with AES256
		{
			inputXML:    `<ServerSideEncryptionConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Rule><ApplyServerSideEncryptionByDefault><SSEAlgorithm>AES256</SSEAlgorithm><KMSMasterKeyID>arn:aws:kms:us-east-1:1234/5678example</KMSMasterKeyID></ApplyServerSideEncryptionByDefault></Rule></ServerSideEncryptionConfiguration>`,
			expectedErr: errors.New("MasterKeyID is allowed with aws:kms only"),
			shouldPass:  false,
		},
		// 5. Invalid XML - master key ID not provided when algorithm is set to aws:kms algorithm
		{
			inputXML:    `<ServerSideEncryptionConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Rule><ApplyServerSideEncryptionByDefault><SSEAlgorithm>aws:kms</SSEAlgorithm></ApplyServerSideEncryptionByDefault></Rule></ServerSideEncryptionConfiguration>`,
			expectedErr: errors.New("MasterKeyID is missing with aws:kms"),
			shouldPass:  false,
		},
		// 6. Invalid Algorithm
		{
			inputXML:    `<ServerSideEncryptionConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Rule><ApplyServerSideEncryptionByDefault><SSEAlgorithm>InvalidAlgorithm</SSEAlgorithm></ApplyServerSideEncryptionByDefault></Rule></ServerSideEncryptionConfiguration>`,
			expectedErr: errors.New("Unknown SSE algorithm"),
			shouldPass:  false,
		},
		// 7. Valid XML without the namespace set
		{
			inputXML:       `<ServerSideEncryptionConfiguration><Rule><ApplyServerSideEncryptionByDefault><SSEAlgorithm>AES256</SSEAlgorithm></ApplyServerSideEncryptionByDefault></Rule></ServerSideEncryptionConfiguration>`,
			expectedErr:    nil,
			shouldPass:     true,
			expectedConfig: actualAES256NoNSConfig,
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
			continue
		}

		if expectedXML, err := xml.Marshal(tc.expectedConfig); err != nil || !bytes.Equal(expectedXML, []byte(tc.inputXML)) {
			t.Fatalf("Test case %d: Expected bucket encryption XML %s but got %s", i+1, string(expectedXML), tc.inputXML)
		}
	}
}

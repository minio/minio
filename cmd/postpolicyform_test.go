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
	"encoding/base64"
	"net/http"
	"strings"
	"testing"

	minio "github.com/minio/minio-go/v7"
	xhttp "github.com/minio/minio/internal/http"
)

func TestParsePostPolicyForm(t *testing.T) {
	testCases := []struct {
		policy  string
		success bool
	}{
		// missing expiration, will fail.
		{
			policy:  `{"conditions":[["eq","$bucket","asdf"],["eq","$key","hello.txt"]],"conditions":[["eq","$success_action_status","201"],["eq","$Content-Type","plain/text"],["eq","$success_action_status","201"],["eq","$x-amz-algorithm","AWS4-HMAC-SHA256"],["eq","$x-amz-credential","Q3AM3UQ867SPQQA43P2F/20210315/us-east-1/s3/aws4_request"],["eq","$x-amz-date","20210315T091621Z"]]}`,
			success: false,
		},
		// invalid json.
		{
			policy:  `{"conditions":[["eq","$bucket","asdf"],["eq","$key","hello.txt"]],"conditions":[["eq","$success_action_status","201"],["eq","$Content-Type","plain/text"],["eq","$success_action_status","201"],["eq","$x-amz-algorithm","AWS4-HMAC-SHA256"],["eq","$x-amz-credential","Q3AM3UQ867SPQQA43P2F/20210315/us-east-1/s3/aws4_request"],["eq","$x-amz-date","20210315T091621Z"]]`,
			success: false,
		},
		// duplicate 'expiration' reject
		{
			policy: `{"expiration":"2021-03-22T09:16:21.310Z","expiration":"2021-03-22T09:16:21.310Z","conditions":[["eq","$bucket","evil"],["eq","$key","hello.txt"],["eq","$success_action_status","201"],["eq","$Content-Type","plain/text"],["eq","$success_action_status","201"],["eq","$x-amz-algorithm","AWS4-HMAC-SHA256"],["eq","$x-amz-credential","Q3AM3UQ867SPQQA43P2F/20210315/us-east-1/s3/aws4_request"],["eq","$x-amz-date","20210315T091621Z"]]}`,
		},
		// duplicate '$bucket' reject
		{
			policy:  `{"expiration":"2021-03-22T09:16:21.310Z","conditions":[["eq","$bucket","good"],["eq","$key","hello.txt"]],"conditions":[["eq","$bucket","evil"],["eq","$key","hello.txt"],["eq","$success_action_status","201"],["eq","$Content-Type","plain/text"],["eq","$success_action_status","201"],["eq","$x-amz-algorithm","AWS4-HMAC-SHA256"],["eq","$x-amz-credential","Q3AM3UQ867SPQQA43P2F/20210315/us-east-1/s3/aws4_request"],["eq","$x-amz-date","20210315T091621Z"]]}`,
			success: false,
		},
		// duplicate conditions, reject
		{
			policy:  `{"expiration":"2021-03-22T09:16:21.310Z","conditions":[["eq","$bucket","asdf"],["eq","$key","hello.txt"]],"conditions":[["eq","$success_action_status","201"],["eq","$Content-Type","plain/text"],["eq","$success_action_status","201"],["eq","$x-amz-algorithm","AWS4-HMAC-SHA256"],["eq","$x-amz-credential","Q3AM3UQ867SPQQA43P2F/20210315/us-east-1/s3/aws4_request"],["eq","$x-amz-date","20210315T091621Z"]]}`,
			success: false,
		},
		// no duplicates, shall be parsed properly.
		{
			policy:  `{"expiration":"2021-03-27T20:35:28.458Z","conditions":[["eq","$bucket","testbucket"],["eq","$key","wtf.txt"],["eq","$x-amz-date","20210320T203528Z"],["eq","$x-amz-algorithm","AWS4-HMAC-SHA256"],["eq","$x-amz-credential","Q3AM3UQ867SPQQA43P2F/20210320/us-east-1/s3/aws4_request"]]}`,
			success: true,
		},
	}

	for _, testCase := range testCases {
		t.Run("", func(t *testing.T) {
			_, err := parsePostPolicyForm(strings.NewReader(testCase.policy))
			if testCase.success && err != nil {
				t.Errorf("Expected success but failed with %s", err)
			}
			if !testCase.success && err == nil {
				t.Errorf("Expected failed but succeeded")
			}
		})
	}
}

type formValues struct {
	http.Header
}

func newFormValues() formValues {
	return formValues{make(http.Header)}
}

func (f formValues) Set(key, value string) formValues {
	f.Header.Set(key, value)
	return f
}

func (f formValues) Add(key, value string) formValues {
	f.Header.Add(key, value)
	return f
}

func (f formValues) Clone() formValues {
	return formValues{f.Header.Clone()}
}

// Test Post Policy parsing and checking conditions
func TestPostPolicyForm(t *testing.T) {
	pp := minio.NewPostPolicy()
	pp.SetBucket("testbucket")
	pp.SetContentType("image/jpeg")
	pp.SetUserMetadata("uuid", "14365123651274")
	pp.SetKeyStartsWith("user/user1/filename")
	pp.SetContentLengthRange(100, 999999) // not testable from this layer, condition is checked in the API handler.
	pp.SetSuccessStatusAction("201")
	pp.SetCondition("eq", "X-Amz-Credential", "KVGKMDUQ23TCZXTLTHLP/20160727/us-east-1/s3/aws4_request")
	pp.SetCondition("eq", "X-Amz-Algorithm", "AWS4-HMAC-SHA256")
	pp.SetCondition("eq", xhttp.AmzDate, "20160727T000000Z")

	defaultFormVals := newFormValues()
	defaultFormVals.Set("Bucket", "testbucket")
	defaultFormVals.Set("Content-Type", "image/jpeg")
	defaultFormVals.Set(xhttp.AmzMetaUUID, "14365123651274")
	defaultFormVals.Set("Key", "user/user1/filename/${filename}/myfile.txt")
	defaultFormVals.Set("X-Amz-Credential", "KVGKMDUQ23TCZXTLTHLP/20160727/us-east-1/s3/aws4_request")
	defaultFormVals.Set("X-Amz-Algorithm", "AWS4-HMAC-SHA256")
	defaultFormVals.Set(xhttp.AmzDate, "20160727T000000Z")
	defaultFormVals.Set("Success_action_status", "201")

	policyCondFailedErr := "Invalid according to Policy: Policy Condition failed"

	type testCase struct {
		name    string
		fv      formValues
		expired bool
		wantErr string
	}

	// Test case just contains fields we override from defaultFormVals.
	testCases := []testCase{
		{
			name:    "happy path no errors",
			fv:      defaultFormVals.Clone(),
			wantErr: "",
		},
		{
			name:    "expired policy document",
			fv:      defaultFormVals.Clone(),
			expired: true,
			wantErr: "Invalid according to Policy: Policy expired",
		},
		{
			name:    "different AMZ date",
			fv:      defaultFormVals.Clone().Set(xhttp.AmzDate, "2017T000000Z"),
			wantErr: policyCondFailedErr,
		},
		{
			name:    "incorrect date",
			fv:      defaultFormVals.Clone().Set(xhttp.AmzDate, "incorrect"),
			wantErr: policyCondFailedErr,
		},
		{
			name:    "key which doesn't start with user/user1/filename",
			fv:      defaultFormVals.Clone().Set("Key", "myfile.txt"),
			wantErr: policyCondFailedErr,
		},
		{
			name:    "incorrect key name",
			fv:      defaultFormVals.Clone().Set("Key", "incorrect"),
			wantErr: policyCondFailedErr,
		},
		{
			name:    "incorrect bucket name",
			fv:      defaultFormVals.Clone().Set("Bucket", "incorrect"),
			wantErr: policyCondFailedErr,
		},
		{
			name:    "incorrect ContentType",
			fv:      defaultFormVals.Clone().Set(xhttp.ContentType, "incorrect"),
			wantErr: policyCondFailedErr,
		},
		{
			name:    "incorrect X-Amz-Algorithm",
			fv:      defaultFormVals.Clone().Set(xhttp.AmzAlgorithm, "incorrect"),
			wantErr: policyCondFailedErr,
		},
		{
			name:    "incorrect X-Amz-Credential",
			fv:      defaultFormVals.Clone().Set(xhttp.AmzCredential, "incorrect"),
			wantErr: policyCondFailedErr,
		},
		{
			name:    "incorrect metadata uuid",
			fv:      defaultFormVals.Clone().Set(xhttp.AmzMetaUUID, "151274"),
			wantErr: "Invalid according to Policy: Policy Condition failed: [eq, $x-amz-meta-uuid, 14365123651274]",
		},
		{
			name:    "unknown key XAmzMetaName is error as it does not appear in policy",
			fv:      defaultFormVals.Clone().Set(xhttp.AmzMetaName, "my-name"),
			wantErr: `Each form field that you specify in a form must appear in the list of policy conditions. "X-Amz-Meta-Name" not specified in the policy.`,
		},
		{
			name:    "unknown key XAmzChecksumAlgo is error as it does not appear in policy",
			fv:      defaultFormVals.Clone().Set(http.CanonicalHeaderKey(xhttp.AmzChecksumAlgo), "algo-val"),
			wantErr: `Each form field that you specify in a form must appear in the list of policy conditions. "X-Amz-Checksum-Algorithm" not specified in the policy.`,
		},
		{
			name:    "unknown key XAmzChecksumCRC32 is error as it does not appear in policy",
			fv:      defaultFormVals.Clone().Set(http.CanonicalHeaderKey(xhttp.AmzChecksumCRC32), "crc32-val"),
			wantErr: `Each form field that you specify in a form must appear in the list of policy conditions. "X-Amz-Checksum-Crc32" not specified in the policy.`,
		},
		{
			name:    "unknown key XAmzChecksumCRC32C is error as it does not appear in policy",
			fv:      defaultFormVals.Clone().Set(http.CanonicalHeaderKey(xhttp.AmzChecksumCRC32C), "crc32c-val"),
			wantErr: `Each form field that you specify in a form must appear in the list of policy conditions. "X-Amz-Checksum-Crc32c" not specified in the policy.`,
		},
		{
			name:    "unknown key XAmzChecksumSHA1 is error as it does not appear in policy",
			fv:      defaultFormVals.Clone().Set(http.CanonicalHeaderKey(xhttp.AmzChecksumSHA1), "sha1-val"),
			wantErr: `Each form field that you specify in a form must appear in the list of policy conditions. "X-Amz-Checksum-Sha1" not specified in the policy.`,
		},
		{
			name:    "unknown key XAmzChecksumSHA256 is error as it does not appear in policy",
			fv:      defaultFormVals.Clone().Set(http.CanonicalHeaderKey(xhttp.AmzChecksumSHA256), "sha256-val"),
			wantErr: `Each form field that you specify in a form must appear in the list of policy conditions. "X-Amz-Checksum-Sha256" not specified in the policy.`,
		},
		{
			name:    "unknown key XAmzChecksumMode is error as it does not appear in policy",
			fv:      defaultFormVals.Clone().Set(http.CanonicalHeaderKey(xhttp.AmzChecksumMode), "mode-val"),
			wantErr: `Each form field that you specify in a form must appear in the list of policy conditions. "X-Amz-Checksum-Mode" not specified in the policy.`,
		},
		{
			name:    "unknown key Content-Encoding is error as it does not appear in policy",
			fv:      defaultFormVals.Clone().Set(http.CanonicalHeaderKey(xhttp.ContentEncoding), "encoding-val"),
			wantErr: `Each form field that you specify in a form must appear in the list of policy conditions. "Content-Encoding" not specified in the policy.`,
		},
		{
			name:    "many bucket values",
			fv:      defaultFormVals.Clone().Add("Bucket", "anotherbucket"),
			wantErr: "Invalid according to Policy: Policy Condition failed: [eq, $bucket, testbucket]. FormValues have multiple values: [testbucket, anotherbucket]",
		},
		{
			name: "XAmzSignature does not have to appear in policy",
			fv:   defaultFormVals.Clone().Set(xhttp.AmzSignature, "my-signature"),
		},
		{
			name: "XIgnoreFoo does not have to appear in policy",
			fv:   defaultFormVals.Clone().Set("X-Ignore-Foo", "my-foo-value"),
		},
		{
			name: "File does not have to appear in policy",
			fv:   defaultFormVals.Clone().Set("File", "file-value"),
		},
		{
			name: "Signature does not have to appear in policy",
			fv:   defaultFormVals.Clone().Set(xhttp.AmzSignatureV2, "signature-value"),
		},
		{
			name: "AWSAccessKeyID does not have to appear in policy",
			fv:   defaultFormVals.Clone().Set(xhttp.AmzAccessKeyID, "access").Set(xhttp.AmzSignatureV2, "signature-value"),
		},
		{
			name: "any form value starting with X-Amz-Server-Side-Encryption- does not have to appear in policy",
			fv: defaultFormVals.Clone().
				Set(xhttp.AmzServerSideEncryptionKmsContext, "context-val").
				Set(xhttp.AmzServerSideEncryptionCustomerAlgorithm, "algo-val"),
		},
	}

	// Run tests
	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			if tt.expired {
				// Expired already.
				pp.SetExpires(UTCNow().AddDate(0, 0, -10))
			} else {
				// Expires in 10 days.
				pp.SetExpires(UTCNow().AddDate(0, 0, 10))
			}

			tt.fv.Set("Policy", base64.StdEncoding.EncodeToString([]byte(pp.String())))

			policyBytes, err := base64.StdEncoding.DecodeString(base64.StdEncoding.EncodeToString([]byte(pp.String())))
			if err != nil {
				t.Fatal(err)
			}

			postPolicyForm, err := parsePostPolicyForm(bytes.NewReader(policyBytes))
			if err != nil {
				t.Fatal(err)
			}

			errStr := ""
			err = checkPostPolicy(tt.fv.Header, postPolicyForm)
			if err != nil {
				errStr = err.Error()
			}
			if errStr != tt.wantErr {
				t.Errorf("test: '%s', want error: '%s', got error: '%s'", tt.name, tt.wantErr, errStr)
			}
		})
	}
}

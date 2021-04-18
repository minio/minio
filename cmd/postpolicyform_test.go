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
	"fmt"
	"net/http"
	"strings"
	"testing"

	minio "github.com/minio/minio-go/v7"
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
		testCase := testCase
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

// Test Post Policy parsing and checking conditions
func TestPostPolicyForm(t *testing.T) {
	pp := minio.NewPostPolicy()
	pp.SetBucket("testbucket")
	pp.SetContentType("image/jpeg")
	pp.SetUserMetadata("uuid", "14365123651274")
	pp.SetKeyStartsWith("user/user1/filename")
	pp.SetContentLengthRange(1048579, 10485760)
	pp.SetSuccessStatusAction("201")

	type testCase struct {
		Bucket              string
		Key                 string
		XAmzDate            string
		XAmzAlgorithm       string
		XAmzCredential      string
		XAmzMetaUUID        string
		ContentType         string
		SuccessActionStatus string
		Policy              string
		Expired             bool
		expectedErr         error
	}

	testCases := []testCase{
		// Everything is fine with this test
		{Bucket: "testbucket", Key: "user/user1/filename/${filename}/myfile.txt", XAmzMetaUUID: "14365123651274", SuccessActionStatus: "201", XAmzCredential: "KVGKMDUQ23TCZXTLTHLP/20160727/us-east-1/s3/aws4_request", XAmzDate: "20160727T000000Z", XAmzAlgorithm: "AWS4-HMAC-SHA256", ContentType: "image/jpeg", expectedErr: nil},
		// Expired policy document
		{Bucket: "testbucket", Key: "user/user1/filename/${filename}/myfile.txt", XAmzMetaUUID: "14365123651274", SuccessActionStatus: "201", XAmzCredential: "KVGKMDUQ23TCZXTLTHLP/20160727/us-east-1/s3/aws4_request", XAmzDate: "20160727T000000Z", XAmzAlgorithm: "AWS4-HMAC-SHA256", ContentType: "image/jpeg", Expired: true, expectedErr: fmt.Errorf("Invalid according to Policy: Policy expired")},
		// Different AMZ date
		{Bucket: "testbucket", Key: "user/user1/filename/${filename}/myfile.txt", XAmzMetaUUID: "14365123651274", XAmzDate: "2017T000000Z", XAmzAlgorithm: "AWS4-HMAC-SHA256", ContentType: "image/jpeg", expectedErr: fmt.Errorf("Invalid according to Policy: Policy Condition failed")},
		// Key which doesn't start with user/user1/filename
		{Bucket: "testbucket", Key: "myfile.txt", XAmzDate: "20160727T000000Z", XAmzMetaUUID: "14365123651274", XAmzAlgorithm: "AWS4-HMAC-SHA256", ContentType: "image/jpeg", expectedErr: fmt.Errorf("Invalid according to Policy: Policy Condition failed")},
		// Incorrect bucket name.
		{Bucket: "incorrect", Key: "user/user1/filename/myfile.txt", XAmzMetaUUID: "14365123651274", XAmzDate: "20160727T000000Z", XAmzAlgorithm: "AWS4-HMAC-SHA256", ContentType: "image/jpeg", expectedErr: fmt.Errorf("Invalid according to Policy: Policy Condition failed")},
		// Incorrect key name
		{Bucket: "testbucket", Key: "incorrect", XAmzDate: "20160727T000000Z", XAmzMetaUUID: "14365123651274", XAmzAlgorithm: "AWS4-HMAC-SHA256", ContentType: "image/jpeg", expectedErr: fmt.Errorf("Invalid according to Policy: Policy Condition failed")},
		// Incorrect date
		{Bucket: "testbucket", Key: "user/user1/filename/${filename}/myfile.txt", XAmzMetaUUID: "14365123651274", XAmzDate: "incorrect", XAmzAlgorithm: "AWS4-HMAC-SHA256", ContentType: "image/jpeg", expectedErr: fmt.Errorf("Invalid according to Policy: Policy Condition failed")},
		// Incorrect ContentType
		{Bucket: "testbucket", Key: "user/user1/filename/${filename}/myfile.txt", XAmzMetaUUID: "14365123651274", XAmzDate: "20160727T000000Z", XAmzAlgorithm: "AWS4-HMAC-SHA256", ContentType: "incorrect", expectedErr: fmt.Errorf("Invalid according to Policy: Policy Condition failed")},
		// Incorrect Metadata
		{Bucket: "testbucket", Key: "user/user1/filename/${filename}/myfile.txt", XAmzMetaUUID: "151274", SuccessActionStatus: "201", XAmzCredential: "KVGKMDUQ23TCZXTLTHLP/20160727/us-east-1/s3/aws4_request", XAmzDate: "20160727T000000Z", XAmzAlgorithm: "AWS4-HMAC-SHA256", ContentType: "image/jpeg", expectedErr: fmt.Errorf("Invalid according to Policy: Policy Condition failed: [eq, $x-amz-meta-uuid, 14365123651274]")},
	}
	// Validate all the test cases.
	for i, tt := range testCases {
		formValues := make(http.Header)
		formValues.Set("Bucket", tt.Bucket)
		formValues.Set("Key", tt.Key)
		formValues.Set("Content-Type", tt.ContentType)
		formValues.Set("X-Amz-Date", tt.XAmzDate)
		formValues.Set("X-Amz-Meta-Uuid", tt.XAmzMetaUUID)
		formValues.Set("X-Amz-Algorithm", tt.XAmzAlgorithm)
		formValues.Set("X-Amz-Credential", tt.XAmzCredential)
		if tt.Expired {
			// Expired already.
			pp.SetExpires(UTCNow().AddDate(0, 0, -10))
		} else {
			// Expires in 10 days.
			pp.SetExpires(UTCNow().AddDate(0, 0, 10))
		}

		formValues.Set("Policy", base64.StdEncoding.EncodeToString([]byte(pp.String())))
		formValues.Set("Success_action_status", tt.SuccessActionStatus)
		policyBytes, err := base64.StdEncoding.DecodeString(base64.StdEncoding.EncodeToString([]byte(pp.String())))
		if err != nil {
			t.Fatal(err)
		}

		postPolicyForm, err := parsePostPolicyForm(bytes.NewReader(policyBytes))
		if err != nil {
			t.Fatal(err)
		}

		err = checkPostPolicy(formValues, postPolicyForm)
		if err != nil && tt.expectedErr != nil && err.Error() != tt.expectedErr.Error() {
			t.Fatalf("Test %d:, Expected %s, got %s", i+1, tt.expectedErr.Error(), err.Error())
		}
	}
}

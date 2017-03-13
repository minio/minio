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
	"encoding/base64"
	"net/http"
	"testing"
)

// Test Post Policy parsing and checking conditions
func TestPostPolicyForm(t *testing.T) {
	type testCase struct {
		Bucket                   string
		Key                      string
		ACL                      string
		XAmzDate                 string
		XAmzServerSideEncryption string
		XAmzAlgorithm            string
		XAmzCredential           string
		XAmzMetaUUID             string
		ContentType              string
		SuccessActionRedirect    string
		Policy                   string
		ErrCode                  APIErrorCode
	}
	testCases := []testCase{
		// Everything is fine with this test
		{Bucket: "testbucket", Key: "user/user1/filename/${filename}/myfile.txt", ACL: "public-read", XAmzServerSideEncryption: "AES256", XAmzMetaUUID: "14365123651274", SuccessActionRedirect: "http://127.0.0.1:9000/", XAmzCredential: "KVGKMDUQ23TCZXTLTHLP/20160727/us-east-1/s3/aws4_request", XAmzDate: "20160727T000000Z", XAmzAlgorithm: "AWS4-HMAC-SHA256", ContentType: "image/jpeg", Policy: "eyAiZXhwaXJhdGlvbiI6ICIyMDE3LTEyLTMwVDEyOjAwOjAwLjAwMFoiLCAiY29uZGl0aW9ucyI6IFsgeyJidWNrZXQiOiAidGVzdGJ1Y2tldCJ9LCBbInN0YXJ0cy13aXRoIiwgIiRrZXkiLCAidXNlci91c2VyMS9maWxlbmFtZSJdLCB7ImFjbCI6ICJwdWJsaWMtcmVhZCJ9LCB7InN1Y2Nlc3NfYWN0aW9uX3JlZGlyZWN0IjogImh0dHA6Ly8xMjcuMC4wLjE6OTAwMC8ifSwgWyJzdGFydHMtd2l0aCIsICIkQ29udGVudC1UeXBlIiwgImltYWdlLyJdLCBbImNvbnRlbnQtbGVuZ3RoLXJhbmdlIiwgMTA0ODU3OSwgMTA0ODU3NjBdLCB7IngtYW16LW1ldGEtdXVpZCI6ICIxNDM2NTEyMzY1MTI3NCJ9LCB7IngtYW16LXNlcnZlci1zaWRlLWVuY3J5cHRpb24iOiAiQUVTMjU2In0sIFsic3RhcnRzLXdpdGgiLCAiJHgtYW16LW1ldGEtdGFnIiwgIiJdLCB7IngtYW16LWNyZWRlbnRpYWwiOiAiS1ZHS01EVVEyM1RDWlhUTFRITFAvMjAxNjA3MjcvdXMtZWFzdC0xL3MzL2F3czRfcmVxdWVzdCJ9LCB7IngtYW16LWFsZ29yaXRobSI6ICJBV1M0LUhNQUMtU0hBMjU2In0sIHsieC1hbXotZGF0ZSI6ICIyMDE2MDcyN1QwMDAwMDBaIiB9IF0gfQ==", ErrCode: ErrNone},
		// Expired policy document
		{Bucket: "testbucket", Key: "user/user1/filename/${filename}/myfile.txt", ACL: "public-read", XAmzServerSideEncryption: "AES256", XAmzMetaUUID: "14365123651274", SuccessActionRedirect: "http://127.0.0.1:9000/", XAmzCredential: "KVGKMDUQ23TCZXTLTHLP/20160727/us-east-1/s3/aws4_request", XAmzDate: "20160727T000000Z", XAmzAlgorithm: "AWS4-HMAC-SHA256", ContentType: "image/jpeg", Policy: "eyAiZXhwaXJhdGlvbiI6ICIyMDE1LTEyLTMwVDEyOjAwOjAwLjAwMFoiLCAiY29uZGl0aW9ucyI6IFsgeyJidWNrZXQiOiAidGVzdGJ1Y2tldCJ9LCBbInN0YXJ0cy13aXRoIiwgIiRrZXkiLCAidXNlci91c2VyMS9maWxlbmFtZSJdLCB7ImFjbCI6ICJwdWJsaWMtcmVhZCJ9LCB7InN1Y2Nlc3NfYWN0aW9uX3JlZGlyZWN0IjogImh0dHA6Ly8xMjcuMC4wLjE6OTAwMC8ifSwgWyJzdGFydHMtd2l0aCIsICIkQ29udGVudC1UeXBlIiwgImltYWdlLyJdLCBbImNvbnRlbnQtbGVuZ3RoLXJhbmdlIiwgMTA0ODU3OSwgMTA0ODU3NjBdLCB7IngtYW16LW1ldGEtdXVpZCI6ICIxNDM2NTEyMzY1MTI3NCJ9LCB7IngtYW16LXNlcnZlci1zaWRlLWVuY3J5cHRpb24iOiAiQUVTMjU2In0sIFsic3RhcnRzLXdpdGgiLCAiJHgtYW16LW1ldGEtdGFnIiwgIiJdLCB7IngtYW16LWNyZWRlbnRpYWwiOiAiS1ZHS01EVVEyM1RDWlhUTFRITFAvMjAxNjA3MjcvdXMtZWFzdC0xL3MzL2F3czRfcmVxdWVzdCJ9LCB7IngtYW16LWFsZ29yaXRobSI6ICJBV1M0LUhNQUMtU0hBMjU2In0sIHsieC1hbXotZGF0ZSI6ICIyMDE2MDcyN1QwMDAwMDBaIiB9IF0gfQo=", ErrCode: ErrPolicyAlreadyExpired},
		// Passing AMZ date with starts-with operator which is fobidden
		{Bucket: "testbucket", Key: "user/user1/filename/${filename}/myfile.txt", ACL: "public-read", XAmzServerSideEncryption: "AES256", XAmzMetaUUID: "14365123651274", SuccessActionRedirect: "http://127.0.0.1:9000/", XAmzCredential: "KVGKMDUQ23TCZXTLTHLP/20160727/us-east-1/s3/aws4_request", XAmzDate: "20160727T000000Z", XAmzAlgorithm: "AWS4-HMAC-SHA256", ContentType: "image/jpeg", Policy: "eyAiZXhwaXJhdGlvbiI6ICIyMTE1LTEyLTMwVDEyOjAwOjAwLjAwMFoiLCAiY29uZGl0aW9ucyI6IFsgeyJidWNrZXQiOiAidGVzdGJ1Y2tldCJ9LCBbInN0YXJ0cy13aXRoIiwgIiRrZXkiLCAidXNlci91c2VyMS9maWxlbmFtZSJdLCB7ImFjbCI6ICJwdWJsaWMtcmVhZCJ9LCB7InN1Y2Nlc3NfYWN0aW9uX3JlZGlyZWN0IjogImh0dHA6Ly8xMjcuMC4wLjE6OTAwMC8ifSwgWyJzdGFydHMtd2l0aCIsICIkQ29udGVudC1UeXBlIiwgImltYWdlLyJdLCBbImNvbnRlbnQtbGVuZ3RoLXJhbmdlIiwgMTA0ODU3OSwgMTA0ODU3NjBdLCB7IngtYW16LW1ldGEtdXVpZCI6ICIxNDM2NTEyMzY1MTI3NCJ9LCB7IngtYW16LXNlcnZlci1zaWRlLWVuY3J5cHRpb24iOiAiQUVTMjU2In0sIFsic3RhcnRzLXdpdGgiLCAiJHgtYW16LW1ldGEtdGFnIiwgIiJdLCB7IngtYW16LWNyZWRlbnRpYWwiOiAiS1ZHS01EVVEyM1RDWlhUTFRITFAvMjAxNjA3MjcvdXMtZWFzdC0xL3MzL2F3czRfcmVxdWVzdCJ9LCB7IngtYW16LWFsZ29yaXRobSI6ICJBV1M0LUhNQUMtU0hBMjU2In0sIFsic3RhcnRzLXdpdGgiLCAiJHgtYW16LWRhdGUiLCAiMjAxNjA3MjdUMDAwMDAwWiJdIF0gfQo=", ErrCode: ErrAccessDenied},
		// Different AMZ date
		{Bucket: "testbucket", Key: "user/user1/filename/${filename}/myfile.txt", ACL: "public-read", XAmzServerSideEncryption: "AES256", XAmzMetaUUID: "14365123651274", XAmzDate: "20160727T000000Z", XAmzAlgorithm: "AWS4-HMAC-SHA256", ContentType: "image/jpeg", Policy: "eyAiZXhwaXJhdGlvbiI6ICIyMDE3LTEyLTMwVDEyOjAwOjAwLjAwMFoiLCAiY29uZGl0aW9ucyI6IFsgeyJidWNrZXQiOiAidGVzdGJ1Y2tldCJ9LCBbInN0YXJ0cy13aXRoIiwgIiRrZXkiLCAidXNlci91c2VyMS9maWxlbmFtZSJdLCB7ImFjbCI6ICJwdWJsaWMtcmVhZCJ9LCB7InN1Y2Nlc3NfYWN0aW9uX3JlZGlyZWN0IjogImh0dHA6Ly8xMjcuMC4wLjE6OTAwMC8ifSwgWyJzdGFydHMtd2l0aCIsICIkQ29udGVudC1UeXBlIiwgImltYWdlLyJdLCB7IngtYW16LW1ldGEtdXVpZCI6ICIxNDM2NTEyMzY1MTI3NCJ9LCB7IngtYW16LXNlcnZlci1zaWRlLWVuY3J5cHRpb24iOiAiQUVTMjU2In0sIFsic3RhcnRzLXdpdGgiLCAiJHgtYW16LW1ldGEtdGFnIiwgIiJdLCB7IngtYW16LWNyZWRlbnRpYWwiOiAiS1ZHS01EVVEyM1RDWlhUTFRITFAvMjAxNjA3MjcvdXMtZWFzdC0xL3MzL2F3czRfcmVxdWVzdCJ9LCB7IngtYW16LWFsZ29yaXRobSI6ICJBV1M0LUhNQUMtU0hBMjU2In0sIHsieC1hbXotZGF0ZSI6ICIyMDIwMDcyN1QwMDAwMDBaIiB9IF0gfQ==", ErrCode: ErrAccessDenied},
		// Key which doesn't start with user/user1/filename
		{Bucket: "testbucket", Key: "myfile.txt", XAmzDate: "20160727T000000Z", ACL: "public-read", XAmzServerSideEncryption: "AES256", XAmzMetaUUID: "14365123651274", XAmzAlgorithm: "AWS4-HMAC-SHA256", ContentType: "image/jpeg", Policy: "eyAiZXhwaXJhdGlvbiI6ICIyMDE3LTEyLTMwVDEyOjAwOjAwLjAwMFoiLCAiY29uZGl0aW9ucyI6IFsgeyJidWNrZXQiOiAidGVzdGJ1Y2tldCJ9LCBbInN0YXJ0cy13aXRoIiwgIiRrZXkiLCAidXNlci91c2VyMS9maWxlbmFtZSJdLCB7ImFjbCI6ICJwdWJsaWMtcmVhZCJ9LCB7InN1Y2Nlc3NfYWN0aW9uX3JlZGlyZWN0IjogImh0dHA6Ly8xMjcuMC4wLjE6OTAwMC8ifSwgWyJzdGFydHMtd2l0aCIsICIkQ29udGVudC1UeXBlIiwgImltYWdlLyJdLCB7IngtYW16LW1ldGEtdXVpZCI6ICIxNDM2NTEyMzY1MTI3NCJ9LCB7IngtYW16LXNlcnZlci1zaWRlLWVuY3J5cHRpb24iOiAiQUVTMjU2In0sIFsic3RhcnRzLXdpdGgiLCAiJHgtYW16LW1ldGEtdGFnIiwgIiJdLCB7IngtYW16LWNyZWRlbnRpYWwiOiAiS1ZHS01EVVEyM1RDWlhUTFRITFAvMjAxNjA3MjcvdXMtZWFzdC0xL3MzL2F3czRfcmVxdWVzdCJ9LCB7IngtYW16LWFsZ29yaXRobSI6ICJBV1M0LUhNQUMtU0hBMjU2In0sIHsieC1hbXotZGF0ZSI6ICIyMDIwMDcyN1QwMDAwMDBaIiB9IF0gfQ==", ErrCode: ErrAccessDenied},
		// Incorrect bucket name.
		{Bucket: "incorrect", Key: "user/user1/filename/myfile.txt", ACL: "public-read", XAmzServerSideEncryption: "AES256", XAmzMetaUUID: "14365123651274", XAmzDate: "20160727T000000Z", XAmzAlgorithm: "AWS4-HMAC-SHA256", ContentType: "image/jpeg", Policy: "eyAiZXhwaXJhdGlvbiI6ICIyMDE3LTEyLTMwVDEyOjAwOjAwLjAwMFoiLCAiY29uZGl0aW9ucyI6IFsgeyJidWNrZXQiOiAidGVzdGJ1Y2tldCJ9LCBbInN0YXJ0cy13aXRoIiwgIiRrZXkiLCAidXNlci91c2VyMS9maWxlbmFtZSJdLCB7ImFjbCI6ICJwdWJsaWMtcmVhZCJ9LCB7InN1Y2Nlc3NfYWN0aW9uX3JlZGlyZWN0IjogImh0dHA6Ly8xMjcuMC4wLjE6OTAwMC8ifSwgWyJzdGFydHMtd2l0aCIsICIkQ29udGVudC1UeXBlIiwgImltYWdlLyJdLCBbImNvbnRlbnQtbGVuZ3RoLXJhbmdlIiwgMTA0ODU3OSwgMTA0ODU3NjBdLCB7IngtYW16LW1ldGEtdXVpZCI6ICIxNDM2NTEyMzY1MTI3NCJ9LCB7IngtYW16LXNlcnZlci1zaWRlLWVuY3J5cHRpb24iOiAiQUVTMjU2In0sIFsic3RhcnRzLXdpdGgiLCAiJHgtYW16LW1ldGEtdGFnIiwgIiJdLCB7IngtYW16LWNyZWRlbnRpYWwiOiAiS1ZHS01EVVEyM1RDWlhUTFRITFAvMjAxNjA3MjcvdXMtZWFzdC0xL3MzL2F3czRfcmVxdWVzdCJ9LCB7IngtYW16LWFsZ29yaXRobSI6ICJBV1M0LUhNQUMtU0hBMjU2In0sIHsieC1hbXotZGF0ZSI6ICIyMDE2MDcyN1QwMDAwMDBaIiB9IF0gfQ==", ErrCode: ErrAccessDenied},
		// Incorrect key name
		{Bucket: "testbucket", Key: "incorrect", ACL: "public-read", XAmzDate: "20160727T000000Z", XAmzServerSideEncryption: "AES256", XAmzMetaUUID: "14365123651274", XAmzAlgorithm: "AWS4-HMAC-SHA256", ContentType: "image/jpeg", Policy: "eyAiZXhwaXJhdGlvbiI6ICIyMDE3LTEyLTMwVDEyOjAwOjAwLjAwMFoiLCAiY29uZGl0aW9ucyI6IFsgeyJidWNrZXQiOiAidGVzdGJ1Y2tldCJ9LCBbInN0YXJ0cy13aXRoIiwgIiRrZXkiLCAidXNlci91c2VyMS9maWxlbmFtZSJdLCB7ImFjbCI6ICJwdWJsaWMtcmVhZCJ9LCB7InN1Y2Nlc3NfYWN0aW9uX3JlZGlyZWN0IjogImh0dHA6Ly8xMjcuMC4wLjE6OTAwMC8ifSwgWyJzdGFydHMtd2l0aCIsICIkQ29udGVudC1UeXBlIiwgImltYWdlLyJdLCBbImNvbnRlbnQtbGVuZ3RoLXJhbmdlIiwgMTA0ODU3OSwgMTA0ODU3NjBdLCB7IngtYW16LW1ldGEtdXVpZCI6ICIxNDM2NTEyMzY1MTI3NCJ9LCB7IngtYW16LXNlcnZlci1zaWRlLWVuY3J5cHRpb24iOiAiQUVTMjU2In0sIFsic3RhcnRzLXdpdGgiLCAiJHgtYW16LW1ldGEtdGFnIiwgIiJdLCB7IngtYW16LWNyZWRlbnRpYWwiOiAiS1ZHS01EVVEyM1RDWlhUTFRITFAvMjAxNjA3MjcvdXMtZWFzdC0xL3MzL2F3czRfcmVxdWVzdCJ9LCB7IngtYW16LWFsZ29yaXRobSI6ICJBV1M0LUhNQUMtU0hBMjU2In0sIHsieC1hbXotZGF0ZSI6ICIyMDE2MDcyN1QwMDAwMDBaIiB9IF0gfQ==", ErrCode: ErrAccessDenied},
		// Incorrect date
		{Bucket: "testbucket", Key: "user/user1/filename/${filename}/myfile.txt", ACL: "public-read", XAmzServerSideEncryption: "AES256", XAmzMetaUUID: "14365123651274", XAmzDate: "incorrect", XAmzAlgorithm: "AWS4-HMAC-SHA256", ContentType: "image/jpeg", Policy: "eyAiZXhwaXJhdGlvbiI6ICIyMDE3LTEyLTMwVDEyOjAwOjAwLjAwMFoiLCAiY29uZGl0aW9ucyI6IFsgeyJidWNrZXQiOiAidGVzdGJ1Y2tldCJ9LCBbInN0YXJ0cy13aXRoIiwgIiRrZXkiLCAidXNlci91c2VyMS9maWxlbmFtZSJdLCB7ImFjbCI6ICJwdWJsaWMtcmVhZCJ9LCB7InN1Y2Nlc3NfYWN0aW9uX3JlZGlyZWN0IjogImh0dHA6Ly8xMjcuMC4wLjE6OTAwMC8ifSwgWyJzdGFydHMtd2l0aCIsICIkQ29udGVudC1UeXBlIiwgImltYWdlLyJdLCBbImNvbnRlbnQtbGVuZ3RoLXJhbmdlIiwgMTA0ODU3OSwgMTA0ODU3NjBdLCB7IngtYW16LW1ldGEtdXVpZCI6ICIxNDM2NTEyMzY1MTI3NCJ9LCB7IngtYW16LXNlcnZlci1zaWRlLWVuY3J5cHRpb24iOiAiQUVTMjU2In0sIFsic3RhcnRzLXdpdGgiLCAiJHgtYW16LW1ldGEtdGFnIiwgIiJdLCB7IngtYW16LWNyZWRlbnRpYWwiOiAiS1ZHS01EVVEyM1RDWlhUTFRITFAvMjAxNjA3MjcvdXMtZWFzdC0xL3MzL2F3czRfcmVxdWVzdCJ9LCB7IngtYW16LWFsZ29yaXRobSI6ICJBV1M0LUhNQUMtU0hBMjU2In0sIHsieC1hbXotZGF0ZSI6ICIyMDE2MDcyN1QwMDAwMDBaIiB9IF0gfQ==", ErrCode: ErrAccessDenied},
		// Incorrect ContentType
		{Bucket: "testbucket", Key: "user/user1/filename/${filename}/myfile.txt", ACL: "public-read", XAmzServerSideEncryption: "AES256", XAmzMetaUUID: "14365123651274", XAmzDate: "20160727T000000Z", XAmzAlgorithm: "AWS4-HMAC-SHA256", ContentType: "incorrect", Policy: "eyAiZXhwaXJhdGlvbiI6ICIyMDE3LTEyLTMwVDEyOjAwOjAwLjAwMFoiLCAiY29uZGl0aW9ucyI6IFsgeyJidWNrZXQiOiAidGVzdGJ1Y2tldCJ9LCBbInN0YXJ0cy13aXRoIiwgIiRrZXkiLCAidXNlci91c2VyMS9maWxlbmFtZSJdLCB7ImFjbCI6ICJwdWJsaWMtcmVhZCJ9LCB7InN1Y2Nlc3NfYWN0aW9uX3JlZGlyZWN0IjogImh0dHA6Ly8xMjcuMC4wLjE6OTAwMC8ifSwgWyJzdGFydHMtd2l0aCIsICIkQ29udGVudC1UeXBlIiwgImltYWdlLyJdLCBbImNvbnRlbnQtbGVuZ3RoLXJhbmdlIiwgMTA0ODU3OSwgMTA0ODU3NjBdLCB7IngtYW16LW1ldGEtdXVpZCI6ICIxNDM2NTEyMzY1MTI3NCJ9LCB7IngtYW16LXNlcnZlci1zaWRlLWVuY3J5cHRpb24iOiAiQUVTMjU2In0sIFsic3RhcnRzLXdpdGgiLCAiJHgtYW16LW1ldGEtdGFnIiwgIiJdLCB7IngtYW16LWNyZWRlbnRpYWwiOiAiS1ZHS01EVVEyM1RDWlhUTFRITFAvMjAxNjA3MjcvdXMtZWFzdC0xL3MzL2F3czRfcmVxdWVzdCJ9LCB7IngtYW16LWFsZ29yaXRobSI6ICJBV1M0LUhNQUMtU0hBMjU2In0sIHsieC1hbXotZGF0ZSI6ICIyMDE2MDcyN1QwMDAwMDBaIiB9IF0gfQ==", ErrCode: ErrAccessDenied},
	}
	// Validate all the test cases.
	for i, tt := range testCases {
		formValues := make(http.Header)
		formValues.Set("Bucket", tt.Bucket)
		formValues.Set("Acl", tt.ACL)
		formValues.Set("Key", tt.Key)
		formValues.Set("X-Amz-Date", tt.XAmzDate)
		formValues.Set("X-Amz-Meta-Uuid", tt.XAmzMetaUUID)
		formValues.Set("X-Amz-Server-Side-Encryption", tt.XAmzServerSideEncryption)
		formValues.Set("X-Amz-Algorithm", tt.XAmzAlgorithm)
		formValues.Set("X-Amz-Credential", tt.XAmzCredential)
		formValues.Set("Content-Type", tt.ContentType)
		formValues.Set("Policy", tt.Policy)
		formValues.Set("Success_action_redirect", tt.SuccessActionRedirect)
		policyBytes, err := base64.StdEncoding.DecodeString(tt.Policy)
		if err != nil {
			t.Fatal(err)
		}

		postPolicyForm, err := parsePostPolicyForm(string(policyBytes))
		if err != nil {
			t.Fatal(err)
		}
		errCode := checkPostPolicy(formValues, postPolicyForm)
		if tt.ErrCode != errCode {
			t.Errorf("Test %d:, Expected %d, got %d", i+1, tt.ErrCode, errCode)
		}
	}
}

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
)

// Test Post Policy parsing and checking conditions
func TestPostPolicyForm(t *testing.T) {

	type testCase struct {
		Bucket        string
		Key           string
		XAmzDate      string
		XAmzAlgorithm string
		ContentType   string
		Policy        string
		ErrCode       APIErrorCode
	}
	testCases := []testCase{
		// Mal formed policy document
		{Bucket: "testbucket", Key: "user/user1/filename/${filename}/myfile.txt", XAmzDate: "20160727T000000Z", XAmzAlgorithm: "AWS4-HMAC-SHA256", ContentType: "image/jpeg", Policy: "==", ErrCode: ErrMalformedPOSTRequest},
		// Different AMZ date
		{Bucket: "testbucket", Key: "user/user1/filename/${filename}/myfile.txt", XAmzDate: "20160727T000000Z", XAmzAlgorithm: "AWS4-HMAC-SHA256", ContentType: "image/jpeg", Policy: "eyAiZXhwaXJhdGlvbiI6ICIyMDE3LTEyLTMwVDEyOjAwOjAwLjAwMFoiLCAiY29uZGl0aW9ucyI6IFsgeyJidWNrZXQiOiAidGVzdGJ1Y2tldCJ9LCBbInN0YXJ0cy13aXRoIiwgIiRrZXkiLCAidXNlci91c2VyMS9maWxlbmFtZSJdLCB7ImFjbCI6ICJwdWJsaWMtcmVhZCJ9LCB7InN1Y2Nlc3NfYWN0aW9uX3JlZGlyZWN0IjogImh0dHA6Ly8xMjcuMC4wLjE6OTAwMC8ifSwgWyJzdGFydHMtd2l0aCIsICIkQ29udGVudC1UeXBlIiwgImltYWdlLyJdLCB7IngtYW16LW1ldGEtdXVpZCI6ICIxNDM2NTEyMzY1MTI3NCJ9LCB7IngtYW16LXNlcnZlci1zaWRlLWVuY3J5cHRpb24iOiAiQUVTMjU2In0sIFsic3RhcnRzLXdpdGgiLCAiJHgtYW16LW1ldGEtdGFnIiwgIiJdLCB7IngtYW16LWNyZWRlbnRpYWwiOiAiS1ZHS01EVVEyM1RDWlhUTFRITFAvMjAxNjA3MjcvdXMtZWFzdC0xL3MzL2F3czRfcmVxdWVzdCJ9LCB7IngtYW16LWFsZ29yaXRobSI6ICJBV1M0LUhNQUMtU0hBMjU2In0sIHsieC1hbXotZGF0ZSI6ICIyMDIwMDcyN1QwMDAwMDBaIiB9IF0gfQ==", ErrCode: ErrAccessDenied},
		// Key which doesn't start with user/user1/filename
		{Bucket: "testbucket", Key: "myfile.txt", XAmzDate: "20160727T000000Z", XAmzAlgorithm: "AWS4-HMAC-SHA256", ContentType: "image/jpeg", Policy: "eyAiZXhwaXJhdGlvbiI6ICIyMDE3LTEyLTMwVDEyOjAwOjAwLjAwMFoiLCAiY29uZGl0aW9ucyI6IFsgeyJidWNrZXQiOiAidGVzdGJ1Y2tldCJ9LCBbInN0YXJ0cy13aXRoIiwgIiRrZXkiLCAidXNlci91c2VyMS9maWxlbmFtZSJdLCB7ImFjbCI6ICJwdWJsaWMtcmVhZCJ9LCB7InN1Y2Nlc3NfYWN0aW9uX3JlZGlyZWN0IjogImh0dHA6Ly8xMjcuMC4wLjE6OTAwMC8ifSwgWyJzdGFydHMtd2l0aCIsICIkQ29udGVudC1UeXBlIiwgImltYWdlLyJdLCB7IngtYW16LW1ldGEtdXVpZCI6ICIxNDM2NTEyMzY1MTI3NCJ9LCB7IngtYW16LXNlcnZlci1zaWRlLWVuY3J5cHRpb24iOiAiQUVTMjU2In0sIFsic3RhcnRzLXdpdGgiLCAiJHgtYW16LW1ldGEtdGFnIiwgIiJdLCB7IngtYW16LWNyZWRlbnRpYWwiOiAiS1ZHS01EVVEyM1RDWlhUTFRITFAvMjAxNjA3MjcvdXMtZWFzdC0xL3MzL2F3czRfcmVxdWVzdCJ9LCB7IngtYW16LWFsZ29yaXRobSI6ICJBV1M0LUhNQUMtU0hBMjU2In0sIHsieC1hbXotZGF0ZSI6ICIyMDIwMDcyN1QwMDAwMDBaIiB9IF0gfQ==", ErrCode: ErrAccessDenied},
		// Everything is fine with this test
		{Bucket: "testbucket", Key: "user/user1/filename/${filename}/myfile.txt", XAmzDate: "20160727T000000Z", XAmzAlgorithm: "AWS4-HMAC-SHA256", ContentType: "image/jpeg", Policy: "eyAiZXhwaXJhdGlvbiI6ICIyMDE3LTEyLTMwVDEyOjAwOjAwLjAwMFoiLCAiY29uZGl0aW9ucyI6IFsgeyJidWNrZXQiOiAidGVzdGJ1Y2tldCJ9LCBbInN0YXJ0cy13aXRoIiwgIiRrZXkiLCAidXNlci91c2VyMS9maWxlbmFtZSJdLCB7ImFjbCI6ICJwdWJsaWMtcmVhZCJ9LCB7InN1Y2Nlc3NfYWN0aW9uX3JlZGlyZWN0IjogImh0dHA6Ly8xMjcuMC4wLjE6OTAwMC8ifSwgWyJzdGFydHMtd2l0aCIsICIkQ29udGVudC1UeXBlIiwgImltYWdlLyJdLCBbImNvbnRlbnQtbGVuZ3RoLXJhbmdlIiwgMTA0ODU3OSwgMTA0ODU3NjBdLCB7IngtYW16LW1ldGEtdXVpZCI6ICIxNDM2NTEyMzY1MTI3NCJ9LCB7IngtYW16LXNlcnZlci1zaWRlLWVuY3J5cHRpb24iOiAiQUVTMjU2In0sIFsic3RhcnRzLXdpdGgiLCAiJHgtYW16LW1ldGEtdGFnIiwgIiJdLCB7IngtYW16LWNyZWRlbnRpYWwiOiAiS1ZHS01EVVEyM1RDWlhUTFRITFAvMjAxNjA3MjcvdXMtZWFzdC0xL3MzL2F3czRfcmVxdWVzdCJ9LCB7IngtYW16LWFsZ29yaXRobSI6ICJBV1M0LUhNQUMtU0hBMjU2In0sIHsieC1hbXotZGF0ZSI6ICIyMDE2MDcyN1QwMDAwMDBaIiB9IF0gfQ==", ErrCode: ErrNone},
	}
	// Validate all the test cases.
	for i, tt := range testCases {
		formValues := make(map[string]string)
		formValues["Bucket"] = tt.Bucket
		formValues["Key"] = tt.Key
		formValues["X-Amz-Date"] = tt.XAmzDate
		formValues["X-Amz-Algorithm"] = tt.XAmzAlgorithm
		formValues["Content-Type"] = tt.ContentType
		formValues["Policy"] = tt.Policy

		err := checkPostPolicy(formValues)
		if tt.ErrCode != err {
			t.Errorf("Test %d:, Expected %d, got %d", i+1, tt.ErrCode, err)
		}
	}
}

// MinIO Cloud Storage, (C) 2015, 2016, 2017, 2018 MinIO, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package crypto

import (
	"net/http"
	"testing"
)

func TestS3String(t *testing.T) {
	const Domain = "SSE-S3"
	if domain := S3.String(); domain != Domain {
		t.Errorf("S3's string method returns wrong domain: got '%s' - want '%s'", domain, Domain)
	}
}

func TestSSECString(t *testing.T) {
	const Domain = "SSE-C"
	if domain := SSEC.String(); domain != Domain {
		t.Errorf("SSEC's string method returns wrong domain: got '%s' - want '%s'", domain, Domain)
	}
}

var ssecUnsealObjectKeyTests = []struct {
	Headers        http.Header
	Bucket, Object string
	Metadata       map[string]string

	ExpectedErr error
}{
	{ // 0 - Valid HTTP headers and valid metadata entries for bucket/object
		Headers: http.Header{
			"X-Amz-Server-Side-Encryption-Customer-Algorithm": []string{"AES256"},
			"X-Amz-Server-Side-Encryption-Customer-Key":       []string{"MzJieXRlc2xvbmdzZWNyZXRrZXltdXN0cHJvdmlkZWQ="},
			"X-Amz-Server-Side-Encryption-Customer-Key-Md5":   []string{"7PpPLAK26ONlVUGOWlusfg=="},
		},
		Bucket: "bucket",
		Object: "object",
		Metadata: map[string]string{
			"X-Minio-Internal-Server-Side-Encryption-Sealed-Key":     "IAAfAMBdYor5tf/UlVaQvwYlw5yKbPBeQqfygqsfHqhu1wHD9KDAP4bw38AhL12prFTS23JbbR9Re5Qv26ZnlQ==",
			"X-Minio-Internal-Server-Side-Encryption-Seal-Algorithm": "DAREv2-HMAC-SHA256",
			"X-Minio-Internal-Server-Side-Encryption-Iv":             "coVfGS3I/CTrqexX5vUN+PQPoP9aUFiPYYrSzqTWfBA=",
		},
		ExpectedErr: nil,
	},
	{ // 1 - Valid HTTP headers but invalid metadata entries for bucket/object2
		Headers: http.Header{
			"X-Amz-Server-Side-Encryption-Customer-Algorithm": []string{"AES256"},
			"X-Amz-Server-Side-Encryption-Customer-Key":       []string{"MzJieXRlc2xvbmdzZWNyZXRrZXltdXN0cHJvdmlkZWQ="},
			"X-Amz-Server-Side-Encryption-Customer-Key-Md5":   []string{"7PpPLAK26ONlVUGOWlusfg=="},
		},
		Bucket: "bucket",
		Object: "object2",
		Metadata: map[string]string{
			"X-Minio-Internal-Server-Side-Encryption-Sealed-Key":     "IAAfAMBdYor5tf/UlVaQvwYlw5yKbPBeQqfygqsfHqhu1wHD9KDAP4bw38AhL12prFTS23JbbR9Re5Qv26ZnlQ==",
			"X-Minio-Internal-Server-Side-Encryption-Seal-Algorithm": "DAREv2-HMAC-SHA256",
			"X-Minio-Internal-Server-Side-Encryption-Iv":             "coVfGS3I/CTrqexX5vUN+PQPoP9aUFiPYYrSzqTWfBA=",
		},
		ExpectedErr: ErrSecretKeyMismatch,
	},
	{ // 2 - Valid HTTP headers but invalid metadata entries for bucket/object
		Headers: http.Header{
			"X-Amz-Server-Side-Encryption-Customer-Algorithm": []string{"AES256"},
			"X-Amz-Server-Side-Encryption-Customer-Key":       []string{"MzJieXRlc2xvbmdzZWNyZXRrZXltdXN0cHJvdmlkZWQ="},
			"X-Amz-Server-Side-Encryption-Customer-Key-Md5":   []string{"7PpPLAK26ONlVUGOWlusfg=="},
		},
		Bucket: "bucket",
		Object: "object",
		Metadata: map[string]string{
			"X-Minio-Internal-Server-Side-Encryption-Sealed-Key": "IAAfAMBdYor5tf/UlVaQvwYlw5yKbPBeQqfygqsfHqhu1wHD9KDAP4bw38AhL12prFTS23JbbR9Re5Qv26ZnlQ==",
			"X-Minio-Internal-Server-Side-Encryption-Iv":         "coVfGS3I/CTrqexX5vUN+PQPoP9aUFiPYYrSzqTWfBA=",
		},
		ExpectedErr: errMissingInternalSealAlgorithm,
	},
	{ // 3 - Invalid HTTP headers for valid metadata entries for bucket/object
		Headers: http.Header{
			"X-Amz-Server-Side-Encryption-Customer-Algorithm": []string{"AES256"},
			"X-Amz-Server-Side-Encryption-Customer-Key":       []string{"MzJieXRlc2xvbmdzZWNyZXRrZXltdXN0cHJvdmlkZWQ="},
		},
		Bucket: "bucket",
		Object: "object",
		Metadata: map[string]string{
			"X-Minio-Internal-Server-Side-Encryption-Sealed-Key":     "IAAfAMBdYor5tf/UlVaQvwYlw5yKbPBeQqfygqsfHqhu1wHD9KDAP4bw38AhL12prFTS23JbbR9Re5Qv26ZnlQ==",
			"X-Minio-Internal-Server-Side-Encryption-Seal-Algorithm": "DAREv2-HMAC-SHA256",
			"X-Minio-Internal-Server-Side-Encryption-Iv":             "coVfGS3I/CTrqexX5vUN+PQPoP9aUFiPYYrSzqTWfBA=",
		},
		ExpectedErr: ErrMissingCustomerKeyMD5,
	},
}

func TestSSECUnsealObjectKey(t *testing.T) {
	for i, test := range ssecUnsealObjectKeyTests {
		if _, err := SSEC.UnsealObjectKey(test.Headers, test.Metadata, test.Bucket, test.Object); err != test.ExpectedErr {
			t.Errorf("Test %d: got: %v - want: %v", i, err, test.ExpectedErr)
		}
	}
}

var sseCopyUnsealObjectKeyTests = []struct {
	Headers        http.Header
	Bucket, Object string
	Metadata       map[string]string

	ExpectedErr error
}{
	{ // 0 - Valid HTTP headers and valid metadata entries for bucket/object
		Headers: http.Header{
			"X-Amz-Copy-Source-Server-Side-Encryption-Customer-Algorithm": []string{"AES256"},
			"X-Amz-Copy-Source-Server-Side-Encryption-Customer-Key":       []string{"MzJieXRlc2xvbmdzZWNyZXRrZXltdXN0cHJvdmlkZWQ="},
			"X-Amz-Copy-Source-Server-Side-Encryption-Customer-Key-Md5":   []string{"7PpPLAK26ONlVUGOWlusfg=="},
		},
		Bucket: "bucket",
		Object: "object",
		Metadata: map[string]string{
			"X-Minio-Internal-Server-Side-Encryption-Sealed-Key":     "IAAfAMBdYor5tf/UlVaQvwYlw5yKbPBeQqfygqsfHqhu1wHD9KDAP4bw38AhL12prFTS23JbbR9Re5Qv26ZnlQ==",
			"X-Minio-Internal-Server-Side-Encryption-Seal-Algorithm": "DAREv2-HMAC-SHA256",
			"X-Minio-Internal-Server-Side-Encryption-Iv":             "coVfGS3I/CTrqexX5vUN+PQPoP9aUFiPYYrSzqTWfBA=",
		},
		ExpectedErr: nil,
	},
	{ // 1 - Valid HTTP headers but invalid metadata entries for bucket/object2
		Headers: http.Header{
			"X-Amz-Copy-Source-Server-Side-Encryption-Customer-Algorithm": []string{"AES256"},
			"X-Amz-Copy-Source-Server-Side-Encryption-Customer-Key":       []string{"MzJieXRlc2xvbmdzZWNyZXRrZXltdXN0cHJvdmlkZWQ="},
			"X-Amz-Copy-Source-Server-Side-Encryption-Customer-Key-Md5":   []string{"7PpPLAK26ONlVUGOWlusfg=="},
		},
		Bucket: "bucket",
		Object: "object2",
		Metadata: map[string]string{
			"X-Minio-Internal-Server-Side-Encryption-Sealed-Key":     "IAAfAMBdYor5tf/UlVaQvwYlw5yKbPBeQqfygqsfHqhu1wHD9KDAP4bw38AhL12prFTS23JbbR9Re5Qv26ZnlQ==",
			"X-Minio-Internal-Server-Side-Encryption-Seal-Algorithm": "DAREv2-HMAC-SHA256",
			"X-Minio-Internal-Server-Side-Encryption-Iv":             "coVfGS3I/CTrqexX5vUN+PQPoP9aUFiPYYrSzqTWfBA=",
		},
		ExpectedErr: ErrSecretKeyMismatch,
	},
	{ // 2 - Valid HTTP headers but invalid metadata entries for bucket/object
		Headers: http.Header{
			"X-Amz-Copy-Source-Server-Side-Encryption-Customer-Algorithm": []string{"AES256"},
			"X-Amz-Copy-Source-Server-Side-Encryption-Customer-Key":       []string{"MzJieXRlc2xvbmdzZWNyZXRrZXltdXN0cHJvdmlkZWQ="},
			"X-Amz-Copy-Source-Server-Side-Encryption-Customer-Key-Md5":   []string{"7PpPLAK26ONlVUGOWlusfg=="},
		},
		Bucket: "bucket",
		Object: "object",
		Metadata: map[string]string{
			"X-Minio-Internal-Server-Side-Encryption-Sealed-Key": "IAAfAMBdYor5tf/UlVaQvwYlw5yKbPBeQqfygqsfHqhu1wHD9KDAP4bw38AhL12prFTS23JbbR9Re5Qv26ZnlQ==",
			"X-Minio-Internal-Server-Side-Encryption-Iv":         "coVfGS3I/CTrqexX5vUN+PQPoP9aUFiPYYrSzqTWfBA=",
		},
		ExpectedErr: errMissingInternalSealAlgorithm,
	},
	{ // 3 - Invalid HTTP headers for valid metadata entries for bucket/object
		Headers: http.Header{
			"X-Amz-Copy-Source-Server-Side-Encryption-Customer-Algorithm": []string{"AES256"},
			"X-Amz-Copy-Source-Server-Side-Encryption-Customer-Key":       []string{"MzJieXRlc2xvbmdzZWNyZXRrZXltdXN0cHJvdmlkZWQ="},
		},
		Bucket: "bucket",
		Object: "object",
		Metadata: map[string]string{
			"X-Minio-Internal-Server-Side-Encryption-Sealed-Key":     "IAAfAMBdYor5tf/UlVaQvwYlw5yKbPBeQqfygqsfHqhu1wHD9KDAP4bw38AhL12prFTS23JbbR9Re5Qv26ZnlQ==",
			"X-Minio-Internal-Server-Side-Encryption-Seal-Algorithm": "DAREv2-HMAC-SHA256",
			"X-Minio-Internal-Server-Side-Encryption-Iv":             "coVfGS3I/CTrqexX5vUN+PQPoP9aUFiPYYrSzqTWfBA=",
		},
		ExpectedErr: ErrMissingCustomerKeyMD5,
	},
}

func TestSSECopyUnsealObjectKey(t *testing.T) {
	for i, test := range sseCopyUnsealObjectKeyTests {
		if _, err := SSECopy.UnsealObjectKey(test.Headers, test.Metadata, test.Bucket, test.Object); err != test.ExpectedErr {
			t.Errorf("Test %d: got: %v - want: %v", i, err, test.ExpectedErr)
		}
	}
}

var s3UnsealObjectKeyTests = []struct {
	KMS            KMS
	Bucket, Object string
	Metadata       map[string]string

	ExpectedErr error
}{
	{ // 0 - Valid KMS key-ID and valid metadata entries for bucket/object
		KMS:    NewKMS([32]byte{}),
		Bucket: "bucket",
		Object: "object",
		Metadata: map[string]string{
			"X-Minio-Internal-Server-Side-Encryption-Iv":                "hhVY0LKR1YtZbzAKxTWUfZt5enDfYX6Fxz1ma8Kiudc=",
			"X-Minio-Internal-Server-Side-Encryption-S3-Sealed-Key":     "IAAfALhsOeD5AE3s5Zgq3DZ5VFGsOa3B0ksVC86veDcaj+fXv2U0VadhPaOKYr9Emd5ssOsO0uIhIIrKiOy9rA==",
			"X-Minio-Internal-Server-Side-Encryption-S3-Kms-Sealed-Key": "IAAfAMRS2iw45FsfiF3QXajSYVWj1lxMpQm6DxDGPtADCX6fJQQ4atHBtfpgqJFyeQmIHsm0FBI+UlHw1Lv4ug==",
			"X-Minio-Internal-Server-Side-Encryption-S3-Kms-Key-Id":     "test-key-1",
			"X-Minio-Internal-Server-Side-Encryption-Seal-Algorithm":    "DAREv2-HMAC-SHA256",
		},
		ExpectedErr: nil,
	},
	{ // 1 - Valid KMS key-ID for invalid metadata entries for bucket/object
		KMS:    NewKMS([32]byte{}),
		Bucket: "bucket",
		Object: "object",
		Metadata: map[string]string{
			"X-Minio-Internal-Server-Side-Encryption-Iv":                "hhVY0LKR1YtZbzAKxTWUfZt5enDfYX6Fxz1ma8Kiudc=",
			"X-Minio-Internal-Server-Side-Encryption-S3-Sealed-Key":     "IAAfALhsOeD5AE3s5Zgq3DZ5VFGsOa3B0ksVC86veDcaj+fXv2U0VadhPaOKYr9Emd5ssOsO0uIhIIrKiOy9rA==",
			"X-Minio-Internal-Server-Side-Encryption-S3-Kms-Sealed-Key": "IAAfAMRS2iw45FsfiF3QXajSYVWj1lxMpQm6DxDGPtADCX6fJQQ4atHBtfpgqJFyeQmIHsm0FBI+UlHw1Lv4ug==",
			"X-Minio-Internal-Server-Side-Encryption-S3-Kms-Key-Id":     "test-key-1",
		},
		ExpectedErr: errMissingInternalSealAlgorithm,
	},
}

func TestS3UnsealObjectKey(t *testing.T) {
	for i, test := range s3UnsealObjectKeyTests {
		if _, err := S3.UnsealObjectKey(test.KMS, test.Metadata, test.Bucket, test.Object); err != test.ExpectedErr {
			t.Errorf("Test %d: got: %v - want: %v", i, err, test.ExpectedErr)
		}
	}
}

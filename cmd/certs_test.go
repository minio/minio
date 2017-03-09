/*
 * Minio Cloud Storage, (C) 2015, 2016, 2017 Minio, Inc.
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
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func createTempFile(prefix, content string) (tempFile string, err error) {
	var tmpfile *os.File

	if tmpfile, err = ioutil.TempFile("", prefix); err != nil {
		return tempFile, err
	}

	if _, err = tmpfile.Write([]byte(content)); err != nil {
		return tempFile, err
	}

	if err = tmpfile.Close(); err != nil {
		return tempFile, err
	}

	tempFile = tmpfile.Name()
	return tempFile, err
}

func TestParsePublicCertFile(t *testing.T) {
	tempFile1, err := createTempFile("public-cert-file", "")
	if err != nil {
		t.Fatalf("Unable to create temporary file. %v", err)
	}
	defer os.Remove(tempFile1)

	tempFile2, err := createTempFile("public-cert-file",
		`-----BEGIN CERTIFICATE-----
MIICdTCCAd4CCQCO5G/W1xcE9TANBgkqhkiG9w0BAQUFADB/MQswCQYDVQQGEwJa
WTEOMAwGA1UECBMFTWluaW8xETAPBgNVBAcTCEludGVybmV0MQ4wDAYDVQQKEwVN
aW5pbzEOMAwGA1UECxMFTWluaW8xDjAMBgNVBAMTBU1pbmlvMR0wGwYJKoZIhvcN
AQkBFg50ZXN0c0BtaW5pby5pbzAeFw0xNjEwMTQxMTM0MjJaFw0xNzEwMTQxMTM0
MjJaMH8xCzAJBgNVBAYTAlpZMQ4wDAYDVQQIEwVNaW5pbzERMA8GA1UEBxMISW50
ZXJuZXQxDjAMBgNVBA-some-junk-Q4wDAYDVQQLEwVNaW5pbzEOMAwGA1UEAxMF
TWluaW8xHTAbBgkqhkiG9w0BCQEWDnRlc3RzQG1pbmlvLmlvMIGfMA0GCSqGSIb3
DQEBAQUAA4GNADCBiQKBgQDwNUYB/Sj79WsUE8qnXzzh2glSzWxUE79sCOpQYK83
HWkrl5WxlG8ZxDR1IQV9Ex/lzigJu8G+KXahon6a+3n5GhNrYRe5kIXHQHz0qvv4
aMulqlnYpvSfC83aaO9GVBtwXS/O4Nykd7QBg4nZlazVmsGk7POOjhpjGShRsqpU
JwIDAQABMA0GCSqGSIb3DQEBBQUAA4GBALqjOA6bD8BEl7hkQ8XwX/owSAL0URDe
nUfCOsXgIIAqgw4uTCLOfCJVZNKmRT+KguvPAQ6Z80vau2UxPX5Q2Q+OHXDRrEnK
FjqSBgLP06Qw7a++bshlWGTt5bHWOneW3EQikedckVuIKPkOCib9yGi4VmBBjdFE
M9ofSEt/bdRD
-----END CERTIFICATE-----`)
	if err != nil {
		t.Fatalf("Unable to create temporary file. %v", err)
	}
	defer os.Remove(tempFile2)

	tempFile3, err := createTempFile("public-cert-file",
		`-----BEGIN CERTIFICATE-----
MIICdTCCAd4CCQCO5G/W1xcE9TANBgkqhkiG9w0BAQUFADB/MQswCQYDVQQGEwJa
WTEOMAwGA1UECBMFTWluaW8xETAPBgNVBAcTCEludGVybmV0MQ4wDAYDVQQKEwVN
aW5pbzEOMAwGA1UECxMFTWluaW8xDjAMBgNVBAMTBU1pbmlvMR0wGwYJKoZIhvcN
AQkBFg50ZXN0c0BtaW5pby5pbzAeFw0xNjEwMTQxMTM0MjJaFw0xNzEwMTQxMTM0
MjJaMH8xCzAJBgNVBAYTAlpZMQ4wDAYDVQQIEwVNaW5pbzERMA8GA1UEBxMISW50
ZXJuZXQxDjAMBgNVBAabababababaQ4wDAYDVQQLEwVNaW5pbzEOMAwGA1UEAxMF
TWluaW8xHTAbBgkqhkiG9w0BCQEWDnRlc3RzQG1pbmlvLmlvMIGfMA0GCSqGSIb3
DQEBAQUAA4GNADCBiQKBgQDwNUYB/Sj79WsUE8qnXzzh2glSzWxUE79sCOpQYK83
HWkrl5WxlG8ZxDR1IQV9Ex/lzigJu8G+KXahon6a+3n5GhNrYRe5kIXHQHz0qvv4
aMulqlnYpvSfC83aaO9GVBtwXS/O4Nykd7QBg4nZlazVmsGk7POOjhpjGShRsqpU
JwIDAQABMA0GCSqGSIb3DQEBBQUAA4GBALqjOA6bD8BEl7hkQ8XwX/owSAL0URDe
nUfCOsXgIIAqgw4uTCLOfCJVZNKmRT+KguvPAQ6Z80vau2UxPX5Q2Q+OHXDRrEnK
FjqSBgLP06Qw7a++bshlWGTt5bHWOneW3EQikedckVuIKPkOCib9yGi4VmBBjdFE
M9ofSEt/bdRD
-----END CERTIFICATE-----`)
	if err != nil {
		t.Fatalf("Unable to create temporary file. %v", err)
	}
	defer os.Remove(tempFile3)

	tempFile4, err := createTempFile("public-cert-file",
		`-----BEGIN CERTIFICATE-----
MIICdTCCAd4CCQCO5G/W1xcE9TANBgkqhkiG9w0BAQUFADB/MQswCQYDVQQGEwJa
WTEOMAwGA1UECBMFTWluaW8xETAPBgNVBAcTCEludGVybmV0MQ4wDAYDVQQKEwVN
aW5pbzEOMAwGA1UECxMFTWluaW8xDjAMBgNVBAMTBU1pbmlvMR0wGwYJKoZIhvcN
AQkBFg50ZXN0c0BtaW5pby5pbzAeFw0xNjEwMTQxMTM0MjJaFw0xNzEwMTQxMTM0
MjJaMH8xCzAJBgNVBAYTAlpZMQ4wDAYDVQQIEwVNaW5pbzERMA8GA1UEBxMISW50
ZXJuZXQxDjAMBgNVBAoTBU1pbmlvMQ4wDAYDVQQLEwVNaW5pbzEOMAwGA1UEAxMF
TWluaW8xHTAbBgkqhkiG9w0BCQEWDnRlc3RzQG1pbmlvLmlvMIGfMA0GCSqGSIb3
DQEBAQUAA4GNADCBiQKBgQDwNUYB/Sj79WsUE8qnXzzh2glSzWxUE79sCOpQYK83
HWkrl5WxlG8ZxDR1IQV9Ex/lzigJu8G+KXahon6a+3n5GhNrYRe5kIXHQHz0qvv4
aMulqlnYpvSfC83aaO9GVBtwXS/O4Nykd7QBg4nZlazVmsGk7POOjhpjGShRsqpU
JwIDAQABMA0GCSqGSIb3DQEBBQUAA4GBALqjOA6bD8BEl7hkQ8XwX/owSAL0URDe
nUfCOsXgIIAqgw4uTCLOfCJVZNKmRT+KguvPAQ6Z80vau2UxPX5Q2Q+OHXDRrEnK
FjqSBgLP06Qw7a++bshlWGTt5bHWOneW3EQikedckVuIKPkOCib9yGi4VmBBjdFE
M9ofSEt/bdRD
-----END CERTIFICATE-----`)
	if err != nil {
		t.Fatalf("Unable to create temporary file. %v", err)
	}
	defer os.Remove(tempFile4)

	tempFile5, err := createTempFile("public-cert-file",
		`-----BEGIN CERTIFICATE-----
MIICdTCCAd4CCQCO5G/W1xcE9TANBgkqhkiG9w0BAQUFADB/MQswCQYDVQQGEwJa
WTEOMAwGA1UECBMFTWluaW8xETAPBgNVBAcTCEludGVybmV0MQ4wDAYDVQQKEwVN
aW5pbzEOMAwGA1UECxMFTWluaW8xDjAMBgNVBAMTBU1pbmlvMR0wGwYJKoZIhvcN
AQkBFg50ZXN0c0BtaW5pby5pbzAeFw0xNjEwMTQxMTM0MjJaFw0xNzEwMTQxMTM0
MjJaMH8xCzAJBgNVBAYTAlpZMQ4wDAYDVQQIEwVNaW5pbzERMA8GA1UEBxMISW50
ZXJuZXQxDjAMBgNVBAoTBU1pbmlvMQ4wDAYDVQQLEwVNaW5pbzEOMAwGA1UEAxMF
TWluaW8xHTAbBgkqhkiG9w0BCQEWDnRlc3RzQG1pbmlvLmlvMIGfMA0GCSqGSIb3
DQEBAQUAA4GNADCBiQKBgQDwNUYB/Sj79WsUE8qnXzzh2glSzWxUE79sCOpQYK83
HWkrl5WxlG8ZxDR1IQV9Ex/lzigJu8G+KXahon6a+3n5GhNrYRe5kIXHQHz0qvv4
aMulqlnYpvSfC83aaO9GVBtwXS/O4Nykd7QBg4nZlazVmsGk7POOjhpjGShRsqpU
JwIDAQABMA0GCSqGSIb3DQEBBQUAA4GBALqjOA6bD8BEl7hkQ8XwX/owSAL0URDe
nUfCOsXgIIAqgw4uTCLOfCJVZNKmRT+KguvPAQ6Z80vau2UxPX5Q2Q+OHXDRrEnK
FjqSBgLP06Qw7a++bshlWGTt5bHWOneW3EQikedckVuIKPkOCib9yGi4VmBBjdFE
M9ofSEt/bdRD
-----END CERTIFICATE-----
-----BEGIN CERTIFICATE-----
MIICdTCCAd4CCQCO5G/W1xcE9TANBgkqhkiG9w0BAQUFADB/MQswCQYDVQQGEwJa
WTEOMAwGA1UECBMFTWluaW8xETAPBgNVBAcTCEludGVybmV0MQ4wDAYDVQQKEwVN
aW5pbzEOMAwGA1UECxMFTWluaW8xDjAMBgNVBAMTBU1pbmlvMR0wGwYJKoZIhvcN
AQkBFg50ZXN0c0BtaW5pby5pbzAeFw0xNjEwMTQxMTM0MjJaFw0xNzEwMTQxMTM0
MjJaMH8xCzAJBgNVBAYTAlpZMQ4wDAYDVQQIEwVNaW5pbzERMA8GA1UEBxMISW50
ZXJuZXQxDjAMBgNVBAoTBU1pbmlvMQ4wDAYDVQQLEwVNaW5pbzEOMAwGA1UEAxMF
TWluaW8xHTAbBgkqhkiG9w0BCQEWDnRlc3RzQG1pbmlvLmlvMIGfMA0GCSqGSIb3
DQEBAQUAA4GNADCBiQKBgQDwNUYB/Sj79WsUE8qnXzzh2glSzWxUE79sCOpQYK83
HWkrl5WxlG8ZxDR1IQV9Ex/lzigJu8G+KXahon6a+3n5GhNrYRe5kIXHQHz0qvv4
aMulqlnYpvSfC83aaO9GVBtwXS/O4Nykd7QBg4nZlazVmsGk7POOjhpjGShRsqpU
JwIDAQABMA0GCSqGSIb3DQEBBQUAA4GBALqjOA6bD8BEl7hkQ8XwX/owSAL0URDe
nUfCOsXgIIAqgw4uTCLOfCJVZNKmRT+KguvPAQ6Z80vau2UxPX5Q2Q+OHXDRrEnK
FjqSBgLP06Qw7a++bshlWGTt5bHWOneW3EQikedckVuIKPkOCib9yGi4VmBBjdFE
M9ofSEt/bdRD
-----END CERTIFICATE-----`)
	if err != nil {
		t.Fatalf("Unable to create temporary file. %v", err)
	}
	defer os.Remove(tempFile5)

	nonexistentErr := fmt.Errorf("open nonexistent-file: no such file or directory")
	if runtime.GOOS == "windows" {
		// Below concatenation is done to get rid of goline error
		// "error strings should not be capitalized or end with punctuation or a newline"
		nonexistentErr = fmt.Errorf("open nonexistent-file:" + " The system cannot find the file specified.")
	}

	testCases := []struct {
		certFile          string
		expectedResultLen int
		expectedErr       error
	}{
		{"nonexistent-file", 0, nonexistentErr},
		{tempFile1, 0, fmt.Errorf("Empty public certificate file %s", tempFile1)},
		{tempFile2, 0, fmt.Errorf("Could not read PEM block from file %s", tempFile2)},
		{tempFile3, 0, fmt.Errorf("asn1: structure error: sequence tag mismatch")},
		{tempFile4, 1, nil},
		{tempFile5, 2, nil},
	}

	for _, testCase := range testCases {
		certs, err := parsePublicCertFile(testCase.certFile)

		if testCase.expectedErr == nil {
			if err != nil {
				t.Fatalf("error: expected = <nil>, got = %v", err)
			}
		} else if err == nil {
			t.Fatalf("error: expected = %v, got = <nil>", testCase.expectedErr)
		} else if testCase.expectedErr.Error() != err.Error() {
			t.Fatalf("error: expected = %v, got = %v", testCase.expectedErr, err)
		}

		if len(certs) != testCase.expectedResultLen {
			t.Fatalf("certs: expected = %v, got = %v", testCase.expectedResultLen, len(certs))
		}
	}
}

func TestGetRootCAs(t *testing.T) {
	emptydir, err := ioutil.TempDir("", "test-get-root-cas")
	if err != nil {
		t.Fatalf("Unable create temp directory. %v", emptydir)
	}
	defer os.RemoveAll(emptydir)

	dir1, err := ioutil.TempDir("", "test-get-root-cas")
	if err != nil {
		t.Fatalf("Unable create temp directory. %v", dir1)
	}
	defer os.RemoveAll(dir1)
	if err = os.Mkdir(filepath.Join(dir1, "empty-dir"), 0755); err != nil {
		t.Fatalf("Unable create empty dir. %v", err)
	}

	dir2, err := ioutil.TempDir("", "test-get-root-cas")
	if err != nil {
		t.Fatalf("Unable create temp directory. %v", dir2)
	}
	defer os.RemoveAll(dir2)
	if err = ioutil.WriteFile(filepath.Join(dir2, "empty-file"), []byte{}, 0644); err != nil {
		t.Fatalf("Unable create test file. %v", err)
	}

	nonexistentErr := fmt.Errorf("open nonexistent-dir: no such file or directory")
	if runtime.GOOS == "windows" {
		// Below concatenation is done to get rid of goline error
		// "error strings should not be capitalized or end with punctuation or a newline"
		nonexistentErr = fmt.Errorf("open nonexistent-dir:" + " The system cannot find the file specified.")
	}

	err1 := fmt.Errorf("read %s: is a directory", filepath.Join(dir1, "empty-dir"))
	if runtime.GOOS == "windows" {
		// Below concatenation is done to get rid of goline error
		// "error strings should not be capitalized or end with punctuation or a newline"
		err1 = fmt.Errorf("read %s:"+" The handle is invalid.", filepath.Join(dir1, "empty-dir"))
	}

	testCases := []struct {
		certCAsDir  string
		expectedErr error
	}{
		{"nonexistent-dir", nonexistentErr},
		{dir1, err1},
		{emptydir, nil},
		{dir2, nil},
	}

	for _, testCase := range testCases {
		_, err := getRootCAs(testCase.certCAsDir)

		if testCase.expectedErr == nil {
			if err != nil {
				t.Fatalf("error: expected = <nil>, got = %v", err)
			}
		} else if err == nil {
			t.Fatalf("error: expected = %v, got = <nil>", testCase.expectedErr)
		} else if testCase.expectedErr.Error() != err.Error() {
			t.Fatalf("error: expected = %v, got = %v", testCase.expectedErr, err)
		}
	}
}

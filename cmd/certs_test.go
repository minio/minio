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

func TestParsePKCS12CertFile(t *testing.T) {
	pfxContent := `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
`
	tempPKCS12File, err := createTempFile("test-parse-pkcs12-cert-file", pfxContent)
	if err != nil {
		t.Fatalf("Unable create temporary pkcs12 file. %v", err)
	}
	defer os.Remove(tempPKCS12File)

	x509Certs, tlsCert, err := parsePKCS12CertFile(tempPKCS12File)
	if err != nil {
		t.Fatalf("error: expected: <nil>, got: %v", err)
	}
	if len(x509Certs) != 1 {
		t.Fatalf("x509certs: expected: 1, got: %v", len(x509Certs))
	}
	if tlsCert == nil {
		t.Fatalf("tlscert: expected: <tls certificate>, got: <nil>")
	}
}

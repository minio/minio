/*
 * Minio Cloud Storage, (C) 2015, 2016 Minio, Inc.
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
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Make sure we have a valid certs path.
func TestGetCertsPath(t *testing.T) {
	path, err := getCertsPath()
	if err != nil {
		t.Error(err)
	}
	if path == "" {
		t.Errorf("expected path to not be an empty string, got: '%s'", path)
	}
	// Ensure it contains some sort of path separator.
	if !strings.ContainsRune(path, os.PathSeparator) {
		t.Errorf("expected path to contain file separator")
	}
	// It should also be an absolute path.
	if !filepath.IsAbs(path) {
		t.Errorf("expected path to be an absolute path")
	}

	// This will error if something goes wrong, so just call it.
	mustGetCertsPath()
}

// Ensure that the certificate and key file getters contain their respective
// file name and endings.
func TestGetFiles(t *testing.T) {
	file := mustGetCertFile()
	if !strings.Contains(file, globalMinioCertFile) {
		t.Errorf("CertFile does not contain %s", globalMinioCertFile)
	}

	file = mustGetKeyFile()
	if !strings.Contains(file, globalMinioKeyFile) {
		t.Errorf("KeyFile does not contain %s", globalMinioKeyFile)
	}
}

// Parses .crt file contents
func TestParseCertificateChain(t *testing.T) {
	// given
	cert := `-----BEGIN CERTIFICATE-----
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
-----END CERTIFICATE-----`

	// when
	certs, err := parseCertificateChain([]byte(cert))

	// then
	if err != nil {
		t.Fatalf("Could not parse certificate: %s", err)
	}

	if len(certs) != 1 {
		t.Fatalf("Expected number of certificates in chain was 1, actual: %d", len(certs))
	}

	if certs[0].Subject.CommonName != "Minio" {
		t.Fatalf("Expected Subject.CommonName was Minio, actual: %s", certs[0].Subject.CommonName)
	}
}

// Parses invalid .crt file contents and returns error
func TestParseInvalidCertificateChain(t *testing.T) {
	// given
	cert := `This is now valid certificate`

	// when
	_, err := parseCertificateChain([]byte(cert))

	// then
	if err == nil {
		t.Fatalf("Expected error but none occurred")
	}
}

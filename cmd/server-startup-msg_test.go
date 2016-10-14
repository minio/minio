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
	"crypto/x509"
	"crypto/x509/pkix"
	"fmt"
	"strings"
	"testing"
	"time"
)

// Tests if we generate storage info.
func TestStorageInfoMsg(t *testing.T) {
	infoStorage := StorageInfo{
		Total: 1024 * 1024 * 1024 * 10,
		Free:  1024 * 1024 * 1024 * 2,
		Backend: struct {
			Type         BackendType
			OnlineDisks  int
			OfflineDisks int
			ReadQuorum   int
			WriteQuorum  int
		}{XL, 7, 1, 4, 5},
	}

	if msg := getStorageInfoMsg(infoStorage); !strings.Contains(msg, "1.0 GiB Free, 5.0 GiB Total") || !strings.Contains(msg, "7 Online, 1 Offline") {
		t.Fatal("Empty message string is not implemented", msg)
	}
}

// Tests if certificate expiry warning will be printed
func TestCertificateExpiryInfo(t *testing.T) {
	// given
	var expiredDate = time.Now().Add(time.Hour * 24 * (globalMinioCertExpireWarnDays - 1))

	var fakeCerts = []*x509.Certificate{
		&x509.Certificate{
			NotAfter: expiredDate,
			Subject: pkix.Name{
				CommonName: "Test cert",
			},
		},
	}

	expectedMsg := colorBlue("\nCertificate expiry info:\n") +
		colorBold(fmt.Sprintf("#1 Test cert will expire on %s\n", expiredDate))

	// when
	msg := getCertificateChainMsg(fakeCerts)

	// then
	if msg != expectedMsg {
		t.Fatalf("Expected message was: %s, got: %s", expectedMsg, msg)
	}
}

// Tests if certificate expiry warning will not be printed if certificate not expired
func TestCertificateNotExpired(t *testing.T) {
	// given
	var expiredDate = time.Now().Add(time.Hour * 24 * (globalMinioCertExpireWarnDays + 1))

	var fakeCerts = []*x509.Certificate{
		&x509.Certificate{
			NotAfter: expiredDate,
			Subject: pkix.Name{
				CommonName: "Test cert",
			},
		},
	}

	// when
	msg := getCertificateChainMsg(fakeCerts)

	// then
	if msg != "" {
		t.Fatalf("Expected empty message was: %s", msg)
	}
}

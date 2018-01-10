/*
 * Minio Cloud Storage, (C) 2016, 2017 Minio, Inc.
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
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	humanize "github.com/dustin/go-humanize"
)

// Tests if we generate storage info.
func TestStorageInfoMsg(t *testing.T) {
	infoStorage := StorageInfo{}
	infoStorage.Total = 10 * humanize.GiByte
	infoStorage.Free = 2 * humanize.GiByte
	infoStorage.Backend.Type = Erasure
	infoStorage.Backend.OnlineDisks = 7
	infoStorage.Backend.OfflineDisks = 1

	if msg := getStorageInfoMsg(infoStorage); !strings.Contains(msg, "2.0 GiB Free, 10 GiB Total") || !strings.Contains(msg, "7 Online, 1 Offline") {
		t.Fatal("Unexpected storage info message, found:", msg)
	}
}

// Tests if certificate expiry warning will be printed
func TestCertificateExpiryInfo(t *testing.T) {
	// given
	var expiredDate = time.Now().Add(time.Hour * 24 * (30 - 1)) // 29 days.

	var fakeCerts = []*x509.Certificate{
		{
			NotAfter: expiredDate,
			Subject: pkix.Name{
				CommonName: "Test cert",
			},
		},
	}

	expectedMsg := colorBlue("\nCertificate expiry info:\n") +
		colorBold(fmt.Sprintf("#1 Test cert will expire on %s\n", expiredDate))

	// When
	msg := getCertificateChainMsg(fakeCerts)

	// Then
	if msg != expectedMsg {
		t.Fatalf("Expected message was: %s, got: %s", expectedMsg, msg)
	}
}

// Tests if certificate expiry warning will not be printed if certificate not expired
func TestCertificateNotExpired(t *testing.T) {
	// given
	var expiredDate = time.Now().Add(time.Hour * 24 * (30 + 1)) // 31 days.

	var fakeCerts = []*x509.Certificate{
		{
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

// Tests stripping standard ports from apiEndpoints.
func TestStripStandardPorts(t *testing.T) {
	apiEndpoints := []string{"http://127.0.0.1:9000", "http://127.0.0.2:80", "https://127.0.0.3:443"}
	expectedAPIEndpoints := []string{"http://127.0.0.1:9000", "http://127.0.0.2", "https://127.0.0.3"}
	newAPIEndpoints := stripStandardPorts(apiEndpoints)

	if !reflect.DeepEqual(expectedAPIEndpoints, newAPIEndpoints) {
		t.Fatalf("Expected %#v, got %#v", expectedAPIEndpoints, newAPIEndpoints)
	}

	apiEndpoints = []string{"http://%%%%%:9000"}
	newAPIEndpoints = stripStandardPorts(apiEndpoints)
	if !reflect.DeepEqual(apiEndpoints, newAPIEndpoints) {
		t.Fatalf("Expected %#v, got %#v", apiEndpoints, newAPIEndpoints)
	}

	apiEndpoints = []string{"http://127.0.0.1:443", "https://127.0.0.1:80"}
	newAPIEndpoints = stripStandardPorts(apiEndpoints)
	if !reflect.DeepEqual(apiEndpoints, newAPIEndpoints) {
		t.Fatalf("Expected %#v, got %#v", apiEndpoints, newAPIEndpoints)
	}
}

// Test printing server common message.
func TestPrintServerCommonMessage(t *testing.T) {
	root, err := newTestConfig(globalMinioDefaultRegion)
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(root)

	apiEndpoints := []string{"http://127.0.0.1:9000"}
	printServerCommonMsg(apiEndpoints)
}

// Tests print cli access message.
func TestPrintCLIAccessMsg(t *testing.T) {
	root, err := newTestConfig(globalMinioDefaultRegion)
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(root)

	apiEndpoints := []string{"http://127.0.0.1:9000"}
	printCLIAccessMsg(apiEndpoints[0], "myminio")
}

// Test print startup message.
func TestPrintStartupMessage(t *testing.T) {
	root, err := newTestConfig(globalMinioDefaultRegion)
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(root)

	apiEndpoints := []string{"http://127.0.0.1:9000"}
	printStartupMessage(apiEndpoints)
}

/*
 * MinIO Cloud Storage, (C) 2018 MinIO, Inc.
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

package certs_test

import (
	"crypto/tls"
	"io"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/minio/minio/pkg/certs"
)

func updateCerts(crt, key string) {
	// ignore error handling
	crtSource, _ := os.Open(crt)
	defer crtSource.Close()
	crtDest, _ := os.Create("server.crt")
	defer crtDest.Close()
	io.Copy(crtDest, crtSource)

	keySource, _ := os.Open(key)
	defer keySource.Close()
	keyDest, _ := os.Create("server.key")
	defer keyDest.Close()
	io.Copy(keyDest, keySource)
}

func TestCertNew(t *testing.T) {
	c, err := certs.New("server.crt", "server.key", tls.LoadX509KeyPair)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Stop()
	hello := &tls.ClientHelloInfo{}
	gcert, err := c.GetCertificate(hello)
	if err != nil {
		t.Fatal(err)
	}
	expectedCert, err := tls.LoadX509KeyPair("server.crt", "server.key")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(gcert.Certificate, expectedCert.Certificate) {
		t.Error("certificate doesn't match expected certificate")
	}
	_, err = certs.New("server.crt", "server2.key", tls.LoadX509KeyPair)
	if err == nil {
		t.Fatal("Expected to fail but got success")
	}
}

func TestValidPairAfterWrite(t *testing.T) {
	expectedCert, err := tls.LoadX509KeyPair("server2.crt", "server2.key")
	if err != nil {
		t.Fatal(err)
	}

	c, err := certs.New("server.crt", "server.key", tls.LoadX509KeyPair)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Stop()

	updateCerts("server2.crt", "server2.key")
	defer updateCerts("server1.crt", "server1.key")

	// Wait for the write event..
	time.Sleep(200 * time.Millisecond)

	hello := &tls.ClientHelloInfo{}
	gcert, err := c.GetCertificate(hello)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(gcert.Certificate, expectedCert.Certificate) {
		t.Error("certificate doesn't match expected certificate")
	}

	rInfo := &tls.CertificateRequestInfo{}
	gcert, err = c.GetClientCertificate(rInfo)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(gcert.Certificate, expectedCert.Certificate) {
		t.Error("client certificate doesn't match expected certificate")
	}
}

func TestStop(t *testing.T) {
	expectedCert, err := tls.LoadX509KeyPair("server2.crt", "server2.key")
	if err != nil {
		t.Fatal(err)
	}

	c, err := certs.New("server.crt", "server.key", tls.LoadX509KeyPair)
	if err != nil {
		t.Fatal(err)
	}
	c.Stop()

	// No one is listening on the event, will be ignored and
	// certificate will not be reloaded.
	updateCerts("server2.crt", "server2.key")
	defer updateCerts("server1.crt", "server1.key")

	hello := &tls.ClientHelloInfo{}
	gcert, err := c.GetCertificate(hello)
	if err != nil {
		t.Fatal(err)
	}

	if reflect.DeepEqual(gcert.Certificate, expectedCert.Certificate) {
		t.Error("certificate shouldn't match, but matched")
	}
}

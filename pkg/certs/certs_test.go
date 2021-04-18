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

package certs_test

import (
	"context"
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
	crtDest, _ := os.Create("public.crt")
	defer crtDest.Close()
	io.Copy(crtDest, crtSource)

	keySource, _ := os.Open(key)
	defer keySource.Close()
	keyDest, _ := os.Create("private.key")
	defer keyDest.Close()
	io.Copy(keyDest, keySource)
}

func TestNewManager(t *testing.T) {
	ctx, cancelFn := context.WithCancel(context.Background())
	defer cancelFn()
	c, err := certs.NewManager(ctx, "public.crt", "private.key", tls.LoadX509KeyPair)
	if err != nil {
		t.Fatal(err)
	}
	hello := &tls.ClientHelloInfo{}
	gcert, err := c.GetCertificate(hello)
	if err != nil {
		t.Fatal(err)
	}
	expectedCert, err := tls.LoadX509KeyPair("public.crt", "private.key")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(gcert.Certificate, expectedCert.Certificate) {
		t.Error("certificate doesn't match expected certificate")
	}
	_, err = certs.NewManager(ctx, "public.crt", "new-private.key", tls.LoadX509KeyPair)
	if err == nil {
		t.Fatal("Expected to fail but got success")
	}
}

func TestValidPairAfterWrite(t *testing.T) {
	ctx, cancelFn := context.WithCancel(context.Background())
	defer cancelFn()
	expectedCert, err := tls.LoadX509KeyPair("new-public.crt", "new-private.key")
	if err != nil {
		t.Fatal(err)
	}

	c, err := certs.NewManager(ctx, "public.crt", "private.key", tls.LoadX509KeyPair)
	if err != nil {
		t.Fatal(err)
	}

	updateCerts("new-public.crt", "new-private.key")
	defer updateCerts("original-public.crt", "original-private.key")

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

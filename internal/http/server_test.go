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

package http

import (
	"crypto/tls"
	"fmt"
	"net/http"
	"reflect"
	"testing"

	"github.com/minio/pkg/v3/certs"
)

func TestNewServer(t *testing.T) {
	nonLoopBackIP := getNonLoopBackIP(t)
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "Hello, world")
	})

	testCases := []struct {
		addrs   []string
		handler http.Handler
		certFn  certs.GetCertificateFunc
	}{
		{[]string{"127.0.0.1:9000"}, handler, nil},
		{[]string{nonLoopBackIP + ":9000"}, handler, nil},
		{[]string{"127.0.0.1:9000", nonLoopBackIP + ":9000"}, handler, nil},
		{[]string{"127.0.0.1:9000"}, handler, getCert},
		{[]string{nonLoopBackIP + ":9000"}, handler, getCert},
		{[]string{"127.0.0.1:9000", nonLoopBackIP + ":9000"}, handler, getCert},
	}

	for i, testCase := range testCases {
		server := NewServer(testCase.addrs).
			UseHandler(testCase.handler)
		if testCase.certFn != nil {
			server = server.UseTLSConfig(&tls.Config{
				PreferServerCipherSuites: true,
				GetCertificate:           testCase.certFn,
			})
		}
		if server == nil {
			t.Fatalf("Case %v: server: expected: <non-nil>, got: <nil>", (i + 1))
		} else if !reflect.DeepEqual(server.Addrs, testCase.addrs) {
			t.Fatalf("Case %v: server.Addrs: expected: %v, got: %v", (i + 1), testCase.addrs, server.Addrs)
		}

		if testCase.certFn == nil {
			if server.TLSConfig != nil {
				t.Fatalf("Case %v: server.TLSConfig: expected: <nil>, got: %v", (i + 1), server.TLSConfig)
			}
		} else {
			if server.TLSConfig == nil {
				t.Fatalf("Case %v: server.TLSConfig: expected: <non-nil>, got: <nil>", (i + 1))
			}
		}

		if server.MaxHeaderBytes != DefaultMaxHeaderBytes {
			t.Fatalf("Case %v: server.MaxHeaderBytes: expected: %v, got: %v", (i + 1), DefaultMaxHeaderBytes, server.MaxHeaderBytes)
		}
	}
}

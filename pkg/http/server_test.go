/*
 * Minio Cloud Storage, (C) 2017 Minio, Inc.
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

package http

import (
	"crypto/tls"
	"fmt"
	"net/http"
	"reflect"
	"testing"
)

func TestNewServer(t *testing.T) {
	nonLoopBackIP := getNonLoopBackIP(t)
	certificate, err := getTLSCert()
	if err != nil {
		t.Fatalf("Unable to parse private/certificate data. %v\n", err)
	}
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "Hello, world")
	})

	testCases := []struct {
		addrs       []string
		handler     http.Handler
		certificate *tls.Certificate
	}{
		{[]string{"127.0.0.1:9000"}, handler, nil},
		{[]string{nonLoopBackIP + ":9000"}, handler, nil},
		{[]string{"127.0.0.1:9000", nonLoopBackIP + ":9000"}, handler, nil},
		{[]string{"127.0.0.1:9000"}, handler, &certificate},
		{[]string{nonLoopBackIP + ":9000"}, handler, &certificate},
		{[]string{"127.0.0.1:9000", nonLoopBackIP + ":9000"}, handler, &certificate},
	}

	for i, testCase := range testCases {
		server := NewServer(testCase.addrs, testCase.handler, testCase.certificate)
		if server == nil {
			t.Fatalf("Case %v: server: expected: <non-nil>, got: <nil>", (i + 1))
		}

		if !reflect.DeepEqual(server.Addrs, testCase.addrs) {
			t.Fatalf("Case %v: server.Addrs: expected: %v, got: %v", (i + 1), testCase.addrs, server.Addrs)
		}

		// Interfaces are not comparable even with reflection.
		// if !reflect.DeepEqual(server.Handler, testCase.handler) {
		// 	t.Fatalf("Case %v: server.Handler: expected: %v, got: %v", (i + 1), testCase.handler, server.Handler)
		// }

		if testCase.certificate == nil {
			if server.TLSConfig != nil {
				t.Fatalf("Case %v: server.TLSConfig: expected: <nil>, got: %v", (i + 1), server.TLSConfig)
			}
		} else {
			if server.TLSConfig == nil {
				t.Fatalf("Case %v: server.TLSConfig: expected: <non-nil>, got: <nil>", (i + 1))
			}
		}

		if server.ShutdownTimeout != DefaultShutdownTimeout {
			t.Fatalf("Case %v: server.ShutdownTimeout: expected: %v, got: %v", (i + 1), DefaultShutdownTimeout, server.ShutdownTimeout)
		}

		if server.TCPKeepAliveTimeout != DefaultTCPKeepAliveTimeout {
			t.Fatalf("Case %v: server.TCPKeepAliveTimeout: expected: %v, got: %v", (i + 1), DefaultTCPKeepAliveTimeout, server.TCPKeepAliveTimeout)
		}

		if server.listenerMutex == nil {
			t.Fatalf("Case %v: server.listenerMutex: expected: <non-nil>, got: <nil>", (i + 1))
		}

		if server.ReadTimeout != DefaultReadTimeout {
			t.Fatalf("Case %v: server.ReadTimeout: expected: %v, got: %v", (i + 1), DefaultReadTimeout, server.ReadTimeout)
		}

		if server.WriteTimeout != DefaultWriteTimeout {
			t.Fatalf("Case %v: server.WriteTimeout: expected: %v, got: %v", (i + 1), DefaultWriteTimeout, server.WriteTimeout)
		}

		if server.MaxHeaderBytes != DefaultMaxHeaderBytes {
			t.Fatalf("Case %v: server.MaxHeaderBytes: expected: %v, got: %v", (i + 1), DefaultMaxHeaderBytes, server.MaxHeaderBytes)
		}
	}
}

var tlsCipherSuitesTests = []struct {
	ciphers    []uint16
	shouldFail bool
}{
	{ciphers: nil, shouldFail: false},
	{ciphers: defaultCipherSuites, shouldFail: false},
	{ciphers: unsupportedCipherSuites, shouldFail: true},
}

// Tests the server accepts only TLS connections when at least
// one of the supported (secure) cipher suites is supported by the client.
func TestTLSCiphers(t *testing.T) {
	certificate, err := getTLSCert()
	if err != nil {
		t.Fatalf("Unable to parse private/certificate data. %v\n", err)
	}
	port := getNextPort()
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write(nil)
	})
	server := NewServer([]string{"127.0.0.1:" + port}, handler, &certificate)
	go func() { server.Start() }()
	defer server.Shutdown()

	for !server.IsStarted() { // block until server is available
	}
	for i, testCase := range tlsCipherSuitesTests {
		client := http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{
					InsecureSkipVerify: true,
					CipherSuites:       testCase.ciphers,
				},
			},
		}
		_, err := client.Get("https://127.0.0.1:" + port)
		if err != nil && !testCase.shouldFail {
			t.Errorf("Test %d: Failed to execute GET request: %s", i, err)
		}
		if err == nil && testCase.shouldFail {
			t.Errorf("Test %d: Successfully connected to server but expected connection failure", i)
		}
	}
}

// Tests whether a client can successfully establish a TLS connection to
// a server with the Go standard library default cipher suites.
func TestStandardTLSCiphers(t *testing.T) {
	certificate, err := getTLSCert()
	if err != nil {
		t.Fatalf("Unable to parse private/certificate data. %v\n", err)
	}
	port := getNextPort()
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "Hello, world")
	})
	server := NewServer([]string{"127.0.0.1:" + port}, handler, &certificate)
	server.TLSConfig.CipherSuites = nil // use Go standard ciphers
	go func() { server.Start() }()
	defer server.Shutdown()

	for !server.IsStarted() { // block until server is available
	}
	client := http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
		},
	}
	_, err = client.Get("https://127.0.0.1:" + port + "/")
	if err != nil {
		t.Fatalf("Failed to execute GET request: %s", err)
	}
}

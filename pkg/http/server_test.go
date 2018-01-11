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
	"time"
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

func TestServerTLSCiphers(t *testing.T) {
	var unsupportedCipherSuites = []uint16{
		tls.TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA,    // Go stack contains (some) countermeasures against timing attacks (Lucky13)
		tls.TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256, // No countermeasures against timing attacks
		tls.TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA,    // Go stack contains (some) countermeasures against timing attacks (Lucky13)
		tls.TLS_ECDHE_ECDSA_WITH_RC4_128_SHA,        // Broken cipher
		tls.TLS_ECDHE_RSA_WITH_3DES_EDE_CBC_SHA,     // Sweet32
		tls.TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA,      // Go stack contains (some) countermeasures against timing attacks (Lucky13)
		tls.TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256,   // No countermeasures against timing attacks
		tls.TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA,      // Go stack contains (some) countermeasures against timing attacks (Lucky13)
		tls.TLS_ECDHE_RSA_WITH_RC4_128_SHA,          // Broken cipher

		// all RSA-PKCS1-v1.5 ciphers are disabled - danger of Bleichenbacher attack variants
		tls.TLS_RSA_WITH_3DES_EDE_CBC_SHA,   // Sweet32
		tls.TLS_RSA_WITH_AES_128_CBC_SHA,    // Go stack contains (some) countermeasures against timing attacks (Lucky13)
		tls.TLS_RSA_WITH_AES_128_CBC_SHA256, // No countermeasures against timing attacks
		tls.TLS_RSA_WITH_AES_256_CBC_SHA,    // Go stack contains (some) countermeasures against timing attacks (Lucky13)
		tls.TLS_RSA_WITH_RC4_128_SHA,        // Broken cipher

		tls.TLS_RSA_WITH_AES_128_GCM_SHA256, // Disabled because of RSA-PKCS1-v1.5 - AES-GCM is considered secure.
		tls.TLS_RSA_WITH_AES_256_GCM_SHA384, // Disabled because of RSA-PKCS1-v1.5 - AES-GCM is considered secure.
	}

	certificate, err := getTLSCert()
	if err != nil {
		t.Fatalf("Unable to parse private/certificate data. %v\n", err)
	}

	testCases := []struct {
		ciphers            []uint16
		resetServerCiphers bool
		expectErr          bool
	}{
		{nil, false, false},
		{defaultCipherSuites, false, false},
		{unsupportedCipherSuites, false, true},
		{nil, true, false},
	}

	for i, testCase := range testCases {
		func() {
			addr := "127.0.0.1:" + getNextPort()

			server := NewServer([]string{addr},
				http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					fmt.Fprintf(w, "Hello, world")
				}),
				&certificate)
			if testCase.resetServerCiphers {
				// Use Go default ciphers.
				server.TLSConfig.CipherSuites = nil
			}

			go func() {
				server.Start()
			}()
			defer server.Shutdown()

			client := http.Client{
				Transport: &http.Transport{
					TLSClientConfig: &tls.Config{
						InsecureSkipVerify: true,
						CipherSuites:       testCase.ciphers,
					},
				},
			}

			// There is no guaranteed way to know whether the HTTP server is started successfully.
			// The only option is to connect and check.  Hence below sleep is used as workaround.
			time.Sleep(1 * time.Second)

			_, err := client.Get("https://" + addr)
			expectErr := (err != nil)

			if expectErr != testCase.expectErr {
				t.Fatalf("test %v: error: expected: %v, got: %v", i+1, testCase.expectErr, expectErr)
			}
		}()
	}
}

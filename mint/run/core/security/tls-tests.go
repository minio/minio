// Mint, (C) 2018 Minio, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software

// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"os"
	"time"

	log "github.com/sirupsen/logrus"
)

const testName = "TLS-tests"

const (
	// PASS indicate that a test passed
	PASS = "PASS"
	// FAIL indicate that a test failed
	FAIL = "FAIL"
	// NA indicates that a test is not applicable
	NA = "NA"
)

func main() {
	log.SetOutput(os.Stdout)
	log.SetFormatter(&mintJSONFormatter{})
	log.SetLevel(log.InfoLevel)

	endpoint := os.Getenv("SERVER_ENDPOINT")
	secure := os.Getenv("ENABLE_HTTPS")
	if secure != "1" {
		log.WithFields(log.Fields{"name:": testName, "status": NA, "message": "TLS is not enabled"}).Info()
		return
	}

	testTLSVersions(endpoint)
	testTLSCiphers(endpoint)
	testTLSEllipticCurves(endpoint)
}

// Tests whether the endpoint accepts TLS1.0 or TLS1.1 connections - fail if so.
// Tests whether the endpoint accepts TLS1.2 connections - fail if not.
func testTLSVersions(endpoint string) {
	const function = "TLSVersions"
	startTime := time.Now()

	// Tests whether the endpoint accepts TLS1.0 or TLS1.1 connections
	args := map[string]interface{}{
		"MinVersion": "tls.VersionTLS10",
		"MaxVersion": "tls.VersionTLS11",
	}
	_, err := tls.Dial("tcp", endpoint, &tls.Config{
		MinVersion: tls.VersionTLS10,
		MaxVersion: tls.VersionTLS11,
	})
	if err == nil {
		failureLog(function, args, startTime, "", "Endpoint accepts insecure connection", err).Error()
		return
	}

	// Tests whether the endpoint accepts TLS1.2 connections
	args = map[string]interface{}{
		"MinVersion": "tls.VersionTLS12",
	}
	_, err = tls.Dial("tcp", endpoint, &tls.Config{
		MinVersion: tls.VersionTLS12,
	})
	if err != nil {
		failureLog(function, args, startTime, "", "Endpoint rejects secure connection", err).Error()
		return
	}
	successLog(function, args, startTime)
}

// Tests whether the endpoint accepts SSL3.0, TLS1.0 or TLS1.1 connections - fail if so.
// Tests whether the endpoint accepts TLS1.2 connections - fail if not.
func testTLSCiphers(endpoint string) {
	const function = "TLSCiphers"
	startTime := time.Now()

	// Tests whether the endpoint accepts insecure ciphers
	args := map[string]interface{}{
		"MinVersion":   "tls.VersionTLS12",
		"CipherSuites": unsupportedCipherSuites,
	}
	_, err := tls.Dial("tcp", endpoint, &tls.Config{
		MinVersion:   tls.VersionTLS12,
		CipherSuites: unsupportedCipherSuites,
	})
	if err == nil {
		failureLog(function, args, startTime, "", "Endpoint accepts insecure cipher suites", err).Error()
		return
	}

	// Tests whether the endpoint accepts at least one secure cipher
	args = map[string]interface{}{
		"MinVersion":   "tls.VersionTLS12",
		"CipherSuites": supportedCipherSuites,
	}
	_, err = tls.Dial("tcp", endpoint, &tls.Config{
		MinVersion:   tls.VersionTLS12,
		CipherSuites: supportedCipherSuites,
	})
	if err != nil {
		failureLog(function, args, startTime, "", "Endpoint rejects all secure cipher suites", err).Error()
		return
	}

	// Tests whether the endpoint accepts at least one default cipher
	args = map[string]interface{}{
		"MinVersion":   "tls.VersionTLS12",
		"CipherSuites": nil,
	}
	_, err = tls.Dial("tcp", endpoint, &tls.Config{
		MinVersion:   tls.VersionTLS12,
		CipherSuites: nil, // default value
	})
	if err != nil {
		failureLog(function, args, startTime, "", "Endpoint rejects default cipher suites", err).Error()
		return
	}
	successLog(function, args, startTime)
}

// Tests whether the endpoint accepts the P-384 or P-521 elliptic curve - fail if so.
// Tests whether the endpoint accepts Curve25519 or P-256 - fail if not.
func testTLSEllipticCurves(endpoint string) {
	const function = "TLSEllipticCurves"
	startTime := time.Now()

	// Tests whether the endpoint accepts curves using non-constant time implementations.
	args := map[string]interface{}{
		"CurvePreferences": unsupportedCurves,
	}
	_, err := tls.Dial("tcp", endpoint, &tls.Config{
		MinVersion:       tls.VersionTLS12,
		CurvePreferences: unsupportedCurves,
		CipherSuites:     supportedCipherSuites,
	})
	if err == nil {
		failureLog(function, args, startTime, "", "Endpoint accepts insecure elliptic curves", err).Error()
		return
	}

	// Tests whether the endpoint accepts curves using constant time implementations.
	args = map[string]interface{}{
		"CurvePreferences": unsupportedCurves,
	}
	_, err = tls.Dial("tcp", endpoint, &tls.Config{
		MinVersion:       tls.VersionTLS12,
		CurvePreferences: supportedCurves,
		CipherSuites:     supportedCipherSuites,
	})
	if err != nil {
		failureLog(function, args, startTime, "", "Endpoint does not accept secure elliptic curves", err).Error()
		return
	}
	successLog(function, args, startTime)
}

func successLog(function string, args map[string]interface{}, startTime time.Time) *log.Entry {
	duration := time.Since(startTime).Nanoseconds() / 1000000
	return log.WithFields(log.Fields{
		"name":     testName,
		"function": function,
		"args":     args,
		"duration": duration,
		"status":   PASS,
	})
}

func failureLog(function string, args map[string]interface{}, startTime time.Time, alert string, message string, err error) *log.Entry {
	duration := time.Since(startTime).Nanoseconds() / 1000000
	fields := log.Fields{
		"name":     testName,
		"function": function,
		"args":     args,
		"duration": duration,
		"status":   FAIL,
		"alert":    alert,
		"message":  message,
	}
	if err != nil {
		fields["error"] = err
	}
	return log.WithFields(fields)
}

type mintJSONFormatter struct {
}

func (f *mintJSONFormatter) Format(entry *log.Entry) ([]byte, error) {
	data := make(log.Fields, len(entry.Data))
	for k, v := range entry.Data {
		switch v := v.(type) {
		case error:
			// Otherwise errors are ignored by `encoding/json`
			// https://github.com/sirupsen/logrus/issues/137
			data[k] = v.Error()
		default:
			data[k] = v
		}
	}

	serialized, err := json.Marshal(data)
	if err != nil {
		return nil, fmt.Errorf("Failed to marshal fields to JSON, %w", err)
	}
	return append(serialized, '\n'), nil
}

// Secure Go implementations of modern TLS ciphers
// The following ciphers are excluded because:
//  - RC4 ciphers:              RC4 is broken
//  - 3DES ciphers:             Because of the 64 bit blocksize of DES (Sweet32)
//  - CBC-SHA256 ciphers:       No countermeasures against Lucky13 timing attack
//  - CBC-SHA ciphers:          Legacy ciphers (SHA-1) and non-constant time
//                              implementation of CBC.
//                              (CBC-SHA ciphers can be enabled again if required)
//  - RSA key exchange ciphers: Disabled because of dangerous PKCS1-v1.5 RSA
//                              padding scheme. See Bleichenbacher attacks.
var supportedCipherSuites = []uint16{
	tls.TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305,
	tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305,
	tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
	tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
	tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
	tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
}

// Supported elliptic curves: Implementations are constant-time.
var supportedCurves = []tls.CurveID{tls.X25519, tls.CurveP256}

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

// Unsupported elliptic curves: Implementations are not constant-time.
var unsupportedCurves = []tls.CurveID{tls.CurveP384, tls.CurveP521}

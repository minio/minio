/*
 * Minio Cloud Storage, (C) 2018 Minio, Inc.
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

package handlers

import (
	"net/http"
	"testing"
)

type headerTest struct {
	key      string // header key
	val      string // header val
	expected string // expected result
}

func TestGetScheme(t *testing.T) {
	headers := []headerTest{
		{xForwardedProto, "https", "https"},
		{xForwardedProto, "http", "http"},
		{xForwardedProto, "HTTP", "http"},
		{xForwardedScheme, "https", "https"},
		{xForwardedScheme, "http", "http"},
		{xForwardedScheme, "HTTP", "http"},
		{forwarded, `For="[2001:db8:cafe::17]:4711`, ""},                    // No proto
		{forwarded, `for=192.0.2.43, for=198.51.100.17;proto=https`, ""},    // Multiple params, will be empty.
		{forwarded, `for=172.32.10.15; proto=https;by=127.0.0.1;`, "https"}, // Space before proto
		{forwarded, `for=192.0.2.60;proto=http;by=203.0.113.43`, "http"},    // Multiple params
	}
	for _, v := range headers {
		req := &http.Request{
			Header: http.Header{
				v.key: []string{v.val},
			}}
		res := GetSourceScheme(req)
		if res != v.expected {
			t.Errorf("wrong header for %s: got %s want %s", v.key, res,
				v.expected)
		}
	}
}

// TestGetSourceIP - check the source ip of a request is parsed correctly.
func TestGetSourceIP(t *testing.T) {
	headers := []headerTest{
		{xForwardedFor, "8.8.8.8", "8.8.8.8"},                                         // Single address
		{xForwardedFor, "8.8.8.8, 8.8.4.4", "8.8.8.8"},                                // Multiple
		{xForwardedFor, "", ""},                                                       // None
		{xRealIP, "8.8.8.8", "8.8.8.8"},                                               // Single address
		{xRealIP, "[2001:db8:cafe::17]:4711", "[2001:db8:cafe::17]:4711"},             // IPv6 address
		{xRealIP, "", ""},                                                             // None
		{forwarded, `for="_gazonk"`, "_gazonk"},                                       // Hostname
		{forwarded, `For="[2001:db8:cafe::17]:4711`, `[2001:db8:cafe::17]:4711`},      // IPv6 address
		{forwarded, `for=192.0.2.60;proto=http;by=203.0.113.43`, `192.0.2.60`},        // Multiple params
		{forwarded, `for=192.0.2.43, for=198.51.100.17`, "192.0.2.43"},                // Multiple params
		{forwarded, `for="workstation.local",for=198.51.100.17`, "workstation.local"}, // Hostname
	}

	for _, v := range headers {
		req := &http.Request{
			Header: http.Header{
				v.key: []string{v.val},
			}}
		res := GetSourceIP(req)
		if res != v.expected {
			t.Errorf("wrong header for %s: got %s want %s", v.key, res,
				v.expected)
		}
	}
}

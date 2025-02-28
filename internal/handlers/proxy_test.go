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
			},
		}
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
		{xRealIP, "[2001:db8:cafe::17]:4711", "[2001:db8:cafe::17]"},                  // IPv6 address
		{xRealIP, "", ""},                                                             // None
		{forwarded, `for="_gazonk"`, "_gazonk"},                                       // Hostname
		{forwarded, `For="[2001:db8:cafe::17]:4711`, `[2001:db8:cafe::17]`},           // IPv6 address
		{forwarded, `for=192.0.2.60;proto=http;by=203.0.113.43`, `192.0.2.60`},        // Multiple params
		{forwarded, `for=192.0.2.43, for=198.51.100.17`, "192.0.2.43"},                // Multiple params
		{forwarded, `for="workstation.local",for=198.51.100.17`, "workstation.local"}, // Hostname
	}

	for _, v := range headers {
		req := &http.Request{
			Header: http.Header{
				v.key: []string{v.val},
			},
		}
		res := GetSourceIP(req)
		if res != v.expected {
			t.Errorf("wrong header for %s: got %s want %s", v.key, res,
				v.expected)
		}
	}
}

func TestXFFDisabled(t *testing.T) {
	req := &http.Request{
		Header: http.Header{
			xForwardedFor: []string{"8.8.8.8"},
			xRealIP:       []string{"1.1.1.1"},
		},
	}
	// When X-Forwarded-For and X-Real-IP headers are both present, X-Forwarded-For takes precedence.
	res := GetSourceIP(req)
	if res != "8.8.8.8" {
		t.Errorf("wrong header, xff takes precedence: got %s, want: 8.8.8.8", res)
	}

	// When explicitly disabled, the XFF header is ignored and X-Real-IP is used.
	enableXFFHeader = false
	defer func() {
		enableXFFHeader = true
	}()
	res = GetSourceIP(req)
	if res != "1.1.1.1" {
		t.Errorf("wrong header, xff is disabled: got %s, want: 1.1.1.1", res)
	}
}

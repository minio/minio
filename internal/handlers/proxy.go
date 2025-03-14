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

// Originally from https://github.com/gorilla/handlers with following license
// https://raw.githubusercontent.com/gorilla/handlers/master/LICENSE, forked
// and heavily modified for MinIO's internal needs.

package handlers

import (
	"net"
	"net/http"
	"regexp"
	"strings"

	"github.com/minio/minio/internal/config"
	"github.com/minio/pkg/v3/env"
)

var (
	// De-facto standard header keys.
	xForwardedFor    = http.CanonicalHeaderKey("X-Forwarded-For")
	xForwardedHost   = http.CanonicalHeaderKey("X-Forwarded-Host")
	xForwardedPort   = http.CanonicalHeaderKey("X-Forwarded-Port")
	xForwardedProto  = http.CanonicalHeaderKey("X-Forwarded-Proto")
	xForwardedScheme = http.CanonicalHeaderKey("X-Forwarded-Scheme")
	xRealIP          = http.CanonicalHeaderKey("X-Real-IP")
)

var (
	// RFC7239 defines a new "Forwarded: " header designed to replace the
	// existing use of X-Forwarded-* headers.
	// e.g. Forwarded: for=192.0.2.60;proto=https;by=203.0.113.43
	forwarded = http.CanonicalHeaderKey("Forwarded")
	// Allows for a sub-match of the first value after 'for=' to the next
	// comma, semi-colon or space. The match is case-insensitive.
	forRegex = regexp.MustCompile(`(?i)(?:for=)([^(;|,| )]+)(.*)`)
	// Allows for a sub-match for the first instance of scheme (http|https)
	// prefixed by 'proto='. The match is case-insensitive.
	protoRegex = regexp.MustCompile(`(?i)^(;|,| )+(?:proto=)(https|http)`)
)

// Used to disable all processing of the X-Forwarded-For header in source IP discovery.
var enableXFFHeader = env.Get("_MINIO_API_XFF_HEADER", config.EnableOn) == config.EnableOn

// GetSourceScheme retrieves the scheme from the X-Forwarded-Proto and RFC7239
// Forwarded headers (in that order).
func GetSourceScheme(r *http.Request) string {
	var scheme string

	// Retrieve the scheme from X-Forwarded-Proto.
	if proto := r.Header.Get(xForwardedProto); proto != "" {
		scheme = strings.ToLower(proto)
	} else if proto = r.Header.Get(xForwardedScheme); proto != "" {
		scheme = strings.ToLower(proto)
	} else if proto := r.Header.Get(forwarded); proto != "" {
		// match should contain at least two elements if the protocol was
		// specified in the Forwarded header. The first element will always be
		// the 'for=', which we ignore, subsequently we proceed to look for
		// 'proto=' which should precede right after `for=` if not
		// we simply ignore the values and return empty. This is in line
		// with the approach we took for returning first ip from multiple
		// params.
		if match := forRegex.FindStringSubmatch(proto); len(match) > 1 {
			if match = protoRegex.FindStringSubmatch(match[2]); len(match) > 1 {
				scheme = strings.ToLower(match[2])
			}
		}
	}

	return scheme
}

// GetSourceIPFromHeaders retrieves the IP from the X-Forwarded-For, X-Real-IP
// and RFC7239 Forwarded headers (in that order)
func GetSourceIPFromHeaders(r *http.Request) string {
	var addr string

	if enableXFFHeader {
		if fwd := r.Header.Get(xForwardedFor); fwd != "" {
			// Only grab the first (client) address. Note that '192.168.0.1,
			// 10.1.1.1' is a valid key for X-Forwarded-For where addresses after
			// the first may represent forwarding proxies earlier in the chain.
			s := strings.Index(fwd, ", ")
			if s == -1 {
				s = len(fwd)
			}
			addr = fwd[:s]
		}
	}

	if addr == "" {
		if fwd := r.Header.Get(xRealIP); fwd != "" {
			// X-Real-IP should only contain one IP address (the client making the
			// request).
			addr = fwd
		} else if fwd := r.Header.Get(forwarded); fwd != "" {
			// match should contain at least two elements if the protocol was
			// specified in the Forwarded header. The first element will always be
			// the 'for=' capture, which we ignore. In the case of multiple IP
			// addresses (for=8.8.8.8, 8.8.4.4, 172.16.1.20 is valid) we only
			// extract the first, which should be the client IP.
			if match := forRegex.FindStringSubmatch(fwd); len(match) > 1 {
				// IPv6 addresses in Forwarded headers are quoted-strings. We strip
				// these quotes.
				addr = strings.Trim(match[1], `"`)
			}
		}
	}

	return addr
}

// GetSourceIPRaw retrieves the IP from the request headers
// and falls back to r.RemoteAddr when necessary.
// however returns without bracketing.
func GetSourceIPRaw(r *http.Request) string {
	addr := GetSourceIPFromHeaders(r)
	if addr == "" {
		addr = r.RemoteAddr
	}

	// Default to remote address if headers not set.
	raddr, _, _ := net.SplitHostPort(addr)
	if raddr == "" {
		return addr
	}
	return raddr
}

// GetSourceIP retrieves the IP from the request headers
// and falls back to r.RemoteAddr when necessary.
func GetSourceIP(r *http.Request) string {
	addr := GetSourceIPRaw(r)
	if strings.ContainsRune(addr, ':') {
		return "[" + addr + "]"
	}
	return addr
}

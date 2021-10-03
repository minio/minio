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
	"context"
	"net"
	"time"

	"github.com/rs/dnscache"
)

// DialContextWithDNSCache is a helper function which returns `net.DialContext` function.
// It randomly fetches an IP from the DNS cache and dials it by the given dial
// function. It dials one by one and returns first connected `net.Conn`.
// If it fails to dial all IPs from cache it returns first error. If no baseDialFunc
// is given, it sets default dial function.
//
// You can use returned dial function for `http.Transport.DialContext`.
//
// In this function, it uses functions from `rand` package. To make it really random,
// you MUST call `rand.Seed` and change the value from the default in your application
func DialContextWithDNSCache(resolver *dnscache.Resolver, baseDialCtx DialContext) DialContext {
	if baseDialCtx == nil {
		// This is same as which `http.DefaultTransport` uses.
		baseDialCtx = (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext
	}
	return func(ctx context.Context, network, addr string) (conn net.Conn, err error) {
		host, port, err := net.SplitHostPort(addr)
		if err != nil {
			return nil, err
		}

		if net.ParseIP(host) != nil {
			// For IP only setups there is no need for DNS lookups.
			return baseDialCtx(ctx, "tcp", addr)
		}

		ips, err := resolver.LookupHost(ctx, host)
		if err != nil {
			return nil, err
		}

		for _, ip := range ips {
			conn, err = baseDialCtx(ctx, "tcp", net.JoinHostPort(ip, port))
			if err == nil {
				break
			}
		}

		return
	}
}

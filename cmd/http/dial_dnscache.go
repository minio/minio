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
	"math/rand"
	"net"
	"sync"
	"time"
)

var randPerm = func(n int) []int {
	return rand.Perm(n)
}

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
func DialContextWithDNSCache(cache *DNSCache, baseDialCtx DialContext) DialContext {
	if baseDialCtx == nil {
		// This is same as which `http.DefaultTransport` uses.
		baseDialCtx = (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext
	}
	return func(ctx context.Context, network, host string) (net.Conn, error) {
		h, p, err := net.SplitHostPort(host)
		if err != nil {
			return nil, err
		}

		// Fetch DNS result from cache.
		//
		// ctxLookup is only used for canceling DNS Lookup.
		ctxLookup, cancelF := context.WithTimeout(ctx, cache.lookupTimeout)
		defer cancelF()
		addrs, err := cache.Fetch(ctxLookup, h)
		if err != nil {
			return nil, err
		}

		var firstErr error
		for _, randomIndex := range randPerm(len(addrs)) {
			conn, err := baseDialCtx(ctx, "tcp", net.JoinHostPort(addrs[randomIndex], p))
			if err == nil {
				return conn, nil
			}
			if firstErr == nil {
				firstErr = err
			}
		}

		return nil, firstErr
	}
}

const (
	// cacheSize is initial size of addr and IP list cache map.
	cacheSize = 64
)

// defaultFreq is default frequency a resolver refreshes DNS cache.
var (
	defaultFreq          = 3 * time.Second
	defaultLookupTimeout = 10 * time.Second
)

// DNSCache is DNS cache resolver which cache DNS resolve results in memory.
type DNSCache struct {
	sync.RWMutex

	lookupHostFn  func(ctx context.Context, host string) ([]string, error)
	lookupTimeout time.Duration
	loggerOnce    func(ctx context.Context, err error, id interface{}, errKind ...interface{})

	cache    map[string][]string
	doneOnce sync.Once
	doneCh   chan struct{}
}

// NewDNSCache initializes DNS cache resolver and starts auto refreshing
// in a new goroutine. To stop auto refreshing, call `Stop()` function.
// Once `Stop()` is called auto refreshing cannot be resumed.
func NewDNSCache(freq time.Duration, lookupTimeout time.Duration, loggerOnce func(ctx context.Context, err error, id interface{}, errKind ...interface{})) *DNSCache {
	if freq <= 0 {
		freq = defaultFreq
	}

	if lookupTimeout <= 0 {
		lookupTimeout = defaultLookupTimeout
	}

	r := &DNSCache{
		lookupHostFn:  net.DefaultResolver.LookupHost,
		lookupTimeout: lookupTimeout,
		loggerOnce:    loggerOnce,
		cache:         make(map[string][]string, cacheSize),
		doneCh:        make(chan struct{}),
	}

	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))

	timer := time.NewTimer(freq)
	go func() {
		defer timer.Stop()

		for {
			select {
			case <-timer.C:
				// Make sure that refreshes on DNS do not be attempted
				// at the same time, allows for reduced load on the
				// DNS servers.
				timer.Reset(time.Duration(rnd.Float64() * float64(freq)))

				r.Refresh()
			case <-r.doneCh:
				return
			}
		}
	}()

	return r
}

// LookupHost lookups address list from DNS server, persist the results
// in-memory cache. `Fetch` is used to obtain the values for a given host.
func (r *DNSCache) LookupHost(ctx context.Context, host string) ([]string, error) {
	addrs, err := r.lookupHostFn(ctx, host)
	if err != nil {
		return nil, err
	}

	r.Lock()
	r.cache[host] = addrs
	r.Unlock()

	return addrs, nil
}

// Fetch fetches IP list from the cache. If IP list of the given addr is not in the cache,
// then it lookups from DNS server by `Lookup` function.
func (r *DNSCache) Fetch(ctx context.Context, host string) ([]string, error) {
	r.RLock()
	addrs, ok := r.cache[host]
	r.RUnlock()
	if ok {
		return addrs, nil
	}
	return r.LookupHost(ctx, host)
}

// Refresh refreshes IP list cache, automatically.
func (r *DNSCache) Refresh() {
	r.RLock()
	hosts := make([]string, 0, len(r.cache))
	for host := range r.cache {
		hosts = append(hosts, host)
	}
	r.RUnlock()

	for _, host := range hosts {
		ctx, cancelF := context.WithTimeout(context.Background(), r.lookupTimeout)
		if _, err := r.LookupHost(ctx, host); err != nil {
			r.loggerOnce(ctx, err, host)
		}
		cancelF()
	}
}

// Stop stops auto refreshing.
func (r *DNSCache) Stop() {
	r.doneOnce.Do(func() {
		close(r.doneCh)
	})
}

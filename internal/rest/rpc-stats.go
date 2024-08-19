// Copyright (c) 2015-2022 MinIO, Inc.
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

package rest

import (
	"net/http"
	"net/http/httptrace"
	"sync/atomic"
	"time"
)

var globalStats = struct {
	errs uint64

	tcpDialErrs                 uint64
	tcpDialCount                uint64
	tcpDialTotalDur             uint64
	tcpTimeForFirstByteTotalDur uint64
}{}

// RPCStats holds information about the DHCP/TCP metrics and errors
type RPCStats struct {
	Errs uint64

	DialAvgDuration uint64
	TTFBAvgDuration uint64
	DialErrs        uint64
}

// GetRPCStats returns RPC stats, include calls errors and dhcp/tcp metrics
func GetRPCStats() RPCStats {
	s := RPCStats{
		Errs:     atomic.LoadUint64(&globalStats.errs),
		DialErrs: atomic.LoadUint64(&globalStats.tcpDialErrs),
	}
	if v := atomic.LoadUint64(&globalStats.tcpDialCount); v > 0 {
		s.DialAvgDuration = atomic.LoadUint64(&globalStats.tcpDialTotalDur) / v
		s.TTFBAvgDuration = atomic.LoadUint64(&globalStats.tcpTimeForFirstByteTotalDur) / v
	}
	return s
}

// Return a function which update the global stats related to tcp connections
func setupReqStatsUpdate(req *http.Request) (*http.Request, func()) {
	var dialStart, dialEnd int64
	start := time.Now()
	trace := &httptrace.ClientTrace{
		GotFirstResponseByte: func() {
			atomic.AddUint64(&globalStats.tcpTimeForFirstByteTotalDur, uint64(time.Since(start)))
		},
		ConnectStart: func(network, addr string) {
			atomic.StoreInt64(&dialStart, time.Now().UnixNano())
		},
		ConnectDone: func(network, addr string, err error) {
			if err == nil {
				atomic.StoreInt64(&dialEnd, time.Now().UnixNano())
			}
		},
	}

	return req.WithContext(httptrace.WithClientTrace(req.Context(), trace)), func() {
		if ds := atomic.LoadInt64(&dialStart); ds > 0 {
			if de := atomic.LoadInt64(&dialEnd); de == 0 {
				atomic.AddUint64(&globalStats.tcpDialErrs, 1)
			} else if de >= ds {
				atomic.AddUint64(&globalStats.tcpDialCount, 1)
				atomic.AddUint64(&globalStats.tcpDialTotalDur, uint64(dialEnd-dialStart))
			}
		}
	}
}

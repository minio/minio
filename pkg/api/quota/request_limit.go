/*
 * Minimalist Object Storage, (C) 2015 Minio, Inc.
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

package quota

import (
	"encoding/binary"
	"net"
	"net/http"
	"time"

	"github.com/minio/minio/pkg/utils/log"
)

// requestLimitHandler
type requestLimitHandler struct {
	handler http.Handler
	quotas  *quotaMap
}

//convert a uint32 to an ipv4
func uint32ToIP(ip uint32) net.IP {
	addr := net.IP{0, 0, 0, 0}
	binary.BigEndian.PutUint32(addr, ip)
	return addr
}

// ServeHTTP is an http.Handler ServeHTTP method
func (h *requestLimitHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	host, _, _ := net.SplitHostPort(req.RemoteAddr)
	longIP := longIP{net.ParseIP(host)}.IptoUint32()
	if h.quotas.IsQuotaMet(longIP) {
		hosts, _ := net.LookupAddr(uint32ToIP(longIP).String())
		log.Debug.Printf("Offending Host: %s, RequestUSED: %d\n", hosts, h.quotas.GetQuotaUsed(longIP))
		writeErrorResponse(w, req, SlowDown, req.URL.Path)
	}
	h.quotas.Add(longIP, 1)
	h.handler.ServeHTTP(w, req)
}

// RequestLimit sets a quote based upon number of requests allowed over a time period
func RequestLimit(h http.Handler, limit int64, duration time.Duration) http.Handler {
	return &requestLimitHandler{
		handler: h,
		quotas: &quotaMap{
			data:        make(map[int64]map[uint32]int64),
			limit:       int64(limit),
			duration:    duration,
			segmentSize: segmentSize(duration),
		},
	}
}

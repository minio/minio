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
	"sync"
	"time"
)

// map[minute][address] = current quota
type quotaMap struct {
	sync.RWMutex
	data     map[int64]map[uint32]uint64
	limit    uint64
	duration int64
}

func (q *quotaMap) Add(ip uint32, size uint64) bool {
	q.Lock()
	defer q.Unlock()
	currentMinute := time.Now().Unix() / q.duration
	expiredQuotas := (time.Now().Unix() / q.duration) - 5
	for time := range q.data {
		if time < expiredQuotas {
			delete(q.data, time)
		}
	}
	if _, ok := q.data[currentMinute]; !ok {
		q.data[currentMinute] = make(map[uint32]uint64)
	}
	currentData, _ := q.data[currentMinute][ip]
	proposedDataSize := currentData + size
	if proposedDataSize > q.limit {
		return false
	}
	q.data[currentMinute][ip] = proposedDataSize
	return true
}

// HttpQuotaHandler
type httpQuotaHandler struct {
	handler http.Handler
	quotas  *quotaMap
	adder   func(uint64) uint64
}

type longIP struct {
	net.IP
}

// []byte to uint32 representation
func (p longIP) IptoUint32() (result uint32) {
	ip := p.To4()
	if ip == nil {
		return 0
	}
	return binary.BigEndian.Uint32(ip)
}

// ServeHTTP is an http.Handler ServeHTTP method
func (h *httpQuotaHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	host, _, _ := net.SplitHostPort(req.RemoteAddr)
	longIP := longIP{net.ParseIP(host)}.IptoUint32()
	if h.quotas.Add(longIP, h.adder(uint64(req.ContentLength))) {
		h.handler.ServeHTTP(w, req)
	}
}

// BandwidthCap sets a quote based upon bandwidth used
func BandwidthCap(h http.Handler, limit int64) http.Handler {
	return &httpQuotaHandler{
		handler: h,
		quotas: &quotaMap{
			data:     make(map[int64]map[uint32]uint64),
			limit:    uint64(limit),
			duration: int64(60),
		},
		adder: func(count uint64) uint64 { return count },
	}
}

// RequestLimit sets a quota based upon request count
func RequestLimit(h http.Handler, limit int64) http.Handler {
	return &httpQuotaHandler{
		handler: h,
		quotas: &quotaMap{
			data:     make(map[int64]map[uint32]uint64),
			limit:    uint64(limit),
			duration: int64(60),
		},
		adder: func(count uint64) uint64 { return 1 },
	}
}

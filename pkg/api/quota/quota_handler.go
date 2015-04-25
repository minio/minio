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
	"log"
	"net/http"
	"strconv"
	"strings"
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
	currentMinute := time.Now().Unix() / q.duration
	expiredQuotas := (time.Now().Unix() / q.duration) - 5
	for time := range q.data {
		if time < expiredQuotas {
			delete(q.data, time)
		}
	}
	log.Println(currentMinute)
	if _, ok := q.data[currentMinute]; !ok {
		q.data[currentMinute] = make(map[uint32]uint64)
	}
	currentData, _ := q.data[currentMinute][ip]
	q.data[currentMinute][ip] = currentData + size
	return false
}

// HttpQuotaHandler
type httpQuotaHandler struct {
	handler http.Handler
	quotas  *quotaMap
}

// ServeHTTP is an http.Handler ServeHTTP method
func (h *httpQuotaHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	ipString := strings.Split(req.RemoteAddr, ":")[0]
	ipSplit := strings.Split(ipString, ".")
	q0, _ := strconv.Atoi(ipSplit[0])
	q1, _ := strconv.Atoi(ipSplit[1])
	q2, _ := strconv.Atoi(ipSplit[2])
	q3, _ := strconv.Atoi(ipSplit[3])
	longIP := uint32(q0)<<24 + uint32(q1)<<16 + uint32(q2)<<8 + uint32(q3)
	h.quotas.Add(longIP, uint64(req.ContentLength))
	log.Println("quota called")
	log.Println(h.quotas)
	h.handler.ServeHTTP(w, req)
}

// Handler implements quotas
func Handler(h http.Handler, limit int64) http.Handler {
	return &httpQuotaHandler{
		handler: h,
		quotas: &quotaMap{
			data:     make(map[int64]map[uint32]uint64),
			limit:    uint64(limit),
			duration: int64(60),
		},
	}
}

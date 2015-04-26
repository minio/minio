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
	"sync"
	"time"
)

// map[minute][address] = current quota
type quotaMap struct {
	sync.RWMutex
	data        map[int64]map[uint32]int64
	limit       int64
	duration    time.Duration
	segmentSize time.Duration
}

func (q *quotaMap) Add(ip uint32, size int64) {
	q.Lock()
	defer q.Unlock()
	q.clean()
	currentMinute := time.Now().UnixNano() / q.segmentSize.Nanoseconds()
	if _, ok := q.data[currentMinute]; !ok {
		q.data[currentMinute] = make(map[uint32]int64)
	}
	currentData, _ := q.data[currentMinute][ip]
	proposedDataSize := currentData + size
	q.data[currentMinute][ip] = proposedDataSize
}

func (q *quotaMap) IsQuotaMet(ip uint32) bool {
	q.clean()
	if q.GetQuotaUsed(ip) >= q.limit {
		return true
	}
	return false
}

func (q *quotaMap) GetQuotaUsed(ip uint32) (total int64) {
	currentMinute := time.Now().UnixNano() / q.segmentSize.Nanoseconds()
	if _, ok := q.data[currentMinute]; !ok {
		q.data[currentMinute] = make(map[uint32]int64)
	}
	for _, segment := range q.data {
		if used, ok := segment[ip]; ok {
			total += used
		}
	}
	return
}

func (q *quotaMap) WillExceedQuota(ip uint32, size int64) (result bool) {
	return q.GetQuotaUsed(ip)+size > q.limit
}

func (q *quotaMap) clean() {
	currentMinute := time.Now().UnixNano() / q.segmentSize.Nanoseconds()
	expiredQuotas := currentMinute - q.duration.Nanoseconds()
	for time := range q.data {
		if time < expiredQuotas {
			delete(q.data, time)
		}
	}
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

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
	"net"
	"net/http"
	"sync"

	"github.com/minio-io/minio/pkg/utils/log"
)

// requestLimitHandler
type connLimit struct {
	sync.RWMutex
	handler     http.Handler
	connections map[uint32]int
	limit       int
}

func (c *connLimit) GetUsed(ip uint32) int {
	return c.connections[ip]
}

func (c *connLimit) TestAndAdd(ip uint32) bool {
	c.Lock()
	defer c.Unlock()
	count, _ := c.connections[ip]
	if count >= c.limit {
		return false
	}
	count = count + 1
	c.connections[ip] = count
	return true
}

func (c *connLimit) Remove(ip uint32) {
	c.Lock()
	defer c.Unlock()
	count, _ := c.connections[ip]
	count = count - 1
	if count <= 0 {
		delete(c.connections, ip)
		return
	}
	c.connections[ip] = count
}

// ServeHTTP is an http.Handler ServeHTTP method
func (c *connLimit) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	host, _, _ := net.SplitHostPort(req.RemoteAddr)
	longIP := longIP{net.ParseIP(host)}.IptoUint32()
	if !c.TestAndAdd(longIP) {
		hosts, _ := net.LookupAddr(uint32ToIP(longIP).String())
		log.Debug.Printf("Offending Host: %s, ConnectionsUSED: %d\n", hosts, c.GetUsed(longIP))
		writeErrorResponse(w, req, ConnectionLimitExceeded, req.URL.Path)
	}
	defer c.Remove(longIP)
	c.handler.ServeHTTP(w, req)
}

// ConnectionLimit limits the number of concurrent connections
func ConnectionLimit(h http.Handler, limit int) http.Handler {
	return &connLimit{
		handler:     h,
		connections: make(map[uint32]int),
		limit:       limit,
	}
}

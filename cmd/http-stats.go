/*
 * Minio Cloud Storage, (C) 2017 Minio, Inc.
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

package cmd

import (
	"fmt"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"go.uber.org/atomic"
)

// ConnStats - Network statistics
// Count total input/output transferred bytes during
// the server's life.
type ConnStats struct {
	totalInputBytes  atomic.Uint64
	totalOutputBytes atomic.Uint64
}

// Increase total input bytes
func (s *ConnStats) incInputBytes(n int) {
	s.totalInputBytes.Add(uint64(n))
}

// Increase total output bytes
func (s *ConnStats) incOutputBytes(n int) {
	s.totalOutputBytes.Add(uint64(n))
}

// Return total input bytes
func (s *ConnStats) getTotalInputBytes() uint64 {
	return s.totalInputBytes.Load()
}

// Return total output bytes
func (s *ConnStats) getTotalOutputBytes() uint64 {
	return s.totalOutputBytes.Load()
}

// Return connection stats (total input/output bytes)
func (s *ConnStats) toServerConnStats() ServerConnStats {
	return ServerConnStats{
		TotalInputBytes:  s.getTotalInputBytes(),
		TotalOutputBytes: s.getTotalOutputBytes(),
	}
}

// Prepare new ConnStats structure
func newConnStats() *ConnStats {
	return &ConnStats{}
}

// HTTPMethodStats holds statistics information about
// a given HTTP method made by all clients
type HTTPMethodStats struct {
	Counter  atomic.Uint64
	Duration atomic.Float64
}

// HTTPStats holds statistics information about
// HTTP requests made by all clients
type HTTPStats struct {
	// HEAD request stats.
	totalHEADs   HTTPMethodStats
	successHEADs HTTPMethodStats

	// GET request stats.
	totalGETs   HTTPMethodStats
	successGETs HTTPMethodStats

	// PUT request stats.
	totalPUTs   HTTPMethodStats
	successPUTs HTTPMethodStats

	// POST request stats.
	totalPOSTs   HTTPMethodStats
	successPOSTs HTTPMethodStats

	// DELETE request stats.
	totalDELETEs   HTTPMethodStats
	successDELETEs HTTPMethodStats
}

func durationStr(totalDuration, totalCount float64) string {
	return fmt.Sprint(time.Duration(totalDuration/totalCount) * time.Second)
}

// Converts http stats into struct to be sent back to the client.
func (st HTTPStats) toServerHTTPStats() ServerHTTPStats {
	serverStats := ServerHTTPStats{}
	serverStats.TotalHEADStats = ServerHTTPMethodStats{
		Count:       st.totalHEADs.Counter.Load(),
		AvgDuration: durationStr(st.totalHEADs.Duration.Load(), float64(st.totalHEADs.Counter.Load())),
	}
	serverStats.SuccessHEADStats = ServerHTTPMethodStats{
		Count:       st.successHEADs.Counter.Load(),
		AvgDuration: durationStr(st.successHEADs.Duration.Load(), float64(st.successHEADs.Counter.Load())),
	}
	serverStats.TotalGETStats = ServerHTTPMethodStats{
		Count:       st.totalGETs.Counter.Load(),
		AvgDuration: durationStr(st.totalGETs.Duration.Load(), float64(st.totalGETs.Counter.Load())),
	}
	serverStats.SuccessGETStats = ServerHTTPMethodStats{
		Count:       st.successGETs.Counter.Load(),
		AvgDuration: durationStr(st.successGETs.Duration.Load(), float64(st.successGETs.Counter.Load())),
	}
	serverStats.TotalPUTStats = ServerHTTPMethodStats{
		Count:       st.totalPUTs.Counter.Load(),
		AvgDuration: durationStr(st.totalPUTs.Duration.Load(), float64(st.totalPUTs.Counter.Load())),
	}
	serverStats.SuccessPUTStats = ServerHTTPMethodStats{
		Count:       st.successPUTs.Counter.Load(),
		AvgDuration: durationStr(st.successPUTs.Duration.Load(), float64(st.successPUTs.Counter.Load())),
	}
	serverStats.TotalPOSTStats = ServerHTTPMethodStats{
		Count:       st.totalPOSTs.Counter.Load(),
		AvgDuration: durationStr(st.totalPOSTs.Duration.Load(), float64(st.totalPOSTs.Counter.Load())),
	}
	serverStats.SuccessPOSTStats = ServerHTTPMethodStats{
		Count:       st.successPOSTs.Counter.Load(),
		AvgDuration: durationStr(st.successPOSTs.Duration.Load(), float64(st.successPOSTs.Counter.Load())),
	}
	serverStats.TotalDELETEStats = ServerHTTPMethodStats{
		Count:       st.totalDELETEs.Counter.Load(),
		AvgDuration: durationStr(st.totalDELETEs.Duration.Load(), float64(st.totalDELETEs.Counter.Load())),
	}
	serverStats.SuccessDELETEStats = ServerHTTPMethodStats{
		Count:       st.successDELETEs.Counter.Load(),
		AvgDuration: durationStr(st.successDELETEs.Duration.Load(), float64(st.successDELETEs.Counter.Load())),
	}
	return serverStats
}

// Update statistics from http request and response data
func (st *HTTPStats) updateStats(r *http.Request, w *httpResponseRecorder, durationSecs float64) {
	// A successful request has a 2xx response code
	successReq := (w.respStatusCode >= 200 && w.respStatusCode < 300)
	// Update stats according to method verb
	switch r.Method {
	case "HEAD":
		st.totalHEADs.Counter.Inc()
		st.totalHEADs.Duration.Add(durationSecs)
		if successReq {
			st.successHEADs.Counter.Inc()
			st.successHEADs.Duration.Add(durationSecs)
		}
	case "GET":
		st.totalGETs.Counter.Inc()
		st.totalGETs.Duration.Add(durationSecs)
		if successReq {
			st.successGETs.Counter.Inc()
			st.successGETs.Duration.Add(durationSecs)
		}
	case "PUT":
		st.totalPUTs.Counter.Inc()
		st.totalPUTs.Duration.Add(durationSecs)
		if successReq {
			st.successPUTs.Counter.Inc()
			st.totalPUTs.Duration.Add(durationSecs)
		}
	case "POST":
		st.totalPOSTs.Counter.Inc()
		st.totalPOSTs.Duration.Add(durationSecs)
		if successReq {
			st.successPOSTs.Counter.Inc()
			st.totalPOSTs.Duration.Add(durationSecs)
		}
	case "DELETE":
		st.totalDELETEs.Counter.Inc()
		st.totalDELETEs.Duration.Add(durationSecs)
		if successReq {
			st.successDELETEs.Counter.Inc()
			st.successDELETEs.Duration.Add(durationSecs)
		}
	}
	// Increment the prometheus http request response histogram with appropriate label
	httpRequestsDuration.With(prometheus.Labels{"request_type": r.Method}).Observe(durationSecs)
}

// Prepare new HTTPStats structure
func newHTTPStats() *HTTPStats {
	return &HTTPStats{}
}

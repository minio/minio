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

package http

import (
	"net"
	"sync/atomic"
	"time"
)

// QuirkConn - similar to golang net.Conn struct, but contains a workaround of the
// following the go bug reported here https://github.com/golang/go/issues/21133.
// Once the bug will be fixed, we can remove this structure and replaces it with
// the standard net.Conn
type QuirkConn struct {
	net.Conn
	hadReadDeadlineInPast int32 // atomic
}

// SetReadDeadline - implements a workaround of SetReadDeadline go bug
func (q *QuirkConn) SetReadDeadline(t time.Time) error {
	inPast := int32(0)
	if t.Before(time.Now()) {
		inPast = 1
	}
	atomic.StoreInt32(&q.hadReadDeadlineInPast, inPast)
	return q.Conn.SetReadDeadline(t)
}

// canSetReadDeadline - returns if it is safe to set a new
// read deadline without triggering golang/go#21133 issue.
func (q *QuirkConn) canSetReadDeadline() bool {
	return atomic.LoadInt32(&q.hadReadDeadlineInPast) != 1
}

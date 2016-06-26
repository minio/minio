/*
 * Minio Cloud Storage, (C) 2016 Minio, Inc.
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

package main

import (
	"net/http"
	"sync"
)

// rateLimit - represents datatype of the functionality implemented to
// limit the number of concurrent http requests.
type rateLimit struct {
	handler     http.Handler
	rqueue      chan struct{}
	releaseOnce sync.Once
}

// acquire and release implement a way to send and receive from the
// channel this is in-turn used to rate limit incoming connections in
// ServeHTTP() http.Handler method.
func (c *rateLimit) acquire() { c.rqueue <- struct{}{} }
func (c *rateLimit) release() { <-c.rqueue }

// ServeHTTP is an http.Handler ServeHTTP method, implemented to rate
// limit incoming HTTP requests.
func (c *rateLimit) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Acquire the connection if queue is not full, otherwise
	// code path waits here until the previous case is true.
	c.acquire()

	// Serves the request.
	c.handler.ServeHTTP(w, r)

	// Release by draining the channel once.
	c.releaseOnce.Do(c.release)
}

// setRateLimitHandler limits the number of concurrent http requests based on MINIO_MAXCONN.
func setRateLimitHandler(handler http.Handler) http.Handler {
	if globalMaxConn == 0 {
		return handler
	} // else proceed to rate limiting.

	// For max connection limit of > '0' we initialize rate limit handler.
	return &rateLimit{
		handler: handler,
		rqueue:  make(chan struct{}, globalMaxConn),
	}
}

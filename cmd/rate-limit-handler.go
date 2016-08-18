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

package cmd

import (
	"errors"
	"net/http"
	"sync"
)

var errTooManyRequests = errors.New("Too many clients in the waiting list")

// rateLimit - represents datatype of the functionality implemented to
// limit the number of concurrent http requests.
type rateLimit struct {
	handler   http.Handler
	lock      sync.Mutex
	workQueue chan struct{}
	waitQueue chan struct{}
}

// acquire and release implement a way to send and receive from the
// channel this is in-turn used to rate limit incoming connections in
// ServeHTTP() http.Handler method.
func (c *rateLimit) acquire() error {
	// Kick out clients when it is really crowded
	if len(c.waitQueue) == cap(c.waitQueue) {
		return errTooManyRequests
	}

	// Add new element in waitQueue to keep track of clients
	// wanting to process their requests
	c.waitQueue <- struct{}{}

	// Moving from wait to work queue is protected by a mutex
	// to avoid draining waitQueue with multiple simultaneous clients.
	c.lock.Lock()
	c.workQueue <- <-c.waitQueue
	c.lock.Unlock()
	return nil
}

// Release one element from workQueue to serve a new client
// in the waiting list
func (c *rateLimit) release() {
	<-c.workQueue
}

// ServeHTTP is an http.Handler ServeHTTP method, implemented to rate
// limit incoming HTTP requests.
func (c *rateLimit) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Acquire the connection if queue is not full, otherwise
	// code path waits here until the previous case is true.
	if err := c.acquire(); err != nil {
		w.WriteHeader(http.StatusTooManyRequests)
		return
	}

	// Serves the request.
	c.handler.ServeHTTP(w, r)

	// Release
	c.release()
}

// setRateLimitHandler limits the number of concurrent http requests based on MINIO_MAXCONN.
func setRateLimitHandler(handler http.Handler) http.Handler {
	if globalMaxConn == 0 {
		return handler
	} // else proceed to rate limiting.

	// For max connection limit of > '0' we initialize rate limit handler.
	return &rateLimit{
		handler:   handler,
		workQueue: make(chan struct{}, globalMaxConn),
		waitQueue: make(chan struct{}, globalMaxConn*4),
	}
}

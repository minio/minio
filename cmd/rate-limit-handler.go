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
)

var errTooManyRequests = errors.New("Too many clients in the waiting list")

// rateLimit - represents datatype of the functionality implemented to
// limit the number of concurrent http requests.
type rateLimit struct {
	handler   http.Handler
	workQueue chan struct{}
	waitQueue chan struct{}
}

// acquire and release implement a way to send and receive from the
// channel this is in-turn used to rate limit incoming connections in
// ServeHTTP() http.Handler method.
func (c *rateLimit) acquire() error {
	//attempt to enter the waitQueue. If no slot is immediately
	//available return error.
	select {
	case c.waitQueue <- struct{}{}:
		//entered wait queue
		break
	default:
		//no slot available for waiting
		return errTooManyRequests
	}

	//block attempting to enter the workQueue. If the workQueue is
	//full, there can be at most cap(waitQueue) == 4*globalMaxConn
	//goroutines waiting here because of the select above.
	select {
	case c.workQueue <- struct{}{}:
		//entered workQueue - so remove one waiter. This step
		//does not block as the waitQueue cannot be empty.
		<-c.waitQueue
	}

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

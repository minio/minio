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

package api

import "net/http"

// rateLimit
type rateLimit struct {
	handler   http.Handler
	rateQueue chan bool
}

func (c rateLimit) Add() {
	c.rateQueue <- true // fill in the queue
	return
}

func (c rateLimit) Remove() {
	<-c.rateQueue // invalidate the queue, after the request is served
	return
}

// ServeHTTP is an http.Handler ServeHTTP method
func (c rateLimit) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	c.Add()                     // add
	c.handler.ServeHTTP(w, req) // serve
	c.Remove()                  // remove
}

// rateLimitHandler limits the number of concurrent http requests
func rateLimitHandler(handle http.Handler, limit int) http.Handler {
	return rateLimit{
		handler:   handle,
		rateQueue: make(chan bool, limit),
	}
}

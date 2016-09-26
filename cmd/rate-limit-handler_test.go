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
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// This test sets globalMaxConn to 1 and starts 6 connections in
// parallel on a server with the rate limit handler configured. This
// should allow one request to execute at a time, and at most 4 to
// wait to execute and the 6th request should get a 429 status code
// error.
func TestRateLimitHandler(t *testing.T) {
	// save the global Max connections
	saveGlobalMaxConn := globalMaxConn

	globalMaxConn = 1
	testHandler := func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(100 * time.Millisecond)
		fmt.Fprintln(w, "Hello client!")
	}
	rlh := setRateLimitHandler(http.HandlerFunc(testHandler))
	ts := httptest.NewServer(rlh)
	respCh := make(chan int)
	startTime := time.Now()
	for i := 0; i < 6; i++ {
		go func(ch chan<- int) {
			resp, err := http.Get(ts.URL)
			if err != nil {
				t.Errorf(
					"Got error requesting test server - %v\n",
					err,
				)
			}
			respCh <- resp.StatusCode
		}(respCh)
	}

	tooManyReqErrCount := 0
	for i := 0; i < 6; i++ {
		code := <-respCh
		if code == 429 {
			tooManyReqErrCount++
		} else if code != 200 {
			t.Errorf("Got non-200 resp code - %d\n", code)
		}
	}
	duration := time.Since(startTime)
	if duration < time.Duration(500*time.Millisecond) {
		// as globalMaxConn is 1, only 1 request will execute
		// at a time, and the five allowed requested will take
		// at least 500 ms.
		t.Errorf("Expected all requests to take at least 500ms, but it was done in %v\n",
			duration)
	}
	if tooManyReqErrCount != 1 {
		t.Errorf("Expected to get 1 error, but got %d",
			tooManyReqErrCount)
	}
	ts.Close()

	// restore the global Max connections
	globalMaxConn = saveGlobalMaxConn
}

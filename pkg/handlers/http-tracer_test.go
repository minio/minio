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

package handlers

import (
	"bytes"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"regexp"
	"testing"
)

func wsTestSuccessHandler(w http.ResponseWriter, r *http.Request) {
	// A very simple health check.
	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "application/json")

	ioutil.ReadAll(r.Body)

	// In the future we could report back on the status of our DB, or our cache
	// (e.g. Redis) by performing a simple PING, and include them in the response.
	io.WriteString(w, `{"success": true}`)
}

func wsTest404Handler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusNotFound)
}

func TestTraceHTTPHandler(t *testing.T) {

	logOutput := bytes.NewBuffer([]byte(""))

	testCases := []struct {
		method            string
		path              string
		sentData          string
		headers           map[string]string
		handler           http.HandlerFunc
		expectedStatus    int
		expectedLogRegexp string
	}{

		{
			method:         "PUT",
			path:           "/test-log",
			sentData:       "sending data",
			headers:        map[string]string{"Test-Header": "TestHeaderValue"},
			handler:        TraceReqHandlerFunc(http.HandlerFunc(wsTestSuccessHandler), logOutput, true),
			expectedStatus: http.StatusOK,
			expectedLogRegexp: `\[REQUEST github.com/minio/minio/pkg/handlers.wsTestSuccessHandler\] \[[^\]]*\] \[[^\]]*\]
PUT /test-log
Host:\ 
Test-Header: TestHeaderValue

sending data

\[RESPONSE\] \[[^\]]*\] \[[^\]]*\]
200 OK

{"success": true}

`,
		},
		{
			method:         "POST",
			path:           "/test-log",
			handler:        TraceReqHandlerFunc(http.HandlerFunc(wsTestSuccessHandler), logOutput, false),
			headers:        map[string]string{"Test-Header": "TestHeaderValue"},
			expectedStatus: http.StatusOK,
			expectedLogRegexp: `\[REQUEST github.com/minio/minio/pkg/handlers.wsTestSuccessHandler\] \[[^\]]*\] \[[^\]]*\]
POST /test-log
Host:\ 
Test-Header: TestHeaderValue

<BODY>

\[RESPONSE\] \[[^\]]*\] \[[^\]]*\]
200 OK

<BODY>

`,
		},
		{
			method:         "POST",
			path:           "/test-log",
			handler:        TraceReqHandlerFunc(http.HandlerFunc(wsTest404Handler), logOutput, false),
			headers:        map[string]string{"Test-Header": "TestHeaderValue"},
			expectedStatus: http.StatusNotFound,
			expectedLogRegexp: `\[REQUEST github.com/minio/minio/pkg/handlers.wsTest404Handler\] \[[^\]]*\] \[[^\]]*\]
POST /test-log
Host:\ 
Test-Header: TestHeaderValue

<BODY>

\[RESPONSE\] \[[^\]]*\] \[[^\]]*\]
404 Not Found

<BODY>

`,
		},
	}

	for i, testCase := range testCases {
		logOutput.Reset()

		req, err := http.NewRequest(testCase.method, testCase.path, bytes.NewBuffer([]byte(testCase.sentData)))
		if err != nil {
			t.Fatalf("Test %d: %v\n", i+1, err)
		}

		for k, v := range testCase.headers {
			req.Header.Set(k, v)
		}

		rr := httptest.NewRecorder()

		handler := testCase.handler
		handler.ServeHTTP(rr, req)

		// Check the status code is what we expect.
		if status := rr.Code; status != testCase.expectedStatus {
			t.Errorf("Test %d: handler returned wrong status code: got %v want %v", i+1,
				status, testCase.expectedStatus)
		}

		matched, err := regexp.MatchString(testCase.expectedLogRegexp, string(logOutput.Bytes()))
		if err != nil {
			t.Fatalf("Test %d: Incorrect regexp: %v", i+1, err)
		}

		if !matched {
			t.Fatalf("Test %d: Unexpected log content, found: `%s`", i+1, string(logOutput.Bytes()))
		}
	}
}

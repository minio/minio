/*
 * Minio Cloud Storage, (C) 2015, 2016, 2017 Minio, Inc.
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
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"reflect"
	"runtime"
	"strings"
	"time"
)

func httpTracelogAll(f http.HandlerFunc) http.HandlerFunc {
	if !globalHTTPTrace {
		return f
	}
	return httpTracelog(f, true)
}

func httpTracelogHeaders(f http.HandlerFunc) http.HandlerFunc {
	if !globalHTTPTrace {
		return f
	}
	return httpTracelog(f, false)
}

func httpTracelog(f http.HandlerFunc, logBody bool) http.HandlerFunc {
	name := runtime.FuncForPC(reflect.ValueOf(f).Pointer()).Name()
	name = strings.TrimPrefix(name, "github.com/minio/minio/cmd.")
	bodyPlaceHolder := []byte("<BODY>")

	return func(w http.ResponseWriter, r *http.Request) {
		const timeFormat = "2006-01-02 15:04:05 -0700"
		var reqBodyRecorder *recordReader

		// Generate short random request ID
		reqID := fmt.Sprintf("%f", float64(time.Now().UnixNano())/1e10)

		reqBodyRecorder = &recordReader{Reader: r.Body, logBody: logBody}
		r.Body = ioutil.NopCloser(reqBodyRecorder)

		// Setup a http response body recorder
		respBodyRecorder := &recordResponseWriter{ResponseWriter: w, logBody: logBody}

		b := bytes.NewBuffer(nil)
		fmt.Fprintf(b, "[REQUEST %s] [%s] [%s]\n", name, reqID, time.Now().Format(timeFormat))

		f(respBodyRecorder, r)

		// Build request log and write it to log file
		fmt.Fprintf(b, "%s %s", r.Method, r.URL.Path)
		if r.URL.RawQuery != "" {
			fmt.Fprintf(b, "?%s", r.URL.RawQuery)
		}
		fmt.Fprintf(b, "\n")

		fmt.Fprintf(b, "Host: %s\n", r.Host)
		for k, v := range r.Header {
			fmt.Fprintf(b, "%s: %s\n", k, v[0])
		}
		fmt.Fprintf(b, "\n")
		if logBody {
			bodyContents := reqBodyRecorder.Data()
			if bodyContents != nil {
				b.Write(bodyContents)
				fmt.Fprintf(b, "\n")
			}
		} else {
			b.Write(bodyPlaceHolder)
			fmt.Fprintf(b, "\n")
		}

		fmt.Fprintf(b, "\n")

		// Build response log and write it to log file
		fmt.Fprintf(b, "[RESPONSE] [%s] [%s]\n", reqID, time.Now().Format(timeFormat))

		b.Write(respBodyRecorder.Headers())
		fmt.Fprintf(b, "\n")
		bodyContents := respBodyRecorder.Body()
		if bodyContents != nil {
			b.Write(bodyContents)
			fmt.Fprintf(b, "\n")
		} else {
			// If there was no error response and body logging is disabled
			// then we print <BODY>
			if !logBody {
				b.Write(bodyPlaceHolder)
				fmt.Fprintf(b, "\n")
			}
		}

		fmt.Fprintf(b, "\n")

		os.Stdout.Write(b.Bytes())
	}
}

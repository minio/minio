/*
 * MinIO Cloud Storage, (C) 2017 MinIO, Inc.
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
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"time"

	trace "github.com/minio/minio/pkg/trace"
)

var traceBodyPlaceHolder = []byte("<BODY>")

// recordRequest - records the first recLen bytes
// of a given io.Reader
type recordRequest struct {
	// Data source to record
	io.Reader
	// Response body should be logged
	logBody bool
	// Internal recording buffer
	buf bytes.Buffer
	// request headers
	headers http.Header
	// total bytes read including header size
	bytesRead int
}

func (r *recordRequest) Read(p []byte) (n int, err error) {
	n, err = r.Reader.Read(p)
	r.bytesRead += n

	if r.logBody {
		r.buf.Write(p[:n])
	}
	if err != nil {
		return n, err
	}
	return n, err
}
func (r *recordRequest) Size() int {
	sz := r.bytesRead
	for k, v := range r.headers {
		sz += len(k) + len(v)
	}
	return sz
}

// Return the bytes that were recorded.
func (r *recordRequest) Data() []byte {
	// If body logging is enabled then we return the actual body
	if r.logBody {
		return r.buf.Bytes()
	}
	// ... otherwise we return <BODY> placeholder
	return traceBodyPlaceHolder
}

// recordResponseWriter - records the first recLen bytes
// of a given http.ResponseWriter
type recordResponseWriter struct {
	// Data source to record
	http.ResponseWriter
	// Response body should be logged
	logBody bool
	// Internal recording buffer
	headers bytes.Buffer
	body    bytes.Buffer
	// The status code of the current HTTP request
	statusCode int
	// Indicate if headers are written in the log
	headersLogged bool
	// number of bytes written
	bytesWritten int
}

// Write the headers into the given buffer
func (r *recordResponseWriter) writeHeaders(w io.Writer, statusCode int, headers http.Header) {
	n, _ := fmt.Fprintf(w, "%d %s\n", statusCode, http.StatusText(statusCode))
	r.bytesWritten += n
	for k, v := range headers {
		n, _ := fmt.Fprintf(w, "%s: %s\n", k, v[0])
		r.bytesWritten += n
	}
}

// Record the headers.
func (r *recordResponseWriter) WriteHeader(i int) {
	r.statusCode = i
	if !r.headersLogged {
		r.writeHeaders(&r.headers, i, r.ResponseWriter.Header())
		r.headersLogged = true
	}
	r.ResponseWriter.WriteHeader(i)
}

func (r *recordResponseWriter) Write(p []byte) (n int, err error) {
	n, err = r.ResponseWriter.Write(p)
	r.bytesWritten += n
	if !r.headersLogged {
		// We assume the response code to be '200 OK' when WriteHeader() is not called,
		// that way following Golang HTTP response behavior.
		r.writeHeaders(&r.headers, http.StatusOK, r.ResponseWriter.Header())
		r.headersLogged = true
	}
	if (r.statusCode != http.StatusOK && r.statusCode != http.StatusPartialContent && r.statusCode != 0) || r.logBody {
		// Always logging error responses.
		r.body.Write(p)
	}
	return n, err
}

func (r *recordResponseWriter) Size() int {
	return r.bytesWritten
}

// Calls the underlying Flush.
func (r *recordResponseWriter) Flush() {
	r.ResponseWriter.(http.Flusher).Flush()
}

// Return response body.
func (r *recordResponseWriter) Body() []byte {
	// If there was an error response or body logging is enabled
	// then we return the body contents
	if r.statusCode >= 400 || r.logBody {
		return r.body.Bytes()
	}
	// ... otherwise we return the <BODY> place holder
	return traceBodyPlaceHolder
}

// getOpName sanitizes the operation name for mc
func getOpName(name string) (op string) {
	op = strings.TrimPrefix(name, "github.com/minio/minio/cmd.")
	op = strings.TrimSuffix(op, "Handler-fm")
	op = strings.Replace(op, "objectAPIHandlers", "s3", 1)
	op = strings.Replace(op, "webAPIHandlers", "s3", 1)
	op = strings.Replace(op, "adminAPIHandlers", "admin", 1)
	op = strings.Replace(op, "(*storageRESTServer)", "internal", 1)
	op = strings.Replace(op, "(*peerRESTServer)", "internal", 1)
	op = strings.Replace(op, "(*lockRESTServer)", "internal", 1)
	op = strings.Replace(op, "stsAPIHandlers", "sts", 1)
	op = strings.Replace(op, "LivenessCheckHandler", "healthcheck", 1)
	op = strings.Replace(op, "ReadinessCheckHandler", "healthcheck", 1)
	return op
}

// Trace gets trace of http request
func Trace(f http.HandlerFunc, logBody bool, w http.ResponseWriter, r *http.Request) trace.Info {
	name := getOpName(runtime.FuncForPC(reflect.ValueOf(f).Pointer()).Name())

	// Setup a http request body recorder
	reqHeaders := cloneHeader(r.Header)
	reqHeaders.Set("Content-Length", strconv.Itoa(int(r.ContentLength)))
	reqHeaders.Set("Host", r.Host)
	for _, enc := range r.TransferEncoding {
		reqHeaders.Add("Transfer-Encoding", enc)
	}

	var reqBodyRecorder *recordRequest
	t := trace.Info{FuncName: name}
	reqBodyRecorder = &recordRequest{Reader: r.Body, logBody: logBody, headers: reqHeaders}
	r.Body = ioutil.NopCloser(reqBodyRecorder)
	t.NodeName = r.Host
	if globalIsDistXL {
		t.NodeName = GetLocalPeer(globalEndpoints)
	}
	// strip port from the host address
	if host, _, err := net.SplitHostPort(t.NodeName); err == nil {
		t.NodeName = host
	}

	rq := trace.RequestInfo{
		Time:     time.Now().UTC(),
		Method:   r.Method,
		Path:     r.URL.Path,
		RawQuery: r.URL.RawQuery,
		Client:   r.RemoteAddr,
		Headers:  reqHeaders,
		Body:     reqBodyRecorder.Data(),
	}

	// Setup a http response body recorder
	respBodyRecorder := &recordResponseWriter{ResponseWriter: w, logBody: logBody}
	f(respBodyRecorder, r)

	rs := trace.ResponseInfo{
		Time:       time.Now().UTC(),
		Headers:    cloneHeader(respBodyRecorder.Header()),
		StatusCode: respBodyRecorder.statusCode,
		Body:       respBodyRecorder.Body(),
	}

	if rs.StatusCode == 0 {
		rs.StatusCode = http.StatusOK
	}

	t.ReqInfo = rq
	t.RespInfo = rs

	t.CallStats = trace.CallStats{Latency: rs.Time.Sub(rq.Time), InputBytes: reqBodyRecorder.Size(), OutputBytes: respBodyRecorder.Size()}
	return t
}

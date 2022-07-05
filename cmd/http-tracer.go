// Copyright (c) 2015-2021 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package cmd

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"reflect"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/minio/madmin-go"
	"github.com/minio/minio/internal/handlers"
	"github.com/minio/minio/internal/logger"
)

// recordRequest - records the first recLen bytes
// of a given io.Reader
type recordRequest struct {
	// Data source to record
	io.Reader
	// Response body should be logged
	logBody bool
	// Internal recording buffer
	buf bytes.Buffer
	// total bytes read including header size
	bytesRead int
}

func (r *recordRequest) Close() error {
	// no-op
	return nil
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

func (r *recordRequest) BodySize() int {
	return r.bytesRead
}

// Return the bytes that were recorded.
func (r *recordRequest) Data() []byte {
	// If body logging is enabled then we return the actual body
	if r.logBody {
		return r.buf.Bytes()
	}
	// ... otherwise we return <BODY> placeholder
	return logger.BodyPlaceHolder
}

var ldapPwdRegex = regexp.MustCompile("(^.*?)LDAPPassword=([^&]*?)(&(.*?))?$")

// redact LDAP password if part of string
func redactLDAPPwd(s string) string {
	parts := ldapPwdRegex.FindStringSubmatch(s)
	if len(parts) > 0 {
		return parts[1] + "LDAPPassword=*REDACTED*" + parts[3]
	}
	return s
}

// getOpName sanitizes the operation name for mc
func getOpName(name string) (op string) {
	op = strings.TrimPrefix(name, "github.com/minio/minio/cmd.")
	op = strings.TrimSuffix(op, "Handler-fm")
	op = strings.Replace(op, "objectAPIHandlers", "s3", 1)
	op = strings.Replace(op, "adminAPIHandlers", "admin", 1)
	op = strings.Replace(op, "(*storageRESTServer)", "storageR", 1)
	op = strings.Replace(op, "(*peerRESTServer)", "peer", 1)
	op = strings.Replace(op, "(*lockRESTServer)", "lockR", 1)
	op = strings.Replace(op, "(*stsAPIHandlers)", "sts", 1)
	op = strings.Replace(op, "ClusterCheckHandler", "health.Cluster", 1)
	op = strings.Replace(op, "ClusterReadCheckHandler", "health.ClusterRead", 1)
	op = strings.Replace(op, "LivenessCheckHandler", "health.Liveness", 1)
	op = strings.Replace(op, "ReadinessCheckHandler", "health.Readiness", 1)
	op = strings.Replace(op, "-fm", "", 1)
	return op
}

type contextTraceReqType string

const contextTraceReqKey = contextTraceReqType("request-trace-info")

// Hold related tracing data of a http request, any handler
// can modify this struct to modify the trace information .
type traceCtxt struct {
	requestRecorder  *recordRequest
	responseRecorder *logger.ResponseWriter
	funcName         string
}

// If trace is enabled, execute the request if it is traced by other handlers
// otherwise, generate a trace event with request information but no response.
func httpTracer(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if globalTrace.NumSubscribers(madmin.TraceS3|madmin.TraceInternal) == 0 {
			h.ServeHTTP(w, r)
			return
		}

		// Create tracing data structure and associate it to the request context
		tc := traceCtxt{}
		ctx := context.WithValue(r.Context(), contextTraceReqKey, &tc)
		r = r.WithContext(ctx)

		// Setup a http request and response body recorder
		reqRecorder := &recordRequest{Reader: r.Body}
		respRecorder := logger.NewResponseWriter(w)

		tc.requestRecorder = reqRecorder
		tc.responseRecorder = respRecorder

		// Execute call.
		r.Body = reqRecorder

		reqStartTime := time.Now().UTC()
		h.ServeHTTP(respRecorder, r)
		reqEndTime := time.Now().UTC()

		tt := madmin.TraceInternal
		if strings.HasPrefix(tc.funcName, "s3.") {
			tt = madmin.TraceS3
		}
		// No need to continue if no subscribers for actual type...
		if globalTrace.NumSubscribers(tt) == 0 {
			return
		}
		// Calculate input body size with headers
		reqHeaders := r.Header.Clone()
		reqHeaders.Set("Host", r.Host)
		if len(r.TransferEncoding) == 0 {
			reqHeaders.Set("Content-Length", strconv.Itoa(int(r.ContentLength)))
		} else {
			reqHeaders.Set("Transfer-Encoding", strings.Join(r.TransferEncoding, ","))
		}
		inputBytes := reqRecorder.BodySize()
		for k, v := range reqHeaders {
			inputBytes += len(k) + len(v)
		}

		// Calculate node name
		nodeName := r.Host
		if globalIsDistErasure {
			nodeName = globalLocalNodeName
		}
		if host, port, err := net.SplitHostPort(nodeName); err == nil {
			if port == "443" || port == "80" {
				nodeName = host
			}
		}

		// Calculate reqPath
		reqPath := r.URL.RawPath
		if reqPath == "" {
			reqPath = r.URL.Path
		}

		// Calculate function name
		funcName := tc.funcName
		if funcName == "" {
			funcName = "<unknown>"
		}

		t := madmin.TraceInfo{
			TraceType: tt,
			FuncName:  funcName,
			NodeName:  nodeName,
			Time:      reqStartTime,
			Duration:  reqEndTime.Sub(respRecorder.StartTime),
			Path:      reqPath,
			HTTP: &madmin.TraceHTTPStats{
				ReqInfo: madmin.TraceRequestInfo{
					Time:     reqStartTime,
					Proto:    r.Proto,
					Method:   r.Method,
					RawQuery: redactLDAPPwd(r.URL.RawQuery),
					Client:   handlers.GetSourceIP(r),
					Headers:  reqHeaders,
					Path:     reqPath,
					Body:     reqRecorder.Data(),
				},
				RespInfo: madmin.TraceResponseInfo{
					Time:       reqEndTime,
					Headers:    respRecorder.Header().Clone(),
					StatusCode: respRecorder.StatusCode,
					Body:       respRecorder.Body(),
				},
				CallStats: madmin.TraceCallStats{
					Latency:         reqEndTime.Sub(respRecorder.StartTime),
					InputBytes:      inputBytes,
					OutputBytes:     respRecorder.Size(),
					TimeToFirstByte: respRecorder.TimeToFirstByte,
				},
			},
		}

		globalTrace.Publish(t)
	})
}

func httpTrace(f http.HandlerFunc, logBody bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		tc, ok := r.Context().Value(contextTraceReqKey).(*traceCtxt)
		if !ok {
			// Tracing is not enabled for this request
			f.ServeHTTP(w, r)
			return
		}

		tc.funcName = getOpName(runtime.FuncForPC(reflect.ValueOf(f).Pointer()).Name())
		tc.requestRecorder.logBody = logBody
		tc.responseRecorder.LogAllBody = logBody
		tc.responseRecorder.LogErrBody = true

		f.ServeHTTP(w, r)
	}
}

func httpTraceAll(f http.HandlerFunc) http.HandlerFunc {
	return httpTrace(f, true)
}

func httpTraceHdrs(f http.HandlerFunc) http.HandlerFunc {
	return httpTrace(f, false)
}

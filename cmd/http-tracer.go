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
	"context"
	"net"
	"net/http"
	"reflect"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/minio/madmin-go/v3"
	"github.com/minio/minio/internal/handlers"
	xhttp "github.com/minio/minio/internal/http"
	"github.com/minio/minio/internal/mcontext"
)

var ldapPwdRegex = regexp.MustCompile("(^.*?)LDAPPassword=([^&]*?)(&(.*?))?$")

// redact LDAP password if part of string
func redactLDAPPwd(s string) string {
	parts := ldapPwdRegex.FindStringSubmatch(s)
	if len(parts) > 3 {
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
	op = strings.Replace(op, "(*peerS3Server)", "s3", 1)
	op = strings.Replace(op, "ClusterCheckHandler", "health.Cluster", 1)
	op = strings.Replace(op, "ClusterReadCheckHandler", "health.ClusterRead", 1)
	op = strings.Replace(op, "LivenessCheckHandler", "health.Liveness", 1)
	op = strings.Replace(op, "ReadinessCheckHandler", "health.Readiness", 1)
	op = strings.Replace(op, "-fm", "", 1)
	return op
}

// If trace is enabled, execute the request if it is traced by other handlers
// otherwise, generate a trace event with request information but no response.
func httpTracerMiddleware(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Setup a http request response recorder - this is needed for
		// http stats requests and audit if enabled.
		respRecorder := xhttp.NewResponseRecorder(w)

		// Setup a http request body recorder
		reqRecorder := &xhttp.RequestRecorder{Reader: r.Body}
		r.Body = reqRecorder

		// Create tracing data structure and associate it to the request context
		tc := mcontext.TraceCtxt{
			AmzReqID:         w.Header().Get(xhttp.AmzRequestID),
			RequestRecorder:  reqRecorder,
			ResponseRecorder: respRecorder,
		}

		r = r.WithContext(context.WithValue(r.Context(), mcontext.ContextTraceKey, &tc))

		reqStartTime := time.Now().UTC()
		h.ServeHTTP(respRecorder, r)
		reqEndTime := time.Now().UTC()

		if globalTrace.NumSubscribers(madmin.TraceS3|madmin.TraceInternal) == 0 {
			// no subscribers nothing to trace.
			return
		}

		tt := madmin.TraceInternal
		if strings.HasPrefix(tc.FuncName, "s3.") {
			tt = madmin.TraceS3
		}

		// Calculate input body size with headers
		reqHeaders := r.Header.Clone()
		reqHeaders.Set("Host", r.Host)
		if len(r.TransferEncoding) == 0 {
			reqHeaders.Set("Content-Length", strconv.Itoa(int(r.ContentLength)))
		} else {
			reqHeaders.Set("Transfer-Encoding", strings.Join(r.TransferEncoding, ","))
		}
		inputBytes := reqRecorder.Size()
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
		funcName := tc.FuncName
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
			Bytes:     int64(inputBytes + respRecorder.Size()),
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
					TimeToFirstByte: respRecorder.TTFB(),
				},
			},
		}

		globalTrace.Publish(t)
	})
}

func httpTrace(f http.HandlerFunc, logBody bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		tc, ok := r.Context().Value(mcontext.ContextTraceKey).(*mcontext.TraceCtxt)
		if !ok {
			// Tracing is not enabled for this request
			f.ServeHTTP(w, r)
			return
		}

		tc.FuncName = getOpName(runtime.FuncForPC(reflect.ValueOf(f).Pointer()).Name())
		tc.RequestRecorder.LogBody = logBody
		tc.ResponseRecorder.LogAllBody = logBody
		tc.ResponseRecorder.LogErrBody = true

		f.ServeHTTP(w, r)
	}
}

func httpTraceAll(f http.HandlerFunc) http.HandlerFunc {
	return httpTrace(f, true)
}

func httpTraceHdrs(f http.HandlerFunc) http.HandlerFunc {
	return httpTrace(f, false)
}

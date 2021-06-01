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
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"reflect"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"github.com/minio/madmin-go"
	"github.com/minio/minio/internal/handlers"
	"github.com/minio/minio/internal/logger"
	jsonrpc "github.com/minio/rpc"
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
	op = strings.Replace(op, "(*webAPIHandlers)", "web", 1)
	op = strings.Replace(op, "(*storageRESTServer)", "internal", 1)
	op = strings.Replace(op, "(*peerRESTServer)", "internal", 1)
	op = strings.Replace(op, "(*lockRESTServer)", "internal", 1)
	op = strings.Replace(op, "(*stsAPIHandlers)", "sts", 1)
	op = strings.Replace(op, "LivenessCheckHandler", "healthcheck", 1)
	op = strings.Replace(op, "ReadinessCheckHandler", "healthcheck", 1)
	op = strings.Replace(op, "-fm", "", 1)
	return op
}

// WebTrace gets trace of web request
func WebTrace(ri *jsonrpc.RequestInfo) madmin.TraceInfo {
	r := ri.Request
	w := ri.ResponseWriter

	name := ri.Method
	// Setup a http request body recorder
	reqHeaders := r.Header.Clone()
	reqHeaders.Set("Host", r.Host)
	if len(r.TransferEncoding) == 0 {
		reqHeaders.Set("Content-Length", strconv.Itoa(int(r.ContentLength)))
	} else {
		reqHeaders.Set("Transfer-Encoding", strings.Join(r.TransferEncoding, ","))
	}

	now := time.Now().UTC()
	t := madmin.TraceInfo{TraceType: madmin.TraceHTTP, FuncName: name, Time: now}
	t.NodeName = r.Host
	if globalIsDistErasure {
		t.NodeName = globalLocalNodeName
	}
	if t.NodeName == "" {
		t.NodeName = globalLocalNodeName
	}

	// strip only standard port from the host address
	if host, port, err := net.SplitHostPort(t.NodeName); err == nil {
		if port == "443" || port == "80" {
			t.NodeName = host
		}
	}

	vars := mux.Vars(r)
	rq := madmin.TraceRequestInfo{
		Time:     now,
		Proto:    r.Proto,
		Method:   r.Method,
		Path:     SlashSeparator + pathJoin(vars["bucket"], vars["object"]),
		RawQuery: redactLDAPPwd(r.URL.RawQuery),
		Client:   handlers.GetSourceIP(r),
		Headers:  reqHeaders,
	}

	rw, ok := w.(*logger.ResponseWriter)
	if ok {
		rs := madmin.TraceResponseInfo{
			Time:       time.Now().UTC(),
			Headers:    rw.Header().Clone(),
			StatusCode: rw.StatusCode,
			Body:       logger.BodyPlaceHolder,
		}

		if rs.StatusCode == 0 {
			rs.StatusCode = http.StatusOK
		}

		t.RespInfo = rs
		t.CallStats = madmin.TraceCallStats{
			Latency:         rs.Time.Sub(rw.StartTime),
			InputBytes:      int(r.ContentLength),
			OutputBytes:     rw.Size(),
			TimeToFirstByte: rw.TimeToFirstByte,
		}
	}

	t.ReqInfo = rq
	return t
}

// Trace gets trace of http request
func Trace(f http.HandlerFunc, logBody bool, w http.ResponseWriter, r *http.Request) madmin.TraceInfo {
	name := getOpName(runtime.FuncForPC(reflect.ValueOf(f).Pointer()).Name())

	// Setup a http request body recorder
	reqHeaders := r.Header.Clone()
	reqHeaders.Set("Host", r.Host)
	if len(r.TransferEncoding) == 0 {
		reqHeaders.Set("Content-Length", strconv.Itoa(int(r.ContentLength)))
	} else {
		reqHeaders.Set("Transfer-Encoding", strings.Join(r.TransferEncoding, ","))
	}

	reqBodyRecorder := &recordRequest{Reader: r.Body, logBody: logBody, headers: reqHeaders}
	r.Body = ioutil.NopCloser(reqBodyRecorder)

	now := time.Now().UTC()
	t := madmin.TraceInfo{TraceType: madmin.TraceHTTP, FuncName: name, Time: now}

	t.NodeName = r.Host
	if globalIsDistErasure {
		t.NodeName = globalLocalNodeName
	}

	if t.NodeName == "" {
		t.NodeName = globalLocalNodeName
	}

	// strip only standard port from the host address
	if host, port, err := net.SplitHostPort(t.NodeName); err == nil {
		if port == "443" || port == "80" {
			t.NodeName = host
		}
	}

	rq := madmin.TraceRequestInfo{
		Time:     now,
		Proto:    r.Proto,
		Method:   r.Method,
		RawQuery: redactLDAPPwd(r.URL.RawQuery),
		Client:   handlers.GetSourceIP(r),
		Headers:  reqHeaders,
	}

	path := r.URL.RawPath
	if path == "" {
		path = r.URL.Path
	}
	rq.Path = path

	rw := logger.NewResponseWriter(w)
	rw.LogErrBody = true
	rw.LogAllBody = logBody

	// Execute call.
	f(rw, r)

	rs := madmin.TraceResponseInfo{
		Time:       time.Now().UTC(),
		Headers:    rw.Header().Clone(),
		StatusCode: rw.StatusCode,
		Body:       rw.Body(),
	}

	// Transfer request body
	rq.Body = reqBodyRecorder.Data()

	if rs.StatusCode == 0 {
		rs.StatusCode = http.StatusOK
	}

	t.ReqInfo = rq
	t.RespInfo = rs

	t.CallStats = madmin.TraceCallStats{
		Latency:         rs.Time.Sub(rw.StartTime),
		InputBytes:      reqBodyRecorder.Size(),
		OutputBytes:     rw.Size(),
		TimeToFirstByte: rw.TimeToFirstByte,
	}
	return t
}

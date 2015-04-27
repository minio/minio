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

package quota

import (
	"errors"
	"github.com/minio-io/minio/pkg/iodine"
	"io"
	"net"
	"net/http"
	"time"
)

// bandwidthQuotaHandler
type bandwidthQuotaHandler struct {
	handler http.Handler
	quotas  *quotaMap
}

var bandwidthQuotaExceeded = ErrorResponse{
	Code:      "BandwithQuotaExceeded",
	Message:   "Bandwidth Quota Exceeded",
	Resource:  "",
	RequestID: "",
	HostID:    "",
}

var bandwidthInsufficientToProceed = ErrorResponse{
	Code:      "BandwidthQuotaWillBeExceeded",
	Message:   "Bandwidth quota will be exceeded with this request",
	Resource:  "",
	RequestID: "",
	HostID:    "",
}

// ServeHTTP is an http.Handler ServeHTTP method
func (h *bandwidthQuotaHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	host, _, _ := net.SplitHostPort(req.RemoteAddr)
	longIP := longIP{net.ParseIP(host)}.IptoUint32()
	if h.quotas.WillExceedQuota(longIP, req.ContentLength) {
		writeError(w, req, bandwidthInsufficientToProceed, 429)
		return
	}
	req.Body = &quotaReader{
		ReadCloser: req.Body,
		quotas:     h.quotas,
		ip:         longIP,
		w:          w,
		req:        req,
	}
	w = &quotaWriter{
		ResponseWriter: w,
		quotas:         h.quotas,
		ip:             longIP,
	}
	h.handler.ServeHTTP(w, req)
}

// BandwidthCap sets a quote based upon bandwidth used
func BandwidthCap(h http.Handler, limit int64, duration time.Duration) http.Handler {
	return &bandwidthQuotaHandler{
		handler: h,
		quotas: &quotaMap{
			data:        make(map[int64]map[uint32]int64),
			limit:       int64(limit),
			duration:    duration,
			segmentSize: segmentSize(duration),
		},
	}
}

type quotaReader struct {
	io.ReadCloser
	quotas *quotaMap
	ip     uint32
	w      http.ResponseWriter
	req    *http.Request
	err    bool
}

func (q *quotaReader) Read(b []byte) (int, error) {
	if q.err {
		return 0, iodine.New(errors.New("Quota Met"), nil)
	}
	if q.quotas.IsQuotaMet(q.ip) {
		q.err = true
		writeError(q.w, q.req, bandwidthQuotaExceeded, 429)
		return 0, iodine.New(errors.New("Quota Met"), nil)
	}
	n, err := q.ReadCloser.Read(b)
	q.quotas.Add(q.ip, int64(n))
	return n, iodine.New(err, nil)
}

func (q *quotaReader) Close() error {
	return iodine.New(q.ReadCloser.Close(), nil)
}

type quotaWriter struct {
	ResponseWriter http.ResponseWriter
	quotas         *quotaMap
	ip             uint32
}

func (q *quotaWriter) Write(b []byte) (int, error) {
	if q.quotas.IsQuotaMet(q.ip) {
		return 0, iodine.New(errors.New("Quota Met"), nil)
	}
	q.quotas.Add(q.ip, int64(len(b)))
	n, err := q.ResponseWriter.Write(b)
	// remove from quota if a full write isn't performed
	q.quotas.Add(q.ip, int64(n-len(b)))
	return n, iodine.New(err, nil)
}
func (q *quotaWriter) Header() http.Header {
	return q.ResponseWriter.Header()
}

func (q *quotaWriter) WriteHeader(status int) {
	if q.quotas.IsQuotaMet(q.ip) {
		return
	}
	q.ResponseWriter.WriteHeader(status)
}

func segmentSize(duration time.Duration) time.Duration {
	var segmentSize time.Duration
	for i := int64(1); i < duration.Nanoseconds(); i = i * 10 {
		segmentSize = time.Duration(i)
	}
	return segmentSize
}

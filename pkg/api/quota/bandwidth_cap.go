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
	"io"
	"net"
	"net/http"
	"time"

	"sync"

	"github.com/minio/minio/pkg/iodine"
	"github.com/minio/minio/pkg/utils/log"
)

// bandwidthQuotaHandler
type bandwidthQuotaHandler struct {
	handler http.Handler
	quotas  *quotaMap
}

// ServeHTTP is an http.Handler ServeHTTP method
func (h *bandwidthQuotaHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	host, _, _ := net.SplitHostPort(req.RemoteAddr)
	longIP := longIP{net.ParseIP(host)}.IptoUint32()
	if h.quotas.WillExceedQuota(longIP, req.ContentLength) {
		hosts, _ := net.LookupAddr(uint32ToIP(longIP).String())
		log.Debug.Printf("Offending Host: %s, BandwidthUsed: %d", hosts, h.quotas.GetQuotaUsed(longIP))
		writeErrorResponse(w, req, BandWidthInsufficientToProceed, req.URL.Path)
		return
	}
	qr := &quotaReader{
		ReadCloser: req.Body,
		quotas:     h.quotas,
		ip:         longIP,
		w:          w,
		req:        req,
		lock:       &sync.RWMutex{},
	}
	req.Body = qr
	w = &quotaWriter{
		ResponseWriter: w,
		quotas:         h.quotas,
		ip:             longIP,
		quotaReader:    qr,
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
	lock   *sync.RWMutex
}

func (q *quotaReader) Read(b []byte) (int, error) {
	log.Println(q.quotas.GetQuotaUsed(q.ip))
	log.Println(q.quotas.limit)
	q.lock.Lock()
	defer q.lock.Unlock()
	if q.err {
		return 0, iodine.New(errors.New("Quota Met"), nil)
	}
	if q.err == false && q.quotas.IsQuotaMet(q.ip) {
		defer q.lock.Unlock()
		q.err = true
		hosts, _ := net.LookupAddr(uint32ToIP(q.ip).String())
		log.Debug.Printf("Offending Host: %s, BandwidthUsed: %d", hosts, q.quotas.GetQuotaUsed(q.ip))
		writeErrorResponse(q.w, q.req, BandWidthQuotaExceeded, q.req.URL.Path)
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
	quotaReader    *quotaReader
}

func (q *quotaWriter) Write(b []byte) (int, error) {
	q.quotaReader.lock.RLock()
	defer q.quotaReader.lock.RUnlock()
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
	q.quotaReader.lock.RLock()
	defer q.quotaReader.lock.RUnlock()
	if q.quotas.IsQuotaMet(q.ip) || q.quotaReader.err {
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

// Copyright 2016 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package storage

import (
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"reflect"
	"strconv"
	"strings"

	"cloud.google.com/go/internal/trace"
	"golang.org/x/net/context"
	"google.golang.org/api/googleapi"
)

var crc32cTable = crc32.MakeTable(crc32.Castagnoli)

// NewReader creates a new Reader to read the contents of the
// object.
// ErrObjectNotExist will be returned if the object is not found.
//
// The caller must call Close on the returned Reader when done reading.
func (o *ObjectHandle) NewReader(ctx context.Context) (*Reader, error) {
	return o.NewRangeReader(ctx, 0, -1)
}

// NewRangeReader reads part of an object, reading at most length bytes
// starting at the given offset. If length is negative, the object is read
// until the end.
func (o *ObjectHandle) NewRangeReader(ctx context.Context, offset, length int64) (r *Reader, err error) {
	ctx = trace.StartSpan(ctx, "cloud.google.com/go/storage.Object.NewRangeReader")
	defer func() { trace.EndSpan(ctx, err) }()

	if err := o.validate(); err != nil {
		return nil, err
	}
	if offset < 0 {
		return nil, fmt.Errorf("storage: invalid offset %d < 0", offset)
	}
	if o.conds != nil {
		if err := o.conds.validate("NewRangeReader"); err != nil {
			return nil, err
		}
	}
	u := &url.URL{
		Scheme:   "https",
		Host:     "storage.googleapis.com",
		Path:     fmt.Sprintf("/%s/%s", o.bucket, o.object),
		RawQuery: conditionsQuery(o.gen, o.conds),
	}
	verb := "GET"
	if length == 0 {
		verb = "HEAD"
	}
	req, err := http.NewRequest(verb, u.String(), nil)
	if err != nil {
		return nil, err
	}
	req = withContext(req, ctx)
	if o.userProject != "" {
		req.Header.Set("X-Goog-User-Project", o.userProject)
	}
	if o.readCompressed {
		req.Header.Set("Accept-Encoding", "gzip")
	}
	if err := setEncryptionHeaders(req.Header, o.encryptionKey, false); err != nil {
		return nil, err
	}

	// Define a function that initiates a Read with offset and length, assuming we
	// have already read seen bytes.
	reopen := func(seen int64) (*http.Response, error) {
		start := offset + seen
		if length < 0 && start > 0 {
			req.Header.Set("Range", fmt.Sprintf("bytes=%d-", start))
		} else if length > 0 {
			// The end character isn't affected by how many bytes we've seen.
			req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, offset+length-1))
		}
		var res *http.Response
		err = runWithRetry(ctx, func() error {
			res, err = o.c.hc.Do(req)
			if err != nil {
				return err
			}
			if res.StatusCode == http.StatusNotFound {
				res.Body.Close()
				return ErrObjectNotExist
			}
			if res.StatusCode < 200 || res.StatusCode > 299 {
				body, _ := ioutil.ReadAll(res.Body)
				res.Body.Close()
				return &googleapi.Error{
					Code:   res.StatusCode,
					Header: res.Header,
					Body:   string(body),
				}
			}
			if start > 0 && length != 0 && res.StatusCode != http.StatusPartialContent {
				res.Body.Close()
				return errors.New("storage: partial request not satisfied")
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
		return res, nil
	}

	res, err := reopen(0)
	if err != nil {
		return nil, err
	}
	var size int64 // total size of object, even if a range was requested.
	if res.StatusCode == http.StatusPartialContent {
		cr := strings.TrimSpace(res.Header.Get("Content-Range"))
		if !strings.HasPrefix(cr, "bytes ") || !strings.Contains(cr, "/") {

			return nil, fmt.Errorf("storage: invalid Content-Range %q", cr)
		}
		size, err = strconv.ParseInt(cr[strings.LastIndex(cr, "/")+1:], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("storage: invalid Content-Range %q", cr)
		}
	} else {
		size = res.ContentLength
	}

	remain := res.ContentLength
	body := res.Body
	if length == 0 {
		remain = 0
		body.Close()
		body = emptyBody
	}
	var (
		checkCRC bool
		crc      uint32
	)
	// Even if there is a CRC header, we can't compute the hash on partial data.
	if remain == size {
		crc, checkCRC = parseCRC32c(res)
	}
	return &Reader{
		body:            body,
		size:            size,
		remain:          remain,
		contentType:     res.Header.Get("Content-Type"),
		contentEncoding: res.Header.Get("Content-Encoding"),
		cacheControl:    res.Header.Get("Cache-Control"),
		wantCRC:         crc,
		checkCRC:        checkCRC,
		reopen:          reopen,
	}, nil
}

func parseCRC32c(res *http.Response) (uint32, bool) {
	const prefix = "crc32c="
	for _, spec := range res.Header["X-Goog-Hash"] {
		if strings.HasPrefix(spec, prefix) {
			c, err := decodeUint32(spec[len(prefix):])
			if err == nil {
				return c, true
			}
		}
	}
	return 0, false
}

var emptyBody = ioutil.NopCloser(strings.NewReader(""))

// Reader reads a Cloud Storage object.
// It implements io.Reader.
//
// Typically, a Reader computes the CRC of the downloaded content and compares it to
// the stored CRC, returning an error from Read if there is a mismatch. This integrity check
// is skipped if transcoding occurs. See https://cloud.google.com/storage/docs/transcoding.
type Reader struct {
	body               io.ReadCloser
	seen, remain, size int64
	contentType        string
	contentEncoding    string
	cacheControl       string
	checkCRC           bool   // should we check the CRC?
	wantCRC            uint32 // the CRC32c value the server sent in the header
	gotCRC             uint32 // running crc
	checkedCRC         bool   // did we check the CRC? (For tests.)
	reopen             func(seen int64) (*http.Response, error)
}

// Close closes the Reader. It must be called when done reading.
func (r *Reader) Close() error {
	return r.body.Close()
}

func (r *Reader) Read(p []byte) (int, error) {
	n, err := r.readWithRetry(p)
	if r.remain != -1 {
		r.remain -= int64(n)
	}
	if r.checkCRC {
		r.gotCRC = crc32.Update(r.gotCRC, crc32cTable, p[:n])
		// Check CRC here. It would be natural to check it in Close, but
		// everybody defers Close on the assumption that it doesn't return
		// anything worth looking at.
		if r.remain == 0 { // Only check if we have Content-Length.
			r.checkedCRC = true
			if r.gotCRC != r.wantCRC {
				return n, fmt.Errorf("storage: bad CRC on read: got %d, want %d",
					r.gotCRC, r.wantCRC)
			}
		}
	}
	return n, err
}

func (r *Reader) readWithRetry(p []byte) (int, error) {
	n := 0
	for len(p[n:]) > 0 {
		m, err := r.body.Read(p[n:])
		n += m
		r.seen += int64(m)
		if !shouldRetryRead(err) {
			return n, err
		}
		// Read failed, but we will try again. Send a ranged read request that takes
		// into account the number of bytes we've already seen.
		res, err := r.reopen(r.seen)
		if err != nil {
			// reopen already retries
			return n, err
		}
		r.body.Close()
		r.body = res.Body
	}
	return n, nil
}

func shouldRetryRead(err error) bool {
	if err == nil {
		return false
	}
	return strings.HasSuffix(err.Error(), "INTERNAL_ERROR") && strings.Contains(reflect.TypeOf(err).String(), "http2")
}

// Size returns the size of the object in bytes.
// The returned value is always the same and is not affected by
// calls to Read or Close.
func (r *Reader) Size() int64 {
	return r.size
}

// Remain returns the number of bytes left to read, or -1 if unknown.
func (r *Reader) Remain() int64 {
	return r.remain
}

// ContentType returns the content type of the object.
func (r *Reader) ContentType() string {
	return r.contentType
}

// ContentEncoding returns the content encoding of the object.
func (r *Reader) ContentEncoding() string {
	return r.contentEncoding
}

// CacheControl returns the cache control of the object.
func (r *Reader) CacheControl() string {
	return r.cacheControl
}

/*
 * MinIO Cloud Storage, (C) 2019 MinIO, Inc.
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

package s3select

import (
	"compress/bzip2"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"

	gzip "github.com/klauspost/pgzip"
)

type countUpReader struct {
	reader    io.Reader
	bytesRead int64
}

func (r *countUpReader) Read(p []byte) (n int, err error) {
	n, err = r.reader.Read(p)
	atomic.AddInt64(&r.bytesRead, int64(n))
	return n, err
}

func (r *countUpReader) BytesRead() int64 {
	if r == nil {
		return 0
	}
	return atomic.LoadInt64(&r.bytesRead)
}

func newCountUpReader(reader io.Reader) *countUpReader {
	return &countUpReader{
		reader: reader,
	}
}

type progressReader struct {
	rc              io.ReadCloser
	scannedReader   *countUpReader
	processedReader *countUpReader

	closedMu sync.Mutex
	gzr      *gzip.Reader
	closed   bool
}

func (pr *progressReader) Read(p []byte) (n int, err error) {
	// This ensures that Close will block until Read has completed.
	// This allows another goroutine to close the reader.
	pr.closedMu.Lock()
	defer pr.closedMu.Unlock()
	if pr.closed {
		return 0, errors.New("progressReader: read after Close")
	}
	return pr.processedReader.Read(p)
}

func (pr *progressReader) Close() error {
	pr.closedMu.Lock()
	defer pr.closedMu.Unlock()
	if pr.closed {
		return nil
	}
	pr.closed = true
	if pr.gzr != nil {
		pr.gzr.Close()
	}
	return pr.rc.Close()
}

func (pr *progressReader) Stats() (bytesScanned, bytesProcessed int64) {
	if pr == nil {
		return 0, 0
	}
	return pr.scannedReader.BytesRead(), pr.processedReader.BytesRead()
}

func newProgressReader(rc io.ReadCloser, compType CompressionType) (*progressReader, error) {
	if rc == nil {
		return nil, errors.New("newProgressReader: nil reader provided")
	}
	scannedReader := newCountUpReader(rc)
	pr := progressReader{
		rc:            rc,
		scannedReader: scannedReader,
	}
	var err error
	var r io.Reader

	switch compType {
	case noneType:
		r = scannedReader
	case gzipType:
		pr.gzr, err = gzip.NewReader(scannedReader)
		if err != nil {
			if errors.Is(err, gzip.ErrHeader) || errors.Is(err, gzip.ErrChecksum) {
				return nil, errInvalidGZIPCompressionFormat(err)
			}
			return nil, errTruncatedInput(err)
		}
		r = pr.gzr
	case bzip2Type:
		r = bzip2.NewReader(scannedReader)
	default:
		return nil, errInvalidCompressionFormat(fmt.Errorf("unknown compression type '%v'", compType))
	}
	pr.processedReader = newCountUpReader(r)

	return &pr, nil
}

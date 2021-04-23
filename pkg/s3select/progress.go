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

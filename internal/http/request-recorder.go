// Copyright (c) 2015-2022 MinIO, Inc.
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

package http

import (
	"bytes"
	"io"
)

// RequestRecorder - records the
// of a given io.Reader
type RequestRecorder struct {
	// Data source to record
	io.Reader
	// Response body should be logged
	LogBody bool

	// internal recording buffer
	buf bytes.Buffer
	// total bytes read including header size
	bytesRead int
}

// Close is a no operation closer
func (r *RequestRecorder) Close() error {
	// no-op
	return nil
}

// Read reads from the internal reader and counts/save the body in the memory
func (r *RequestRecorder) Read(p []byte) (n int, err error) {
	n, err = r.Reader.Read(p)
	r.bytesRead += n

	if r.LogBody {
		r.buf.Write(p[:n])
	}
	if err != nil {
		return n, err
	}
	return n, err
}

// Size returns the body size if the currently read object
func (r *RequestRecorder) Size() int {
	return r.bytesRead
}

// Data returns the bytes that were recorded.
func (r *RequestRecorder) Data() []byte {
	// If body logging is enabled then we return the actual body
	if r.LogBody {
		return r.buf.Bytes()
	}
	// ... otherwise we return <BLOB> placeholder
	return blobBody
}

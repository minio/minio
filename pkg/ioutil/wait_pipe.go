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

// Forked from golang.org/pkg/os.ReadFile with NOATIME support.
// Copyright 2009 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the https://golang.org/LICENSE file.

package ioutil

import (
	"io"
	"sync"
)

// PipeWriter is similar to io.PipeWriter with wait group
type PipeWriter struct {
	*io.PipeWriter
	done func()
}

// CloseWithError close with supplied error the writer end.
func (w *PipeWriter) CloseWithError(err error) error {
	err = w.PipeWriter.CloseWithError(err)
	w.done()
	return err
}

// PipeReader is similar to io.PipeReader with wait group
type PipeReader struct {
	*io.PipeReader
	wait func()
}

// CloseWithError close with supplied error the reader end
func (r *PipeReader) CloseWithError(err error) error {
	err = r.PipeReader.CloseWithError(err)
	r.wait()
	return err
}

// WaitPipe implements wait-group backend io.Pipe to provide
// synchronization between read() end with write() end.
func WaitPipe() (*PipeReader, *PipeWriter) {
	r, w := io.Pipe()
	var wg sync.WaitGroup
	wg.Add(1)
	return &PipeReader{
			PipeReader: r,
			wait:       wg.Wait,
		}, &PipeWriter{
			PipeWriter: w,
			done:       wg.Done,
		}
}

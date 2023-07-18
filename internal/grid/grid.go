// Copyright (c) 2015-2023 MinIO, Inc.
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

// Package grid provides single-connection two-way grid communication.
package grid

import "sync"

const (
	debugPrint = true
)

var internalByteBuffer = sync.Pool{
	New: func() any { return make([]byte, 0, 4096) },
}

// GetByteBuffer can be replaced with a function that returns a small
// byte buffer.
// When replacing PutByteBuffer should also be replaced
// There is no minimum size.
var GetByteBuffer = func() []byte {
	return internalByteBuffer.Get().([]byte)
}

// PutByteBuffer is for returning byte buffers.
var PutByteBuffer = func(b []byte) {
	if cap(b) > 1024 && cap(b) < 64<<10 {
		internalByteBuffer.Put(b)
	}
}

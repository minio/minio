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
	"io"

	"github.com/minio/minio/internal/bpool"
	xioutil "github.com/minio/minio/internal/ioutil"
	"github.com/minio/minio/internal/logger"
)

// Heal heals the shard files on non-nil writers. Note that the quorum passed is 1
// as healing should continue even if it has been successful healing only one shard file.
func (e Erasure) Heal(ctx context.Context, readers []io.ReaderAt, writers []io.Writer, size int64, bp *bpool.BytePoolCap) error {
	r, w := xioutil.WaitPipe()
	go func() {
		_, err := e.Decode(ctx, w, readers, 0, size, size, nil)
		w.CloseWithError(err)
	}()

	// Fetch buffer for I/O, returns from the pool if not allocates a new one and returns.
	var buffer []byte
	switch {
	case size == 0:
		buffer = make([]byte, 1) // Allocate atleast a byte to reach EOF
	case size >= e.blockSize:
		buffer = bp.Get()
		defer bp.Put(buffer)
	case size < e.blockSize:
		// No need to allocate fully blockSizeV1 buffer if the incoming data is smaller.
		buffer = make([]byte, size, 2*size+int64(e.parityBlocks+e.dataBlocks-1))
	}

	// quorum is 1 because CreateFile should continue writing as long as we are writing to even 1 disk.
	n, err := e.Encode(ctx, r, writers, buffer, 1)
	if err == nil && n != size {
		logger.LogIf(ctx, errLessData)
		err = errLessData
	}
	r.CloseWithError(err)
	return err
}

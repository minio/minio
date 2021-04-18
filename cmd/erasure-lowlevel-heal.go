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

	"github.com/minio/minio/cmd/logger"
)

// Heal heals the shard files on non-nil writers. Note that the quorum passed is 1
// as healing should continue even if it has been successful healing only one shard file.
func (e Erasure) Heal(ctx context.Context, readers []io.ReaderAt, writers []io.Writer, size int64) error {
	r, w := io.Pipe()
	go func() {
		if _, err := e.Decode(ctx, w, readers, 0, size, size, nil); err != nil {
			w.CloseWithError(err)
			return
		}
		w.Close()
	}()
	buf := make([]byte, e.blockSize)
	// quorum is 1 because CreateFile should continue writing as long as we are writing to even 1 disk.
	n, err := e.Encode(ctx, r, writers, buf, 1)
	if err != nil {
		return err
	}
	if n != size {
		logger.LogIf(ctx, errLessData)
		return errLessData
	}
	return nil
}

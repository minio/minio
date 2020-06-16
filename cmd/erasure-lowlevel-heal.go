/*
 * MinIO Cloud Storage, (C) 2016-2020 MinIO, Inc.
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
		if err := e.Decode(ctx, w, readers, 0, size, size, nil); err != nil {
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

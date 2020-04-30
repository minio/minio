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

package cmd

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"hash"
	"io"

	"github.com/minio/minio/cmd/logger"
)

type errHashMismatch struct {
	message string
}

func (err *errHashMismatch) Error() string {
	return err.message
}

// Calculates bitrot in chunks and writes the hash into the stream.
type streamingBitrotWriter struct {
	iow       *io.PipeWriter
	h         hash.Hash
	shardSize int64
	canClose  chan struct{} // Needed to avoid race explained in Close() call.
}

func (b *streamingBitrotWriter) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	b.h.Reset()
	b.h.Write(p)
	hashBytes := b.h.Sum(nil)
	_, err := b.iow.Write(hashBytes)
	if err != nil {
		return 0, err
	}
	return b.iow.Write(p)
}

func (b *streamingBitrotWriter) Close() error {
	err := b.iow.Close()
	// Wait for all data to be written before returning else it causes race conditions.
	// Race condition is because of io.PipeWriter implementation. i.e consider the following
	// sequent of operations:
	// 1) pipe.Write()
	// 2) pipe.Close()
	// Now pipe.Close() can return before the data is read on the other end of the pipe and written to the disk
	// Hence an immediate Read() on the file can return incorrect data.
	<-b.canClose
	return err
}

// Returns streaming bitrot writer implementation.
func newStreamingBitrotWriter(disk StorageAPI, volume, filePath string, length int64, algo BitrotAlgorithm, shardSize int64) io.WriteCloser {
	r, w := io.Pipe()
	h := algo.New()
	bw := &streamingBitrotWriter{w, h, shardSize, make(chan struct{})}
	go func() {
		totalFileSize := int64(-1) // For compressed objects length will be unknown (represented by length=-1)
		if length != -1 {
			bitrotSumsTotalSize := ceilFrac(length, shardSize) * int64(h.Size()) // Size used for storing bitrot checksums.
			totalFileSize = bitrotSumsTotalSize + length
		}
		err := disk.CreateFile(volume, filePath, totalFileSize, r)
		r.CloseWithError(err)
		close(bw.canClose)
	}()
	return bw
}

// ReadAt() implementation which verifies the bitrot hash available as part of the stream.
type streamingBitrotReader struct {
	disk       StorageAPI
	rc         io.ReadCloser
	volume     string
	filePath   string
	tillOffset int64
	currOffset int64
	h          hash.Hash
	shardSize  int64
	hashBytes  []byte
}

func (b *streamingBitrotReader) Close() error {
	if b.rc == nil {
		return nil
	}
	return b.rc.Close()
}

func (b *streamingBitrotReader) ReadAt(buf []byte, offset int64) (int, error) {
	var err error
	if offset%b.shardSize != 0 {
		// Offset should always be aligned to b.shardSize
		// Can never happen unless there are programmer bugs
		return 0, errUnexpected
	}
	if b.rc == nil {
		// For the first ReadAt() call we need to open the stream for reading.
		b.currOffset = offset
		streamOffset := (offset/b.shardSize)*int64(b.h.Size()) + offset
		b.rc, err = b.disk.ReadFileStream(b.volume, b.filePath, streamOffset, b.tillOffset-streamOffset)
		if err != nil {
			return 0, err
		}
	}
	if offset != b.currOffset {
		// Can never happen unless there are programmer bugs
		return 0, errUnexpected
	}
	b.h.Reset()
	_, err = io.ReadFull(b.rc, b.hashBytes)
	if err != nil {
		return 0, err
	}
	_, err = io.ReadFull(b.rc, buf)
	if err != nil {
		return 0, err
	}
	b.h.Write(buf)

	if !bytes.Equal(b.h.Sum(nil), b.hashBytes) {
		err := &errHashMismatch{fmt.Sprintf("hashes do not match expected %s, got %s",
			hex.EncodeToString(b.hashBytes), hex.EncodeToString(b.h.Sum(nil)))}
		logger.LogIf(GlobalContext, err)
		return 0, err
	}
	b.currOffset += int64(len(buf))
	return len(buf), nil
}

// Returns streaming bitrot reader implementation.
func newStreamingBitrotReader(disk StorageAPI, volume, filePath string, tillOffset int64, algo BitrotAlgorithm, shardSize int64) *streamingBitrotReader {
	h := algo.New()
	return &streamingBitrotReader{
		disk,
		nil,
		volume,
		filePath,
		ceilFrac(tillOffset, shardSize)*int64(h.Size()) + tillOffset,
		0,
		h,
		shardSize,
		make([]byte, h.Size()),
	}
}

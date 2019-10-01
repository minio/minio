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
	"errors"
	"fmt"
	"hash"
	"io"
)

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
	disk             StorageAPI
	rc               io.ReadCloser
	shardSize        int64
	volume, path     string
	hash             hash.Hash
	computed, stored []byte
}

func (b *streamingBitrotReader) Close() error {
	if b.rc == nil {
		return nil
	}
	return b.rc.Close()
}

func (b *streamingBitrotReader) ReadAt(buf []byte, offset int64) (int, error) {
	var err error
	if b.rc == nil {
		// For the first ReadAt() call we need to open the stream for reading.
		streamOffset := (offset / b.shardSize) * (int64(b.hash.Size()) + offset)
		b.rc, err = b.disk.ReadFileStream(b.volume, b.path, streamOffset, -1)
		if err != nil {
			return 0, err
		}
	}
	_, err = io.ReadFull(b.rc, b.stored)
	if err != nil {
		if err == io.ErrUnexpectedEOF {
			err = errors.New("bitrot: only partial checksum found")
		}
		return 0, err
	}

	n, err := io.ReadFull(b.rc, buf)
	if err != nil && err != io.ErrUnexpectedEOF {
		return 0, err
	}

	b.hash.Reset()
	b.hash.Write(buf[:n])
	b.hash.Sum(b.computed[:0])
	if !bytes.Equal(b.computed, b.stored) {
		err = fmt.Errorf("hashes do not match expected %s, got %s",
			hex.EncodeToString(b.stored), hex.EncodeToString(b.computed))
		return 0, err

	}
	return n, err
}

// Returns streaming bitrot reader implementation.
func newStreamingBitrotReader(disk StorageAPI, volume, filePath string, algo BitrotAlgorithm, shardSize int64) *streamingBitrotReader {
	hash := algo.New()
	return &streamingBitrotReader{
		disk:      disk,
		rc:        nil, // We open the stream on the first read
		volume:    volume,
		path:      filePath,
		shardSize: shardSize,
		hash:      hash,
		stored:    make([]byte, hash.Size()),
		computed:  make([]byte, hash.Size()),
	}
}

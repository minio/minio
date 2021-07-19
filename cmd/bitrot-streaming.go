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
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"sync"

	xhttp "github.com/minio/minio/internal/http"
	"github.com/minio/minio/internal/ioutil"
	"github.com/minio/minio/internal/logger"
)

// Calculates bitrot in chunks and writes the hash into the stream.
type streamingBitrotWriter struct {
	iow          io.WriteCloser
	closeWithErr func(err error) error
	h            hash.Hash
	shardSize    int64
	canClose     *sync.WaitGroup
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
		b.closeWithErr(err)
		return 0, err
	}
	n, err := b.iow.Write(p)
	if err != nil {
		b.closeWithErr(err)
		return n, err
	}
	return n, err
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
	if b.canClose != nil {
		b.canClose.Wait()
	}
	return err
}

// newStreamingBitrotWriterBuffer returns streaming bitrot writer implementation.
// The output is written to the supplied writer w.
func newStreamingBitrotWriterBuffer(w io.Writer, algo BitrotAlgorithm, shardSize int64) io.Writer {
	return &streamingBitrotWriter{iow: ioutil.NopCloser(w), h: algo.New(), shardSize: shardSize, canClose: nil, closeWithErr: func(err error) error {
		// Similar to CloseWithError on pipes we always return nil.
		return nil
	}}
}

// Returns streaming bitrot writer implementation.
func newStreamingBitrotWriter(disk StorageAPI, volume, filePath string, length int64, algo BitrotAlgorithm, shardSize int64) io.Writer {
	r, w := io.Pipe()
	h := algo.New()

	bw := &streamingBitrotWriter{iow: w, closeWithErr: w.CloseWithError, h: h, shardSize: shardSize, canClose: &sync.WaitGroup{}}
	bw.canClose.Add(1)
	go func() {
		totalFileSize := int64(-1) // For compressed objects length will be unknown (represented by length=-1)
		if length != -1 {
			bitrotSumsTotalSize := ceilFrac(length, shardSize) * int64(h.Size()) // Size used for storing bitrot checksums.
			totalFileSize = bitrotSumsTotalSize + length
		}
		r.CloseWithError(disk.CreateFile(context.TODO(), volume, filePath, totalFileSize, r))
		bw.canClose.Done()
	}()
	return bw
}

// ReadAt() implementation which verifies the bitrot hash available as part of the stream.
type streamingBitrotReader struct {
	disk       StorageAPI
	data       []byte
	rc         io.Reader
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
	if closer, ok := b.rc.(io.Closer); ok {
		// drain the body for connection re-use at network layer.
		xhttp.DrainBody(struct {
			io.Reader
			io.Closer
		}{
			Reader: b.rc,
			Closer: closeWrapper(func() error { return nil }),
		})
		return closer.Close()
	}
	return nil
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
		if len(b.data) == 0 && b.tillOffset != streamOffset {
			b.rc, err = b.disk.ReadFileStream(context.TODO(), b.volume, b.filePath, streamOffset, b.tillOffset-streamOffset)
			if err != nil {
				logger.LogIf(GlobalContext,
					fmt.Errorf("Error(%w) reading erasure shards at (%s: %s/%s), will attempt to reconstruct if we have quorum",
						err, b.disk, b.volume, b.filePath))
			}
		} else {
			b.rc = io.NewSectionReader(bytes.NewReader(b.data), streamOffset, b.tillOffset-streamOffset)
		}
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
		logger.LogIf(GlobalContext, fmt.Errorf("Disk: %s  -> %s/%s - content hash does not match - expected %s, got %s",
			b.disk, b.volume, b.filePath, hex.EncodeToString(b.hashBytes), hex.EncodeToString(b.h.Sum(nil))))
		return 0, errFileCorrupt
	}
	b.currOffset += int64(len(buf))
	return len(buf), nil
}

// Returns streaming bitrot reader implementation.
func newStreamingBitrotReader(disk StorageAPI, data []byte, volume, filePath string, tillOffset int64, algo BitrotAlgorithm, shardSize int64) *streamingBitrotReader {
	h := algo.New()
	return &streamingBitrotReader{
		disk:       disk,
		data:       data,
		volume:     volume,
		filePath:   filePath,
		tillOffset: ceilFrac(tillOffset, shardSize)*int64(h.Size()) + tillOffset,
		h:          h,
		shardSize:  shardSize,
		hashBytes:  make([]byte, h.Size()),
	}
}

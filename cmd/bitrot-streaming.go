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
	"errors"
	"hash"
	"io"
	"sync"

	xhttp "github.com/minio/minio/internal/http"
	"github.com/minio/minio/internal/ioutil"
	"github.com/minio/minio/internal/ringbuffer"
)

// Calculates bitrot in chunks and writes the hash into the stream.
type streamingBitrotWriter struct {
	iow          io.WriteCloser
	closeWithErr func(err error)
	h            hash.Hash
	shardSize    int64
	canClose     *sync.WaitGroup
	byteBuf      []byte
	finished     bool
}

func (b *streamingBitrotWriter) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if b.finished {
		return 0, errors.New("bitrot write not allowed")
	}
	if int64(len(p)) > b.shardSize {
		return 0, errors.New("unexpected bitrot buffer size")
	}
	if int64(len(p)) < b.shardSize {
		b.finished = true
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
	if n != len(p) {
		err = io.ErrShortWrite
		b.closeWithErr(err)
	}
	return n, err
}

func (b *streamingBitrotWriter) Close() error {
	// Close the underlying writer.
	// This will also flush the ring buffer if used.
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

	// Recycle the buffer.
	if b.byteBuf != nil {
		globalBytePoolCap.Load().Put(b.byteBuf)
		b.byteBuf = nil
	}
	return err
}

// newStreamingBitrotWriterBuffer returns streaming bitrot writer implementation.
// The output is written to the supplied writer w.
func newStreamingBitrotWriterBuffer(w io.Writer, algo BitrotAlgorithm, shardSize int64) io.Writer {
	return &streamingBitrotWriter{iow: ioutil.NopCloser(w), h: algo.New(), shardSize: shardSize, canClose: nil, closeWithErr: func(err error) {}}
}

// Returns streaming bitrot writer implementation.
func newStreamingBitrotWriter(disk StorageAPI, origvolume, volume, filePath string, length int64, algo BitrotAlgorithm, shardSize int64) io.Writer {
	h := algo.New()
	buf := globalBytePoolCap.Load().Get()
	rb := ringbuffer.NewBuffer(buf[:cap(buf)]).SetBlocking(true)

	bw := &streamingBitrotWriter{
		iow:          ioutil.NewDeadlineWriter(rb.WriteCloser(), globalDriveConfig.GetMaxTimeout()),
		closeWithErr: rb.CloseWithError,
		h:            h,
		shardSize:    shardSize,
		canClose:     &sync.WaitGroup{},
		byteBuf:      buf,
	}
	bw.canClose.Add(1)
	go func() {
		defer bw.canClose.Done()

		totalFileSize := int64(-1) // For compressed objects length will be unknown (represented by length=-1)
		if length != -1 {
			bitrotSumsTotalSize := ceilFrac(length, shardSize) * int64(h.Size()) // Size used for storing bitrot checksums.
			totalFileSize = bitrotSumsTotalSize + length
		}
		rb.CloseWithError(disk.CreateFile(context.TODO(), origvolume, volume, filePath, totalFileSize, rb))
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
		// drain the body for connection reuse at network layer.
		xhttp.DrainBody(io.NopCloser(b.rc))
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

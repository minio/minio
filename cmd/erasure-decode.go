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
	"errors"
	"io"
	"sync"
	"sync/atomic"

	"github.com/minio/minio/cmd/logger"
)

var errHealRequired = errors.New("heal required")

// Reads in parallel from readers.
type parallelReader struct {
	readers       []io.ReaderAt
	orgReaders    []io.ReaderAt
	dataBlocks    int
	offset        int64
	shardSize     int64
	shardFileSize int64
	buf           [][]byte
	readerToBuf   []int
}

// newParallelReader returns parallelReader.
func newParallelReader(readers []io.ReaderAt, e Erasure, offset, totalLength int64) *parallelReader {
	r2b := make([]int, len(readers))
	for i := range r2b {
		r2b[i] = i
	}
	return &parallelReader{
		readers:       readers,
		orgReaders:    readers,
		dataBlocks:    e.dataBlocks,
		offset:        (offset / e.blockSize) * e.ShardSize(),
		shardSize:     e.ShardSize(),
		shardFileSize: e.ShardFileSize(totalLength),
		buf:           make([][]byte, len(readers)),
		readerToBuf:   r2b,
	}
}

// preferReaders can mark readers as preferred.
// These will be chosen before others.
func (p *parallelReader) preferReaders(prefer []bool) {
	if len(prefer) != len(p.orgReaders) {
		return
	}
	// Copy so we don't change our input.
	tmp := make([]io.ReaderAt, len(p.orgReaders))
	copy(tmp, p.orgReaders)
	p.readers = tmp
	// next is the next non-preferred index.
	next := 0
	for i, ok := range prefer {
		if !ok || p.readers[i] == nil {
			continue
		}
		if i == next {
			next++
			continue
		}
		// Move reader with index i to index next.
		// Do this by swapping next and i
		p.readers[next], p.readers[i] = p.readers[i], p.readers[next]
		p.readerToBuf[next] = i
		p.readerToBuf[i] = next
		next++
	}
}

// Returns if buf can be erasure decoded.
func (p *parallelReader) canDecode(buf [][]byte) bool {
	bufCount := 0
	for _, b := range buf {
		if len(b) > 0 {
			bufCount++
		}
	}
	return bufCount >= p.dataBlocks
}

// Read reads from readers in parallel. Returns p.dataBlocks number of bufs.
func (p *parallelReader) Read(dst [][]byte) ([][]byte, error) {
	newBuf := dst
	if len(dst) != len(p.readers) {
		newBuf = make([][]byte, len(p.readers))
	} else {
		for i := range newBuf {
			newBuf[i] = newBuf[i][:0]
		}
	}
	var newBufLK sync.RWMutex

	if p.offset+p.shardSize > p.shardFileSize {
		p.shardSize = p.shardFileSize - p.offset
	}
	if p.shardSize == 0 {
		return newBuf, nil
	}

	readTriggerCh := make(chan bool, len(p.readers))
	for i := 0; i < p.dataBlocks; i++ {
		// Setup read triggers for p.dataBlocks number of reads so that it reads in parallel.
		readTriggerCh <- true
	}

	healRequired := int32(0) // Atomic bool flag.
	readerIndex := 0
	var wg sync.WaitGroup
	// if readTrigger is true, it implies next disk.ReadAt() should be tried
	// if readTrigger is false, it implies previous disk.ReadAt() was successful and there is no need
	// to try reading the next disk.
	for readTrigger := range readTriggerCh {
		newBufLK.RLock()
		canDecode := p.canDecode(newBuf)
		newBufLK.RUnlock()
		if canDecode {
			break
		}
		if readerIndex == len(p.readers) {
			break
		}
		if !readTrigger {
			continue
		}
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			rr := p.readers[i]
			if rr == nil {
				// Since reader is nil, trigger another read.
				readTriggerCh <- true
				return
			}
			bufIdx := p.readerToBuf[i]
			if p.buf[bufIdx] == nil {
				// Reading first time on this disk, hence the buffer needs to be allocated.
				// Subsequent reads will re-use this buffer.
				p.buf[bufIdx] = make([]byte, p.shardSize)
			}
			// For the last shard, the shardsize might be less than previous shard sizes.
			// Hence the following statement ensures that the buffer size is reset to the right size.
			p.buf[bufIdx] = p.buf[bufIdx][:p.shardSize]
			_, err := rr.ReadAt(p.buf[bufIdx], p.offset)
			if err != nil {
				if _, ok := err.(*errHashMismatch); ok {
					atomic.StoreInt32(&healRequired, 1)
				}

				// This will be communicated upstream.
				p.orgReaders[bufIdx] = nil
				p.readers[i] = nil
				// Since ReadAt returned error, trigger another read.
				readTriggerCh <- true
				return
			}
			newBufLK.Lock()
			newBuf[bufIdx] = p.buf[bufIdx]
			newBufLK.Unlock()
			// Since ReadAt returned success, there is no need to trigger another read.
			readTriggerCh <- false
		}(readerIndex)
		readerIndex++
	}
	wg.Wait()

	if p.canDecode(newBuf) {
		p.offset += p.shardSize
		if healRequired != 0 {
			return newBuf, errHealRequired
		}
		return newBuf, nil
	}

	return nil, errErasureReadQuorum
}

type errDecodeHealRequired struct {
	err error
}

func (err *errDecodeHealRequired) Error() string {
	return err.err.Error()
}

func (err *errDecodeHealRequired) Unwrap() error {
	return err.err
}

// Decode reads from readers, reconstructs data if needed and writes the data to the writer.
// A set of preferred drives can be supplied. In that case they will be used and the data reconstructed.
func (e Erasure) Decode(ctx context.Context, writer io.Writer, readers []io.ReaderAt, offset, length, totalLength int64, prefer []bool) error {
	healRequired, err := e.decode(ctx, writer, readers, offset, length, totalLength, prefer)
	if healRequired {
		return &errDecodeHealRequired{err}
	}

	return err
}

// Decode reads from readers, reconstructs data if needed and writes the data to the writer.
func (e Erasure) decode(ctx context.Context, writer io.Writer, readers []io.ReaderAt, offset, length, totalLength int64, prefer []bool) (bool, error) {
	if offset < 0 || length < 0 {
		logger.LogIf(ctx, errInvalidArgument)
		return false, errInvalidArgument
	}
	if offset+length > totalLength {
		logger.LogIf(ctx, errInvalidArgument)
		return false, errInvalidArgument
	}
	if length == 0 {
		return false, nil
	}

	reader := newParallelReader(readers, e, offset, totalLength)
	if len(prefer) == len(readers) {
		reader.preferReaders(prefer)
	}

	startBlock := offset / e.blockSize
	endBlock := (offset + length) / e.blockSize

	var healRequired bool
	var bytesWritten int64
	var bufs [][]byte
	for block := startBlock; block <= endBlock; block++ {
		var blockOffset, blockLength int64
		switch {
		case startBlock == endBlock:
			blockOffset = offset % e.blockSize
			blockLength = length
		case block == startBlock:
			blockOffset = offset % e.blockSize
			blockLength = e.blockSize - blockOffset
		case block == endBlock:
			blockOffset = 0
			blockLength = (offset + length) % e.blockSize
		default:
			blockOffset = 0
			blockLength = e.blockSize
		}
		if blockLength == 0 {
			break
		}
		var err error
		bufs, err = reader.Read(bufs)
		if err != nil {
			if errors.Is(err, errHealRequired) {
				// errHealRequired is only returned if there are be enough data for reconstruction.
				healRequired = true
			} else {
				return healRequired, err
			}
		}
		if err = e.DecodeDataBlocks(bufs); err != nil {
			logger.LogIf(ctx, err)
			return healRequired, err
		}
		n, err := writeDataBlocks(ctx, writer, bufs, e.dataBlocks, blockOffset, blockLength)
		if err != nil {
			return healRequired, err
		}
		bytesWritten += n
	}
	if bytesWritten != length {
		logger.LogIf(ctx, errLessData)
		return healRequired, errLessData
	}

	return healRequired, nil
}

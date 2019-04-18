/*
 * MinIO Cloud Storage, (C) 2016 MinIO, Inc.
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
	"sync"

	"github.com/minio/minio/cmd/logger"
)

// Reads in parallel from readers.
type parallelReader struct {
	readers       []io.ReaderAt
	dataBlocks    int
	offset        int64
	shardSize     int64
	shardFileSize int64
	buf           [][]byte
}

// newParallelReader returns parallelReader.
func newParallelReader(readers []io.ReaderAt, e Erasure, offset, totalLength int64) *parallelReader {
	return &parallelReader{
		readers,
		e.dataBlocks,
		(offset / e.blockSize) * e.ShardSize(),
		e.ShardSize(),
		e.ShardFileSize(totalLength),
		make([][]byte, len(readers)),
	}
}

// Returns if buf can be erasure decoded.
func (p *parallelReader) canDecode(buf [][]byte) bool {
	bufCount := 0
	for _, b := range buf {
		if b != nil {
			bufCount++
		}
	}
	return bufCount >= p.dataBlocks
}

// Read reads from readers in parallel. Returns p.dataBlocks number of bufs.
func (p *parallelReader) Read() ([][]byte, error) {
	newBuf := make([][]byte, len(p.readers))
	var newBufLK sync.RWMutex

	if p.offset+p.shardSize > p.shardFileSize {
		p.shardSize = p.shardFileSize - p.offset
	}

	readTriggerCh := make(chan bool, len(p.readers))
	for i := 0; i < p.dataBlocks; i++ {
		// Setup read triggers for p.dataBlocks number of reads so that it reads in parallel.
		readTriggerCh <- true
	}

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
			disk := p.readers[i]
			if disk == nil {
				// Since disk is nil, trigger another read.
				readTriggerCh <- true
				return
			}
			if p.buf[i] == nil {
				// Reading first time on this disk, hence the buffer needs to be allocated.
				// Subsequent reads will re-use this buffer.
				p.buf[i] = make([]byte, p.shardSize)
			}
			// For the last shard, the shardsize might be less than previous shard sizes.
			// Hence the following statement ensures that the buffer size is reset to the right size.
			p.buf[i] = p.buf[i][:p.shardSize]
			_, err := disk.ReadAt(p.buf[i], p.offset)
			if err != nil {
				p.readers[i] = nil
				// Since ReadAt returned error, trigger another read.
				readTriggerCh <- true
				return
			}
			newBufLK.Lock()
			newBuf[i] = p.buf[i]
			newBufLK.Unlock()
			// Since ReadAt returned success, there is no need to trigger another read.
			readTriggerCh <- false
		}(readerIndex)
		readerIndex++
	}
	wg.Wait()

	if p.canDecode(newBuf) {
		p.offset += p.shardSize
		return newBuf, nil
	}

	return nil, errXLReadQuorum
}

// Decode reads from readers, reconstructs data if needed and writes the data to the writer.
func (e Erasure) Decode(ctx context.Context, writer io.Writer, readers []io.ReaderAt, offset, length, totalLength int64) error {
	if offset < 0 || length < 0 {
		logger.LogIf(ctx, errInvalidArgument)
		return errInvalidArgument
	}
	if offset+length > totalLength {
		logger.LogIf(ctx, errInvalidArgument)
		return errInvalidArgument
	}
	if length == 0 {
		return nil
	}

	reader := newParallelReader(readers, e, offset, totalLength)

	startBlock := offset / e.blockSize
	endBlock := (offset + length) / e.blockSize

	var bytesWritten int64
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
		bufs, err := reader.Read()
		if err != nil {
			return err
		}
		if err = e.DecodeDataBlocks(bufs); err != nil {
			logger.LogIf(ctx, err)
			return err
		}
		n, err := writeDataBlocks(ctx, writer, bufs, e.dataBlocks, blockOffset, blockLength)
		if err != nil {
			return err
		}
		bytesWritten += n
	}
	if bytesWritten != length {
		logger.LogIf(ctx, errLessData)
		return errLessData
	}
	return nil
}

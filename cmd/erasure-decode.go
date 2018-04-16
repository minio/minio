/*
 * Minio Cloud Storage, (C) 2016 Minio, Inc.
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

// Reads in parallel from bitrotReaders.
type parallelReader struct {
	readers       []*bitrotReader
	dataBlocks    int
	offset        int64
	shardSize     int64
	shardFileSize int64
}

// newParallelReader returns parallelReader.
func newParallelReader(readers []*bitrotReader, dataBlocks int, offset int64, fileSize int64, blocksize int64) *parallelReader {
	shardSize := ceilFrac(blocksize, int64(dataBlocks))
	shardFileSize := getErasureShardFileSize(blocksize, fileSize, dataBlocks)
	return &parallelReader{
		readers,
		dataBlocks,
		(offset / blocksize) * shardSize,
		shardSize,
		shardFileSize,
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

// Read reads from bitrotReaders in parallel. Returns p.dataBlocks number of bufs.
func (p *parallelReader) Read() ([][]byte, error) {
	type errIdx struct {
		idx int
		buf []byte
		err error
	}

	errCh := make(chan errIdx)
	currReaderIndex := 0
	newBuf := make([][]byte, len(p.readers))

	if p.offset+p.shardSize > p.shardFileSize {
		p.shardSize = p.shardFileSize - p.offset
	}

	read := func(currReaderIndex int) {
		b, err := p.readers[currReaderIndex].ReadChunk(p.offset, p.shardSize)
		errCh <- errIdx{currReaderIndex, b, err}
	}

	readerCount := 0
	for _, r := range p.readers {
		if r != nil {
			readerCount++
		}
	}
	if readerCount < p.dataBlocks {
		return nil, errXLReadQuorum
	}

	readerCount = 0
	for i, r := range p.readers {
		if r == nil {
			continue
		}
		go read(i)
		readerCount++
		if readerCount == p.dataBlocks {
			currReaderIndex = i + 1
			break
		}
	}

	for errVal := range errCh {
		if errVal.err == nil {
			newBuf[errVal.idx] = errVal.buf
			if p.canDecode(newBuf) {
				p.offset += int64(p.shardSize)
				return newBuf, nil
			}
			continue
		}
		p.readers[errVal.idx] = nil
		for currReaderIndex < len(p.readers) {
			if p.readers[currReaderIndex] != nil {
				break
			}
			currReaderIndex++
		}

		if currReaderIndex == len(p.readers) {
			break
		}
		go read(currReaderIndex)
		currReaderIndex++
	}

	return nil, errXLReadQuorum
}

// Decode reads from readers, reconstructs data if needed and writes the data to the writer.
func (e Erasure) Decode(ctx context.Context, writer io.Writer, readers []*bitrotReader, offset, length, totalLength int64) error {
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

	reader := newParallelReader(readers, e.dataBlocks, offset, totalLength, e.blockSize)

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

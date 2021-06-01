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
	"fmt"
	"io"

	"github.com/klauspost/reedsolomon"
	"github.com/minio/minio/internal/logger"
)

// getDataBlockLen - get length of data blocks from encoded blocks.
func getDataBlockLen(enBlocks [][]byte, dataBlocks int) int {
	size := 0
	// Figure out the data block length.
	for _, block := range enBlocks[:dataBlocks] {
		size += len(block)
	}
	return size
}

// Writes all the data blocks from encoded blocks until requested
// outSize length. Provides a way to skip bytes until the offset.
func writeDataBlocks(ctx context.Context, dst io.Writer, enBlocks [][]byte, dataBlocks int, offset int64, length int64) (int64, error) {
	// Offset and out size cannot be negative.
	if offset < 0 || length < 0 {
		logger.LogIf(ctx, errUnexpected)
		return 0, errUnexpected
	}

	// Do we have enough blocks?
	if len(enBlocks) < dataBlocks {
		logger.LogIf(ctx, fmt.Errorf("diskBlocks(%d)/dataBlocks(%d) - %w", len(enBlocks), dataBlocks, reedsolomon.ErrTooFewShards))
		return 0, reedsolomon.ErrTooFewShards
	}

	// Do we have enough data?
	if int64(getDataBlockLen(enBlocks, dataBlocks)) < length {
		logger.LogIf(ctx, fmt.Errorf("getDataBlockLen(enBlocks, dataBlocks)(%d)/length(%d) - %w", getDataBlockLen(enBlocks, dataBlocks), length, reedsolomon.ErrShortData))
		return 0, reedsolomon.ErrShortData
	}

	// Counter to decrement total left to write.
	write := length

	// Counter to increment total written.
	var totalWritten int64

	// Write all data blocks to dst.
	for _, block := range enBlocks[:dataBlocks] {
		// Skip blocks until we have reached our offset.
		if offset >= int64(len(block)) {
			// Decrement offset.
			offset -= int64(len(block))
			continue
		} else {
			// Skip until offset.
			block = block[offset:]

			// Reset the offset for next iteration to read everything
			// from subsequent blocks.
			offset = 0
		}

		// We have written all the blocks, write the last remaining block.
		if write < int64(len(block)) {
			n, err := io.Copy(dst, bytes.NewReader(block[:write]))
			if err != nil {
				// The writer will be closed incase of range queries, which will emit ErrClosedPipe.
				// The reader pipe might be closed at ListObjects io.EOF ignore it.
				if err != io.ErrClosedPipe && err != io.EOF {
					logger.LogIf(ctx, err)
				}
				return 0, err
			}
			totalWritten += n
			break
		}

		// Copy the block.
		n, err := io.Copy(dst, bytes.NewReader(block))
		if err != nil {
			// The writer will be closed incase of range queries, which will emit ErrClosedPipe.
			// The reader pipe might be closed at ListObjects io.EOF ignore it.
			if err != io.ErrClosedPipe && err != io.EOF {
				logger.LogIf(ctx, err)
			}
			return 0, err
		}

		// Decrement output size.
		write -= n

		// Increment written.
		totalWritten += n
	}

	// Success.
	return totalWritten, nil
}

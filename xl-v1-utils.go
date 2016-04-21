package main

import (
	"errors"
	"strconv"
	"time"
)

// checkBlockSize return the size of a single block.
// The first non-zero size is returned,
// or 0 if all blocks are size 0.
func checkBlockSize(blocks [][]byte) int {
	for _, block := range blocks {
		if len(block) != 0 {
			return len(block)

		}

	}
	return 0

}

// calculate the blockSize based on input length and total number of
// data blocks.
func getEncodedBlockLen(inputLen, dataBlocks int) (curBlockSize int) {
	curBlockSize = (inputLen + dataBlocks - 1) / dataBlocks
	return

}

// Returns file size from the metadata.
func getFileSize(metadata map[string]string) (int64, error) {
	sizeStr, ok := metadata["file.size"]
	if !ok {
		return 0, errors.New("missing 'file.size' in meta data")

	}
	return strconv.ParseInt(sizeStr, 10, 64)

}

func getModTime(metadata map[string]string) (time.Time, error) {
	timeStr, ok := metadata["file.modTime"]
	if !ok {
		return time.Time{}, errors.New("missing 'file.modTime' in meta data")

	}
	return time.Parse(timeFormatAMZ, timeStr)

}

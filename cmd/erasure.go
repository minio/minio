/*
 * Minio Cloud Storage, (C) 2017 Minio, Inc.
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

	"github.com/klauspost/reedsolomon"
	"github.com/minio/minio/cmd/logger"
)

// OfflineDisk represents an unavailable disk.
var OfflineDisk StorageAPI // zero value is nil

// ErasureStorage - erasure encoding details.
type ErasureStorage struct {
	erasure                  reedsolomon.Encoder
	dataBlocks, parityBlocks int
	blockSize                int64
}

// NewErasureStorage creates a new ErasureStorage.
func NewErasureStorage(ctx context.Context, dataBlocks, parityBlocks int, blockSize int64) (s ErasureStorage, err error) {
	shardsize := int(ceilFrac(blockSize, int64(dataBlocks)))
	erasure, err := reedsolomon.New(dataBlocks, parityBlocks, reedsolomon.WithAutoGoroutines(shardsize))
	if err != nil {
		logger.LogIf(ctx, err)
		return s, err
	}
	s = ErasureStorage{
		erasure:      erasure,
		dataBlocks:   dataBlocks,
		parityBlocks: parityBlocks,
		blockSize:    blockSize,
	}
	return
}

// ErasureEncode encodes the given data and returns the erasure-coded data.
// It returns an error if the erasure coding failed.
func (s *ErasureStorage) ErasureEncode(ctx context.Context, data []byte) ([][]byte, error) {
	if len(data) == 0 {
		return make([][]byte, s.dataBlocks+s.parityBlocks), nil
	}
	encoded, err := s.erasure.Split(data)
	if err != nil {
		logger.LogIf(ctx, err)
		return nil, err
	}
	if err = s.erasure.Encode(encoded); err != nil {
		logger.LogIf(ctx, err)
		return nil, err
	}
	return encoded, nil
}

// ErasureDecodeDataBlocks decodes the given erasure-coded data.
// It only decodes the data blocks but does not verify them.
// It returns an error if the decoding failed.
func (s *ErasureStorage) ErasureDecodeDataBlocks(data [][]byte) error {
	needsReconstruction := false
	for _, b := range data[:s.dataBlocks] {
		if b == nil {
			needsReconstruction = true
			break
		}
	}
	if !needsReconstruction {
		return nil
	}
	if err := s.erasure.ReconstructData(data); err != nil {
		return err
	}
	return nil
}

// ErasureDecodeDataAndParityBlocks decodes the given erasure-coded data and verifies it.
// It returns an error if the decoding failed.
func (s *ErasureStorage) ErasureDecodeDataAndParityBlocks(ctx context.Context, data [][]byte) error {
	if err := s.erasure.Reconstruct(data); err != nil {
		logger.LogIf(ctx, err)
		return err
	}
	return nil
}

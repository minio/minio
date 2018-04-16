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

// Erasure - erasure encoding details.
type Erasure struct {
	encoder                  reedsolomon.Encoder
	dataBlocks, parityBlocks int
	blockSize                int64
}

// NewErasure creates a new ErasureStorage.
func NewErasure(ctx context.Context, dataBlocks, parityBlocks int, blockSize int64) (e Erasure, err error) {
	shardsize := int(ceilFrac(blockSize, int64(dataBlocks)))
	erasure, err := reedsolomon.New(dataBlocks, parityBlocks, reedsolomon.WithAutoGoroutines(shardsize))
	if err != nil {
		logger.LogIf(ctx, err)
		return e, err
	}
	e = Erasure{
		encoder:      erasure,
		dataBlocks:   dataBlocks,
		parityBlocks: parityBlocks,
		blockSize:    blockSize,
	}
	return
}

// EncodeData encodes the given data and returns the erasure-coded data.
// It returns an error if the erasure coding failed.
func (e *Erasure) EncodeData(ctx context.Context, data []byte) ([][]byte, error) {
	if len(data) == 0 {
		return make([][]byte, e.dataBlocks+e.parityBlocks), nil
	}
	encoded, err := e.encoder.Split(data)
	if err != nil {
		logger.LogIf(ctx, err)
		return nil, err
	}
	if err = e.encoder.Encode(encoded); err != nil {
		logger.LogIf(ctx, err)
		return nil, err
	}
	return encoded, nil
}

// DecodeDataBlocks decodes the given erasure-coded data.
// It only decodes the data blocks but does not verify them.
// It returns an error if the decoding failed.
func (e *Erasure) DecodeDataBlocks(data [][]byte) error {
	needsReconstruction := false
	for _, b := range data[:e.dataBlocks] {
		if b == nil {
			needsReconstruction = true
			break
		}
	}
	if !needsReconstruction {
		return nil
	}
	if err := e.encoder.ReconstructData(data); err != nil {
		return err
	}
	return nil
}

// DecodeDataAndParityBlocks decodes the given erasure-coded data and verifies it.
// It returns an error if the decoding failed.
func (e *Erasure) DecodeDataAndParityBlocks(ctx context.Context, data [][]byte) error {
	if err := e.encoder.Reconstruct(data); err != nil {
		logger.LogIf(ctx, err)
		return err
	}
	return nil
}

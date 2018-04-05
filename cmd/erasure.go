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
	"crypto/subtle"
	"hash"

	"github.com/klauspost/reedsolomon"
	"github.com/minio/minio/cmd/logger"
)

// OfflineDisk represents an unavailable disk.
var OfflineDisk StorageAPI // zero value is nil

// ErasureFileInfo contains information about an erasure file operation (create, read, heal).
type ErasureFileInfo struct {
	Size      int64
	Algorithm BitrotAlgorithm
	Checksums [][]byte
}

// ErasureStorage represents an array of disks.
// The disks contain erasure coded and bitrot-protected data.
type ErasureStorage struct {
	disks                    []StorageAPI
	erasure                  reedsolomon.Encoder
	dataBlocks, parityBlocks int
}

// NewErasureStorage creates a new ErasureStorage. The storage erasure codes and protects all data written to
// the disks.
func NewErasureStorage(ctx context.Context, disks []StorageAPI, dataBlocks, parityBlocks int, blockSize int64) (s ErasureStorage, err error) {
	shardsize := (int(blockSize) + dataBlocks - 1) / dataBlocks
	erasure, err := reedsolomon.New(dataBlocks, parityBlocks, reedsolomon.WithAutoGoroutines(shardsize))
	if err != nil {
		logger.LogIf(ctx, err)
		return s, err
	}
	s = ErasureStorage{
		disks:        make([]StorageAPI, len(disks)),
		erasure:      erasure,
		dataBlocks:   dataBlocks,
		parityBlocks: parityBlocks,
	}
	copy(s.disks, disks)
	return
}

// ErasureEncode encodes the given data and returns the erasure-coded data.
// It returns an error if the erasure coding failed.
func (s *ErasureStorage) ErasureEncode(ctx context.Context, data []byte) ([][]byte, error) {
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

// NewBitrotVerifier returns a new BitrotVerifier implementing the given algorithm.
func NewBitrotVerifier(algorithm BitrotAlgorithm, checksum []byte) *BitrotVerifier {
	return &BitrotVerifier{algorithm.New(), algorithm, checksum, false}
}

// BitrotVerifier can be used to verify protected data.
type BitrotVerifier struct {
	hash.Hash

	algorithm BitrotAlgorithm
	sum       []byte
	verified  bool
}

// Verify returns true iff the computed checksum of the verifier matches the the checksum provided when the verifier
// was created.
func (v *BitrotVerifier) Verify() bool {
	v.verified = true
	return subtle.ConstantTimeCompare(v.Sum(nil), v.sum) == 1
}

// IsVerified returns true iff Verify was called at least once.
func (v *BitrotVerifier) IsVerified() bool { return v.verified }

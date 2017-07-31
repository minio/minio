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
	"crypto/subtle"
	"io"

	"github.com/klauspost/reedsolomon"
	"github.com/minio/minio/pkg/bitrot"
)

// OfflineDisk represents an unavailable disk.
var OfflineDisk StorageAPI // zero value is nil

// ErasureFileInfo contains information about an erasure file operation (create, read, heal).
type ErasureFileInfo struct {
	Size      int64
	Algorithm bitrot.Algorithm
	Key       []byte
	Checksums [][]byte
}

// XLStorage represents an array of disks.
// The disks contain erasure coded and bitrot-protected data.
type XLStorage struct {
	disks                    []StorageAPI
	erasure                  reedsolomon.Encoder
	dataBlocks, parityBlocks int
	readQuorum, writeQuorum  int
}

// NewXLStorage creates a new XLStorage. The storage erasure codes and protects all data written to
// the disks.
func NewXLStorage(disks []StorageAPI, dataBlocks, parityBlocks int, readQuorum, writeQuorum int) (s XLStorage, err error) {
	erasure, err := reedsolomon.New(dataBlocks, parityBlocks)
	if err != nil {
		return s, Errorf("failed to create erasure coding: %v", err)
	}
	s = XLStorage{
		disks:        make([]StorageAPI, len(disks)),
		erasure:      erasure,
		readQuorum:   readQuorum,
		writeQuorum:  writeQuorum,
		dataBlocks:   dataBlocks,
		parityBlocks: parityBlocks,
	}
	copy(s.disks, disks)
	return
}

// AppendWriters creates one writer for every disk of the storage array.
// Every writer appends data to the corresponding storage disk.
func (s *XLStorage) AppendWriters(volume, path string) []io.Writer {
	writers := make([]io.Writer, len(s.disks))
	for i := range writers {
		writers[i] = StorageWriter(s.disks[i], volume, path)
	}
	return writers
}

// ErasureEncode encodes the given data and returns the erasure-coded data.
// It returns an error if the erasure coding failed.
func (s *XLStorage) ErasureEncode(data []byte) ([][]byte, error) {
	encoded, err := s.erasure.Split(data)
	if err != nil {
		return nil, Errorf("failed to split data: %v", err)
	}
	if err = s.erasure.Encode(encoded); err != nil {
		return nil, Errorf("failed to encode data: %v", err)
	}
	return encoded, nil
}

// ErasureDecodeDataBlocks decodes the given erasure-coded data.
// It only decodes the data blocks but does not verify them.
// It returns an error if the decoding failed.
func (s *XLStorage) ErasureDecodeDataBlocks(data [][]byte) error {
	if err := s.erasure.Reconstruct(data); err != nil {
		return Errorf("failed to reconstruct data: %v", err)
	}
	return nil
}

// ErasureDecodeDataAndParityBlocks decodes the given erasure-coded data and verifies it.
// It returns an error if the decoding failed.
func (s *XLStorage) ErasureDecodeDataAndParityBlocks(data [][]byte) error {
	if err := s.erasure.Reconstruct(data); err != nil {
		return Errorf("failed to reconstruct data: %v", err)
	}
	if ok, err := s.erasure.Verify(data); !ok || err != nil {
		if err != nil {
			return Errorf("failed to verify data: %v", err)
		}
		return Errorf("data verification failed: %v", "data is corrupted")
	}
	return nil
}

// NewBitrotVerifier creates a new instance of a bitrot verification mechanism. The verifier implements the given algorithm.
// It returns an error if the algorithm is not available or the key cannot be used by the algorithm.
func NewBitrotVerifier(algorithm bitrot.Algorithm, key, checksum []byte) (*BitrotVerifier, error) {
	if !algorithm.Available() {
		return nil, traceError(errBitrotHashAlgoInvalid)
	}
	h, err := algorithm.New(key, bitrot.Verify)
	if err != nil {
		return nil, traceError(err)
	}
	return &BitrotVerifier{h, algorithm, key, checksum, false}, nil
}

// BitrotVerifier can be used to verify protected data.
type BitrotVerifier struct {
	bitrot.Hash

	algorithm bitrot.Algorithm
	key       []byte
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

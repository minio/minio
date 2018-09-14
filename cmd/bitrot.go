/*
 * Minio Cloud Storage, (C) 2018 Minio, Inc.
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
	"hash"

	"github.com/minio/highwayhash"
	"github.com/minio/minio/cmd/logger"
	sha256 "github.com/minio/sha256-simd"
	"golang.org/x/crypto/blake2b"
)

// magic HH-256 key as HH-256 hash of the first 100 decimals of π as utf-8 string with a zero key.
var magicHighwayHash256Key = []byte("\x4b\xe7\x34\xfa\x8e\x23\x8a\xcd\x26\x3e\x83\xe6\xbb\x96\x85\x52\x04\x0f\x93\x5d\xa3\x9f\x44\x14\x97\xe0\x9d\x13\x22\xde\x36\xa0")

// BitrotAlgorithm specifies a algorithm used for bitrot protection.
type BitrotAlgorithm uint

const (
	// SHA256 represents the SHA-256 hash function
	SHA256 BitrotAlgorithm = 1 + iota
	// HighwayHash256 represents the HighwayHash-256 hash function
	HighwayHash256
	// BLAKE2b512 represents the BLAKE2b-512 hash function
	BLAKE2b512
)

// DefaultBitrotAlgorithm is the default algorithm used for bitrot protection.
const (
	DefaultBitrotAlgorithm = HighwayHash256
)

var bitrotAlgorithms = map[BitrotAlgorithm]string{
	SHA256:         "sha256",
	BLAKE2b512:     "blake2b",
	HighwayHash256: "highwayhash256",
}

// New returns a new hash.Hash calculating the given bitrot algorithm.
func (a BitrotAlgorithm) New() hash.Hash {
	switch a {
	case SHA256:
		return sha256.New()
	case BLAKE2b512:
		b2, _ := blake2b.New512(nil) // New512 never returns an error if the key is nil
		return b2
	case HighwayHash256:
		hh, _ := highwayhash.New(magicHighwayHash256Key) // New will never return error since key is 256 bit
		return hh
	default:
		logger.CriticalIf(context.Background(), errors.New("Unsupported bitrot algorithm"))
		return nil
	}
}

// Available reports whether the given algorithm is available.
func (a BitrotAlgorithm) Available() bool {
	_, ok := bitrotAlgorithms[a]
	return ok
}

// String returns the string identifier for a given bitrot algorithm.
// If the algorithm is not supported String panics.
func (a BitrotAlgorithm) String() string {
	name, ok := bitrotAlgorithms[a]
	if !ok {
		logger.CriticalIf(context.Background(), errors.New("Unsupported bitrot algorithm"))
	}
	return name
}

// NewBitrotVerifier returns a new BitrotVerifier implementing the given algorithm.
func NewBitrotVerifier(algorithm BitrotAlgorithm, checksum []byte) *BitrotVerifier {
	return &BitrotVerifier{algorithm, checksum}
}

// BitrotVerifier can be used to verify protected data.
type BitrotVerifier struct {
	algorithm BitrotAlgorithm
	sum       []byte
}

// BitrotAlgorithmFromString returns a bitrot algorithm from the given string representation.
// It returns 0 if the string representation does not match any supported algorithm.
// The zero value of a bitrot algorithm is never supported.
func BitrotAlgorithmFromString(s string) (a BitrotAlgorithm) {
	for alg, name := range bitrotAlgorithms {
		if name == s {
			return alg
		}
	}
	return
}

// To read bit-rot verified data.
type bitrotReader struct {
	disk      StorageAPI
	volume    string
	filePath  string
	verifier  *BitrotVerifier // Holds the bit-rot info
	endOffset int64           // Affects the length of data requested in disk.ReadFile depending on Read()'s offset
	buf       []byte          // Holds bit-rot verified data
}

// newBitrotReader returns bitrotReader.
// Note that the buffer is allocated later in Read(). This is because we will know the buffer length only
// during the bitrotReader.Read(). Depending on when parallelReader fails-over, the buffer length can be different.
func newBitrotReader(disk StorageAPI, volume, filePath string, algo BitrotAlgorithm, endOffset int64, sum []byte) *bitrotReader {
	return &bitrotReader{
		disk:      disk,
		volume:    volume,
		filePath:  filePath,
		verifier:  &BitrotVerifier{algo, sum},
		endOffset: endOffset,
		buf:       nil,
	}
}

// ReadChunk returns requested data.
func (b *bitrotReader) ReadChunk(offset int64, length int64) ([]byte, error) {
	if b.buf == nil {
		b.buf = make([]byte, b.endOffset-offset)
		if _, err := b.disk.ReadFile(b.volume, b.filePath, offset, b.buf, b.verifier); err != nil {
			ctx := context.Background()
			logger.GetReqInfo(ctx).AppendTags("disk", b.disk.String())
			logger.LogIf(ctx, err)
			return nil, err
		}
	}
	if int64(len(b.buf)) < length {
		logger.LogIf(context.Background(), errLessData)
		return nil, errLessData
	}
	retBuf := b.buf[:length]
	b.buf = b.buf[length:]
	return retBuf, nil
}

// To calculate the bit-rot of the written data.
type bitrotWriter struct {
	disk     StorageAPI
	volume   string
	filePath string
	h        hash.Hash
}

// newBitrotWriter returns bitrotWriter.
func newBitrotWriter(disk StorageAPI, volume, filePath string, algo BitrotAlgorithm) *bitrotWriter {
	return &bitrotWriter{
		disk:     disk,
		volume:   volume,
		filePath: filePath,
		h:        algo.New(),
	}
}

// Append appends the data and while calculating the hash.
func (b *bitrotWriter) Append(buf []byte) error {
	n, err := b.h.Write(buf)
	if err != nil {
		return err
	}
	if n != len(buf) {
		logger.LogIf(context.Background(), errUnexpected)
		return errUnexpected
	}
	if err = b.disk.AppendFile(b.volume, b.filePath, buf); err != nil {
		logger.LogIf(context.Background(), err)
		return err
	}
	return nil
}

// Sum returns bit-rot sum.
func (b *bitrotWriter) Sum() []byte {
	return b.h.Sum(nil)
}

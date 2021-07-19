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
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"

	"github.com/minio/highwayhash"
	"github.com/minio/minio/internal/logger"
	"golang.org/x/crypto/blake2b"
)

// magic HH-256 key as HH-256 hash of the first 100 decimals of π as utf-8 string with a zero key.
var magicHighwayHash256Key = []byte("\x4b\xe7\x34\xfa\x8e\x23\x8a\xcd\x26\x3e\x83\xe6\xbb\x96\x85\x52\x04\x0f\x93\x5d\xa3\x9f\x44\x14\x97\xe0\x9d\x13\x22\xde\x36\xa0")

var bitrotAlgorithms = map[BitrotAlgorithm]string{
	SHA256:          "sha256",
	BLAKE2b512:      "blake2b",
	HighwayHash256:  "highwayhash256",
	HighwayHash256S: "highwayhash256S",
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
	case HighwayHash256S:
		hh, _ := highwayhash.New(magicHighwayHash256Key) // New will never return error since key is 256 bit
		return hh
	default:
		logger.CriticalIf(GlobalContext, errors.New("Unsupported bitrot algorithm"))
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
		logger.CriticalIf(GlobalContext, errors.New("Unsupported bitrot algorithm"))
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

func newBitrotWriter(disk StorageAPI, volume, filePath string, length int64, algo BitrotAlgorithm, shardSize int64) io.Writer {
	if algo == HighwayHash256S {
		return newStreamingBitrotWriter(disk, volume, filePath, length, algo, shardSize)
	}
	return newWholeBitrotWriter(disk, volume, filePath, algo, shardSize)
}

func newBitrotReader(disk StorageAPI, data []byte, bucket string, filePath string, tillOffset int64, algo BitrotAlgorithm, sum []byte, shardSize int64) io.ReaderAt {
	if algo == HighwayHash256S {
		return newStreamingBitrotReader(disk, data, bucket, filePath, tillOffset, algo, shardSize)
	}
	return newWholeBitrotReader(disk, bucket, filePath, algo, tillOffset, sum)
}

// Close all the readers.
func closeBitrotReaders(rs []io.ReaderAt) {
	for _, r := range rs {
		if br, ok := r.(io.Closer); ok {
			br.Close()
		}
	}
}

// Close all the writers.
func closeBitrotWriters(ws []io.Writer) {
	for _, w := range ws {
		if bw, ok := w.(io.Closer); ok {
			bw.Close()
		}
	}
}

// Returns hash sum for whole-bitrot, nil for streaming-bitrot.
func bitrotWriterSum(w io.Writer) []byte {
	if bw, ok := w.(*wholeBitrotWriter); ok {
		return bw.Sum(nil)
	}
	return nil
}

// Returns the size of the file with bitrot protection
func bitrotShardFileSize(size int64, shardSize int64, algo BitrotAlgorithm) int64 {
	if algo != HighwayHash256S {
		return size
	}
	return ceilFrac(size, shardSize)*int64(algo.New().Size()) + size
}

// bitrotVerify a single stream of data.
func bitrotVerify(r io.Reader, wantSize, partSize int64, algo BitrotAlgorithm, want []byte, shardSize int64) error {
	if algo != HighwayHash256S {
		h := algo.New()
		if n, err := io.Copy(h, r); err != nil || n != wantSize {
			// Premature failure in reading the object, file is corrupt.
			return errFileCorrupt
		}
		if !bytes.Equal(h.Sum(nil), want) {
			return errFileCorrupt
		}
		return nil
	}

	h := algo.New()
	hashBuf := make([]byte, h.Size())
	left := wantSize

	// Calculate the size of the bitrot file and compare
	// it with the actual file size.
	if left != bitrotShardFileSize(partSize, shardSize, algo) {
		return errFileCorrupt
	}

	bufp := xlPoolSmall.Get().(*[]byte)
	defer xlPoolSmall.Put(bufp)

	for left > 0 {
		// Read expected hash...
		h.Reset()
		n, err := io.ReadFull(r, hashBuf)
		if err != nil {
			// Read's failed for object with right size, file is corrupt.
			return err
		}
		// Subtract hash length..
		left -= int64(n)
		if left < shardSize {
			shardSize = left
		}

		read, err := io.CopyBuffer(h, io.LimitReader(r, shardSize), *bufp)
		if err != nil {
			// Read's failed for object with right size, at different offsets.
			return errFileCorrupt
		}

		left -= read
		if !bytes.Equal(h.Sum(nil), hashBuf[:n]) {
			return errFileCorrupt
		}
	}
	return nil
}

// bitrotSelfTest performs a self-test to ensure that bitrot
// algorithms compute correct checksums. If any algorithm
// produces an incorrect checksum it fails with a hard error.
//
// bitrotSelfTest tries to catch any issue in the bitrot implementation
// early instead of silently corrupting data.
func bitrotSelfTest() {
	var checksums = map[BitrotAlgorithm]string{
		SHA256:          "a7677ff19e0182e4d52e3a3db727804abc82a5818749336369552e54b838b004",
		BLAKE2b512:      "e519b7d84b1c3c917985f544773a35cf265dcab10948be3550320d156bab612124a5ae2ae5a8c73c0eea360f68b0e28136f26e858756dbfe7375a7389f26c669",
		HighwayHash256:  "39c0407ed3f01b18d22c85db4aeff11e060ca5f43131b0126731ca197cd42313",
		HighwayHash256S: "39c0407ed3f01b18d22c85db4aeff11e060ca5f43131b0126731ca197cd42313",
	}
	for algorithm := range bitrotAlgorithms {
		if !algorithm.Available() {
			continue
		}

		checksum, err := hex.DecodeString(checksums[algorithm])
		if err != nil {
			logger.Fatal(errSelfTestFailure, fmt.Sprintf("bitrot: failed to decode %v checksum %s for selftest: %v", algorithm, checksums[algorithm], err))
		}
		var (
			hash = algorithm.New()
			msg  = make([]byte, 0, hash.Size()*hash.BlockSize())
			sum  = make([]byte, 0, hash.Size())
		)
		for i := 0; i < hash.Size()*hash.BlockSize(); i += hash.Size() {
			hash.Write(msg)
			sum = hash.Sum(sum[:0])
			msg = append(msg, sum...)
			hash.Reset()
		}
		if !bytes.Equal(sum, checksum) {
			logger.Fatal(errSelfTestFailure, fmt.Sprintf("bitrot: %v selftest checksum mismatch: got %x - want %x", algorithm, sum, checksum))
		}
	}
}

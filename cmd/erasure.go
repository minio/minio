package cmd

import (
	"crypto/rand"
	"crypto/subtle"
	"io"

	"github.com/klauspost/reedsolomon"
	"github.com/minio/minio/pkg/bitrot"
)

// OfflineDisk represents an unavailable disk.
var OfflineDisk StorageAPI // zero value is nil

// MissingShard indicates a missing data/parity shard for erasure coding
var MissingShard []byte // zero value is nil

// ErasureFileInfo contains information about a erasure file operation (create, read, heal).
type ErasureFileInfo struct {
	Size      int64
	Algorithm bitrot.Algorithm
	Keys      [][]byte
	Checksums [][]byte
}

// XLStorage represents an array of disks.
// The disks contains erasure coded and bitrot-protected data.
type XLStorage []StorageAPI

// Clone creates a copy of storage.
func (s XLStorage) Clone() XLStorage {
	s0 := make(XLStorage, len(s))
	copy(s0, s)
	return s0
}

// ErasureEncode encodes the given data and returns the erasure-coded data.
// It returns an error if the erasure coding failed.
func (s XLStorage) ErasureEncode(data []byte) ([][]byte, error) {
	encoder, err := reedsolomon.New(len(s)/2, len(s)/2)
	if err != nil {
		return nil, Errorf("failed to create erasure coding: %v", err)
	}
	encoded, err := encoder.Split(data)
	if err != nil {
		return nil, Errorf("failed to split data: %v", err)
	}
	if err = encoder.Encode(encoded); err != nil {
		return nil, Errorf("failed to encode data: %v", err)
	}
	return encoded, nil
}

// ErasureDecode decodes the given erasure-coded data.
// It returns an error if the decoding failed.
func (s XLStorage) ErasureDecode(data [][]byte) error {
	decoder, err := reedsolomon.New(len(s)/2, len(s)/2)
	if err != nil {
		return Errorf("failed to create erasure coding: %v", err)
	}
	if err = decoder.Reconstruct(data); err != nil {
		return Errorf("failed to split data: %v", err)
	}
	if ok, err := decoder.Verify(data); !ok || err != nil {
		if err != nil {
			return Errorf("failed to encode data: %v", err)
		}
		return Errorf("failed to verify reconstructed data: %s", "data corrupted")
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

// NewBitrotProtector creates a new instance of a bitrot protection mechanism. The protector implements the given algorithm.
// It returns an error if the algorithm is not available. NewBitrotProtector will generate a new key if the algorithm requires
// one - so the given io.Reader should return random data.
func NewBitrotProtector(algorithm bitrot.Algorithm, random io.Reader) (key []byte, hasher bitrot.Hash, err error) {
	if !algorithm.Available() {
		return nil, nil, traceError(errBitrotHashAlgoInvalid)
	}
	if random == nil {
		random = rand.Reader
	}
	key, err = algorithm.GenerateKey(random)
	if err != nil {
		return nil, nil, traceError(err)
	}
	hasher, err = algorithm.New(key, bitrot.Protect)
	if err != nil {
		return nil, nil, traceError(err)
	}
	return
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

// IsVerified returns true iff Verify was called at least one.
func (v *BitrotVerifier) IsVerified() bool { return v.verified }

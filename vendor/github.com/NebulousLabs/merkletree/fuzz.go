// +build gofuzz

package merkletree

import (
	"bytes"
	"crypto/sha256"
)

// Fuzz is called by go-fuzz to look for inputs to BuildReaderProof that will
// not verify correctly.
func Fuzz(data []byte) int {
	// Use the first two bytes to determine the proof index.
	if len(data) < 2 {
		return -1
	}
	index := 256*uint64(data[0]) + uint64(data[1])
	data = data[2:]

	// Build a reader proof for index 'index' using the remaining data as input
	// to the reader. '64' is chosen as the only input size because that is the
	// size relevant to the Sia project.
	merkleRoot, proofSet, numLeaves, err := BuildReaderProof(bytes.NewReader(data), sha256.New(), 64, index)
	if err != nil {
		return 0
	}
	if !VerifyProof(sha256.New(), merkleRoot, proofSet, index, numLeaves) {
		panic("verification failed!")
	}

	// Output is more interesting when there is enough data to contain the
	// index.
	if uint64(len(data)) > 64*index {
		return 1
	}
	return 0
}

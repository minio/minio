package erasure

import (
	"bytes"
	"errors"
	"io"

	"github.com/minio-io/minio/pkg/storage/donut/erasure/erasure1"
)

// EncoderTechnique - encoder matrix type
type EncoderTechnique int

const (
	// Vandermonde matrix type
	Vandermonde EncoderTechnique = iota
	// Cauchy matrix type
	Cauchy
)

// DataHeader represents the structure serialized to gob.
type DataHeader struct {
	// object + block stored
	Key string
	// chunk index of encoded block
	ChunkIndex uint8
	// Original Length of the block output
	OriginalLength uint32
	// Data Blocks
	EncoderK uint8
	// Parity Blocks
	EncoderM uint8
	// Matrix Technique
	EncoderTechnique EncoderTechnique
}

// Write latest donut format
func Write(target io.Writer, key string, part uint8, length uint32, k, m uint8, technique EncoderTechnique, data io.Reader) error {
	var versionedTechnique erasure1.EncoderTechnique
	switch {
	case technique == Vandermonde:
		versionedTechnique = erasure1.Vandermonde
	case technique == Cauchy:
		versionedTechnique = erasure1.Cauchy
	default:
		errors.New("Unknown encoder technique")
	}
	return erasure1.Write(target, key, part, length, k, m, versionedTechnique, data)
}

// Read any donut format
func Read(input io.Reader) (DataHeader, io.Reader, error) {
	// when version2 is created, create a method in version2 that can transform version1 structure to version2
	actualHeader, err := erasure1.ReadHeader(input)
	if err != nil {
		return DataHeader{}, nil, err
	}

	header := DataHeader{}

	header.Key = actualHeader.Key
	header.ChunkIndex = actualHeader.ChunkIndex
	header.OriginalLength = actualHeader.OriginalLength
	header.EncoderK = actualHeader.EncoderK
	header.EncoderM = actualHeader.EncoderM

	switch {
	case actualHeader.EncoderTechnique == erasure1.Vandermonde:
		header.EncoderTechnique = Vandermonde
	case actualHeader.EncoderTechnique == erasure1.Cauchy:
		header.EncoderTechnique = Cauchy
	default:
		errors.New("Unknown encoder technique")
	}

	buffer := new(bytes.Buffer)

	if _, err = io.Copy(buffer, input); err != nil {
		return DataHeader{}, nil, err
	}

	return header, buffer, nil
}

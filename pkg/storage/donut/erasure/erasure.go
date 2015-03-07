package erasure

import (
	"errors"
	"io"

	"github.com/minio-io/minio/pkg/storage/donut/erasure/erasure1"
)

type EncoderTechnique int

const (
	// Vandermonde matrix type
	Vandermonde EncoderTechnique = iota
	// Cauchy matrix type
	Cauchy
)

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
func Read(reader io.Reader) (io.Reader, error) {
	// when version2 is created, create a method in version2 that can transform version1 structure to version2
	return nil, errors.New("Not Implemented")
}

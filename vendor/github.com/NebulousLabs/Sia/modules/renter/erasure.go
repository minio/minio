package renter

import (
	"io"

	"github.com/klauspost/reedsolomon"

	"github.com/NebulousLabs/Sia/modules"
)

// rsCode is a Reed-Solomon encoder/decoder. It implements the
// modules.ErasureCoder interface.
type rsCode struct {
	enc reedsolomon.Encoder

	numPieces  int
	dataPieces int
}

// NumPieces returns the number of pieces returned by Encode.
func (rs *rsCode) NumPieces() int { return rs.numPieces }

// MinPieces return the minimum number of pieces that must be present to
// recover the original data.
func (rs *rsCode) MinPieces() int { return rs.dataPieces }

// Encode splits data into equal-length pieces, some containing the original
// data and some containing parity data.
func (rs *rsCode) Encode(data []byte) ([][]byte, error) {
	pieces, err := rs.enc.Split(data)
	if err != nil {
		return nil, err
	}
	// err should not be possible if Encode is called on the result of Split,
	// but no harm in checking anyway.
	err = rs.enc.Encode(pieces)
	if err != nil {
		return nil, err
	}
	return pieces, nil
}

// Recover recovers the original data from pieces (including parity) and
// writes it to w. pieces should be identical to the slice returned by
// Encode (length and order must be preserved), but with missing elements
// set to nil.
func (rs *rsCode) Recover(pieces [][]byte, n uint64, w io.Writer) error {
	err := rs.enc.Reconstruct(pieces)
	if err != nil {
		return err
	}
	return rs.enc.Join(w, pieces, int(n))
}

// NewRSCode creates a new Reed-Solomon encoder/decoder using the supplied
// parameters.
func NewRSCode(nData, nParity int) (modules.ErasureCoder, error) {
	enc, err := reedsolomon.New(nData, nParity)
	if err != nil {
		return nil, err
	}
	return &rsCode{
		enc:        enc,
		numPieces:  nData + nParity,
		dataPieces: nData,
	}, nil
}

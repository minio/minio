package encoding

import (
	"encoding/binary"
	"io"
)

// EncInt64 encodes an int64 as a slice of 8 bytes.
func EncInt64(i int64) (b []byte) {
	b = make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(i))
	return
}

// DecInt64 decodes a slice of 8 bytes into an int64.
// If len(b) < 8, the slice is padded with zeros.
func DecInt64(b []byte) int64 {
	b2 := make([]byte, 8)
	copy(b2, b)
	return int64(binary.LittleEndian.Uint64(b2))
}

// EncUint64 encodes a uint64 as a slice of 8 bytes.
func EncUint64(i uint64) (b []byte) {
	b = make([]byte, 8)
	binary.LittleEndian.PutUint64(b, i)
	return
}

// DecUint64 decodes a slice of 8 bytes into a uint64.
// If len(b) < 8, the slice is padded with zeros.
func DecUint64(b []byte) uint64 {
	b2 := make([]byte, 8)
	copy(b2, b)
	return binary.LittleEndian.Uint64(b2)
}

// WriteUint64 writes u to w.
func WriteUint64(w io.Writer, u uint64) error {
	_, err := w.Write(EncUint64(u))
	return err
}

// WriteInt64 writes i to w.
func WriteInt(w io.Writer, i int) error {
	return WriteUint64(w, uint64(i))
}

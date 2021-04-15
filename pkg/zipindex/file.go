package zipindex

import (
	"encoding/binary"
	"errors"
	"io"

	"github.com/klauspost/compress/zstd"
	"github.com/tinylib/msgp/msgp"
)

//go:generate msgp -file $GOFILE -unexported

// File is a sparse representation of a File inside a zip file.
//msgp:tuple File
type File struct {
	Name               string
	CompressedSize64   uint64
	UncompressedSize64 uint64
	Offset             int64 // Offset where file data header starts.
	CRC32              uint32
	Method             uint16
}

type Files []File

const currentVerPlain = 1
const currentVerCompressed = 2

var zstdEnc, _ = zstd.NewWriter(nil, zstd.WithWindowSize(128<<10), zstd.WithEncoderConcurrency(2))
var zstdDec, _ = zstd.NewReader(nil, zstd.WithDecoderLowmem(true), zstd.WithDecoderConcurrency(2))

//msgp:tuple filesAsStructs
type filesAsStructs struct {
	Names   []string
	CSizes  []int64
	USizes  []int64
	Offsets []int64
	Methods []uint16
	Crcs    []byte
}

func (f Files) Serialize() ([]byte, error) {
	if true {
		payload, err := f.MarshalMsg(nil)
		if err != nil {
			return nil, err
		}
		res := make([]byte, 0, len(payload))
		if len(payload) < 200 {
			res = append(res, currentVerPlain)
			return append(res, payload...), nil
		}
		res = append(res, currentVerCompressed)
		return zstdEnc.EncodeAll(payload, res), nil
	}

	// TODO: Much more efficient on multiple files, but much more complex...
	x := filesAsStructs{
		Names:   make([]string, len(f)),
		CSizes:  make([]int64, len(f)),
		USizes:  make([]int64, len(f)),
		Offsets: make([]int64, len(f)),
		Methods: make([]uint16, len(f)),
		Crcs:    make([]byte, len(f)*4),
	}
	for i, file := range f {
		x.Names[i] = file.Name
		x.CSizes[i] = int64(file.CompressedSize64)
		x.USizes[i] = int64(file.UncompressedSize64) - x.CSizes[i]
		if i > 0 {
			// Try to predict offset
			file.Offset -= f[i-1].Offset + int64(f[i-1].UncompressedSize64) + fileHeaderLen + int64(len(f[i-1].Name)+dataDescriptorLen)
			file.Method ^= f[i-1].Method
		}
		x.Offsets[i] = file.Offset
		x.Methods[i] = file.Method
		binary.LittleEndian.PutUint32(x.Crcs[i*4:], file.CRC32)
	}
	payload, err := x.MarshalMsg(nil)
	if err != nil {
		return nil, err
	}
	res := make([]byte, 0, len(payload))
	res = append(res, currentVerCompressed)
	return zstdEnc.EncodeAll(payload, res), nil
}

func (f Files) Find(name string) *File {
	for _, file := range f {
		if file.Name == name {
			return &file
		}
	}
	return nil
}

func unpackPayload(b []byte) ([]byte, error) {
	if len(b) < 1 {
		return nil, io.ErrUnexpectedEOF
	}
	switch b[0] {
	case currentVerPlain:
		b = b[1:]
	case currentVerCompressed:
		decoded, err := zstdDec.DecodeAll(b[1:], nil)
		if err != nil {
			return nil, err
		}
		b = decoded
	default:
		return nil, errors.New("unknown version")
	}
	return b, nil
}

func DeserializeFiles(b []byte) (Files, error) {
	b, err := unpackPayload(b)
	if err != nil {
		return nil, err
	}
	var dst Files
	_, err = dst.UnmarshalMsg(b)
	return dst, err
}

func FindSerialized(b []byte, name string) (*File, error) {
	buf, err := unpackPayload(b)
	if err != nil {
		return nil, err
	}
	n, buf, err := msgp.ReadArrayHeaderBytes(buf)
	if err != nil {
		return nil, err
	}
	var f File
	for i := 0; i < int(n); i++ {
		buf, err = f.UnmarshalMsg(buf)
		if err != nil {
			return nil, err
		}
		if f.Name == name {
			return &f, nil
		}
	}
	return nil, io.EOF
}

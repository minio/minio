/*
 * MinIO Cloud Storage, (C) 2021 MinIO, Inc.
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

// Files is a collection of files.
type Files []File

const currentVerPlain = 1
const currentVerCompressed = 2
const currentVerCompressedStructs = 3

var zstdEnc, _ = zstd.NewWriter(nil, zstd.WithWindowSize(128<<10), zstd.WithEncoderConcurrency(2), zstd.WithEncoderLevel(zstd.SpeedBetterCompression))
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

// Serialize the files.
func (f Files) Serialize() ([]byte, error) {
	if len(f) < 10 {
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

	// Encode many files as struct of arrays...
	x := filesAsStructs{
		Names:   make([]string, len(f)),
		CSizes:  make([]int64, len(f)),
		USizes:  make([]int64, len(f)),
		Offsets: make([]int64, len(f)),
		Methods: make([]uint16, len(f)),
		Crcs:    make([]byte, len(f)*4),
	}
	for i, file := range f {
		x.CSizes[i] = int64(file.CompressedSize64)
		if i > 0 {
			// Try to predict offset from previous file..
			file.Offset -= f[i-1].Offset + int64(f[i-1].CompressedSize64) + fileHeaderLen + int64(len(f[i-1].Name)+dataDescriptorLen)
			// Only encode when method changes.
			file.Method ^= f[i-1].Method
			// Use previous size as base.
			x.CSizes[i] = int64(file.CompressedSize64) - int64(f[i-1].CompressedSize64)
		}
		x.Names[i] = file.Name
		// Uncompressed size is the size from the compressed.
		x.USizes[i] = int64(file.UncompressedSize64) - int64(f[i].CompressedSize64)
		x.Offsets[i] = file.Offset
		x.Methods[i] = file.Method
		binary.LittleEndian.PutUint32(x.Crcs[i*4:], file.CRC32)
	}
	payload, err := x.MarshalMsg(nil)
	if err != nil {
		return nil, err
	}
	res := make([]byte, 0, len(payload))
	res = append(res, currentVerCompressedStructs)
	return zstdEnc.EncodeAll(payload, res), nil
}

// Find the file with the provided name.
// Search is linear.
func (f Files) Find(name string) *File {
	for _, file := range f {
		if file.Name == name {
			return &file
		}
	}
	return nil
}

// unpackPayload unpacks and optionally decompresses the payload.
func unpackPayload(b []byte) ([]byte, bool, error) {
	if len(b) < 1 {
		return nil, false, io.ErrUnexpectedEOF
	}
	var out []byte
	switch b[0] {
	case currentVerPlain:
		out = b[1:]
	case currentVerCompressed, currentVerCompressedStructs:
		decoded, err := zstdDec.DecodeAll(b[1:], nil)
		if err != nil {
			return nil, false, err
		}
		out = decoded
	default:
		return nil, false, errors.New("unknown version")
	}
	return out, b[0] == currentVerCompressedStructs, nil
}

// DeserializeFiles will de-serialize the files.
func DeserializeFiles(b []byte) (Files, error) {
	b, structs, err := unpackPayload(b)
	if err != nil {
		return nil, err
	}
	if !structs {
		var dst Files
		_, err = dst.UnmarshalMsg(b)
		return dst, err
	}

	var dst filesAsStructs
	if _, err = dst.UnmarshalMsg(b); err != nil {
		return nil, err
	}
	files := make(Files, len(dst.Names))
	var cur File
	for i := range files {
		cur = File{
			Name:             dst.Names[i],
			CompressedSize64: uint64(dst.CSizes[i] + int64(cur.CompressedSize64)),
			CRC32:            binary.LittleEndian.Uint32(dst.Crcs[i*4:]),
			Method:           dst.Methods[i] ^ cur.Method,
		}
		cur.UncompressedSize64 = uint64(dst.USizes[i] + int64(cur.CompressedSize64))
		if i == 0 {
			cur.Offset = dst.Offsets[i]
		} else {
			cur.Offset = dst.Offsets[i] + files[i-1].Offset + int64(files[i-1].CompressedSize64) + fileHeaderLen + int64(len(files[i-1].Name)) + dataDescriptorLen
		}
		files[i] = cur

	}
	return files, err
}

// FindSerialized will locate a file by name and return it.
// Returns nil, io.EOF if not found.
func FindSerialized(b []byte, name string) (*File, error) {
	buf, structs, err := unpackPayload(b)
	if err != nil {
		return nil, err
	}
	if !structs {
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
	// Files are packed as an array of arrays...
	idx := -1
	var zb0001 uint32
	bts := buf
	zb0001, bts, err = msgp.ReadArrayHeaderBytes(bts)
	if err != nil {
		err = msgp.WrapError(err)
		return nil, err
	}
	if zb0001 != 6 {
		err = msgp.ArrayError{Wanted: 6, Got: zb0001}
		return nil, err
	}
	var zb0002 uint32
	zb0002, bts, err = msgp.ReadArrayHeaderBytes(bts)
	if err != nil {
		err = msgp.WrapError(err, "Names")
		return nil, err
	}
	for i := 0; i < int(zb0002); i++ {
		var got []byte
		got, bts, err = msgp.ReadStringZC(bts)
		if err != nil {
			err = msgp.WrapError(err, "Names")
			return nil, err
		}
		if string(got) == name {
			idx = i
			break
		}
	}
	if idx < 0 {
		return nil, io.EOF
	}

	var dst filesAsStructs
	if _, err = dst.UnmarshalMsg(buf); err != nil {
		return nil, err
	}
	var cur, prev File
	for i := range dst.Names {
		cur = File{
			Name:             dst.Names[i],
			CompressedSize64: uint64(dst.CSizes[i] + int64(cur.CompressedSize64)),
			CRC32:            binary.LittleEndian.Uint32(dst.Crcs[i*4:]),
			Method:           dst.Methods[i] ^ cur.Method,
		}
		cur.UncompressedSize64 = uint64(dst.USizes[i] + int64(cur.CompressedSize64))
		if i == 0 {
			cur.Offset = dst.Offsets[i]
		} else {
			cur.Offset = dst.Offsets[i] + prev.Offset + int64(prev.CompressedSize64) + fileHeaderLen + int64(len(prev.Name)) + dataDescriptorLen
		}
		prev = cur
		if i == idx {
			// We have what we want...
			break
		}
	}
	return &cur, nil
}

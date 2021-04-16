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

var zstdEnc, _ = zstd.NewWriter(nil, zstd.WithWindowSize(128<<10), zstd.WithEncoderConcurrency(2))
var zstdDec, _ = zstd.NewReader(nil, zstd.WithDecoderLowmem(true), zstd.WithDecoderConcurrency(2))

// Serialize the files.
func (f Files) Serialize() ([]byte, error) {
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

// DeserializeFiles will de-serialize the files.
func DeserializeFiles(b []byte) (Files, error) {
	b, err := unpackPayload(b)
	if err != nil {
		return nil, err
	}
	var dst Files
	_, err = dst.UnmarshalMsg(b)
	return dst, err
}

// FindSerialized will locate a file by name and return it.
// Returns nil, io.EOF if not found.
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

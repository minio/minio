// MinIO Cloud Storage, (C) 2021 MinIO, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package s3v4

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"strconv"
	"strings"
	"time"
)

// ErrInvalidSignature indicates that a S3 signature V4
// verification failed.
var ErrInvalidSignature = errors.New("s3v4: invalid signature")

// Chunk is an S3 streaming signature v4 chunk.
type Chunk struct {
	size      int
	signature []byte
	payload   []byte
}

// WriteTo writes the S3 chunk to the given io.Writer
func (c *Chunk) WriteTo(w io.Writer) (n int64, err error) {
	nn, err := io.WriteString(w, strconv.FormatUint(uint64(c.size), 16))
	n += int64(nn)
	if err != nil {
		return n, err
	}
	nn, err = io.WriteString(w, ";chunk-signature="+hex.EncodeToString(c.signature)+"\r\n")
	n += int64(nn)
	if err != nil {
		return n, err
	}
	nn, err = w.Write(c.payload)
	n += int64(nn)
	return n, err
}

// ReadFrom reads and parses an S3 chunk from the given io.Reader
func (c *Chunk) ReadFrom(r io.Reader) (int64, error) {
	br, ok := r.(byteReader)
	if !ok {
		br = singleByteReader{r}
	}
	return c.readFrom(br)
}

func (c *Chunk) readFrom(r byteReader) (n int64, err error) {
	const SizeLimit = 16 * 1 << 20
	var size int
	for {
		b, err := r.ReadByte()
		n++
		if err != nil {
			if err == io.EOF {
				return n, io.ErrUnexpectedEOF
			}
			return n, err
		}

		if b == ';' {
			break
		}

		switch {
		case b >= '0' && b <= '9':
			size = size<<4 | int(b-'0')
		case b >= 'a' && b <= 'f':
			size = size<<4 | int(b-('a'-10))
		case b >= 'A' && b <= 'F':
			size = size<<4 | int(b-('A'-10))
		default:
			return n, ErrInvalidSignature
		}

		if size > SizeLimit {
			return n, ErrInvalidSignature
		}
	}

	var binSignature [16 + 64 + 2]byte
	nn, err := io.ReadFull(r, binSignature[:])
	n += int64(nn)
	if err != nil {
		return n, err
	}
	if !bytes.HasPrefix(binSignature[:], []byte("chunk-signature=")) {
		return n, ErrInvalidSignature
	}
	if !bytes.HasSuffix(binSignature[:], []byte("\r\n")) {
		return n, ErrInvalidSignature
	}

	c.signature, err = hex.DecodeString(string(binSignature[16:80]))
	if err != nil {
		return n, err
	}
	c.size = size
	c.payload = c.payload[:0]

	if size == 0 {
		return n, io.EOF
	}
	if cap(c.payload) >= size {
		c.payload = c.payload[:size]
	} else {
		c.payload = make([]byte, size)
	}

	nn, err = io.ReadFull(r, c.payload)
	n += int64(nn)
	if err != nil {
		if err == io.EOF {
			return n, io.ErrUnexpectedEOF
		}
		return n, err
	}
	if nn != size {
		return n, ErrInvalidSignature
	}

	b, err := r.ReadByte()
	if err != nil {
		if err == io.EOF {
			return n, io.ErrUnexpectedEOF
		}
		return n, err
	}
	n++
	if b != '\r' {
		return n, ErrInvalidSignature
	}
	b, err = r.ReadByte()
	if err != nil {
		if err == io.EOF {
			return n, io.ErrUnexpectedEOF
		}
		return n, err
	}
	n++
	if b != '\n' {
		return n, ErrInvalidSignature
	}
	return n, nil
}

func (c *Chunk) verify(key []byte, date time.Time, scope string, prevSignature []byte) bool {
	const EmptySHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

	var s strings.Builder
	s.WriteString("AWS-HMAC-SHA256-PAYLOAD")
	s.WriteByte('\n')
	s.WriteString(date.Format("20060102T150405Z"))
	s.WriteByte('\n')
	s.WriteString(scope)
	s.WriteByte('\n')
	s.WriteString(hex.EncodeToString(prevSignature))
	s.WriteByte('\n')
	s.WriteString(EmptySHA256)
	s.WriteByte('\n')

	sum := sha256.Sum256(c.payload)
	s.WriteString(hex.EncodeToString(sum[:]))

	h := hmac.New(sha256.New, key)
	h.Write([]byte(s.String()))
	return hmac.Equal(h.Sum(nil), c.signature)
}

type byteReader interface {
	io.Reader
	io.ByteReader
}

type singleByteReader struct {
	io.Reader
}

func (r singleByteReader) Read(p []byte) (int, error) { return r.Reader.Read(p) }

func (r singleByteReader) ReadByte() (byte, error) {
	var b [1]byte
	_, err := io.ReadFull(r.Reader, b[:])
	return b[0], err
}

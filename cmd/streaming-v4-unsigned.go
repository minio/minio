// Copyright (c) 2015-2023 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package cmd

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// newUnsignedV4ChunkedReader returns a new s3UnsignedChunkedReader that translates the data read from r
// out of HTTP "chunked" format before returning it.
// The s3ChunkedReader returns io.EOF when the final 0-length chunk is read.
func newUnsignedV4ChunkedReader(req *http.Request, trailer bool, signature bool) (io.ReadCloser, APIErrorCode) {
	if signature {
		if errCode := doesSignatureMatch(unsignedPayloadTrailer, req, globalSite.Region(), serviceS3); errCode != ErrNone {
			return nil, errCode
		}
	}
	if trailer {
		// Discard anything unsigned.
		req.Trailer = make(http.Header)
		trailers := req.Header.Values(awsTrailerHeader)
		for _, key := range trailers {
			req.Trailer.Add(key, "")
		}
	} else {
		req.Trailer = nil
	}
	return &s3UnsignedChunkedReader{
		trailers: req.Trailer,
		reader:   bufio.NewReader(req.Body),
		buffer:   make([]byte, 64*1024),
	}, ErrNone
}

// Represents the overall state that is required for decoding a
// AWS Signature V4 chunked reader.
type s3UnsignedChunkedReader struct {
	reader   *bufio.Reader
	trailers http.Header

	buffer []byte
	offset int
	err    error
	debug  bool
}

func (cr *s3UnsignedChunkedReader) Close() (err error) {
	return cr.err
}

// Read - implements `io.Reader`, which transparently decodes
// the incoming AWS Signature V4 streaming signature.
func (cr *s3UnsignedChunkedReader) Read(buf []byte) (n int, err error) {
	// First, if there is any unread data, copy it to the client
	// provided buffer.
	if cr.offset > 0 {
		n = copy(buf, cr.buffer[cr.offset:])
		if n == len(buf) {
			cr.offset += n
			return n, nil
		}
		cr.offset = 0
		buf = buf[n:]
	}
	// mustRead reads from input and compares against provided slice.
	mustRead := func(b ...byte) error {
		for _, want := range b {
			got, err := cr.reader.ReadByte()
			if err == io.EOF {
				return io.ErrUnexpectedEOF
			}
			if got != want {
				if cr.debug {
					fmt.Printf("mustread: want: %q got: %q\n", string(want), string(got))
				}
				return errMalformedEncoding
			}
			if err != nil {
				return err
			}
		}
		return nil
	}
	var size int
	for {
		b, err := cr.reader.ReadByte()
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		if err != nil {
			cr.err = err
			return n, cr.err
		}
		if b == '\r' { // \r\n denotes end of size.
			err := mustRead('\n')
			if err != nil {
				cr.err = err
				return n, cr.err
			}
			break
		}

		// Manually deserialize the size since AWS specified
		// the chunk size to be of variable width. In particular,
		// a size of 16 is encoded as `10` while a size of 64 KB
		// is `10000`.
		switch {
		case b >= '0' && b <= '9':
			size = size<<4 | int(b-'0')
		case b >= 'a' && b <= 'f':
			size = size<<4 | int(b-('a'-10))
		case b >= 'A' && b <= 'F':
			size = size<<4 | int(b-('A'-10))
		default:
			if cr.debug {
				fmt.Printf("err size: %v\n", string(b))
			}
			cr.err = errMalformedEncoding
			return n, cr.err
		}
		if size > maxChunkSize {
			cr.err = errChunkTooBig
			return n, cr.err
		}
	}

	if cap(cr.buffer) < size {
		cr.buffer = make([]byte, size)
	} else {
		cr.buffer = cr.buffer[:size]
	}

	// Now, we read the payload.
	_, err = io.ReadFull(cr.reader, cr.buffer)
	if err == io.EOF && size != 0 {
		err = io.ErrUnexpectedEOF
	}
	if err != nil && err != io.EOF {
		cr.err = err
		return n, cr.err
	}

	// If the chunk size is zero we return io.EOF. As specified by AWS,
	// only the last chunk is zero-sized.
	if len(cr.buffer) == 0 {
		if cr.debug {
			fmt.Println("EOF")
		}
		if cr.trailers != nil {
			err = cr.readTrailers()
			if cr.debug {
				fmt.Println("trailer returned:", err)
			}
			if err != nil {
				cr.err = err
				return 0, err
			}
		}
		cr.err = io.EOF
		return n, cr.err
	}
	// read final terminator.
	err = mustRead('\r', '\n')
	if err != nil && err != io.EOF {
		cr.err = err
		return n, cr.err
	}

	cr.offset = copy(buf, cr.buffer)
	n += cr.offset
	return n, err
}

// readTrailers will read all trailers and populate cr.trailers with actual values.
func (cr *s3UnsignedChunkedReader) readTrailers() error {
	var valueBuffer bytes.Buffer
	// Read value
	for {
		v, err := cr.reader.ReadByte()
		if err != nil {
			if err == io.EOF {
				return io.ErrUnexpectedEOF
			}
		}
		if v != '\r' {
			valueBuffer.WriteByte(v)
			continue
		}
		// Must end with \r\n\r\n
		var tmp [3]byte
		_, err = io.ReadFull(cr.reader, tmp[:])
		if err != nil {
			if err == io.EOF {
				return io.ErrUnexpectedEOF
			}
		}
		if !bytes.Equal(tmp[:], []byte{'\n', '\r', '\n'}) {
			if cr.debug {
				fmt.Printf("got %q, want %q\n", string(tmp[:]), "\n\r\n")
			}
			return errMalformedEncoding
		}
		break
	}

	// Parse trailers.
	wantTrailers := make(map[string]struct{}, len(cr.trailers))
	for k := range cr.trailers {
		wantTrailers[strings.ToLower(k)] = struct{}{}
	}
	input := bufio.NewScanner(bytes.NewReader(valueBuffer.Bytes()))
	for input.Scan() {
		line := strings.TrimSpace(input.Text())
		if line == "" {
			continue
		}
		// Find first separator.
		idx := strings.IndexByte(line, trailerKVSeparator[0])
		if idx <= 0 || idx >= len(line) {
			if cr.debug {
				fmt.Printf("Could not find separator, got %q\n", line)
			}
			return errMalformedEncoding
		}
		key := strings.ToLower(line[:idx])
		value := line[idx+1:]
		if _, ok := wantTrailers[key]; !ok {
			if cr.debug {
				fmt.Printf("Unknown key %q - expected on of %v\n", key, cr.trailers)
			}
			return errMalformedEncoding
		}
		cr.trailers.Set(key, value)
		delete(wantTrailers, key)
	}

	// Check if we got all we want.
	if len(wantTrailers) > 0 {
		return io.ErrUnexpectedEOF
	}
	return nil
}

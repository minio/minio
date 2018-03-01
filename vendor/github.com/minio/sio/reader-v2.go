// Copyright (C) 2018 Minio Inc.
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

package sio

import (
	"io"
)

type encReaderV20 struct {
	authEncV20
	src io.Reader

	buffer   packageV20
	offset   int
	lastByte byte

	firstRead bool
}

// encryptReaderV20 returns an io.Reader wrapping the given io.Reader.
// The returned io.Reader encrypts everything it reads using DARE 2.0.
func encryptReaderV20(src io.Reader, config *Config) (*encReaderV20, error) {
	ae, err := newAuthEncV20(config)
	if err != nil {
		return nil, err
	}
	return &encReaderV20{
		authEncV20: ae,
		src:        src,
		buffer:     make(packageV20, maxPackageSize),
		firstRead:  true,
	}, nil
}

func (r *encReaderV20) Read(p []byte) (n int, err error) {
	if r.firstRead {
		r.firstRead = false
		_, err = io.ReadFull(r.src, r.buffer[headerSize:headerSize+1])
		if err != nil && err != io.EOF { // ErrUnexpectedEOF cannot happen b/c we read just one single byte
			return 0, err
		}
		if err == io.EOF {
			r.finalized = true
			return 0, io.EOF
		}
		r.lastByte = r.buffer[headerSize]
	}

	if r.offset > 0 { // write the buffered package to p
		remaining := r.buffer.Length() - r.offset
		if len(p) < remaining {
			r.offset += copy(p, r.buffer[r.offset:r.offset+len(p)])
			return len(p), nil
		}
		n = copy(p, r.buffer[r.offset:r.offset+remaining])
		p = p[remaining:]
		r.offset = 0
	}
	if r.finalized {
		return n, io.EOF
	}
	for len(p) >= maxPackageSize {
		r.buffer[headerSize] = r.lastByte
		nn, err := io.ReadFull(r.src, r.buffer[headerSize+1:headerSize+1+maxPayloadSize]) // try to read the max. payload
		if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
			return n, err // failed to read from src
		}
		if err == io.EOF || err == io.ErrUnexpectedEOF { // read less than 64KB -> final package
			r.SealFinal(p, r.buffer[headerSize:headerSize+1+nn])
			return n + headerSize + tagSize + 1 + nn, io.EOF
		}
		r.lastByte = r.buffer[headerSize+maxPayloadSize] // save last read byte for the next package
		r.Seal(p, r.buffer[headerSize:headerSize+maxPayloadSize])
		p = p[maxPackageSize:]
		n += maxPackageSize
	}
	if len(p) > 0 {
		r.buffer[headerSize] = r.lastByte
		nn, err := io.ReadFull(r.src, r.buffer[headerSize+1:headerSize+1+maxPayloadSize]) // try to read the max. payload
		if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
			return n, err // failed to read from src
		}
		if err == io.EOF || err == io.ErrUnexpectedEOF { // read less than 64KB -> final package
			r.SealFinal(r.buffer, r.buffer[headerSize:headerSize+1+nn])
			if len(p) > r.buffer.Length() {
				n += copy(p, r.buffer[:r.buffer.Length()])
				return n, io.EOF
			}
		} else {
			r.lastByte = r.buffer[headerSize+maxPayloadSize] // save last read byte for the next package
			r.Seal(r.buffer, r.buffer[headerSize:headerSize+maxPayloadSize])
		}
		r.offset = copy(p, r.buffer[:len(p)]) // len(p) < len(r.buffer) - otherwise we would be still in the for-loop
		n += r.offset
	}
	return n, nil
}

type decReaderV20 struct {
	authDecV20
	src io.Reader

	buffer packageV20
	offset int
}

// decryptReaderV20 returns an io.Reader wrapping the given io.Reader.
// The returned io.Reader decrypts everything it reads using DARE 2.0.
func decryptReaderV20(src io.Reader, config *Config) (*decReaderV20, error) {
	ad, err := newAuthDecV20(config)
	if err != nil {
		return nil, err
	}
	return &decReaderV20{
		authDecV20: ad,
		src:        src,
		buffer:     make(packageV20, maxPackageSize),
	}, nil
}

func (r *decReaderV20) Read(p []byte) (n int, err error) {
	if r.offset > 0 { // write the buffered plaintext to p
		remaining := len(r.buffer.Payload()) - r.offset
		if len(p) < remaining {
			n = copy(p, r.buffer.Payload()[r.offset:r.offset+len(p)])
			r.offset += n
			return n, nil
		}
		n = copy(p, r.buffer.Payload()[r.offset:])
		p = p[remaining:]
		r.offset = 0
	}
	for len(p) >= maxPayloadSize {
		nn, err := io.ReadFull(r.src, r.buffer)
		if err == io.EOF && !r.finalized {
			return n, errUnexpectedEOF // reached EOF but not seen final package yet
		}
		if err != nil && err != io.ErrUnexpectedEOF {
			return n, err // reading from src failed or reached EOF
		}
		if err = r.Open(p, r.buffer[:nn]); err != nil {
			return n, err // decryption failed
		}
		p = p[len(r.buffer.Payload()):]
		n += len(r.buffer.Payload())
	}
	if len(p) > 0 {
		nn, err := io.ReadFull(r.src, r.buffer)
		if err == io.EOF && !r.finalized {
			return n, errUnexpectedEOF // reached EOF but not seen final package yet
		}
		if err != nil && err != io.ErrUnexpectedEOF {
			return n, err // reading from src failed or reached EOF
		}
		if err = r.Open(r.buffer[headerSize:], r.buffer[:nn]); err != nil {
			return n, err // decryption failed
		}
		if payload := r.buffer.Payload(); len(p) < len(payload) {
			r.offset = copy(p, payload[:len(p)])
			n += r.offset
		} else {
			n += copy(p, payload)
		}
	}
	return n, nil
}

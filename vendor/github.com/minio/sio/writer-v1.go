// Copyright (C) 2017 Minio Inc.
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

import "io"

type decWriterV10 struct {
	authDecV10
	dst io.Writer

	buffer packageV10
	offset int
}

// decryptWriterV10 returns an io.WriteCloser wrapping the given io.Writer.
// The returned io.WriteCloser decrypts everything written to it using DARE 1.0
// and writes all decrypted plaintext to the wrapped io.Writer.
//
// The io.WriteCloser must be closed to finalize the decryption successfully.
func decryptWriterV10(dst io.Writer, config *Config) (*decWriterV10, error) {
	ad, err := newAuthDecV10(config)
	if err != nil {
		return nil, err
	}
	return &decWriterV10{
		authDecV10: ad,
		dst:        dst,
		buffer:     make(packageV10, maxPackageSize),
	}, nil
}

func (w *decWriterV10) Write(p []byte) (n int, err error) {
	if w.offset > 0 && w.offset < headerSize { // buffer the header -> special code b/c we don't know when to decrypt without header
		remaining := headerSize - w.offset
		if len(p) < remaining {
			n = copy(w.buffer[w.offset:], p)
			w.offset += n
			return
		}
		n = copy(w.buffer[w.offset:], p[:remaining])
		p = p[remaining:]
		w.offset += n
	}
	if w.offset >= headerSize { // buffer the ciphertext and tag -> here we know when to decrypt
		remaining := w.buffer.Length() - w.offset
		if len(p) < remaining {
			nn := copy(w.buffer[w.offset:], p)
			w.offset += nn
			return n + nn, err
		}
		n += copy(w.buffer[w.offset:], p[:remaining])
		if err = w.Open(w.buffer.Payload(), w.buffer[:w.buffer.Length()]); err != nil {
			return n, err
		}
		if err = flush(w.dst, w.buffer.Payload()); err != nil { // write to underlying io.Writer
			return n, err
		}
		p = p[remaining:]
		w.offset = 0
	}
	for len(p) > headerSize {
		packageLen := headerSize + tagSize + headerV10(p).Len()
		if len(p) < packageLen { // p contains not the full package -> cannot decrypt
			w.offset = copy(w.buffer[:], p)
			n += w.offset
			return n, err
		}
		if err = w.Open(w.buffer[headerSize:packageLen-tagSize], p[:packageLen]); err != nil {
			return n, err
		}
		if err = flush(w.dst, w.buffer[headerSize:packageLen-tagSize]); err != nil { // write to underlying io.Writer
			return n, err
		}
		p = p[packageLen:]
		n += packageLen
	}
	if len(p) > 0 {
		w.offset = copy(w.buffer[:], p)
		n += w.offset
	}
	return n, nil
}

func (w *decWriterV10) Close() error {
	if w.offset > 0 {
		if w.offset <= headerSize+tagSize {
			return errInvalidPayloadSize // the payload is always > 0
		}
		header := headerV10(w.buffer[:headerSize])
		if w.offset < headerSize+header.Len()+tagSize {
			return errInvalidPayloadSize // there is less data than specified by the header
		}
		if err := w.Open(w.buffer.Payload(), w.buffer[:w.buffer.Length()]); err != nil {
			return err
		}
		if err := flush(w.dst, w.buffer.Payload()); err != nil { // write to underlying io.Writer
			return err
		}
	}
	if dst, ok := w.dst.(io.Closer); ok {
		return dst.Close()
	}
	return nil
}

type encWriterV10 struct {
	authEncV10
	dst io.Writer

	buffer      packageV10
	offset      int
	payloadSize int
}

// encryptWriterV10 returns an io.WriteCloser wrapping the given io.Writer.
// The returned io.WriteCloser encrypts everything written to it using DARE 1.0
// and writes all encrypted ciphertext as well as the package header and tag
// to the wrapped io.Writer.
//
// The io.WriteCloser must be closed to finalize the encryption successfully.
func encryptWriterV10(dst io.Writer, config *Config) (*encWriterV10, error) {
	ae, err := newAuthEncV10(config)
	if err != nil {
		return nil, err
	}
	return &encWriterV10{
		authEncV10:  ae,
		dst:         dst,
		buffer:      make(packageV10, maxPackageSize),
		payloadSize: config.PayloadSize,
	}, nil
}

func (w *encWriterV10) Write(p []byte) (n int, err error) {
	if w.offset > 0 { // buffer the plaintext
		remaining := w.payloadSize - w.offset
		if len(p) < remaining {
			n = copy(w.buffer[headerSize+w.offset:], p)
			w.offset += n
			return
		}
		n = copy(w.buffer[headerSize+w.offset:], p[:remaining])
		w.Seal(w.buffer, w.buffer[headerSize:headerSize+w.payloadSize])
		if err = flush(w.dst, w.buffer[:w.buffer.Length()]); err != nil { // write to underlying io.Writer
			return n, err
		}
		p = p[remaining:]
		w.offset = 0
	}
	for len(p) >= w.payloadSize {
		w.Seal(w.buffer[:], p[:w.payloadSize])
		if err = flush(w.dst, w.buffer[:w.buffer.Length()]); err != nil { // write to underlying io.Writer
			return n, err
		}
		p = p[w.payloadSize:]
		n += w.payloadSize
	}
	if len(p) > 0 {
		w.offset = copy(w.buffer[headerSize:], p)
		n += w.offset
	}
	return
}

func (w *encWriterV10) Close() error {
	if w.offset > 0 {
		w.Seal(w.buffer[:], w.buffer[headerSize:headerSize+w.offset])
		if err := flush(w.dst, w.buffer[:w.buffer.Length()]); err != nil { // write to underlying io.Writer
			return err
		}
	}
	if dst, ok := w.dst.(io.Closer); ok {
		return dst.Close()
	}
	return nil
}

func flush(w io.Writer, p []byte) error {
	n, err := w.Write(p)
	if err != nil {
		return err
	}
	if n != len(p) { // not neccasary if the w follows the io.Writer doc *precisely*
		return io.ErrShortWrite
	}
	return nil
}

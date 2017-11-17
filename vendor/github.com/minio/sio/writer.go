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

import (
	"crypto/cipher"
	"io"
)

type decryptedWriter struct {
	dst    io.Writer
	config Config

	sequenceNumber uint32
	ciphers        []cipher.AEAD

	pack   [headerSize + payloadSize + tagSize]byte
	offset int
}

func (w *decryptedWriter) Write(p []byte) (n int, err error) {
	if w.offset > 0 && w.offset < headerSize {
		remaining := headerSize - w.offset
		if len(p) < remaining {
			n = copy(w.pack[w.offset:], p)
			w.offset += n
			return
		}
		n = copy(w.pack[w.offset:], p[:remaining])
		p = p[remaining:]
		w.offset += n
	}
	if w.offset >= headerSize {
		remaining := headerSize + header(w.pack[:]).Len() + tagSize - w.offset
		if len(p) < remaining {
			nn := copy(w.pack[w.offset:], p)
			w.offset += nn
			return n + nn, err
		}
		n += copy(w.pack[w.offset:], p[:remaining])
		if err = w.decrypt(w.pack[:]); err != nil {
			return n, err
		}
		p = p[remaining:]
		w.offset = 0
	}
	for len(p) > headerSize {
		header := header(p)
		if len(p) < headerSize+header.Len()+tagSize {
			w.offset = copy(w.pack[:], p)
			n += w.offset
			return
		}
		if err = w.decrypt(p); err != nil {
			return n, err
		}
		p = p[headerSize+header.Len()+tagSize:]
		n += headerSize + header.Len() + tagSize
	}
	w.offset = copy(w.pack[:], p)
	n += w.offset
	return
}

func (w *decryptedWriter) Close() error {
	if w.offset > 0 {
		if w.offset < headerSize {
			return errMissingHeader
		}
		if w.offset < headerSize+header(w.pack[:]).Len()+tagSize {
			return errPayloadTooShort
		}
		if err := w.decrypt(w.pack[:]); err != nil {
			return err
		}
	}
	if dst, ok := w.dst.(io.Closer); ok {
		return dst.Close()
	}
	return nil
}

func (w *decryptedWriter) decrypt(src []byte) error {
	header := header(src)
	if err := w.config.verifyHeader(header); err != nil {
		return err
	}
	if header.SequenceNumber() != w.sequenceNumber {
		return errPackageOutOfOrder
	}

	aeadCipher := w.ciphers[header.Cipher()]
	plaintext, err := aeadCipher.Open(w.pack[headerSize:headerSize], header[4:headerSize], src[headerSize:headerSize+header.Len()+tagSize], header[:4])
	if err != nil {
		return errTagMismatch
	}

	n, err := w.dst.Write(plaintext)
	if err != nil {
		return err
	}
	if n != len(plaintext) {
		return io.ErrShortWrite
	}

	w.sequenceNumber++
	return nil
}

type encryptedWriter struct {
	dst    io.Writer
	config Config

	nonce          [8]byte
	sequenceNumber uint32
	cipher         cipher.AEAD

	pack   [headerSize + payloadSize + tagSize]byte
	offset int
}

func (w *encryptedWriter) Write(p []byte) (n int, err error) {
	if w.offset > 0 {
		remaining := payloadSize - w.offset
		if len(p) < remaining {
			n = copy(w.pack[headerSize+w.offset:], p)
			w.offset += n
			return
		}
		n = copy(w.pack[headerSize+w.offset:], p[:remaining])
		if err = w.encrypt(w.pack[headerSize : headerSize+payloadSize]); err != nil {
			return
		}
		p = p[remaining:]
		w.offset = 0
	}
	for len(p) >= payloadSize {
		if err = w.encrypt(p[:payloadSize]); err != nil {
			return
		}
		p = p[payloadSize:]
		n += payloadSize
	}
	if len(p) > 0 {
		w.offset = copy(w.pack[headerSize:], p)
		n += w.offset
	}
	return
}

func (w *encryptedWriter) Close() error {
	if w.offset > 0 {
		return w.encrypt(w.pack[headerSize : headerSize+w.offset])
	}
	if dst, ok := w.dst.(io.Closer); ok {
		return dst.Close()
	}
	return nil
}

func (w *encryptedWriter) encrypt(src []byte) error {
	header := header(w.pack[:])
	header.SetVersion(w.config.MaxVersion)
	header.SetCipher(w.config.CipherSuites[0])
	header.SetLen(len(src))
	header.SetSequenceNumber(w.sequenceNumber)
	header.SetNonce(w.nonce)

	w.cipher.Seal(w.pack[headerSize:headerSize], header[4:headerSize], src, header[:4])

	n, err := w.dst.Write(w.pack[:headerSize+len(src)+tagSize])
	if err != nil {
		return err
	}
	if n != headerSize+len(src)+tagSize {
		return io.ErrShortWrite
	}

	w.sequenceNumber++
	return nil
}

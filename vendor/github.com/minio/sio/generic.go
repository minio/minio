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
	"bytes"
	"io"
)

type decWriter struct {
	config Config
	dst    io.Writer

	firstWrite bool
}

// decryptWriter returns an io.WriteCloser wrapping the given io.Writer.
// The returned io.WriteCloser detects whether the data written to it is
// encrypted using DARE 1.0 or 2.0 and decrypts it using the correct DARE
// version.
func decryptWriter(w io.Writer, config *Config) *decWriter {
	return &decWriter{
		config:     *config,
		dst:        w,
		firstWrite: true,
	}
}

func (w *decWriter) Write(p []byte) (n int, err error) {
	if w.firstWrite {
		if len(p) == 0 {
			return 0, nil
		}
		w.firstWrite = false
		switch p[0] {
		default:
			return 0, errUnsupportedVersion
		case Version10:
			w.dst, err = decryptWriterV10(w.dst, &w.config)
			if err != nil {
				return 0, err
			}
		case Version20:
			w.dst, err = decryptWriterV20(w.dst, &w.config)
			if err != nil {
				return 0, err
			}
		}
	}
	return w.dst.Write(p)
}

func (w *decWriter) Close() error {
	if closer, ok := w.dst.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}

type decReader struct {
	config Config
	src    io.Reader

	firstRead bool
}

// decryptReader returns an io.Reader wrapping the given io.Reader.
// The returned io.Reader detects whether the underlying io.Reader returns
// DARE 1.0 or 2.0 encrypted data and decrypts it using the correct DARE version.
func decryptReader(r io.Reader, config *Config) *decReader {
	return &decReader{
		config:    *config,
		src:       r,
		firstRead: true,
	}
}

func (r *decReader) Read(p []byte) (n int, err error) {
	if r.firstRead {
		if len(p) == 0 {
			return 0, nil
		}
		var version [1]byte
		if _, err = io.ReadFull(r.src, version[:]); err != nil {
			return 0, err
		}
		r.firstRead = false
		r.src = io.MultiReader(bytes.NewReader(version[:]), r.src)
		switch version[0] {
		default:
			return 0, errUnsupportedVersion
		case Version10:
			r.src, err = decryptReaderV10(r.src, &r.config)
			if err != nil {
				return 0, err
			}
		case Version20:
			r.src, err = decryptReaderV20(r.src, &r.config)
			if err != nil {
				return 0, err
			}
		}
	}
	return r.src.Read(p)
}

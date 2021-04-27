// Copyright 2009 The Go Authors. All rights reserved.
// Copyright 2012 The Gorilla Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE.ORIG file.

// Modified to be used with MinIO, uses jsoniter and
// supports tracing JSON-rpc calls.

package rpc

import (
	"io"
	"net/http"
	"strings"
	"unicode"

	"github.com/klauspost/compress/flate"
	"github.com/klauspost/compress/gzip"
)

// gzipWriter writes and closes the gzip writer.
type gzipWriter struct {
	w *gzip.Writer
}

func (gw *gzipWriter) Write(p []byte) (n int, err error) {
	defer gw.w.Close()
	return gw.w.Write(p)
}

// gzipEncoder implements the gzip compressed http encoder.
type gzipEncoder struct {
}

func (enc *gzipEncoder) Encode(w http.ResponseWriter) io.Writer {
	w.Header().Set("Content-Encoding", "gzip")
	return &gzipWriter{gzip.NewWriter(w)}
}

// flateWriter writes and closes the flate writer.
type flateWriter struct {
	w *flate.Writer
}

func (fw *flateWriter) Write(p []byte) (n int, err error) {
	defer fw.w.Close()
	return fw.w.Write(p)
}

// flateEncoder implements the flate compressed http encoder.
type flateEncoder struct {
}

func (enc *flateEncoder) Encode(w http.ResponseWriter) io.Writer {
	fw, err := flate.NewWriter(w, flate.DefaultCompression)
	if err != nil {
		return w
	}
	w.Header().Set("Content-Encoding", "deflate")
	return &flateWriter{fw}
}

// CompressionSelector generates the compressed http encoder.
type CompressionSelector struct {
}

// Select method selects the correct compression encoder based on http HEADER.
func (*CompressionSelector) Select(r *http.Request) Encoder {
	encHeader := r.Header.Get("Accept-Encoding")
	encTypes := strings.FieldsFunc(encHeader, func(r rune) bool {
		return unicode.IsSpace(r) || r == ','
	})

	for _, enc := range encTypes {
		switch enc {
		case "gzip":
			return &gzipEncoder{}
		case "deflate":
			return &flateEncoder{}
		}
	}

	return DefaultEncoder
}

package zcn

import (
	"errors"
	"io"
)

func newMinioReader(source io.Reader) *MinioReader {
	return &MinioReader{source}
}

type MinioReader struct {
	io.Reader
}

func (r *MinioReader) Read(p []byte) (n int, err error) {
	if n, err = io.ReadAtLeast(r.Reader, p, len(p)); err != nil {
		if errors.Is(err, io.ErrUnexpectedEOF) {
			return n, io.EOF
		}
	}
	return
}

package zcn

import (
	"errors"
	"io"
	"time"
)

func newMinioReader(source io.Reader) *MinioReader {
	return &MinioReader{source}
}

// MinioReader Reader that returns io.EOF for io.ErrUnexpectedEOF error
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

func getTimeOut(size uint64) time.Duration {
	switch {
	case size >= oneGB:
		return time.Minute * 30
	case size >= 500*oneMB:
		return time.Minute * 5
	case size >= hundredMB:
		return time.Minute * 3
	case size >= 50*oneMB:
		return time.Minute
	default:
		return time.Second * 30
	}
}

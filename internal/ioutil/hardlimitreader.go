package ioutil

import (
	"errors"
	"io"
)

var ErrOverread = errors.New("input provided more bytes than allowed")

// HardLimitReader returns a Reader that reads from r
// but returns an error if the source provides more data than allowed.
// This means the source *will* be overread unless EOF is returned prior.
// The underlying implementation is a *HardLimitedReader.
// This will ensure that at most n bytes are returned and EOF is reached.
func HardLimitReader(r io.Reader, n int64) io.Reader { return &HardLimitedReader{r, n} }

// A HardLimitedReader reads from R but limits the amount of
// data returned to just N bytes. Each call to Read
// updates N to reflect the new amount remaining.
// Read returns EOF when N <= 0 or when the underlying R returns EOF.
type HardLimitedReader struct {
	R io.Reader // underlying reader
	N int64     // max bytes remaining
}

func (l *HardLimitedReader) Read(p []byte) (n int, err error) {
	if l.N < 0 {
		return 0, ErrOverread
	}
	n, err = l.R.Read(p)
	l.N -= int64(n)
	if l.N < 0 {
		return 0, ErrOverread
	}
	return
}

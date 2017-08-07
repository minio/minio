package binarydist

import (
	"errors"
)

type seekBuffer struct {
	buf []byte
	pos int
}

func (b *seekBuffer) Write(p []byte) (n int, err error) {
	n = copy(b.buf[b.pos:], p)
	if n == len(p) {
		b.pos += n
		return n, nil
	}
	b.buf = append(b.buf, p[n:]...)
	b.pos += len(p)
	return len(p), nil
}

func (b *seekBuffer) Seek(offset int64, whence int) (ret int64, err error) {
	var abs int64
	switch whence {
	case 0:
		abs = offset
	case 1:
		abs = int64(b.pos) + offset
	case 2:
		abs = int64(len(b.buf)) + offset
	default:
		return 0, errors.New("binarydist: invalid whence")
	}
	if abs < 0 {
		return 0, errors.New("binarydist: negative position")
	}
	if abs >= 1<<31 {
		return 0, errors.New("binarydist: position out of range")
	}
	b.pos = int(abs)
	return abs, nil
}

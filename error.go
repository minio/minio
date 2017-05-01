package main

import (
	"os"
	"runtime"
)

func ErrorWithStack(err error) errorWithStack {
	buf := make([]byte, 1024)
	for {
		n := runtime.Stack(buf, false)
		if n < len(buf) {
			buf = buf[:n]
			break
		}
		buf = make([]byte, 2*len(buf))
	}

	return errorWithStack{
		error: err,
		buf:   buf,
	}
}

type errorWithStack struct {
	error
	buf []byte
}

func (s *errorWithStack) Stack() string {
	return string(s.buf)
}

func (s *errorWithStack) Print() {
	os.Stderr.Write(s.buf)
}

package jstream

import (
	"io"
)

const (
	chunk   = 4095 // ~4k
	maxUint = ^uint(0)
	maxInt  = int(maxUint >> 1)
)

type scanner struct {
	pos       int // position in reader
	ipos      int // internal buffer position
	ifill     int // internal buffer fill
	end       int
	buf       [chunk + 1]byte // internal buffer (with a lookback size of 1)
	nbuf      [chunk]byte     // next internal buffer
	fillReq   chan struct{}
	fillReady chan int
}

func newScanner(r io.Reader) *scanner {
	sr := &scanner{
		end:       maxInt,
		fillReq:   make(chan struct{}),
		fillReady: make(chan int),
	}

	go func() {
		var rpos int // total bytes read into buffer

		for _ = range sr.fillReq {
		scan:
			n, err := r.Read(sr.nbuf[:])

			if n == 0 {
				switch err {
				case io.EOF: // reader is exhausted
					sr.end = rpos
					close(sr.fillReady)
					return
				case nil: // no data and no error, retry fill
					goto scan
				default:
					panic(err)
				}
			}

			rpos += n
			sr.fillReady <- n
		}
	}()

	sr.fillReq <- struct{}{} // initial fill

	return sr
}

// remaining returns the number of unread bytes
// if EOF for the underlying reader has not yet been found,
// maximum possible integer value will be returned
func (s *scanner) remaining() int {
	if s.end == maxInt {
		return maxInt
	}
	return s.end - s.pos
}

// read byte at current position (without advancing)
func (s *scanner) cur() byte { return s.buf[s.ipos] }

// read next byte
func (s *scanner) next() byte {
	if s.pos >= s.end {
		return byte(0)
	}
	s.ipos++

	if s.ipos > s.ifill { // internal buffer is exhausted
		s.ifill = <-s.fillReady
		s.buf[0] = s.buf[len(s.buf)-1] // copy current last item to guarantee lookback
		copy(s.buf[1:], s.nbuf[:])     // copy contents of pre-filled next buffer
		s.ipos = 1                     // move to beginning of internal buffer

		// request next fill to be prepared
		if s.end == maxInt {
			s.fillReq <- struct{}{}
		}
	}

	s.pos++
	return s.buf[s.ipos]
}

// back undoes a previous call to next(), moving backward one byte in the internal buffer.
// as we only guarantee a lookback buffer size of one, any subsequent calls to back()
// before calling next() may panic
func (s *scanner) back() {
	if s.ipos <= 0 {
		panic("back buffer exhausted")
	}
	s.ipos--
	s.pos--
}

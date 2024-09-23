package jstream

import (
	"unicode/utf8"
)

type scratch struct {
	data []byte
	fill int
}

// reset scratch buffer
func (s *scratch) reset() { s.fill = 0 }

// bytes returns the written contents of scratch buffer
func (s *scratch) bytes() []byte { return s.data[0:s.fill] }

// grow scratch buffer
func (s *scratch) grow() {
	ndata := make([]byte, cap(s.data)*2)
	copy(ndata, s.data)
	s.data = ndata
}

// append single byte to scratch buffer
func (s *scratch) add(c byte) {
	if s.fill+1 >= cap(s.data) {
		s.grow()
	}

	s.data[s.fill] = c
	s.fill++
}

// append encoded rune to scratch buffer
func (s *scratch) addRune(r rune) int {
	if s.fill+utf8.UTFMax >= cap(s.data) {
		s.grow()
	}

	n := utf8.EncodeRune(s.data[s.fill:], r)
	s.fill += n
	return n
}

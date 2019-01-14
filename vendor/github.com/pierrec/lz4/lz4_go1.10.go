//+build go1.10

package lz4

import (
	"fmt"
	"strings"
)

func (h Header) String() string {
	var s strings.Builder

	s.WriteString(fmt.Sprintf("%T{", h))
	if h.BlockChecksum {
		s.WriteString("BlockChecksum: true ")
	}
	if h.NoChecksum {
		s.WriteString("NoChecksum: true ")
	}
	if bs := h.BlockMaxSize; bs != 0 && bs != 4<<20 {
		s.WriteString(fmt.Sprintf("BlockMaxSize: %d ", bs))
	}
	if l := h.CompressionLevel; l != 0 {
		s.WriteString(fmt.Sprintf("CompressionLevel: %d ", l))
	}
	s.WriteByte('}')

	return s.String()
}

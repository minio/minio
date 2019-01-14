// +build gofuzz

package lzo

import "bytes"

func Fuzz(data []byte) int {
	Decompress1X(bytes.NewBuffer(data), 0, 0)
	return 0
}

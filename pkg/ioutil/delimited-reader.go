/*
 * Minio Cloud Storage, (C) 2018 Minio, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ioutil

import (
	"bufio"
	"io"
)

var (
	nByte byte = 10 // the byte that corresponds to the '\n' rune.
	rByte byte = 13 // the byte that corresponds to the '\r' rune.
)

// DelimitedReader reduces the custom delimiter to `\n`.
type DelimitedReader struct {
	r           *bufio.Reader
	delimiter   []rune // Select can have upto 2 characters as delimiter.
	assignEmpty bool   // Decides whether the next read byte should be discarded.
}

// NewDelimitedReader detects the custom delimiter and replaces with `\n`.
func NewDelimitedReader(r io.Reader, delimiter []rune) *DelimitedReader {
	return &DelimitedReader{r: bufio.NewReader(r), delimiter: delimiter, assignEmpty: false}
}

// Reads and replaces the custom delimiter with `\n`.
func (r *DelimitedReader) Read(p []byte) (n int, err error) {
	n, err = r.r.Read(p)
	if err != nil {
		return
	}
	for i, b := range p {
		if r.assignEmpty {
			swapAndNullify(p, i)
			r.assignEmpty = false
			continue
		}
		if b == rByte && rune(b) != r.delimiter[0] {
			// Replace the carriage returns with `\n`.
			// Mac styled csv will have `\r` as their record delimiter.
			p[i] = nByte
		} else if rune(b) == r.delimiter[0] { // Eg, `\r\n`,`ab`,`a` are valid delimiters
			if i+1 == len(p) && len(r.delimiter) > 1 {
				// If the first delimiter match falls on the boundary,
				// Peek the next byte and if it matches, discard it in the next byte read.
				if nextByte, nerr := r.r.Peek(1); nerr == nil {
					if rune(nextByte[0]) == r.delimiter[1] {
						p[i] = nByte
						// To Discard in the next read.
						r.assignEmpty = true
					}
				}
			} else if len(r.delimiter) > 1 && rune(p[i+1]) == r.delimiter[1] {
				// The second delimiter falls in the same chunk.
				p[i] = nByte
				r.assignEmpty = true
			} else if len(r.delimiter) == 1 {
				// Replace with `\n` incase of single charecter delimiter match.
				p[i] = nByte
			}
		}
	}
	return
}

// Occupy the first byte space and nullify the last byte.
func swapAndNullify(p []byte, n int) {
	for i := n; i < len(p)-1; i++ {
		p[i] = p[i+1]
	}
	p[len(p)-1] = 0
}

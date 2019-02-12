// Copyright 2017, Google
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package base

import (
	"bytes"
	"errors"
	"fmt"
)

func noEscape(c byte) bool {
	switch c {
	case '.', '_', '-', '/', '~', '!', '$', '\'', '(', ')', '*', ';', '=', ':', '@':
		return true
	}
	return false
}

func escape(s string) string {
	// cribbed from url.go, kinda
	b := &bytes.Buffer{}
	for i := 0; i < len(s); i++ {
		switch c := s[i]; {
		case c == '/':
			b.WriteByte(c)
		case 'a' <= c && c <= 'z' || 'A' <= c && c <= 'Z' || '0' <= c && c <= '9':
			b.WriteByte(c)
		case noEscape(c):
			b.WriteByte(c)
		default:
			fmt.Fprintf(b, "%%%X", c)
		}
	}
	return b.String()
}

func unescape(s string) (string, error) {
	b := &bytes.Buffer{}
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch c {
		case '/':
			b.WriteString("/")
		case '+':
			b.WriteString(" ")
		case '%':
			if len(s)-i < 3 {
				return "", errors.New("unescape: bad encoding")
			}
			b.WriteByte(unhex(s[i+1])<<4 | unhex(s[i+2]))
			i += 2
		default:
			b.WriteByte(c)
		}
	}
	return b.String(), nil
}

func unhex(c byte) byte {
	switch {
	case '0' <= c && c <= '9':
		return c - '0'
	case 'a' <= c && c <= 'f':
		return c - 'a' + 10
	case 'A' <= c && c <= 'F':
		return c - 'A' + 10
	}
	return 0
}

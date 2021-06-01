// Copyright (c) 2015-2021 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package kms

import (
	"bytes"
	"sort"
	"unicode/utf8"
)

// Context is a set of key-value pairs that
// are associated with a generate data encryption
// key (DEK).
//
// A KMS implementation may bind the context to the
// generated DEK such that the same context must be
// provided when decrypting an encrypted DEK.
type Context map[string]string

// MarshalText returns a canonical text representation of
// the Context.

// MarshalText sorts the context keys and writes the sorted
// key-value pairs as canonical JSON object. The sort order
// is based on the un-escaped keys. It never returns an error.
func (c Context) MarshalText() ([]byte, error) {
	if len(c) == 0 {
		return []byte{'{', '}'}, nil
	}

	// Pre-allocate a buffer - 128 bytes is an arbitrary
	// heuristic value that seems like a good starting size.
	var b = bytes.NewBuffer(make([]byte, 0, 128))
	if len(c) == 1 {
		for k, v := range c {
			b.WriteString(`{"`)
			escapeStringJSON(b, k)
			b.WriteString(`":"`)
			escapeStringJSON(b, v)
			b.WriteString(`"}`)
		}
		return b.Bytes(), nil
	}

	sortedKeys := make([]string, 0, len(c))
	for k := range c {
		sortedKeys = append(sortedKeys, k)
	}
	sort.Strings(sortedKeys)

	b.WriteByte('{')
	for i, k := range sortedKeys {
		b.WriteByte('"')
		escapeStringJSON(b, k)
		b.WriteString(`":"`)
		escapeStringJSON(b, c[k])
		b.WriteByte('"')
		if i < len(sortedKeys)-1 {
			b.WriteByte(',')
		}
	}
	b.WriteByte('}')
	return b.Bytes(), nil
}

// Adapted from Go stdlib.

var hexTable = "0123456789abcdef"

// escapeStringJSON will escape a string for JSON and write it to dst.
func escapeStringJSON(dst *bytes.Buffer, s string) {
	start := 0
	for i := 0; i < len(s); {
		if b := s[i]; b < utf8.RuneSelf {
			if htmlSafeSet[b] {
				i++
				continue
			}
			if start < i {
				dst.WriteString(s[start:i])
			}
			dst.WriteByte('\\')
			switch b {
			case '\\', '"':
				dst.WriteByte(b)
			case '\n':
				dst.WriteByte('n')
			case '\r':
				dst.WriteByte('r')
			case '\t':
				dst.WriteByte('t')
			default:
				// This encodes bytes < 0x20 except for \t, \n and \r.
				// If escapeHTML is set, it also escapes <, >, and &
				// because they can lead to security holes when
				// user-controlled strings are rendered into JSON
				// and served to some browsers.
				dst.WriteString(`u00`)
				dst.WriteByte(hexTable[b>>4])
				dst.WriteByte(hexTable[b&0xF])
			}
			i++
			start = i
			continue
		}
		c, size := utf8.DecodeRuneInString(s[i:])
		if c == utf8.RuneError && size == 1 {
			if start < i {
				dst.WriteString(s[start:i])
			}
			dst.WriteString(`\ufffd`)
			i += size
			start = i
			continue
		}
		// U+2028 is LINE SEPARATOR.
		// U+2029 is PARAGRAPH SEPARATOR.
		// They are both technically valid characters in JSON strings,
		// but don't work in JSONP, which has to be evaluated as JavaScript,
		// and can lead to security holes there. It is valid JSON to
		// escape them, so we do so unconditionally.
		// See http://timelessrepo.com/json-isnt-a-javascript-subset for discussion.
		if c == '\u2028' || c == '\u2029' {
			if start < i {
				dst.WriteString(s[start:i])
			}
			dst.WriteString(`\u202`)
			dst.WriteByte(hexTable[c&0xF])
			i += size
			start = i
			continue
		}
		i += size
	}
	if start < len(s) {
		dst.WriteString(s[start:])
	}
}

// htmlSafeSet holds the value true if the ASCII character with the given
// array position can be safely represented inside a JSON string, embedded
// inside of HTML <script> tags, without any additional escaping.
//
// All values are true except for the ASCII control characters (0-31), the
// double quote ("), the backslash character ("\"), HTML opening and closing
// tags ("<" and ">"), and the ampersand ("&").
var htmlSafeSet = [utf8.RuneSelf]bool{
	' ':      true,
	'!':      true,
	'"':      false,
	'#':      true,
	'$':      true,
	'%':      true,
	'&':      false,
	'\'':     true,
	'(':      true,
	')':      true,
	'*':      true,
	'+':      true,
	',':      true,
	'-':      true,
	'.':      true,
	'/':      true,
	'0':      true,
	'1':      true,
	'2':      true,
	'3':      true,
	'4':      true,
	'5':      true,
	'6':      true,
	'7':      true,
	'8':      true,
	'9':      true,
	':':      true,
	';':      true,
	'<':      false,
	'=':      true,
	'>':      false,
	'?':      true,
	'@':      true,
	'A':      true,
	'B':      true,
	'C':      true,
	'D':      true,
	'E':      true,
	'F':      true,
	'G':      true,
	'H':      true,
	'I':      true,
	'J':      true,
	'K':      true,
	'L':      true,
	'M':      true,
	'N':      true,
	'O':      true,
	'P':      true,
	'Q':      true,
	'R':      true,
	'S':      true,
	'T':      true,
	'U':      true,
	'V':      true,
	'W':      true,
	'X':      true,
	'Y':      true,
	'Z':      true,
	'[':      true,
	'\\':     false,
	']':      true,
	'^':      true,
	'_':      true,
	'`':      true,
	'a':      true,
	'b':      true,
	'c':      true,
	'd':      true,
	'e':      true,
	'f':      true,
	'g':      true,
	'h':      true,
	'i':      true,
	'j':      true,
	'k':      true,
	'l':      true,
	'm':      true,
	'n':      true,
	'o':      true,
	'p':      true,
	'q':      true,
	'r':      true,
	's':      true,
	't':      true,
	'u':      true,
	'v':      true,
	'w':      true,
	'x':      true,
	'y':      true,
	'z':      true,
	'{':      true,
	'|':      true,
	'}':      true,
	'~':      true,
	'\u007f': true,
}

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

package quick

import (
	"bufio"
	"bytes"
	"fmt"
	"io"

	"github.com/cheggaaa/pb"
)

const errorFmt = "%5d: %s  <<<<"

// FormatJSONSyntaxError generates a pretty printed json syntax error since
// golang doesn't provide an easy way to report the location of the error
func FormatJSONSyntaxError(data io.Reader, offset int64) (highlight string) {
	var readLine bytes.Buffer
	var errLine = 1
	var readBytes int64

	bio := bufio.NewReader(data)

	// termWidth is set to a default one to use when we are
	// not able to calculate terminal width via OS syscalls
	termWidth := 25

	// errorShift is the length of the minimum needed place for
	// error msg accessories, like <--, etc.. We calculate it
	// dynamically to avoid an eventual bug after modifying errorFmt
	errorShift := len(fmt.Sprintf(errorFmt, 1, ""))

	if width, err := pb.GetTerminalWidth(); err == nil {
		termWidth = width
	}

	for {
		b, err := bio.ReadByte()
		if err != nil {
			break
		}
		readBytes++
		if readBytes > offset {
			break
		}
		if b == '\n' {
			readLine.Reset()
			errLine++
			continue
		} else if b == '\t' {
			readLine.WriteByte(' ')
		} else if b == '\r' {
			break
		}
		readLine.WriteByte(b)
	}

	lineLen := readLine.Len()
	idx := lineLen - termWidth + errorShift
	if idx < 0 || idx > lineLen-1 {
		idx = 0
	}

	return fmt.Sprintf(errorFmt, errLine, readLine.String()[idx:])
}

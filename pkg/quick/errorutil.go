/*
 * Quick - Quick key value store for config files and persistent state files
 *
 * Minio Client (C) 2015 Minio, Inc.
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

package quick

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/cheggaaa/pb"
)

const errorFmt = "%5d: %s <--  "

// FormatJSONSyntaxError generates a pretty printed json syntax error since
// golang doesn't provide an easy way to report the location of the error
func FormatJSONSyntaxError(data io.Reader, sErr *json.SyntaxError) error {
	if sErr == nil {
		return nil
	}

	var readLine bytes.Buffer

	bio := bufio.NewReader(data)
	errLine := int64(1)
	readBytes := int64(0)

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
			if err != io.EOF {
				return err
			}
			break
		}
		readBytes++
		if readBytes > sErr.Offset {
			break
		}
		switch b {
		case '\n':
			readLine.Reset()
			errLine++
		case '\t':
			readLine.WriteByte(' ')
		case '\r':
			break
		default:
			readLine.WriteByte(b)
		}
	}

	lineLen := readLine.Len()
	idx := lineLen - termWidth + errorShift
	if idx < 0 || idx > lineLen-1 {
		idx = 0
	}

	errorStr := fmt.Sprintf("JSON syntax error at line %d, col %d : %s.\n",
		errLine, readLine.Len(), sErr)
	errorStr += fmt.Sprintf(errorFmt, errLine, readLine.String()[idx:])

	return errors.New(errorStr)
}

/*
 * Quick - Quick key value store for config files and persistent state files
 *
 * Quick (C) 2015 Minio, Inc.
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
	"errors"
	"fmt"
	"io"

	"github.com/cheggaaa/pb"
	"github.com/tidwall/gjson"
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

	return fmt.Sprintf(errorFmt, errLine, readLine.String()[idx:])
}

// doCheckDupJSONKeys recursively detects duplicate json keys
func doCheckDupJSONKeys(key, value gjson.Result) error {
	// Key occurrences map of the current scope to count
	// if there is any duplicated json key.
	keysOcc := make(map[string]int)

	// Holds the found error
	var checkErr error

	// Iterate over keys in the current json scope
	value.ForEach(func(k, v gjson.Result) bool {
		// If current key is not null, check if its
		// value contains some duplicated keys.
		if k.Type != gjson.Null {
			keysOcc[k.String()]++
			checkErr = doCheckDupJSONKeys(k, v)
		}
		return checkErr == nil
	})

	// Check found err
	if checkErr != nil {
		return errors.New(key.String() + " => " + checkErr.Error())
	}

	// Check for duplicated keys
	for k, v := range keysOcc {
		if v > 1 {
			return errors.New(key.String() + " => `" + k + "` entry is duplicated")
		}
	}

	return nil
}

// Check recursively if a key is duplicated in the same json scope
// e.g.:
//  `{ "key" : { "key" ..` is accepted
//  `{ "key" : { "subkey" : "val1", "subkey": "val2" ..` throws subkey duplicated error
func checkDupJSONKeys(json string) error {
	// Parse config with gjson library
	config := gjson.Parse(json)

	// Create a fake rootKey since root json doesn't seem to have representation
	// in gjson library.
	rootKey := gjson.Result{Type: gjson.String, Str: "config.json"}

	// Check if loaded json contains any duplicated keys
	return doCheckDupJSONKeys(rootKey, config)
}

// CheckDuplicateKeys - checks for duplicate entries in a JSON file
func CheckDuplicateKeys(json string) error {
	return checkDupJSONKeys(json)
}

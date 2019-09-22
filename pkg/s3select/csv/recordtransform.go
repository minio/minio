/*
 * MinIO Cloud Storage, (C) 2019 MinIO, Inc.
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

package csv

import (
	"bytes"
	"io"
)

// recordTransform will convert records to always have newline records.
type recordTransform struct {
	reader io.Reader
	// recordDelimiter can be up to 2 characters.
	recordDelimiter []byte
	oneByte         []byte
	useOneByte      bool
}

func (rr *recordTransform) Read(p []byte) (n int, err error) {
	if rr.useOneByte {
		p[0] = rr.oneByte[0]
		rr.useOneByte = false
		n, err = rr.reader.Read(p[1:])
		n++
	} else {
		n, err = rr.reader.Read(p)
	}

	if err != nil {
		return n, err
	}

	// Do nothing if record-delimiter is already newline.
	if string(rr.recordDelimiter) == "\n" {
		return n, nil
	}

	// Change record delimiters to newline.
	if len(rr.recordDelimiter) == 1 {
		for idx := 0; idx < len(p); {
			i := bytes.Index(p[idx:], rr.recordDelimiter)
			if i < 0 {
				break
			}
			idx += i
			p[idx] = '\n'
		}
		return n, nil
	}

	// 2 characters...
	for idx := 0; idx < len(p); {
		i := bytes.Index(p[idx:], rr.recordDelimiter)
		if i < 0 {
			break
		}
		idx += i

		p[idx] = '\n'
		p = append(p[:idx+1], p[idx+2:]...)
		n--
	}

	if p[n-1] != rr.recordDelimiter[0] {
		return n, nil
	}

	if _, err = rr.reader.Read(rr.oneByte); err != nil {
		return n, err
	}

	if rr.oneByte[0] == rr.recordDelimiter[1] {
		p[n-1] = '\n'
		return n, nil
	}

	rr.useOneByte = true
	return n, nil
}

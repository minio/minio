/*
 * Mini Object Storage, (C) 2015 Minio, Inc.
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

package donut

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"io"
)

type Header struct{}
type Donut struct {
	file io.Writer
}

func (donut Donut) Write(header Header, object io.Reader) error {
	var newObjectBuffer bytes.Buffer
	var headerBuffer bytes.Buffer

	// create gob header
	headerEncoder := gob.NewEncoder(&headerBuffer)
	err := headerEncoder.Encode(header)
	if err != nil {
		return err
	}

	// prefix consists of a version number and a length
	var headerPrefixBuffer bytes.Buffer
	// write version
	var version int
	version = 1
	err = binary.Write(&headerPrefixBuffer, binary.LittleEndian, version)
	if err != nil {
		return err
	}

	// write length
	var headerLength int
	headerLength = headerBuffer.Len()
	err = binary.Write(&headerPrefixBuffer, binary.LittleEndian, headerLength)
	if err != nil {
		return err
	}

	// write header prefix
	io.Copy(&newObjectBuffer, &headerPrefixBuffer)

	// write header
	io.Copy(&newObjectBuffer, &headerBuffer)

	// write header marker

	// TODO

	// write data
	_, err = io.Copy(&newObjectBuffer, object)
	if err != nil {
		return err
	}

	// write footer
	// TODO

	// write footer marker
	// TODO

	// write new object
	_, err = io.Copy(donut.file, &newObjectBuffer)
	if err != nil {
		return err
	}
	return nil
}

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
	"io"
)

/*
   ** DONUT v1 Spec **

   BlockStart      [4]byte             // Magic="MINI"
   VersionMajor    uint16
   VersionMinor    uint16
   VersionPatch    uint16
   VersionReserved uint16
   Reserved        uint64
   GobHeaderLen    uint32
   GobHeader       io.Reader           // matches length
   BlockData       [4]byte             // Magic="DATA"
   Data            io.Reader           // matches length
   BlockLen        uint64              // length to block start
   BlockEnd        [4]byte             // Magic="INIM"
*/

type DonutStructure struct {
	BlockStart      [4]byte // Magic="MINI"
	VersionMajor    uint16
	VersionMinor    uint16
	VersionPatch    uint16
	VersionReserved uint16
	Reserved        uint64
	GobHeaderLen    uint32
	GobHeader       GobHeader
	BlockData       [4]byte
	Data            io.Reader
	BlockLen        uint64
	BlockEnd        [4]byte
}

type DonutFooter struct {
	BlockLen uint64
	BlockEnd uint32 // Magic="INIM"
}

type Donut struct {
	file io.Writer
	// mutex
}

type GobHeader struct{}

func (donut *Donut) Write(gobHeader GobHeader, object io.Reader) error {
	// TODO mutex
	// Create bytes buffer representing the new object
	donutStructure := DonutStructure{
		BlockStart:      [4]byte{'M', 'I', 'N', 'I'},
		VersionMajor:    1,
		VersionMinor:    0,
		VersionPatch:    0,
		VersionReserved: 0,
		Reserved:        0,
		GobHeaderLen:    0,
		GobHeader:       gobHeader,
		BlockData:       [4]byte{'D', 'A', 'T', 'A'},
		Data:            object,
		BlockLen:        0,
		BlockEnd:        [4]byte{'I', 'N', 'I', 'M'},
	}
	if err := WriteStructure(donut.file, donutStructure); err != nil {
		return err
	}
	return nil
}

func WriteStructure(target io.Writer, donutStructure DonutStructure) error { return nil }

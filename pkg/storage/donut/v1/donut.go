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

package v1

import (
	"encoding/binary"
	"errors"
	"io"
)

/*

       DONUT v1 Spec
   **********************
   BlockStart      [4]byte             // Magic="MINI"=1229867341
   VersionMajor    uint16
   VersionMinor    uint16
   VersionPatch    uint16
   VersionReserved uint16
   Reserved        uint64
   GobHeaderLen    uint32
   GobHeader       io.Reader           // matches length
   BlockData       [4]byte             // Magic="DATA"=1096040772
   Data            io.Reader           // matches length
   BlockLen        uint64              // length to block start
   BlockEnd        [4]byte             // Magic="INIM"=1296649801

*/

var (
	MagicMINI = binary.LittleEndian.Uint32([]byte{'M', 'I', 'N', 'I'})
	MagicDATA = binary.LittleEndian.Uint32([]byte{'D', 'A', 'T', 'A'})
	MagicINIM = binary.LittleEndian.Uint32([]byte{'I', 'N', 'I', 'M'})
)

type DonutFormat struct {
	BlockStart      uint32 // Magic="MINI"=1229867341
	VersionMajor    uint16
	VersionMinor    uint16
	VersionPatch    uint16
	VersionReserved uint16
	Reserved        uint64
	GobHeaderLen    uint32
	GobHeader       GobHeader
	BlockData       uint32 // Magic="DATA"=1096040772
	Data            io.Reader
	BlockLen        uint64
	BlockEnd        uint32
}

type DonutFooter struct {
	BlockLen uint64
	BlockEnd uint32 // Magic="INIM"=1229867341
}

type Donut struct {
	file io.Writer
	// mutex
}

type GobHeader struct{}

func (donut *Donut) Write(gobHeader GobHeader, object io.Reader) error {
	// TODO mutex
	// Create bytes buffer representing the new object
	donutFormat := DonutFormat{
		BlockStart:      MagicMINI,
		VersionMajor:    1,
		VersionMinor:    0,
		VersionPatch:    0,
		VersionReserved: 0,
		Reserved:        0,
		GobHeaderLen:    0,
		GobHeader:       gobHeader,
		BlockData:       MagicDATA,
		Data:            object,
		BlockLen:        0,
		BlockEnd:        MagicINIM,
	}
	if err := donut.WriteFormat(donut.file, donutFormat); err != nil {
		return err
	}
	return nil
}

func (donut *Donut) WriteFormat(target io.Writer, donutFormat DonutFormat) error {
	if err := binary.Write(target, binary.LittleEndian, donutFormat.BlockStart); err != nil {
		return err
	}
	if err := binary.Write(target, binary.LittleEndian, donutFormat.VersionMajor); err != nil {
		return err
	}
	if err := binary.Write(target, binary.LittleEndian, donutFormat.VersionMinor); err != nil {
		return err
	}
	if err := binary.Write(target, binary.LittleEndian, donutFormat.VersionPatch); err != nil {
		return err
	}
	if err := binary.Write(target, binary.LittleEndian, donutFormat.VersionReserved); err != nil {
		return err
	}
	if err := binary.Write(target, binary.LittleEndian, donutFormat.Reserved); err != nil {
		return err
	}
	if err := binary.Write(target, binary.LittleEndian, donutFormat.GobHeaderLen); err != nil {
		return err
	}
	if err := binary.Write(target, binary.LittleEndian, donutFormat.GobHeader); err != nil {
		return err
	}
	if err := binary.Write(target, binary.LittleEndian, donutFormat.BlockData); err != nil {
		return err
	}
	if count, err := io.Copy(target, donutFormat.Data); uint64(count) != donutFormat.BlockLen || err != nil {
		if err == nil {
			return err
		}
		return errors.New("Copy failed, count incorrect.")
	}
	if err := binary.Write(target, binary.LittleEndian, donutFormat.BlockLen); err != nil {
		return err
	}
	if err := binary.Write(target, binary.LittleEndian, donutFormat.BlockEnd); err != nil {
		return err
	}

	return nil
}

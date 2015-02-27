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
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"io"
	"io/ioutil"
	"os"
	"sync"

	"github.com/minio-io/minio/pkg/storage/erasure"
	"github.com/minio-io/minio/pkg/utils/checksum/crc32c"
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
	GobHeader       []byte
	HeaderCrc32c    uint32
	BlockData       uint32 // Magic="DATA"=1096040772
	Data            io.Reader
	FooterCrc       uint32
	BlockLen        uint64
	BlockEnd        uint32
}

type DonutFooter struct {
	BlockLen uint64
	BlockEnd uint32 // Magic="INIM"=1229867341
}

type Donut struct {
	file  io.ReadWriteSeeker
	mutex *sync.RWMutex
}

type GobHeader struct {
	Blocks        []EncodedChunk
	Md5sum        []byte
	EncoderParams erasure.EncoderParams
}

type EncodedChunk struct {
	Crc    uint32
	Length int
	Offset int
}

func New(file io.ReadWriteSeeker) *Donut {
	donut := Donut{}
	donut.mutex = new(sync.RWMutex)
	donut.file = file
	return &donut
}

func (donut *Donut) WriteGob(gobHeader GobHeader) (bytes.Buffer, error) {
	var gobBuffer bytes.Buffer
	encoder := gob.NewEncoder(&gobBuffer)
	err := encoder.Encode(gobHeader)
	if err != nil {
		return bytes.Buffer{}, err
	}
	return gobBuffer, nil
}

func (donut *Donut) WriteEnd(target io.Writer, donutFormat DonutFormat) error {
	var tempBuffer bytes.Buffer
	if err := binary.Write(&tempBuffer, binary.LittleEndian, donutFormat.BlockLen); err != nil {
		return err
	}
	if err := binary.Write(&tempBuffer, binary.LittleEndian, donutFormat.BlockEnd); err != nil {
		return err
	}
	crc, err := crc32c.Crc32c(tempBuffer.Bytes())
	if err != nil {
		return err
	}
	if err := binary.Write(target, binary.LittleEndian, crc); err != nil {
		return err
	}
	if _, err := io.Copy(target, &tempBuffer); err != nil {
		return err
	}
	return nil
}

func (donut *Donut) WriteData(target io.Writer, donutFormat DonutFormat) error {
	var b bytes.Buffer
	if count, err := io.Copy(&b, donutFormat.Data); uint64(count) != donutFormat.BlockLen || err != nil {
		if err == nil {
			return binary.Write(target, binary.LittleEndian, b.Bytes())
		}
		return errors.New("Copy failed, count incorrect.")
	}
	return nil
}

func (donut *Donut) WriteBegin(target io.Writer, donutFormat DonutFormat) error {
	var headerBytes bytes.Buffer
	if err := binary.Write(&headerBytes, binary.LittleEndian, donutFormat.BlockStart); err != nil {
		return err
	}
	if err := binary.Write(&headerBytes, binary.LittleEndian, donutFormat.VersionMajor); err != nil {
		return err
	}
	if err := binary.Write(&headerBytes, binary.LittleEndian, donutFormat.VersionMinor); err != nil {
		return err
	}
	if err := binary.Write(&headerBytes, binary.LittleEndian, donutFormat.VersionPatch); err != nil {
		return err
	}
	if err := binary.Write(&headerBytes, binary.LittleEndian, donutFormat.VersionReserved); err != nil {
		return err
	}
	if err := binary.Write(&headerBytes, binary.LittleEndian, donutFormat.Reserved); err != nil {
		return err
	}
	if err := binary.Write(&headerBytes, binary.LittleEndian, donutFormat.GobHeaderLen); err != nil {
		return err
	}
	if err := binary.Write(&headerBytes, binary.LittleEndian, donutFormat.GobHeader); err != nil {
		return err
	}
	crc, err := crc32c.Crc32c(headerBytes.Bytes())
	if err != nil {
		return err
	}
	if err := binary.Write(&headerBytes, binary.LittleEndian, crc); err != nil {
		return err
	}
	if err := binary.Write(&headerBytes, binary.LittleEndian, donutFormat.BlockData); err != nil {
		return err
	}
	io.Copy(target, &headerBytes)
	return nil
}

func (donut *Donut) Write(gobHeader GobHeader, object io.Reader) error {
	donut.mutex.Lock()
	defer donut.mutex.Unlock()

	gobBytes, err := donut.WriteGob(gobHeader)
	if err != nil {
		return err
	}

	// Create bytes buffer representing the new object
	donutFormat := DonutFormat{
		BlockStart:      MagicMINI,
		VersionMajor:    1,
		VersionMinor:    0,
		VersionPatch:    0,
		VersionReserved: 0,
		Reserved:        0,
		GobHeaderLen:    uint32(gobBytes.Len()),
		GobHeader:       gobBytes.Bytes(),
		BlockData:       MagicDATA,
		Data:            object,
		BlockLen:        0,
		BlockEnd:        MagicINIM,
	}

	tempBuffer, err := ioutil.TempFile(os.TempDir(), "minio-staging")
	if err != nil {
		return err
	}
	defer os.Remove(tempBuffer.Name())
	// write header
	if err := donut.WriteBegin(tempBuffer, donutFormat); err != nil {
		return err
	}

	// write data
	if err := donut.WriteData(tempBuffer, donutFormat); err != nil {
		return err
	}

	// write footer crc
	if err := donut.WriteEnd(tempBuffer, donutFormat); err != nil {
		return err
	}

	// write footer
	donut.file.Seek(0, 2)
	tempBuffer.Seek(0, 0)
	io.Copy(donut.file, tempBuffer)

	return nil
}

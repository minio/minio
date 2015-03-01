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
	"errors"
	"io"

	"github.com/minio-io/minio/pkg/utils/checksum/crc32c"
	"github.com/minio-io/minio/pkg/utils/crypto/sha512"
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
   DataLen         uint64
   HeaderCrc32c    uint32
   BlockData       [4]byte             // Magic="DATA"=1096040772
   Data            io.Reader           // matches length
   HeaderCrc32c    uint32
   DataSha512      [64]byte
   BlockLen        uint64              // length of entire frame, inclusive of MINI and INIM
   BlockEnd        [4]byte             // Magic="INIM"=1296649801

*/

var (
	MagicMINI = binary.LittleEndian.Uint32([]byte{'M', 'I', 'N', 'I'})
	MagicDATA = binary.LittleEndian.Uint32([]byte{'D', 'A', 'T', 'A'})
	MagicINIM = binary.LittleEndian.Uint32([]byte{'I', 'N', 'I', 'M'})
)

type DonutFrameHeader struct {
	MagicMINI       uint32
	VersionMajor    uint16
	VersionMinor    uint16
	VersionPatch    uint16
	VersionReserved uint16
	Reserved        uint64
	DataLength      uint64
}
type Crc32c uint32
type Sha512 [sha512.Size]byte

type DonutFrameFooter struct {
	DataSha512   Sha512
	OffsetToMINI uint64
	MagicINIM    uint32
}

type Data bytes.Buffer

func Write(target io.Writer, reader io.Reader, length uint64) error {
	// write header
	header := DonutFrameHeader{
		MagicMINI:       MagicMINI,
		VersionMajor:    1,
		VersionMinor:    0,
		VersionPatch:    0,
		VersionReserved: 0,
		Reserved:        0,
		DataLength:      length,
	}
	var headerBytes bytes.Buffer
	binary.Write(&headerBytes, binary.LittleEndian, header)
	headerCrc := crc32c.Sum32(headerBytes.Bytes())

	binary.Write(&headerBytes, binary.LittleEndian, headerCrc)
	binary.Write(&headerBytes, binary.LittleEndian, MagicDATA)
	// write header
	headerLen, err := io.Copy(target, &headerBytes)
	if err != nil {
		return err
	}
	// write DATA
	// create sha512 tee
	sumReader, sumWriter := io.Pipe()
	defer sumWriter.Close()
	checksumChannel := make(chan checksumValue)
	go generateChecksum(sumReader, checksumChannel)
	teeReader := io.TeeReader(reader, sumWriter)
	dataLength, err := io.Copy(target, teeReader)
	if err != nil {
		return err
	}
	if uint64(dataLength) != length {
		return errors.New("Specified data length and amount written mismatched")
	}
	sumWriter.Close()
	dataChecksum := <-checksumChannel
	if dataChecksum.err != nil {
		return dataChecksum.err
	}
	// generate footer
	frameFooter := DonutFrameFooter{
		DataSha512:   dataChecksum.checksum,
		OffsetToMINI: length + uint64(headerLen) + uint64(80), /*footer size*/
		MagicINIM:    MagicINIM,
	}
	var frameFooterBytes bytes.Buffer
	binary.Write(&frameFooterBytes, binary.LittleEndian, frameFooter)
	// write footer crc
	footerChecksum := crc32c.Sum32(frameFooterBytes.Bytes())
	if err := binary.Write(target, binary.LittleEndian, footerChecksum); err != nil {
		return err
	}
	// write write footer
	_, err = io.Copy(target, &frameFooterBytes)
	if err != nil {
		return err
	}
	return nil
}

type checksumValue struct {
	checksum Sha512
	err      error
}

func generateChecksum(reader io.Reader, c chan<- checksumValue) {
	checksum, err := sha512.SumStream(reader)
	result := checksumValue{
		checksum: checksum,
		err:      err,
	}
	c <- result
}

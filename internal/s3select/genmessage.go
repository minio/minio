//go:build ignore
// +build ignore

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

package s3select

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
)

func genRecordsHeader() {
	buf := new(bytes.Buffer)

	buf.WriteByte(13)
	buf.WriteString(":message-type")
	buf.WriteByte(7)
	buf.Write([]byte{0, 5})
	buf.WriteString("event")

	buf.WriteByte(13)
	buf.WriteString(":content-type")
	buf.WriteByte(7)
	buf.Write([]byte{0, 24})
	buf.WriteString("application/octet-stream")

	buf.WriteByte(11)
	buf.WriteString(":event-type")
	buf.WriteByte(7)
	buf.Write([]byte{0, 7})
	buf.WriteString("Records")

	fmt.Println(buf.Bytes())
}

// Continuation Message
// ====================
// Header specification
// --------------------
// Continuation messages contain two headers, as follows:
// https://docs.aws.amazon.com/AmazonS3/latest/API/images/s3select-frame-diagram-cont.png
//
// Payload specification
// ---------------------
// Continuation messages have no payload.
func genContinuationMessage() {
	buf := new(bytes.Buffer)

	buf.WriteByte(13)
	buf.WriteString(":message-type")
	buf.WriteByte(7)
	buf.Write([]byte{0, 5})
	buf.WriteString("event")

	buf.WriteByte(11)
	buf.WriteString(":event-type")
	buf.WriteByte(7)
	buf.Write([]byte{0, 4})
	buf.WriteString("Cont")

	header := buf.Bytes()
	headerLength := len(header)
	payloadLength := 0
	totalLength := totalByteLength(headerLength, payloadLength)

	buf = new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, uint32(totalLength))
	binary.Write(buf, binary.BigEndian, uint32(headerLength))
	prelude := buf.Bytes()
	binary.Write(buf, binary.BigEndian, crc32.ChecksumIEEE(prelude))
	buf.Write(header)
	message := buf.Bytes()
	binary.Write(buf, binary.BigEndian, crc32.ChecksumIEEE(message))

	fmt.Println(buf.Bytes())
}

func genProgressHeader() {
	buf := new(bytes.Buffer)

	buf.WriteByte(13)
	buf.WriteString(":message-type")
	buf.WriteByte(7)
	buf.Write([]byte{0, 5})
	buf.WriteString("event")

	buf.WriteByte(13)
	buf.WriteString(":content-type")
	buf.WriteByte(7)
	buf.Write([]byte{0, 8})
	buf.WriteString("text/xml")

	buf.WriteByte(11)
	buf.WriteString(":event-type")
	buf.WriteByte(7)
	buf.Write([]byte{0, 8})
	buf.WriteString("Progress")

	fmt.Println(buf.Bytes())
}

func genStatsHeader() {
	buf := new(bytes.Buffer)

	buf.WriteByte(13)
	buf.WriteString(":message-type")
	buf.WriteByte(7)
	buf.Write([]byte{0, 5})
	buf.WriteString("event")

	buf.WriteByte(13)
	buf.WriteString(":content-type")
	buf.WriteByte(7)
	buf.Write([]byte{0, 8})
	buf.WriteString("text/xml")

	buf.WriteByte(11)
	buf.WriteString(":event-type")
	buf.WriteByte(7)
	buf.Write([]byte{0, 5})
	buf.WriteString("Stats")

	fmt.Println(buf.Bytes())
}

// End Message
// ===========
// Header specification
// --------------------
// End messages contain two headers, as follows:
// https://docs.aws.amazon.com/AmazonS3/latest/API/images/s3select-frame-diagram-end.png
//
// Payload specification
// ---------------------
// End messages have no payload.
func genEndMessage() {
	buf := new(bytes.Buffer)

	buf.WriteByte(13)
	buf.WriteString(":message-type")
	buf.WriteByte(7)
	buf.Write([]byte{0, 5})
	buf.WriteString("event")

	buf.WriteByte(11)
	buf.WriteString(":event-type")
	buf.WriteByte(7)
	buf.Write([]byte{0, 3})
	buf.WriteString("End")

	header := buf.Bytes()
	headerLength := len(header)
	payloadLength := 0
	totalLength := totalByteLength(headerLength, payloadLength)

	buf = new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, uint32(totalLength))
	binary.Write(buf, binary.BigEndian, uint32(headerLength))
	prelude := buf.Bytes()
	binary.Write(buf, binary.BigEndian, crc32.ChecksumIEEE(prelude))
	buf.Write(header)
	message := buf.Bytes()
	binary.Write(buf, binary.BigEndian, crc32.ChecksumIEEE(message))

	fmt.Println(buf.Bytes())
}

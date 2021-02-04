// +build ignore

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

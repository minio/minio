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
	"net/http"
	"strconv"
	"sync/atomic"
	"time"
)

// A message is in the format specified in
// https://docs.aws.amazon.com/AmazonS3/latest/API/images/s3select-frame-diagram-frame-overview.png
// hence the calculation is made accordingly.
func totalByteLength(headerLength, payloadLength int) int {
	return 4 + 4 + 4 + headerLength + payloadLength + 4
}

func genMessage(header, payload []byte) []byte {
	headerLength := len(header)
	payloadLength := len(payload)
	totalLength := totalByteLength(headerLength, payloadLength)

	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, uint32(totalLength))
	binary.Write(buf, binary.BigEndian, uint32(headerLength))
	prelude := buf.Bytes()
	binary.Write(buf, binary.BigEndian, crc32.ChecksumIEEE(prelude))
	buf.Write(header)
	if payload != nil {
		buf.Write(payload)
	}
	message := buf.Bytes()
	binary.Write(buf, binary.BigEndian, crc32.ChecksumIEEE(message))

	return buf.Bytes()
}

// Refer genRecordsHeader().
var recordsHeader = []byte{
	13, ':', 'm', 'e', 's', 's', 'a', 'g', 'e', '-', 't', 'y', 'p', 'e', 7, 0, 5, 'e', 'v', 'e', 'n', 't',
	13, ':', 'c', 'o', 'n', 't', 'e', 'n', 't', '-', 't', 'y', 'p', 'e', 7, 0, 24, 'a', 'p', 'p', 'l', 'i', 'c', 'a', 't', 'i', 'o', 'n', '/', 'o', 'c', 't', 'e', 't', '-', 's', 't', 'r', 'e', 'a', 'm',
	11, ':', 'e', 'v', 'e', 'n', 't', '-', 't', 'y', 'p', 'e', 7, 0, 7, 'R', 'e', 'c', 'o', 'r', 'd', 's',
}

const (
	// Chosen for compatibility with AWS JAVA SDK
	// It has a a buffer size of 128K:
	// https://github.com/aws/aws-sdk-java/blob/master/aws-java-sdk-s3/src/main/java/com/amazonaws/services/s3/internal/eventstreaming/MessageDecoder.java#L26
	// but we must make sure there is always space to add 256 bytes:
	// https://github.com/aws/aws-sdk-java/blob/master/aws-java-sdk-s3/src/main/java/com/amazonaws/services/s3/model/SelectObjectContentEventStream.java#L197
	maxRecordMessageLength = (128 << 10) - 256
)

var (
	bufLength = payloadLenForMsgLen(maxRecordMessageLength)
)

// newRecordsMessage - creates new Records Message which can contain a single record, partial records,
// or multiple records. Depending on the size of the result, a response can contain one or more of these messages.
//
// Header specification
// Records messages contain three headers, as follows:
// https://docs.aws.amazon.com/AmazonS3/latest/API/images/s3select-frame-diagram-record.png
//
// Payload specification
// Records message payloads can contain a single record, partial records, or multiple records.
func newRecordsMessage(payload []byte) []byte {
	return genMessage(recordsHeader, payload)
}

// payloadLenForMsgLen computes the length of the payload in a record
// message given the total length of the message.
func payloadLenForMsgLen(messageLength int) int {
	headerLength := len(recordsHeader)
	payloadLength := messageLength - 4 - 4 - 4 - headerLength - 4
	return payloadLength
}

// continuationMessage - S3 periodically sends this message to keep the TCP connection open.
// These messages appear in responses at random. The client must detect the message type and process accordingly.
//
// Header specification:
// Continuation messages contain two headers, as follows:
// https://docs.aws.amazon.com/AmazonS3/latest/API/images/s3select-frame-diagram-cont.png
//
// Payload specification:
// Continuation messages have no payload.
var continuationMessage = []byte{
	0, 0, 0, 57, // total byte-length.
	0, 0, 0, 41, // headers byte-length.
	139, 161, 157, 242, // prelude crc.
	13, ':', 'm', 'e', 's', 's', 'a', 'g', 'e', '-', 't', 'y', 'p', 'e', 7, 0, 5, 'e', 'v', 'e', 'n', 't', // headers.
	11, ':', 'e', 'v', 'e', 'n', 't', '-', 't', 'y', 'p', 'e', 7, 0, 4, 'C', 'o', 'n', 't', // headers.
	156, 134, 74, 13, // message crc.
}

// Refer genProgressHeader().
var progressHeader = []byte{
	13, ':', 'm', 'e', 's', 's', 'a', 'g', 'e', '-', 't', 'y', 'p', 'e', 7, 0, 5, 'e', 'v', 'e', 'n', 't',
	13, ':', 'c', 'o', 'n', 't', 'e', 'n', 't', '-', 't', 'y', 'p', 'e', 7, 0, 8, 't', 'e', 'x', 't', '/', 'x', 'm', 'l',
	11, ':', 'e', 'v', 'e', 'n', 't', '-', 't', 'y', 'p', 'e', 7, 0, 8, 'P', 'r', 'o', 'g', 'r', 'e', 's', 's',
}

// newProgressMessage - creates new Progress Message. S3 periodically sends this message, if requested.
// It contains information about the progress of a query that has started but has not yet completed.
//
// Header specification:
// Progress messages contain three headers, as follows:
// https://docs.aws.amazon.com/AmazonS3/latest/API/images/s3select-frame-diagram-progress.png
//
// Payload specification:
// Progress message payload is an XML document containing information about the progress of a request.
//   * BytesScanned => Number of bytes that have been processed before being uncompressed (if the file is compressed).
//   * BytesProcessed => Number of bytes that have been processed after being uncompressed (if the file is compressed).
//   * BytesReturned => Current number of bytes of records payload data returned by S3.
//
// For uncompressed files, BytesScanned and BytesProcessed are equal.
//
// Example:
//
// <?xml version="1.0" encoding="UTF-8"?>
// <Progress>
//   <BytesScanned>512</BytesScanned>
//   <BytesProcessed>1024</BytesProcessed>
//   <BytesReturned>1024</BytesReturned>
// </Progress>
//
func newProgressMessage(bytesScanned, bytesProcessed, bytesReturned int64) []byte {
	payload := []byte(`<?xml version="1.0" encoding="UTF-8"?><Progress><BytesScanned>` +
		strconv.FormatInt(bytesScanned, 10) + `</BytesScanned><BytesProcessed>` +
		strconv.FormatInt(bytesProcessed, 10) + `</BytesProcessed><BytesReturned>` +
		strconv.FormatInt(bytesReturned, 10) + `</BytesReturned></Stats>`)
	return genMessage(progressHeader, payload)
}

// Refer genStatsHeader().
var statsHeader = []byte{
	13, ':', 'm', 'e', 's', 's', 'a', 'g', 'e', '-', 't', 'y', 'p', 'e', 7, 0, 5, 'e', 'v', 'e', 'n', 't',
	13, ':', 'c', 'o', 'n', 't', 'e', 'n', 't', '-', 't', 'y', 'p', 'e', 7, 0, 8, 't', 'e', 'x', 't', '/', 'x', 'm', 'l',
	11, ':', 'e', 'v', 'e', 'n', 't', '-', 't', 'y', 'p', 'e', 7, 0, 5, 'S', 't', 'a', 't', 's',
}

// newStatsMessage - creates new Stats Message. S3 sends this message at the end of the request.
// It contains statistics about the query.
//
// Header specification:
// Stats messages contain three headers, as follows:
// https://docs.aws.amazon.com/AmazonS3/latest/API/images/s3select-frame-diagram-stats.png
//
// Payload specification:
// Stats message payload is an XML document containing information about a request's stats when processing is complete.
//   * BytesScanned => Number of bytes that have been processed before being uncompressed (if the file is compressed).
//   * BytesProcessed => Number of bytes that have been processed after being uncompressed (if the file is compressed).
//   * BytesReturned => Total number of bytes of records payload data returned by S3.
//
// For uncompressed files, BytesScanned and BytesProcessed are equal.
//
// Example:
//
// <?xml version="1.0" encoding="UTF-8"?>
// <Stats>
//      <BytesScanned>512</BytesScanned>
//      <BytesProcessed>1024</BytesProcessed>
//      <BytesReturned>1024</BytesReturned>
// </Stats>
func newStatsMessage(bytesScanned, bytesProcessed, bytesReturned int64) []byte {
	payload := []byte(`<?xml version="1.0" encoding="UTF-8"?><Stats><BytesScanned>` +
		strconv.FormatInt(bytesScanned, 10) + `</BytesScanned><BytesProcessed>` +
		strconv.FormatInt(bytesProcessed, 10) + `</BytesProcessed><BytesReturned>` +
		strconv.FormatInt(bytesReturned, 10) + `</BytesReturned></Stats>`)
	return genMessage(statsHeader, payload)
}

// endMessage - indicates that the request is complete, and no more messages will be sent.
// You should not assume that the request is complete until the client receives an End message.
//
// Header specification:
// End messages contain two headers, as follows:
// https://docs.aws.amazon.com/AmazonS3/latest/API/images/s3select-frame-diagram-end.png
//
// Payload specification:
// End messages have no payload.
var endMessage = []byte{
	0, 0, 0, 56, // total byte-length.
	0, 0, 0, 40, // headers byte-length.
	193, 198, 132, 212, // prelude crc.
	13, ':', 'm', 'e', 's', 's', 'a', 'g', 'e', '-', 't', 'y', 'p', 'e', 7, 0, 5, 'e', 'v', 'e', 'n', 't', // headers.
	11, ':', 'e', 'v', 'e', 'n', 't', '-', 't', 'y', 'p', 'e', 7, 0, 3, 'E', 'n', 'd', // headers.
	207, 151, 211, 146, // message crc.
}

// newErrorMessage - creates new Request Level Error Message. S3 sends this message if the request failed for any reason.
// It contains the error code and error message for the failure. If S3 sends a RequestLevelError message,
// it doesn't send an End message.
//
// Header specification:
// Request-level error messages contain three headers, as follows:
// https://docs.aws.amazon.com/AmazonS3/latest/API/images/s3select-frame-diagram-error.png
//
// Payload specification:
// Request-level error messages have no payload.
func newErrorMessage(errorCode, errorMessage []byte) []byte {
	buf := new(bytes.Buffer)

	buf.Write([]byte{13, ':', 'm', 'e', 's', 's', 'a', 'g', 'e', '-', 't', 'y', 'p', 'e', 7, 0, 5, 'e', 'r', 'r', 'o', 'r'})

	buf.Write([]byte{14, ':', 'e', 'r', 'r', 'o', 'r', '-', 'm', 'e', 's', 's', 'a', 'g', 'e', 7})
	binary.Write(buf, binary.BigEndian, uint16(len(errorMessage)))
	buf.Write(errorMessage)

	buf.Write([]byte{11, ':', 'e', 'r', 'r', 'o', 'r', '-', 'c', 'o', 'd', 'e', 7})
	binary.Write(buf, binary.BigEndian, uint16(len(errorCode)))
	buf.Write(errorCode)

	return genMessage(buf.Bytes(), nil)
}

// NewErrorMessage - creates new Request Level Error Message specified in
// https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectSELECTContent.html.
func NewErrorMessage(errorCode, errorMessage string) []byte {
	return newErrorMessage([]byte(errorCode), []byte(errorMessage))
}

type messageWriter struct {
	writer          http.ResponseWriter
	getProgressFunc func() (int64, int64)
	bytesReturned   int64

	payloadBuffer      []byte
	payloadBufferIndex int
	payloadCh          chan *bytes.Buffer

	finBytesScanned, finBytesProcessed int64

	errCh  chan []byte
	doneCh chan struct{}
}

func (writer *messageWriter) write(data []byte) bool {
	if _, err := writer.writer.Write(data); err != nil {
		return false
	}

	writer.writer.(http.Flusher).Flush()
	return true
}

func (writer *messageWriter) start() {
	keepAliveTicker := time.NewTicker(1 * time.Second)
	var progressTicker *time.Ticker
	var progressTickerC <-chan time.Time
	if writer.getProgressFunc != nil {
		progressTicker = time.NewTicker(1 * time.Minute)
		progressTickerC = progressTicker.C
	}
	recordStagingTicker := time.NewTicker(500 * time.Millisecond)

	// Exit conditions:
	//
	// 1. If a writer.write() returns false, select loop below exits and
	// closes `doneCh` to indicate to caller to also exit.
	//
	// 2. If caller (Evaluate()) has an error, it sends an error
	// message and waits for this go-routine to quit in
	// FinishWithError()
	//
	// 3. If caller is done, it waits for this go-routine to exit
	// in Finish()

	quitFlag := false
	for !quitFlag {
		select {
		case data := <-writer.errCh:
			quitFlag = true
			// Flush collected records before sending error message
			if !writer.flushRecords() {
				break
			}
			writer.write(data)

		case payload, ok := <-writer.payloadCh:
			if !ok {
				// payloadCh is closed by caller to
				// indicate finish with success
				quitFlag = true

				if !writer.flushRecords() {
					break
				}
				// Write Stats message, then End message
				bytesReturned := atomic.LoadInt64(&writer.bytesReturned)
				if !writer.write(newStatsMessage(writer.finBytesScanned, writer.finBytesProcessed, bytesReturned)) {
					break
				}
				writer.write(endMessage)
			} else {
				for payload.Len() > 0 {
					copiedLen := copy(writer.payloadBuffer[writer.payloadBufferIndex:], payload.Bytes())
					writer.payloadBufferIndex += copiedLen
					payload.Next(copiedLen)

					// If buffer is filled, flush it now!
					freeSpace := bufLength - writer.payloadBufferIndex
					if freeSpace == 0 {
						if !writer.flushRecords() {
							quitFlag = true
							break
						}
					}
				}

				bufPool.Put(payload)
			}

		case <-recordStagingTicker.C:
			if !writer.flushRecords() {
				quitFlag = true
			}

		case <-keepAliveTicker.C:
			if !writer.write(continuationMessage) {
				quitFlag = true
			}

		case <-progressTickerC:
			bytesScanned, bytesProcessed := writer.getProgressFunc()
			bytesReturned := atomic.LoadInt64(&writer.bytesReturned)
			if !writer.write(newProgressMessage(bytesScanned, bytesProcessed, bytesReturned)) {
				quitFlag = true
			}
		}
	}
	close(writer.doneCh)

	recordStagingTicker.Stop()
	keepAliveTicker.Stop()
	if progressTicker != nil {
		progressTicker.Stop()
	}

	// Whatever drain the payloadCh to prevent from memory leaking.
	for len(writer.payloadCh) > 0 {
		payload := <-writer.payloadCh
		bufPool.Put(payload)
	}
}

// Sends a single whole record.
func (writer *messageWriter) SendRecord(payload *bytes.Buffer) error {
	select {
	case writer.payloadCh <- payload:
		return nil
	case <-writer.doneCh:
		return fmt.Errorf("messageWriter is done")
	}
}

func (writer *messageWriter) flushRecords() bool {
	if writer.payloadBufferIndex == 0 {
		return true
	}
	result := writer.write(newRecordsMessage(writer.payloadBuffer[0:writer.payloadBufferIndex]))
	if result {
		atomic.AddInt64(&writer.bytesReturned, int64(writer.payloadBufferIndex))
		writer.payloadBufferIndex = 0
	}
	return result
}

// Finish is the last call to the message writer - it sends any
// remaining record payload, then sends statistics and finally the end
// message.
func (writer *messageWriter) Finish(bytesScanned, bytesProcessed int64) error {
	select {
	case <-writer.doneCh:
		return fmt.Errorf("messageWriter is done")
	default:
		writer.finBytesScanned = bytesScanned
		writer.finBytesProcessed = bytesProcessed
		close(writer.payloadCh)
		// Wait until the `start` go-routine is done.
		<-writer.doneCh
		return nil
	}
}

func (writer *messageWriter) FinishWithError(errorCode, errorMessage string) error {
	select {
	case <-writer.doneCh:
		return fmt.Errorf("messageWriter is done")
	case writer.errCh <- newErrorMessage([]byte(errorCode), []byte(errorMessage)):
		// Wait until the `start` go-routine is done.
		<-writer.doneCh
		return nil
	}
}

// newMessageWriter creates a message writer that writes to the HTTP
// response writer
func newMessageWriter(w http.ResponseWriter, getProgressFunc func() (bytesScanned, bytesProcessed int64)) *messageWriter {
	writer := &messageWriter{
		writer:          w,
		getProgressFunc: getProgressFunc,

		payloadBuffer: make([]byte, bufLength),
		payloadCh:     make(chan *bytes.Buffer, 1),

		errCh:  make(chan []byte),
		doneCh: make(chan struct{}),
	}
	go writer.start()
	return writer
}

/*
 * Minio Cloud Storage, (C) 2018 Minio, Inc.
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
// DO NOT EDIT THIS PACKAGE DIRECTLY: This follows the protocol defined by
// AmazonS3 found at
// https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectSELECTContent.html
// Consult the Spec before making direct edits.

package s3select

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
)

// Record Headers
// -11 -event type - 7 - 7 "Records"
// -13 -content-type -7 -24 "application/octet-stream"
// -13 -message-type -7 5 "event"
// This is predefined from AMZ protocol found here:
// https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectSELECTContent.html
var recordHeaders []byte

// End Headers
// -13 -message-type -7 -5 "event"
// -11 -:event-type -7  -3 "End"
// This is predefined from AMZ protocol found here:
// https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectSELECTContent.html
var endHeaders []byte

// Continuation Headers
// -13 -message-type -7 -5 "event"
// -11 -:event-type -7  -4 "Cont"
// This is predefined from AMZ protocol found here:
// https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectSELECTContent.html
var contHeaders []byte

// Stat Headers
// -11 -event type - 7 - 5 "Stat" -20
// -13 -content-type -7 -8 "text/xml" -25
// -13 -message-type -7 -5 "event"     -22
// This is predefined from AMZ protocol found here:
// https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectSELECTContent.html
var statHeaders []byte

// Progress Headers
// -11 -event type - 7 - 8 "Progress" -23
// -13 -content-type -7 -8 "text/xml" -25
// -13 -message-type -7 -5 "event"     -22
// This is predefined from AMZ protocol found here:
// https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectSELECTContent.html
var progressHeaders []byte

// The length of the nonvariable portion of the ErrHeaders
// The below are the specifications of the header for a "error" event
// -11 -error-code - 7 - DEFINED "DEFINED"
// -14 -error-message -7 -DEFINED "DEFINED"
// -13 -message-type -7 -5 "error"
// This is predefined from AMZ protocol found here:
// https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectSELECTContent.html
var errHdrLen int

func init() {
	recordHeaders = writeRecordHeader()
	endHeaders = writeEndHeader()
	contHeaders = writeContHeader()
	statHeaders = writeStatHeader()
	progressHeaders = writeProgressHeader()
	errHdrLen = 55

}

// encodeString encodes a string in a []byte, lenBytes is the number of bytes
// used to encode the length of the string.
func encodeHeaderStringValue(s string) []byte {
	n := uint16(len(s))
	lenSlice := make([]byte, 2)
	binary.BigEndian.PutUint16(lenSlice[0:], n)
	return append(lenSlice, []byte(s)...)
}
func encodeHeaderStringName(s string) []byte {
	lenSlice := make([]byte, 1)
	lenSlice[0] = byte(len(s))
	return append(lenSlice, []byte(s)...)
}

// encodeNumber encodes a number in a []byte, lenBytes is the number of bytes
// used to encode the length of the string.
func encodeNumber(n byte, lenBytes int) []byte {
	lenSlice := make([]byte, lenBytes)
	lenSlice[0] = n
	return lenSlice
}

// writePayloadSize writes the 4byte payload size portion of the protocol.
func writePayloadSize(payloadSize int, headerLength int) []byte {
	totalByteLen := make([]byte, 4)
	totalMsgLen := uint32(payloadSize + headerLength + 16)
	binary.BigEndian.PutUint32(totalByteLen, totalMsgLen)
	return totalByteLen
}

// writeHeaderSize writes the 4byte header size portion of the protocol.
func writeHeaderSize(headerLength int) []byte {
	totalHeaderLen := make([]byte, 4)
	totalLen := uint32(headerLength)
	binary.BigEndian.PutUint32(totalHeaderLen, totalLen)
	return totalHeaderLen
}

//  writeCRC writes the CRC for both the prelude and and the end of the protocol.
func writeCRC(buffer []byte) []byte {
	// Calculate the  CRC here:
	crc := make([]byte, 4)
	cksum := crc32.ChecksumIEEE(buffer)
	binary.BigEndian.PutUint32(crc, cksum)
	return crc
}

// writePayload writes the Payload for those protocols which the Payload is
// necessary.
func writePayload(myPayload string) []byte {
	convertedPayload := []byte(myPayload)
	payloadStore := make([]byte, len(convertedPayload))
	copy(payloadStore[0:], myPayload)
	return payloadStore
}

// writeRecordHeader is a function which writes the headers for the continuation
// Message
func writeRecordHeader() []byte {
	// This is predefined from AMZ protocol found here:
	// https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectSELECTContent.html
	var currentMessage = &bytes.Buffer{}
	// 11 -event type - 7 - 7 "Records"
	// header name
	currentMessage.Write(encodeHeaderStringName(":event-type"))
	// header type
	currentMessage.Write(encodeNumber(7, 1))
	// header value and header value length
	currentMessage.Write(encodeHeaderStringValue("Records"))
	// Creation of the Header for Content-Type 	// 13 -content-type -7 -24
	// "application/octet-stream"
	// header name
	currentMessage.Write(encodeHeaderStringName(":content-type"))
	// header type
	currentMessage.Write(encodeNumber(7, 1))
	// header value and header value length
	currentMessage.Write(encodeHeaderStringValue("application/octet-stream"))
	// Creation of the Header for message-type 13 -message-type -7 5 "event"
	// header name
	currentMessage.Write(encodeHeaderStringName(":message-type"))
	// header type
	currentMessage.Write(encodeNumber(7, 1))
	// header value and header value length
	currentMessage.Write(encodeHeaderStringValue("event"))
	return currentMessage.Bytes()
}

// writeEndHeader is a function which writes the headers for the continuation
// Message
func writeEndHeader() []byte {
	// This is predefined from AMZ protocol found here:
	// https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectSELECTContent.html
	var currentMessage = &bytes.Buffer{}
	// header name
	currentMessage.Write(encodeHeaderStringName(":event-type"))
	// header type
	currentMessage.Write(encodeNumber(7, 1))
	// header value and header value length
	currentMessage.Write(encodeHeaderStringValue("End"))

	// Creation of the Header for message-type 13 -message-type -7 5 "event"
	// header name
	currentMessage.Write(encodeHeaderStringName(":message-type"))
	// header type
	currentMessage.Write(encodeNumber(7, 1))
	// header value and header value length
	currentMessage.Write(encodeHeaderStringValue("event"))
	return currentMessage.Bytes()
}

// writeContHeader is a function which writes the headers for the continuation
// Message
func writeContHeader() []byte {
	// This is predefined from AMZ protocol found here:
	// https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectSELECTContent.html
	var currentMessage = &bytes.Buffer{}
	// header name
	currentMessage.Write(encodeHeaderStringName(":event-type"))
	// header type
	currentMessage.Write(encodeNumber(7, 1))
	// header value and header value length
	currentMessage.Write(encodeHeaderStringValue("Cont"))

	// Creation of the Header for message-type 13 -message-type -7 5 "event"
	// header name
	currentMessage.Write(encodeHeaderStringName(":message-type"))
	// header type
	currentMessage.Write(encodeNumber(7, 1))
	// header value and header value length
	currentMessage.Write(encodeHeaderStringValue("event"))
	return currentMessage.Bytes()

}

// writeStatHeader is a function which writes the headers for the Stat
// Message
func writeStatHeader() []byte {
	// This is predefined from AMZ protocol found here:
	// https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectSELECTContent.html
	var currentMessage = &bytes.Buffer{}
	// header name
	currentMessage.Write(encodeHeaderStringName(":event-type"))
	// header type
	currentMessage.Write(encodeNumber(7, 1))
	// header value and header value length
	currentMessage.Write(encodeHeaderStringValue("Stats"))
	// Creation of the Header for Content-Type 	// 13 -content-type -7 -8
	// "text/xml"
	// header name
	currentMessage.Write(encodeHeaderStringName(":content-type"))
	// header type
	currentMessage.Write(encodeNumber(7, 1))
	// header value and header value length
	currentMessage.Write(encodeHeaderStringValue("text/xml"))

	// Creation of the Header for message-type 13 -message-type -7 5 "event"
	currentMessage.Write(encodeHeaderStringName(":message-type"))
	// header type
	currentMessage.Write(encodeNumber(7, 1))
	// header value and header value length
	currentMessage.Write(encodeHeaderStringValue("event"))
	return currentMessage.Bytes()

}

// writeProgressHeader is a function which writes the headers for the Progress
// Message
func writeProgressHeader() []byte {
	// This is predefined from AMZ protocol found here:
	// https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectSELECTContent.html
	var currentMessage = &bytes.Buffer{}
	// header name
	currentMessage.Write(encodeHeaderStringName(":event-type"))
	// header type
	currentMessage.Write(encodeNumber(7, 1))
	// header value and header value length
	currentMessage.Write(encodeHeaderStringValue("Progress"))
	// Creation of the Header for Content-Type 	// 13 -content-type -7 -8
	// "text/xml"
	// header name
	currentMessage.Write(encodeHeaderStringName(":content-type"))
	// header type
	currentMessage.Write(encodeNumber(7, 1))
	// header value and header value length
	currentMessage.Write(encodeHeaderStringValue("text/xml"))

	// Creation of the Header for message-type 13 -message-type -7 5 "event"
	// header name
	currentMessage.Write(encodeHeaderStringName(":message-type"))
	// header type
	currentMessage.Write(encodeNumber(7, 1))
	// header value and header value length
	currentMessage.Write(encodeHeaderStringValue("event"))
	return currentMessage.Bytes()

}

// writeRecordMessage is the function which constructs the binary message for a
// record message to be sent.
func writeRecordMessage(payload string, currentMessage *bytes.Buffer) *bytes.Buffer {
	// The below are the specifications of the header for a "record" event
	// 11 -event type - 7 - 7 "Records"
	// 13 -content-type -7 -24 "application/octet-stream"
	// 13 -message-type -7 5 "event"
	// This is predefined from AMZ protocol found here:
	// https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectSELECTContent.html
	headerLen := len(recordHeaders)
	// Writes the total size of the message.
	currentMessage.Write(writePayloadSize(len(payload), headerLen))
	// Writes the total size of the header.
	currentMessage.Write(writeHeaderSize(headerLen))
	// Writes the CRC of the Prelude
	currentMessage.Write(writeCRC(currentMessage.Bytes()))
	currentMessage.Write(recordHeaders)

	// This part is where the payload is written, this will be only one row, since
	// we're sending one message at a types
	currentMessage.Write(writePayload(payload))

	// Now we do a CRC check on the entire messages
	currentMessage.Write(writeCRC(currentMessage.Bytes()))
	return currentMessage

}

// writeContinuationMessage is the function which constructs the binary message
// for a continuation message to be sent.
func writeContinuationMessage(currentMessage *bytes.Buffer) *bytes.Buffer {
	// 11 -event type - 7 - 4 "Cont"
	// 13 -message-type -7 5 "event"
	// This is predefined from AMZ protocol found here:
	// https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectSELECTContent.html
	headerLen := len(contHeaders)
	currentMessage.Write(writePayloadSize(0, headerLen))

	currentMessage.Write(writeHeaderSize(headerLen))

	// Calculate the Prelude CRC here:
	currentMessage.Write(writeCRC(currentMessage.Bytes()))

	currentMessage.Write(contHeaders)

	//Now we do a CRC check on the entire messages
	currentMessage.Write(writeCRC(currentMessage.Bytes()))
	return currentMessage

}

// writeEndMessage is the function which constructs the binary message
// for a end message to be sent.
func writeEndMessage(currentMessage *bytes.Buffer) *bytes.Buffer {
	// 11 -event type - 7 - 3 "End"
	// 13 -message-type -7 5 "event"
	// This is predefined from AMZ protocol found here:
	// https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectSELECTContent.html
	headerLen := len(endHeaders)
	currentMessage.Write(writePayloadSize(0, headerLen))

	currentMessage.Write(writeHeaderSize(headerLen))

	//Calculate the Prelude CRC here:
	currentMessage.Write(writeCRC(currentMessage.Bytes()))

	currentMessage.Write(endHeaders)

	// Now we do a CRC check on the entire messages
	currentMessage.Write(writeCRC(currentMessage.Bytes()))
	return currentMessage

}

// writeStateMessage is the function which constructs the binary message for a
// state message to be sent.
func writeStatMessage(payload string, currentMessage *bytes.Buffer) *bytes.Buffer {
	// 11 -event type - 7 - 5 "Stat" 20
	// 13 -content-type -7 -8 "text/xml" 25
	// 13 -message-type -7 5 "event"     22
	// This is predefined from AMZ protocol found here:
	// https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectSELECTContent.html
	headerLen := len(statHeaders)

	currentMessage.Write(writePayloadSize(len(payload), headerLen))

	currentMessage.Write(writeHeaderSize(headerLen))

	currentMessage.Write(writeCRC(currentMessage.Bytes()))

	currentMessage.Write(statHeaders)

	// This part is where the payload is written, this will be only one row, since
	// we're sending one message at a types
	currentMessage.Write(writePayload(payload))

	// Now we do a CRC check on the entire messages
	currentMessage.Write(writeCRC(currentMessage.Bytes()))
	return currentMessage

}

// writeProgressMessage is the function which constructs the binary message for
// a progress message to be sent.
func writeProgressMessage(payload string, currentMessage *bytes.Buffer) *bytes.Buffer {
	// The below are the specifications of the header for a "Progress" event
	// 11 -event type - 7 - 8 "Progress" 23
	// 13 -content-type -7 -8 "text/xml" 25
	// 13 -message-type -7 5 "event"     22
	// This is predefined from AMZ protocol found here:
	// https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectSELECTContent.html
	headerLen := len(progressHeaders)

	currentMessage.Write(writePayloadSize(len(payload), headerLen))

	currentMessage.Write(writeHeaderSize(headerLen))

	currentMessage.Write(writeCRC(currentMessage.Bytes()))

	currentMessage.Write(progressHeaders)

	// This part is where the payload is written, this will be only one row, since
	// we're sending one message at a types
	currentMessage.Write(writePayload(payload))

	// Now we do a CRC check on the entire messages
	currentMessage.Write(writeCRC(currentMessage.Bytes()))
	return currentMessage

}

// writeErrorMessage is the function which constructs the binary message for a
// error message to be sent.
func writeErrorMessage(errorMessage error, currentMessage *bytes.Buffer) *bytes.Buffer {

	// The below are the specifications of the header for a "error" event
	// 11 -error-code - 7 - DEFINED "DEFINED"
	// 14 -error-message -7 -DEFINED "DEFINED"
	// 13 -message-type -7 5 "error"
	// This is predefined from AMZ protocol found here:
	// https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectSELECTContent.html
	sizeOfErrorCode := len(errorCodeResponse[errorMessage])
	sizeOfErrorMessage := len(errorMessage.Error())
	headerLen := errHdrLen + sizeOfErrorCode + sizeOfErrorMessage

	currentMessage.Write(writePayloadSize(0, headerLen))

	currentMessage.Write(writeHeaderSize(headerLen))

	currentMessage.Write(writeCRC(currentMessage.Bytes()))
	// header name
	currentMessage.Write(encodeHeaderStringName(":error-code"))
	// header type
	currentMessage.Write(encodeNumber(7, 1))
	// header value and header value length
	currentMessage.Write(encodeHeaderStringValue(errorCodeResponse[errorMessage]))

	//  14 -error-message -7 -DEFINED "DEFINED"

	// header name
	currentMessage.Write(encodeHeaderStringName(":error-message"))
	// header type
	currentMessage.Write(encodeNumber(7, 1))
	// header value and header value length
	currentMessage.Write(encodeHeaderStringValue(errorMessage.Error()))
	// Creation of the Header for message-type 13 -message-type -7 5 "error"
	// header name
	currentMessage.Write(encodeHeaderStringName(":message-type"))
	// header type
	currentMessage.Write(encodeNumber(7, 1))
	// header value and header value length
	currentMessage.Write(encodeHeaderStringValue("error"))

	// Now we do a CRC check on the entire messages
	currentMessage.Write(writeCRC(currentMessage.Bytes()))
	return currentMessage

}

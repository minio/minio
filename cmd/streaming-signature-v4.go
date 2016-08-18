/*
 * Minio Cloud Storage, (C) 2016 Minio, Inc.
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

// Package cmd This file implements helper functions to validate Streaming AWS
// Signature Version '4' authorization header.
package cmd

import (
	"bufio"
	"bytes"
	"encoding/hex"
	"errors"
	"hash"
	"io"
	"net/http"
	"time"

	"github.com/minio/sha256-simd"
)

// Streaming AWS Signature Version '4' constants.
const (
	emptySHA256            = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	streamingContentSHA256 = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"
	signV4ChunkedAlgorithm = "AWS4-HMAC-SHA256-PAYLOAD"
)

// getChunkSignature - get chunk signature.
func getChunkSignature(seedSignature string, date time.Time, hashedChunk string) string {
	// Access credentials.
	cred := serverConfig.GetCredential()

	// Server region.
	region := serverConfig.GetRegion()

	// Calculate string to sign.
	stringToSign := signV4ChunkedAlgorithm + "\n" +
		date.Format(iso8601Format) + "\n" +
		getScope(date, region) + "\n" +
		seedSignature + "\n" +
		emptySHA256 + "\n" +
		hashedChunk

	// Get hmac signing key.
	signingKey := getSigningKey(cred.SecretAccessKey, date, region)

	// Calculate signature.
	newSignature := getSignature(signingKey, stringToSign)

	return newSignature
}

// calculateSeedSignature - Calculate seed signature in accordance with
//     - http://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-streaming.html
// returns signature, error otherwise if the signature mismatches or any other
// error while parsing and validating.
func calculateSeedSignature(r *http.Request) (signature string, date time.Time, errCode APIErrorCode) {
	// Access credentials.
	cred := serverConfig.GetCredential()

	// Server region.
	region := serverConfig.GetRegion()

	// Copy request.
	req := *r

	// Save authorization header.
	v4Auth := req.Header.Get("Authorization")

	// Parse signature version '4' header.
	signV4Values, errCode := parseSignV4(v4Auth)
	if errCode != ErrNone {
		return "", time.Time{}, errCode
	}

	// Payload streaming.
	payload := streamingContentSHA256

	// Payload for STREAMING signature should be 'STREAMING-AWS4-HMAC-SHA256-PAYLOAD'
	if payload != req.Header.Get("X-Amz-Content-Sha256") {
		return "", time.Time{}, ErrContentSHA256Mismatch
	}

	// Extract all the signed headers along with its values.
	extractedSignedHeaders, errCode := extractSignedHeaders(signV4Values.SignedHeaders, req.Header)
	if errCode != ErrNone {
		return "", time.Time{}, errCode
	}
	// Verify if the access key id matches.
	if signV4Values.Credential.accessKey != cred.AccessKeyID {
		return "", time.Time{}, ErrInvalidAccessKeyID
	}

	// Verify if region is valid.
	sRegion := signV4Values.Credential.scope.region
	// Should validate region, only if region is set. Some operations
	// do not need region validated for example GetBucketLocation.
	if !isValidRegion(sRegion, region) {
		return "", time.Time{}, ErrInvalidRegion
	}

	// Extract date, if not present throw error.
	var dateStr string
	if dateStr = req.Header.Get(http.CanonicalHeaderKey("x-amz-date")); dateStr == "" {
		if dateStr = r.Header.Get("Date"); dateStr == "" {
			return "", time.Time{}, ErrMissingDateHeader
		}
	}
	// Parse date header.
	var err error
	date, err = time.Parse(iso8601Format, dateStr)
	if err != nil {
		errorIf(err, "Unable to parse date", dateStr)
		return "", time.Time{}, ErrMalformedDate
	}

	// Query string.
	queryStr := req.URL.Query().Encode()

	// Get canonical request.
	canonicalRequest := getCanonicalRequest(extractedSignedHeaders, payload, queryStr, req.URL.Path, req.Method, req.Host)

	// Get string to sign from canonical request.
	stringToSign := getStringToSign(canonicalRequest, date, region)

	// Get hmac signing key.
	signingKey := getSigningKey(cred.SecretAccessKey, date, region)

	// Calculate signature.
	newSignature := getSignature(signingKey, stringToSign)

	// Verify if signature match.
	if newSignature != signV4Values.Signature {
		return "", time.Time{}, ErrSignatureDoesNotMatch
	}

	// Return caculated signature.
	return newSignature, date, ErrNone
}

const maxLineLength = 4096 // assumed <= bufio.defaultBufSize 4KiB.

// lineTooLong is generated as chunk header is bigger than 4KiB.
var errLineTooLong = errors.New("header line too long")

// Malformed encoding is generated when chunk header is wrongly formed.
var errMalformedEncoding = errors.New("malformed chunked encoding")

// newSignV4ChunkedReader returns a new s3ChunkedReader that translates the data read from r
// out of HTTP "chunked" format before returning it.
// The s3ChunkedReader returns io.EOF when the final 0-length chunk is read.
//
// NewChunkedReader is not needed by normal applications. The http package
// automatically decodes chunking when reading response bodies.
func newSignV4ChunkedReader(req *http.Request) (io.Reader, APIErrorCode) {
	seedSignature, seedDate, errCode := calculateSeedSignature(req)
	if errCode != ErrNone {
		return nil, errCode
	}
	return &s3ChunkedReader{
		reader:            bufio.NewReader(req.Body),
		seedSignature:     seedSignature,
		seedDate:          seedDate,
		chunkSHA256Writer: sha256.New(),
	}, ErrNone
}

// Represents the overall state that is required for decoding a
// AWS Signature V4 chunked reader.
type s3ChunkedReader struct {
	reader            *bufio.Reader
	seedSignature     string
	seedDate          time.Time
	dataChunkRead     bool
	chunkSignature    string
	chunkSHA256Writer hash.Hash // Calculates sha256 of chunk data.
	n                 uint64    // Unread bytes in chunk
	err               error
}

// Read chunk reads the chunk token signature portion.
func (cr *s3ChunkedReader) readS3ChunkHeader() {
	// Read the first chunk line until CRLF.
	var hexChunkSize, hexChunkSignature []byte
	hexChunkSize, hexChunkSignature, cr.err = readChunkLine(cr.reader)
	if cr.err != nil {
		return
	}
	// <hex>;token=value - converts the hex into its uint64 form.
	cr.n, cr.err = parseHexUint(hexChunkSize)
	if cr.err != nil {
		return
	}
	if cr.n == 0 {
		cr.err = io.EOF
	}
	// is the data part already read?, set this to false.
	cr.dataChunkRead = false
	// Reset sha256 hasher for a fresh start.
	cr.chunkSHA256Writer.Reset()
	// Save the incoming chunk signature.
	cr.chunkSignature = string(hexChunkSignature)
}

// Validate if the underlying buffer has chunk header.
func (cr *s3ChunkedReader) s3ChunkHeaderAvailable() bool {
	n := cr.reader.Buffered()
	if n > 0 {
		// Peek without seeking to look for trailing '\n'.
		peek, _ := cr.reader.Peek(n)
		return bytes.IndexByte(peek, '\n') >= 0
	}
	return false
}

// Read - implements `io.Reader`, which transparently decodes
// the incoming AWS Signature V4 streaming signature.
func (cr *s3ChunkedReader) Read(buf []byte) (n int, err error) {
	for cr.err == nil {
		if cr.n == 0 {
			// For no chunk header available, we don't have to
			// proceed to read again.
			if n > 0 && !cr.s3ChunkHeaderAvailable() {
				// We've read enough. Don't potentially block
				// reading a new chunk header.
				break
			}
			// If the chunk has been read, proceed to validate the rolling signature.
			if cr.dataChunkRead {
				// Calculate the hashed chunk.
				hashedChunk := hex.EncodeToString(cr.chunkSHA256Writer.Sum(nil))
				// Calculate the chunk signature.
				newSignature := getChunkSignature(cr.seedSignature, cr.seedDate, hashedChunk)
				if cr.chunkSignature != newSignature {
					// Chunk signature doesn't match we return signature does not match.
					cr.err = errSignatureMismatch
					break
				}
				// Newly calculated signature becomes the seed for the next chunk
				// this follows the chaining.
				cr.seedSignature = newSignature
			}
			// Proceed to read the next chunk header.
			cr.readS3ChunkHeader()
			continue
		}
		// With requested buffer of zero length, no need to read further.
		if len(buf) == 0 {
			break
		}
		rbuf := buf
		// Make sure to read only the specified payload size, stagger
		// the rest for subsequent requests.
		if uint64(len(rbuf)) > cr.n {
			rbuf = rbuf[:cr.n]
		}
		var n0 int
		n0, cr.err = cr.reader.Read(rbuf)

		// Calculate sha256.
		cr.chunkSHA256Writer.Write(rbuf[:n0])
		// Set since we have read the chunk read.
		cr.dataChunkRead = true

		n += n0
		buf = buf[n0:]
		// Decrements the 'cr.n' for future reads.
		cr.n -= uint64(n0)

		// If we're at the end of a chunk.
		if cr.n == 0 && cr.err == nil {
			// Read the next two bytes to verify if they are "\r\n".
			cr.err = checkCRLF(cr.reader)
		}
	}
	// Return number of bytes read, and error if any.
	return n, cr.err
}

// checkCRLF - check if reader only has '\r\n' CRLF character.
// returns malformed encoding if it doesn't.
func checkCRLF(reader io.Reader) (err error) {
	var buf = make([]byte, 2)
	if _, err = io.ReadFull(reader, buf[:2]); err == nil {
		if buf[0] != '\r' || buf[1] != '\n' {
			err = errMalformedEncoding
		}
	}
	return err
}

// Read a line of bytes (up to \n) from b.
// Give up if the line exceeds maxLineLength.
// The returned bytes are owned by the bufio.Reader
// so they are only valid until the next bufio read.
func readChunkLine(b *bufio.Reader) ([]byte, []byte, error) {
	buf, err := b.ReadSlice('\n')
	if err != nil {
		// We always know when EOF is coming.
		// If the caller asked for a line, there should be a line.
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		} else if err == bufio.ErrBufferFull {
			err = errLineTooLong
		}
		return nil, nil, err
	}
	if len(buf) >= maxLineLength {
		return nil, nil, errLineTooLong
	}
	// Parse s3 specific chunk extension and fetch the values.
	hexChunkSize, hexChunkSignature := parseS3ChunkExtension(buf)
	return hexChunkSize, hexChunkSignature, nil
}

// trimTrailingWhitespace - trim trailing white space.
func trimTrailingWhitespace(b []byte) []byte {
	for len(b) > 0 && isASCIISpace(b[len(b)-1]) {
		b = b[:len(b)-1]
	}
	return b
}

// isASCIISpace - is ascii space?
func isASCIISpace(b byte) bool {
	return b == ' ' || b == '\t' || b == '\n' || b == '\r'
}

// Constant s3 chunk encoding signature.
const s3ChunkSignatureStr = ";chunk-signature="

// parses3ChunkExtension removes any s3 specific chunk-extension from buf.
// For example,
//     "10000;chunk-signature=..." => "10000", "chunk-signature=..."
func parseS3ChunkExtension(buf []byte) ([]byte, []byte) {
	buf = trimTrailingWhitespace(buf)
	semi := bytes.Index(buf, []byte(s3ChunkSignatureStr))
	// Chunk signature not found, return the whole buffer.
	if semi == -1 {
		return buf, nil
	}
	return buf[:semi], parseChunkSignature(buf[semi:])
}

// parseChunkSignature - parse chunk signature.
func parseChunkSignature(chunk []byte) []byte {
	chunkSplits := bytes.SplitN(chunk, []byte(s3ChunkSignatureStr), 2)
	return chunkSplits[1]
}

// parse hex to uint64.
func parseHexUint(v []byte) (n uint64, err error) {
	for i, b := range v {
		switch {
		case '0' <= b && b <= '9':
			b = b - '0'
		case 'a' <= b && b <= 'f':
			b = b - 'a' + 10
		case 'A' <= b && b <= 'F':
			b = b - 'A' + 10
		default:
			return 0, errors.New("invalid byte in chunk length")
		}
		if i == 16 {
			return 0, errors.New("http chunk length too large")
		}
		n <<= 4
		n |= uint64(b)
	}
	return
}

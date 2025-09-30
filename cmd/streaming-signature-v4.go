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

// Package cmd This file implements helper functions to validate Streaming AWS
// Signature Version '4' authorization header.
package cmd

import (
	"bufio"
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/minio/minio/internal/auth"
	"github.com/minio/minio/internal/hash/sha256"
	xhttp "github.com/minio/minio/internal/http"
)

// Streaming AWS Signature Version '4' constants.
const (
	emptySHA256                   = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	streamingContentSHA256        = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"
	streamingContentSHA256Trailer = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER"
	signV4ChunkedAlgorithm        = "AWS4-HMAC-SHA256-PAYLOAD"
	signV4ChunkedAlgorithmTrailer = "AWS4-HMAC-SHA256-TRAILER"
	streamingContentEncoding      = "aws-chunked"
	awsTrailerHeader              = "X-Amz-Trailer"
	trailerKVSeparator            = ":"
)

// getChunkSignature - get chunk signature.
// Does not update anything in cr.
func (cr *s3ChunkedReader) getChunkSignature() string {
	hashedChunk := hex.EncodeToString(cr.chunkSHA256Writer.Sum(nil))

	// Calculate string to sign.
	alg := signV4ChunkedAlgorithm + "\n"
	stringToSign := alg +
		cr.seedDate.Format(iso8601Format) + "\n" +
		getScope(cr.seedDate, cr.region) + "\n" +
		cr.seedSignature + "\n" +
		emptySHA256 + "\n" +
		hashedChunk

	// Get hmac signing key.
	signingKey := getSigningKey(cr.cred.SecretKey, cr.seedDate, cr.region, serviceS3)

	// Calculate signature.
	newSignature := getSignature(signingKey, stringToSign)

	return newSignature
}

// getTrailerChunkSignature - get trailer chunk signature.
func (cr *s3ChunkedReader) getTrailerChunkSignature() string {
	hashedChunk := hex.EncodeToString(cr.chunkSHA256Writer.Sum(nil))

	// Calculate string to sign.
	alg := signV4ChunkedAlgorithmTrailer + "\n"
	stringToSign := alg +
		cr.seedDate.Format(iso8601Format) + "\n" +
		getScope(cr.seedDate, cr.region) + "\n" +
		cr.seedSignature + "\n" +
		hashedChunk

	// Get hmac signing key.
	signingKey := getSigningKey(cr.cred.SecretKey, cr.seedDate, cr.region, serviceS3)

	// Calculate signature.
	newSignature := getSignature(signingKey, stringToSign)

	return newSignature
}

// calculateSeedSignature - Calculate seed signature in accordance with
//   - http://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-streaming.html
//
// returns signature, error otherwise if the signature mismatches or any other
// error while parsing and validating.
func calculateSeedSignature(r *http.Request, trailers bool) (cred auth.Credentials, signature string, region string, date time.Time, errCode APIErrorCode) {
	// Copy request.
	req := *r

	// Save authorization header.
	v4Auth := req.Header.Get(xhttp.Authorization)

	// Parse signature version '4' header.
	signV4Values, errCode := parseSignV4(v4Auth, globalSite.Region(), serviceS3)
	if errCode != ErrNone {
		return cred, "", "", time.Time{}, errCode
	}

	// Payload streaming.
	payload := streamingContentSHA256
	if trailers {
		payload = streamingContentSHA256Trailer
	}

	// Payload for STREAMING signature should be 'STREAMING-AWS4-HMAC-SHA256-PAYLOAD'
	if payload != req.Header.Get(xhttp.AmzContentSha256) {
		return cred, "", "", time.Time{}, ErrContentSHA256Mismatch
	}

	// Extract all the signed headers along with its values.
	extractedSignedHeaders, errCode := extractSignedHeaders(signV4Values.SignedHeaders, r)
	if errCode != ErrNone {
		return cred, "", "", time.Time{}, errCode
	}

	cred, _, errCode = checkKeyValid(r, signV4Values.Credential.accessKey)
	if errCode != ErrNone {
		return cred, "", "", time.Time{}, errCode
	}

	// Verify if region is valid.
	region = signV4Values.Credential.scope.region

	// Extract date, if not present throw error.
	var dateStr string
	if dateStr = req.Header.Get("x-amz-date"); dateStr == "" {
		if dateStr = r.Header.Get("Date"); dateStr == "" {
			return cred, "", "", time.Time{}, ErrMissingDateHeader
		}
	}

	// Parse date header.
	var err error
	date, err = time.Parse(iso8601Format, dateStr)
	if err != nil {
		return cred, "", "", time.Time{}, ErrMalformedDate
	}

	// Query string.
	queryStr := req.Form.Encode()

	// Get canonical request.
	canonicalRequest := getCanonicalRequest(extractedSignedHeaders, payload, queryStr, req.URL.Path, req.Method)

	// Get string to sign from canonical request.
	stringToSign := getStringToSign(canonicalRequest, date, signV4Values.Credential.getScope())

	// Get hmac signing key.
	signingKey := getSigningKey(cred.SecretKey, signV4Values.Credential.scope.date, region, serviceS3)

	// Calculate signature.
	newSignature := getSignature(signingKey, stringToSign)

	// Verify if signature match.
	if !compareSignatureV4(newSignature, signV4Values.Signature) {
		return cred, "", "", time.Time{}, ErrSignatureDoesNotMatch
	}

	// Return calculated signature.
	return cred, newSignature, region, date, ErrNone
}

const maxLineLength = 4 * humanize.KiByte // assumed <= bufio.defaultBufSize 4KiB

// lineTooLong is generated as chunk header is bigger than 4KiB.
var errLineTooLong = errors.New("header line too long")

// malformed encoding is generated when chunk header is wrongly formed.
var errMalformedEncoding = errors.New("malformed chunked encoding")

// chunk is considered too big if its bigger than > 16MiB.
var errChunkTooBig = errors.New("chunk too big: choose chunk size <= 16MiB")

// newSignV4ChunkedReader returns a new s3ChunkedReader that translates the data read from r
// out of HTTP "chunked" format before returning it.
// The s3ChunkedReader returns io.EOF when the final 0-length chunk is read.
//
// NewChunkedReader is not needed by normal applications. The http package
// automatically decodes chunking when reading response bodies.
func newSignV4ChunkedReader(req *http.Request, trailer bool) (io.ReadCloser, APIErrorCode) {
	cred, seedSignature, region, seedDate, errCode := calculateSeedSignature(req, trailer)
	if errCode != ErrNone {
		return nil, errCode
	}

	if trailer {
		// Discard anything unsigned.
		req.Trailer = make(http.Header)
		trailers := req.Header.Values(awsTrailerHeader)
		for _, key := range trailers {
			req.Trailer.Add(key, "")
		}
	} else {
		req.Trailer = nil
	}
	return &s3ChunkedReader{
		trailers:          req.Trailer,
		reader:            bufio.NewReader(req.Body),
		cred:              cred,
		seedSignature:     seedSignature,
		seedDate:          seedDate,
		region:            region,
		chunkSHA256Writer: sha256.New(),
		buffer:            make([]byte, 64*1024),
		debug:             false,
	}, ErrNone
}

// Represents the overall state that is required for decoding a
// AWS Signature V4 chunked reader.
type s3ChunkedReader struct {
	reader        *bufio.Reader
	cred          auth.Credentials
	seedSignature string
	seedDate      time.Time
	region        string
	trailers      http.Header

	chunkSHA256Writer hash.Hash // Calculates sha256 of chunk data.
	buffer            []byte
	offset            int
	err               error
	debug             bool // Print details on failure. Add your own if more are needed.
}

func (cr *s3ChunkedReader) Close() (err error) {
	return nil
}

// Now, we read one chunk from the underlying reader.
// A chunk has the following format:
//
//	<chunk-size-as-hex> + ";chunk-signature=" + <signature-as-hex> + "\r\n" + <payload> + "\r\n"
//
// First, we read the chunk size but fail if it is larger
// than 16 MiB. We must not accept arbitrary large chunks.
// One 16 MiB is a reasonable max limit.
//
// Then we read the signature and payload data. We compute the SHA256 checksum
// of the payload and verify that it matches the expected signature value.
//
// The last chunk is *always* 0-sized. So, we must only return io.EOF if we have encountered
// a chunk with a chunk size = 0. However, this chunk still has a signature and we must
// verify it.
const maxChunkSize = 16 << 20 // 16 MiB

// Read - implements `io.Reader`, which transparently decodes
// the incoming AWS Signature V4 streaming signature.
func (cr *s3ChunkedReader) Read(buf []byte) (n int, err error) {
	if cr.err != nil {
		if cr.debug {
			fmt.Printf("s3ChunkedReader: Returning err: %v (%T)\n", cr.err, cr.err)
		}
		return 0, cr.err
	}
	defer func() {
		if err != nil && err != io.EOF {
			if cr.debug {
				fmt.Println("Read err:", err)
			}
		}
	}()
	// First, if there is any unread data, copy it to the client
	// provided buffer.
	if cr.offset > 0 {
		n = copy(buf, cr.buffer[cr.offset:])
		if n == len(buf) {
			cr.offset += n
			return n, nil
		}
		cr.offset = 0
		buf = buf[n:]
	}

	var size int
	for {
		b, err := cr.reader.ReadByte()
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		if err != nil {
			cr.err = err
			return n, cr.err
		}
		if b == ';' { // separating character
			break
		}

		// Manually deserialize the size since AWS specified
		// the chunk size to be of variable width. In particular,
		// a size of 16 is encoded as `10` while a size of 64 KB
		// is `10000`.
		switch {
		case b >= '0' && b <= '9':
			size = size<<4 | int(b-'0')
		case b >= 'a' && b <= 'f':
			size = size<<4 | int(b-('a'-10))
		case b >= 'A' && b <= 'F':
			size = size<<4 | int(b-('A'-10))
		default:
			cr.err = errMalformedEncoding
			return n, cr.err
		}
		if size > maxChunkSize {
			cr.err = errChunkTooBig
			return n, cr.err
		}
	}

	// Now, we read the signature of the following payload and expect:
	//   chunk-signature=" + <signature-as-hex> + "\r\n"
	//
	// The signature is 64 bytes long (hex-encoded SHA256 hash) and
	// starts with a 16 byte header: len("chunk-signature=") + 64 == 80.
	var signature [80]byte
	_, err = io.ReadFull(cr.reader, signature[:])
	if err == io.EOF {
		err = io.ErrUnexpectedEOF
	}
	if err != nil {
		cr.err = err
		return n, cr.err
	}
	if !bytes.HasPrefix(signature[:], []byte("chunk-signature=")) {
		cr.err = errMalformedEncoding
		return n, cr.err
	}
	b, err := cr.reader.ReadByte()
	if err == io.EOF {
		err = io.ErrUnexpectedEOF
	}
	if err != nil {
		cr.err = err
		return n, cr.err
	}
	if b != '\r' {
		cr.err = errMalformedEncoding
		return n, cr.err
	}
	b, err = cr.reader.ReadByte()
	if err == io.EOF {
		err = io.ErrUnexpectedEOF
	}
	if err != nil {
		cr.err = err
		return n, cr.err
	}
	if b != '\n' {
		cr.err = errMalformedEncoding
		return n, cr.err
	}

	if cap(cr.buffer) < size {
		cr.buffer = make([]byte, size)
	} else {
		cr.buffer = cr.buffer[:size]
	}

	// Now, we read the payload and compute its SHA-256 hash.
	_, err = io.ReadFull(cr.reader, cr.buffer)
	if err == io.EOF && size != 0 {
		err = io.ErrUnexpectedEOF
	}
	if err != nil && err != io.EOF {
		cr.err = err
		return n, cr.err
	}

	// Once we have read the entire chunk successfully, we verify
	// that the received signature matches our computed signature.
	cr.chunkSHA256Writer.Write(cr.buffer)
	newSignature := cr.getChunkSignature()
	if !compareSignatureV4(string(signature[16:]), newSignature) {
		cr.err = errSignatureMismatch
		return n, cr.err
	}
	cr.seedSignature = newSignature
	cr.chunkSHA256Writer.Reset()

	// If the chunk size is zero we return io.EOF. As specified by AWS,
	// only the last chunk is zero-sized.
	if len(cr.buffer) == 0 {
		if cr.debug {
			fmt.Println("EOF. Reading Trailers:", cr.trailers)
		}
		if cr.trailers != nil {
			err = cr.readTrailers()
			if cr.debug {
				fmt.Println("trailers returned:", err, "now:", cr.trailers)
			}
			if err != nil {
				cr.err = err
				return 0, err
			}
		}
		cr.err = io.EOF
		return n, cr.err
	}

	b, err = cr.reader.ReadByte()
	if b != '\r' || err != nil {
		if cr.debug {
			fmt.Printf("want %q, got %q\n", "\r", string(b))
		}
		cr.err = errMalformedEncoding
		return n, cr.err
	}
	b, err = cr.reader.ReadByte()
	if err == io.EOF {
		err = io.ErrUnexpectedEOF
	}
	if err != nil {
		cr.err = err
		return n, cr.err
	}
	if b != '\n' {
		if cr.debug {
			fmt.Printf("want %q, got %q\n", "\r", string(b))
		}
		cr.err = errMalformedEncoding
		return n, cr.err
	}

	cr.offset = copy(buf, cr.buffer)
	n += cr.offset
	return n, err
}

// readTrailers will read all trailers and populate cr.trailers with actual values.
func (cr *s3ChunkedReader) readTrailers() error {
	if cr.debug {
		fmt.Printf("pre trailer sig: %s\n", cr.seedSignature)
	}
	var valueBuffer bytes.Buffer
	// Read value
	for {
		v, err := cr.reader.ReadByte()
		if err != nil {
			if err == io.EOF {
				return io.ErrUnexpectedEOF
			}
		}
		if v != '\r' {
			valueBuffer.WriteByte(v)
			continue
		}
		// End of buffer, do not add to value.
		v, err = cr.reader.ReadByte()
		if err != nil {
			if err == io.EOF {
				return io.ErrUnexpectedEOF
			}
		}
		if v != '\n' {
			return errMalformedEncoding
		}
		break
	}

	// Read signature
	var signatureBuffer bytes.Buffer
	for {
		v, err := cr.reader.ReadByte()
		if err != nil {
			if err == io.EOF {
				return io.ErrUnexpectedEOF
			}
		}
		if v != '\r' {
			signatureBuffer.WriteByte(v)
			continue
		}
		var tmp [3]byte
		_, err = io.ReadFull(cr.reader, tmp[:])
		if err != nil {
			if err == io.EOF {
				return io.ErrUnexpectedEOF
			}
		}
		if string(tmp[:]) != "\n\r\n" {
			if cr.debug {
				fmt.Printf("signature, want %q, got %q", "\n\r\n", string(tmp[:]))
			}
			return errMalformedEncoding
		}
		// No need to write final newlines to buffer.
		break
	}

	// Verify signature.
	sig := signatureBuffer.Bytes()
	if !bytes.HasPrefix(sig, []byte("x-amz-trailer-signature:")) {
		if cr.debug {
			fmt.Printf("prefix, want prefix %q, got %q", "x-amz-trailer-signature:", string(sig))
		}
		return errMalformedEncoding
	}

	// TODO: It seems like we may have to be prepared to rewrite and sort trailing headers:
	// https://docs.aws.amazon.com/IAM/latest/UserGuide/create-signed-request.html

	// Any value must end with a newline.
	// Not all clients send that.
	trailerRaw := valueBuffer.Bytes()
	if len(trailerRaw) > 0 && trailerRaw[len(trailerRaw)-1] != '\n' {
		valueBuffer.Write([]byte{'\n'})
	}
	sig = sig[len("x-amz-trailer-signature:"):]
	sig = bytes.TrimSpace(sig)
	cr.chunkSHA256Writer.Write(valueBuffer.Bytes())
	wantSig := cr.getTrailerChunkSignature()
	if !compareSignatureV4(string(sig), wantSig) {
		if cr.debug {
			fmt.Printf("signature, want: %q, got %q\nSignature buffer: %q\n", wantSig, string(sig), valueBuffer.String())
		}
		return errSignatureMismatch
	}

	// Parse trailers.
	wantTrailers := make(map[string]struct{}, len(cr.trailers))
	for k := range cr.trailers {
		wantTrailers[strings.ToLower(k)] = struct{}{}
	}
	input := bufio.NewScanner(bytes.NewReader(valueBuffer.Bytes()))
	for input.Scan() {
		line := strings.TrimSpace(input.Text())
		if line == "" {
			continue
		}
		// Find first separator.
		idx := strings.IndexByte(line, trailerKVSeparator[0])
		if idx <= 0 || idx >= len(line) {
			if cr.debug {
				fmt.Printf("index, ':' not found in %q\n", line)
			}
			return errMalformedEncoding
		}
		key := line[:idx]
		value := line[idx+1:]
		if _, ok := wantTrailers[key]; !ok {
			if cr.debug {
				fmt.Printf("%q not found in %q\n", key, cr.trailers)
			}
			return errMalformedEncoding
		}
		cr.trailers.Set(key, value)
		delete(wantTrailers, key)
	}

	// Check if we got all we want.
	if len(wantTrailers) > 0 {
		return io.ErrUnexpectedEOF
	}
	return nil
}

// readCRLF - check if reader only has '\r\n' CRLF character.
// returns malformed encoding if it doesn't.
func readCRLF(reader io.Reader) error {
	buf := make([]byte, 2)
	_, err := io.ReadFull(reader, buf[:2])
	if err != nil {
		return err
	}
	if buf[0] != '\r' || buf[1] != '\n' {
		return errMalformedEncoding
	}
	return nil
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
		switch err {
		case io.EOF:
			err = io.ErrUnexpectedEOF
		case bufio.ErrBufferFull:
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
//
//	"10000;chunk-signature=..." => "10000", "chunk-signature=..."
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
			b -= '0'
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
	return n, err
}

/*
 * Minio Cloud Storage, (C) 2015, 2016 Minio, Inc.
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

package cmd

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"path"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	snappy "github.com/golang/snappy"
	"github.com/minio/minio/cmd/crypto"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/dns"
	"github.com/minio/minio/pkg/hash"
	"github.com/minio/minio/pkg/ioutil"
	"github.com/minio/minio/pkg/wildcard"
	"github.com/skyrings/skyring-common/tools/uuid"
)

const (
	// Minio meta bucket.
	minioMetaBucket = ".minio.sys"
	// Multipart meta prefix.
	mpartMetaPrefix = "multipart"
	// Minio Multipart meta prefix.
	minioMetaMultipartBucket = minioMetaBucket + "/" + mpartMetaPrefix
	// Minio Tmp meta prefix.
	minioMetaTmpBucket = minioMetaBucket + "/tmp"
	// DNS separator (period), used for bucket name validation.
	dnsDelimiter = "."
)

// isMinioBucket returns true if given bucket is a Minio internal
// bucket and false otherwise.
func isMinioMetaBucketName(bucket string) bool {
	return bucket == minioMetaBucket ||
		bucket == minioMetaMultipartBucket ||
		bucket == minioMetaTmpBucket
}

// IsValidBucketName verifies that a bucket name is in accordance with
// Amazon's requirements (i.e. DNS naming conventions). It must be 3-63
// characters long, and it must be a sequence of one or more labels
// separated by periods. Each label can contain lowercase ascii
// letters, decimal digits and hyphens, but must not begin or end with
// a hyphen. See:
// http://docs.aws.amazon.com/AmazonS3/latest/dev/BucketRestrictions.html
func IsValidBucketName(bucket string) bool {
	// Special case when bucket is equal to one of the meta buckets.
	if isMinioMetaBucketName(bucket) {
		return true
	}
	if len(bucket) < 3 || len(bucket) > 63 {
		return false
	}

	// Split on dot and check each piece conforms to rules.
	allNumbers := true
	pieces := strings.Split(bucket, dnsDelimiter)
	for _, piece := range pieces {
		if len(piece) == 0 || piece[0] == '-' ||
			piece[len(piece)-1] == '-' {
			// Current piece has 0-length or starts or
			// ends with a hyphen.
			return false
		}
		// Now only need to check if each piece is a valid
		// 'label' in AWS terminology and if the bucket looks
		// like an IP address.
		isNotNumber := false
		for i := 0; i < len(piece); i++ {
			switch {
			case (piece[i] >= 'a' && piece[i] <= 'z' ||
				piece[i] == '-'):
				// Found a non-digit character, so
				// this piece is not a number.
				isNotNumber = true
			case piece[i] >= '0' && piece[i] <= '9':
				// Nothing to do.
			default:
				// Found invalid character.
				return false
			}
		}
		allNumbers = allNumbers && !isNotNumber
	}
	// Does the bucket name look like an IP address?
	return !(len(pieces) == 4 && allNumbers)
}

// IsValidObjectName verifies an object name in accordance with Amazon's
// requirements. It cannot exceed 1024 characters and must be a valid UTF8
// string.
//
// See:
// http://docs.aws.amazon.com/AmazonS3/latest/dev/UsingMetadata.html
//
// You should avoid the following characters in a key name because of
// significant special handling for consistency across all
// applications.
//
// Rejects strings with following characters.
//
// - Backslash ("\")
//
// additionally minio does not support object names with trailing "/".
func IsValidObjectName(object string) bool {
	if len(object) == 0 {
		return false
	}
	if hasSuffix(object, slashSeparator) || hasPrefix(object, slashSeparator) {
		return false
	}
	return IsValidObjectPrefix(object)
}

// IsValidObjectPrefix verifies whether the prefix is a valid object name.
// Its valid to have a empty prefix.
func IsValidObjectPrefix(object string) bool {
	if hasBadPathComponent(object) {
		return false
	}
	if len(object) > 1024 {
		return false
	}
	if !utf8.ValidString(object) {
		return false
	}
	// Reject unsupported characters in object name.
	if strings.ContainsAny(object, "\\") {
		return false
	}
	return true
}

// Slash separator.
const slashSeparator = "/"

// retainSlash - retains slash from a path.
func retainSlash(s string) string {
	return strings.TrimSuffix(s, slashSeparator) + slashSeparator
}

// pathJoin - like path.Join() but retains trailing "/" of the last element
func pathJoin(elem ...string) string {
	trailingSlash := ""
	if len(elem) > 0 {
		if hasSuffix(elem[len(elem)-1], slashSeparator) {
			trailingSlash = "/"
		}
	}
	return path.Join(elem...) + trailingSlash
}

// mustGetUUID - get a random UUID.
func mustGetUUID() string {
	uuid, err := uuid.New()
	if err != nil {
		logger.CriticalIf(context.Background(), err)
	}

	return uuid.String()
}

// Create an s3 compatible MD5sum for complete multipart transaction.
func getCompleteMultipartMD5(ctx context.Context, parts []CompletePart) (string, error) {
	var finalMD5Bytes []byte
	for _, part := range parts {
		md5Bytes, err := hex.DecodeString(part.ETag)
		if err != nil {
			logger.LogIf(ctx, err)
			return "", err
		}
		finalMD5Bytes = append(finalMD5Bytes, md5Bytes...)
	}
	s3MD5 := fmt.Sprintf("%s-%d", getMD5Hash(finalMD5Bytes), len(parts))
	return s3MD5, nil
}

// Clean unwanted fields from metadata
func cleanMetadata(metadata map[string]string) map[string]string {
	// Remove STANDARD StorageClass
	metadata = removeStandardStorageClass(metadata)
	// Clean meta etag keys 'md5Sum', 'etag'.
	return cleanMetadataKeys(metadata, "md5Sum", "etag")
}

// Filter X-Amz-Storage-Class field only if it is set to STANDARD.
// This is done since AWS S3 doesn't return STANDARD Storage class as response header.
func removeStandardStorageClass(metadata map[string]string) map[string]string {
	if metadata[amzStorageClass] == standardStorageClass {
		delete(metadata, amzStorageClass)
	}
	return metadata
}

// cleanMetadataKeys takes keyNames to be filtered
// and returns a new map with all the entries with keyNames removed.
func cleanMetadataKeys(metadata map[string]string, keyNames ...string) map[string]string {
	var newMeta = make(map[string]string)
	for k, v := range metadata {
		if contains(keyNames, k) {
			continue
		}
		newMeta[k] = v
	}
	return newMeta
}

// Extracts etag value from the metadata.
func extractETag(metadata map[string]string) string {
	// md5Sum tag is kept for backward compatibility.
	etag, ok := metadata["md5Sum"]
	if !ok {
		etag = metadata["etag"]
	}
	// Success.
	return etag
}

// Prefix matcher string matches prefix in a platform specific way.
// For example on windows since its case insensitive we are supposed
// to do case insensitive checks.
func hasPrefix(s string, prefix string) bool {
	if runtime.GOOS == globalWindowsOSName {
		return strings.HasPrefix(strings.ToLower(s), strings.ToLower(prefix))
	}
	return strings.HasPrefix(s, prefix)
}

// Suffix matcher string matches suffix in a platform specific way.
// For example on windows since its case insensitive we are supposed
// to do case insensitive checks.
func hasSuffix(s string, suffix string) bool {
	if runtime.GOOS == globalWindowsOSName {
		return strings.HasSuffix(strings.ToLower(s), strings.ToLower(suffix))
	}
	return strings.HasSuffix(s, suffix)
}

// Validates if two strings are equal.
func isStringEqual(s1 string, s2 string) bool {
	if runtime.GOOS == globalWindowsOSName {
		return strings.EqualFold(s1, s2)
	}
	return s1 == s2
}

// Ignores all reserved bucket names or invalid bucket names.
func isReservedOrInvalidBucket(bucketEntry string) bool {
	bucketEntry = strings.TrimSuffix(bucketEntry, slashSeparator)
	if !IsValidBucketName(bucketEntry) {
		return true
	}
	return isMinioMetaBucket(bucketEntry) || isMinioReservedBucket(bucketEntry)
}

// Returns true if input bucket is a reserved minio meta bucket '.minio.sys'.
func isMinioMetaBucket(bucketName string) bool {
	return bucketName == minioMetaBucket
}

// Returns true if input bucket is a reserved minio bucket 'minio'.
func isMinioReservedBucket(bucketName string) bool {
	return bucketName == minioReservedBucket
}

// returns a slice of hosts by reading a slice of DNS records
func getHostsSlice(records []dns.SrvRecord) []string {
	var hosts []string
	for _, r := range records {
		hosts = append(hosts, r.Host)
	}
	return hosts
}

// returns a host (and corresponding port) from a slice of DNS records
func getHostFromSrv(records []dns.SrvRecord) string {
	rand.Seed(time.Now().Unix())
	srvRecord := records[rand.Intn(len(records))]
	return net.JoinHostPort(srvRecord.Host, fmt.Sprintf("%d", srvRecord.Port))
}

// IsCompressed returns true if the object is marked as compressed.
func (o ObjectInfo) IsCompressed() bool {
	_, ok := o.UserDefined[ReservedMetadataPrefix+"compression"]
	return ok
}

// GetActualSize - read the decompressed size from the meta json.
func (o ObjectInfo) GetActualSize() int64 {
	metadata := o.UserDefined
	sizeStr, ok := metadata[ReservedMetadataPrefix+"actual-size"]
	if ok {
		size, err := strconv.ParseInt(sizeStr, 10, 64)
		if err == nil {
			return size
		}
	}
	return -1
}

// Disabling compression for encrypted enabled requests.
// Using compression and encryption together enables room for side channel attacks.
// Eliminate non-compressible objects by extensions/content-types.
func isCompressible(header http.Header, object string) bool {
	if hasServerSideEncryptionHeader(header) || excludeForCompression(header, object) {
		return false
	}
	return true
}

// Eliminate the non-compressible objects.
func excludeForCompression(header http.Header, object string) bool {
	objStr := object
	contentType := header.Get("Content-Type")
	if globalIsCompressionEnabled {
		// We strictly disable compression for standard extensions/content-types (`compressed`).
		if hasStringSuffixInSlice(objStr, standardExcludeCompressExtensions) || hasPattern(standardExcludeCompressContentTypes, contentType) {
			return true
		}
		// Filter compression includes.
		if len(globalCompressExtensions) > 0 || len(globalCompressMimeTypes) > 0 {
			extensions := globalCompressExtensions
			mimeTypes := globalCompressMimeTypes
			if hasStringSuffixInSlice(objStr, extensions) || hasPattern(mimeTypes, contentType) {
				return false
			}
			return true
		}
		return false
	}
	return true
}

// Utility which returns if a string is present in the list.
func hasStringSuffixInSlice(str string, list []string) bool {
	for _, v := range list {
		if strings.HasSuffix(str, v) {
			return true
		}
	}
	return false
}

// Returns true if any of the given wildcard patterns match the matchStr.
func hasPattern(patterns []string, matchStr string) bool {
	for _, pattern := range patterns {
		if ok := wildcard.MatchSimple(pattern, matchStr); ok {
			return true
		}
	}
	return false
}

// Returns the part file name which matches the partNumber and etag.
func getPartFile(entries []string, partNumber int, etag string) string {
	for _, entry := range entries {
		if strings.HasPrefix(entry, fmt.Sprintf("%.5d.%s.", partNumber, etag)) {
			return entry
		}
	}
	return ""
}

// Returs the compressed offset which should be skipped.
func getCompressedOffsets(objectInfo ObjectInfo, offset int64) (int64, int64) {
	var compressedOffset int64
	var skipLength int64
	var cumulativeActualSize int64
	if len(objectInfo.Parts) > 0 {
		for _, part := range objectInfo.Parts {
			cumulativeActualSize += part.ActualSize
			if cumulativeActualSize <= offset {
				compressedOffset += part.Size
			} else {
				skipLength = cumulativeActualSize - part.ActualSize
				break
			}
		}
	}
	return compressedOffset, offset - skipLength
}

// byBucketName is a collection satisfying sort.Interface.
type byBucketName []BucketInfo

func (d byBucketName) Len() int           { return len(d) }
func (d byBucketName) Swap(i, j int)      { d[i], d[j] = d[j], d[i] }
func (d byBucketName) Less(i, j int) bool { return d[i].Name < d[j].Name }

// GetObjectReader is a type that wraps a reader with a lock to
// provide a ReadCloser interface that unlocks on Close()
type GetObjectReader struct {
	ObjInfo ObjectInfo
	pReader io.Reader

	cleanUpFns []func()
	once       sync.Once
}

// NewGetObjectReaderFromReader sets up a GetObjectReader with a given
// reader. This ignores any object properties.
func NewGetObjectReaderFromReader(r io.Reader, oi ObjectInfo, cleanupFns ...func()) *GetObjectReader {
	return &GetObjectReader{
		ObjInfo:    oi,
		pReader:    r,
		cleanUpFns: cleanupFns,
	}
}

// ObjReaderFn is a function type that takes a reader and returns
// GetObjectReader and an error. Request headers are passed to provide
// encryption parameters. cleanupFns allow cleanup funcs to be
// registered for calling after usage of the reader.
type ObjReaderFn func(inputReader io.Reader, h http.Header, cleanupFns ...func()) (r *GetObjectReader, err error)

// NewGetObjectReader creates a new GetObjectReader. The cleanUpFns
// are called on Close() in reverse order as passed here. NOTE: It is
// assumed that clean up functions do not panic (otherwise, they may
// not all run!).
func NewGetObjectReader(rs *HTTPRangeSpec, oi ObjectInfo, cleanUpFns ...func()) (
	fn ObjReaderFn, off, length int64, err error) {

	// Call the clean-up functions immediately in case of exit
	// with error
	defer func() {
		if err != nil {
			for i := len(cleanUpFns) - 1; i >= 0; i-- {
				cleanUpFns[i]()
			}
		}
	}()

	isEncrypted := crypto.IsEncrypted(oi.UserDefined)
	isCompressed := oi.IsCompressed()
	var skipLen int64
	// Calculate range to read (different for
	// e.g. encrypted/compressed objects)
	switch {
	case isEncrypted:
		var seqNumber uint32
		var partStart int
		off, length, skipLen, seqNumber, partStart, err = oi.GetDecryptedRange(rs)
		if err != nil {
			return nil, 0, 0, err
		}
		var decSize int64
		decSize, err = oi.DecryptedSize()
		if err != nil {
			return nil, 0, 0, err
		}
		var decRangeLength int64
		decRangeLength, err = rs.GetLength(decSize)
		if err != nil {
			return nil, 0, 0, err
		}

		// We define a closure that performs decryption given
		// a reader that returns the desired range of
		// encrypted bytes. The header parameter is used to
		// provide encryption parameters.
		fn = func(inputReader io.Reader, h http.Header, cFns ...func()) (r *GetObjectReader, err error) {

			copySource := h.Get(crypto.SSECopyAlgorithm) != ""

			cFns = append(cleanUpFns, cFns...)
			// Attach decrypter on inputReader
			var decReader io.Reader
			decReader, err = DecryptBlocksRequestR(inputReader, h,
				off, length, seqNumber, partStart, oi, copySource)
			if err != nil {
				// Call the cleanup funcs
				for i := len(cFns) - 1; i >= 0; i-- {
					cFns[i]()
				}
				return nil, err
			}

			// Decrypt the ETag before top layer consumes this value.
			oi.ETag = getDecryptedETag(h, oi, copySource)

			// Apply the skipLen and limit on the
			// decrypted stream
			decReader = io.LimitReader(ioutil.NewSkipReader(decReader, skipLen), decRangeLength)

			// Assemble the GetObjectReader
			r = &GetObjectReader{
				ObjInfo:    oi,
				pReader:    decReader,
				cleanUpFns: cFns,
			}
			return r, nil
		}
	case isCompressed:
		// Read the decompressed size from the meta.json.
		actualSize := oi.GetActualSize()
		if actualSize < 0 {
			return nil, 0, 0, errInvalidDecompressedSize
		}
		off, length = int64(0), oi.Size
		decOff, decLength := int64(0), actualSize
		if rs != nil {
			off, length, err = rs.GetOffsetLength(actualSize)
			if err != nil {
				return nil, 0, 0, err
			}
			// Incase of range based queries on multiparts, the offset and length are reduced.
			off, decOff = getCompressedOffsets(oi, off)
			decLength = length
			length = oi.Size - off

			// For negative length we read everything.
			if decLength < 0 {
				decLength = actualSize - decOff
			}

			// Reply back invalid range if the input offset and length fall out of range.
			if decOff > actualSize || decOff+decLength > actualSize {
				return nil, 0, 0, errInvalidRange
			}
		}
		fn = func(inputReader io.Reader, _ http.Header, cFns ...func()) (r *GetObjectReader, err error) {
			// Decompression reader.
			snappyReader := snappy.NewReader(inputReader)
			// Apply the skipLen and limit on the
			// decompressed stream
			decReader := io.LimitReader(ioutil.NewSkipReader(snappyReader, decOff), decLength)
			oi.Size = decLength

			// Assemble the GetObjectReader
			r = &GetObjectReader{
				ObjInfo:    oi,
				pReader:    decReader,
				cleanUpFns: append(cleanUpFns, cFns...),
			}
			return r, nil
		}

	default:
		off, length, err = rs.GetOffsetLength(oi.Size)
		if err != nil {
			return nil, 0, 0, err
		}
		fn = func(inputReader io.Reader, _ http.Header, cFns ...func()) (r *GetObjectReader, err error) {
			r = &GetObjectReader{
				ObjInfo:    oi,
				pReader:    inputReader,
				cleanUpFns: append(cleanUpFns, cFns...),
			}
			return r, nil
		}
	}

	return fn, off, length, nil
}

// Close - calls the cleanup actions in reverse order
func (g *GetObjectReader) Close() error {
	// sync.Once is used here to ensure that Close() is
	// idempotent.
	g.once.Do(func() {
		for i := len(g.cleanUpFns) - 1; i >= 0; i-- {
			g.cleanUpFns[i]()
		}
	})
	return nil
}

// Read - to implement Reader interface.
func (g *GetObjectReader) Read(p []byte) (n int, err error) {
	n, err = g.pReader.Read(p)
	if err != nil {
		// Calling code may not Close() in case of error, so
		// we ensure it.
		g.Close()
	}
	return
}

//SealMD5CurrFn seals md5sum with object encryption key and returns sealed
// md5sum
type SealMD5CurrFn func([]byte) []byte

// PutObjReader is a type that wraps sio.EncryptReader and
// underlying hash.Reader in a struct
type PutObjReader struct {
	*hash.Reader              // actual data stream
	rawReader    *hash.Reader // original data stream
	sealMD5Fn    SealMD5CurrFn
}

// Size returns the absolute number of bytes the Reader
// will return during reading. It returns -1 for unlimited
// data.
func (p *PutObjReader) Size() int64 {
	return p.Reader.Size()
}

// MD5CurrentHexString returns the current MD5Sum or encrypted MD5Sum
// as a hex encoded string
func (p *PutObjReader) MD5CurrentHexString() string {
	md5sumCurr := p.rawReader.MD5Current()
	if p.sealMD5Fn != nil {
		md5sumCurr = p.sealMD5Fn(md5sumCurr)
	}
	return hex.EncodeToString(md5sumCurr)
}

// NewPutObjReader returns a new PutObjReader and holds
// reference to underlying data stream from client and the encrypted
// data reader
func NewPutObjReader(rawReader *hash.Reader, encReader *hash.Reader, encKey []byte) *PutObjReader {
	p := PutObjReader{Reader: rawReader, rawReader: rawReader}

	if len(encKey) != 0 && encReader != nil {
		var objKey crypto.ObjectKey
		copy(objKey[:], encKey)
		p.sealMD5Fn = sealETagFn(objKey)
		p.Reader = encReader
	}

	return &p
}

func sealETag(encKey crypto.ObjectKey, md5CurrSum []byte) []byte {
	var emptyKey [32]byte
	if bytes.Equal(encKey[:], emptyKey[:]) {
		return md5CurrSum
	}
	return encKey.SealETag(md5CurrSum)
}

func sealETagFn(key crypto.ObjectKey) SealMD5CurrFn {
	fn := func(md5sumcurr []byte) []byte {
		return sealETag(key, md5sumcurr)
	}
	return fn
}

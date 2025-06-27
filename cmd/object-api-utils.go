// Copyright (c) 2015-2024 MinIO, Inc.
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

package cmd

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"path"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
	"unsafe"

	"github.com/google/uuid"
	"github.com/klauspost/compress/s2"
	"github.com/klauspost/readahead"
	"github.com/minio/minio-go/v7/pkg/s3utils"
	"github.com/minio/minio/internal/config/compress"
	"github.com/minio/minio/internal/config/dns"
	"github.com/minio/minio/internal/config/storageclass"
	"github.com/minio/minio/internal/crypto"
	"github.com/minio/minio/internal/hash"
	xhttp "github.com/minio/minio/internal/http"
	xioutil "github.com/minio/minio/internal/ioutil"
	"github.com/minio/minio/internal/logger"
	"github.com/minio/pkg/v3/trie"
	"github.com/minio/pkg/v3/wildcard"
	"github.com/valyala/bytebufferpool"
)

const (
	// MinIO meta bucket.
	minioMetaBucket = ".minio.sys"
	// Multipart meta prefix.
	mpartMetaPrefix = "multipart"
	// MinIO Multipart meta prefix.
	minioMetaMultipartBucket = minioMetaBucket + SlashSeparator + mpartMetaPrefix
	// MinIO tmp meta prefix.
	minioMetaTmpBucket = minioMetaBucket + "/tmp"
	// MinIO tmp meta prefix for deleted objects.
	minioMetaTmpDeletedBucket = minioMetaTmpBucket + "/.trash"

	// DNS separator (period), used for bucket name validation.
	dnsDelimiter = "."
	// On compressed files bigger than this;
	compReadAheadSize = 100 << 20
	// Read this many buffers ahead.
	compReadAheadBuffers = 5
	// Size of each buffer.
	compReadAheadBufSize = 1 << 20
	// Pad Encrypted+Compressed files to a multiple of this.
	compPadEncrypted = 256
	// Disable compressed file indices below this size
	compMinIndexSize = 8 << 20
)

// getkeyeparator - returns the separator to be used for
// persisting on drive.
//
// - ":" is used on non-windows platforms
// - "_" is used on windows platforms
func getKeySeparator() string {
	if runtime.GOOS == globalWindowsOSName {
		return "_"
	}
	return ":"
}

// isMinioBucket returns true if given bucket is a MinIO internal
// bucket and false otherwise.
func isMinioMetaBucketName(bucket string) bool {
	return strings.HasPrefix(bucket, minioMetaBucket)
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
		for i := range len(piece) {
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
	return len(pieces) != 4 || !allNumbers
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
// additionally minio does not support object names with trailing SlashSeparator.
func IsValidObjectName(object string) bool {
	if len(object) == 0 {
		return false
	}
	if HasSuffix(object, SlashSeparator) {
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
	if !utf8.ValidString(object) {
		return false
	}
	if strings.Contains(object, `//`) {
		return false
	}
	// This is valid for AWS S3 but it will never
	// work with file systems, we will reject here
	// to return object name invalid rather than
	// a cryptic error from the file system.
	return !strings.ContainsRune(object, 0)
}

// checkObjectNameForLengthAndSlash -check for the validity of object name length and prefis as slash
func checkObjectNameForLengthAndSlash(bucket, object string) error {
	// Check for the length of object name
	if len(object) > 1024 {
		return ObjectNameTooLong{
			Bucket: bucket,
			Object: object,
		}
	}
	// Check for slash as prefix in object name
	if HasPrefix(object, SlashSeparator) {
		return ObjectNamePrefixAsSlash{
			Bucket: bucket,
			Object: object,
		}
	}
	if runtime.GOOS == globalWindowsOSName {
		// Explicitly disallowed characters on windows.
		// Avoids most problematic names.
		if strings.ContainsAny(object, `\:*?"|<>`) {
			return ObjectNameInvalid{
				Bucket: bucket,
				Object: object,
			}
		}
	}
	return nil
}

// SlashSeparator - slash separator.
const SlashSeparator = "/"

// SlashSeparatorChar - slash separator.
const SlashSeparatorChar = '/'

// retainSlash - retains slash from a path.
func retainSlash(s string) string {
	if s == "" {
		return s
	}
	return strings.TrimSuffix(s, SlashSeparator) + SlashSeparator
}

// pathsJoinPrefix - like pathJoin retains trailing SlashSeparator
// for all elements, prepends them with 'prefix' respectively.
func pathsJoinPrefix(prefix string, elem ...string) (paths []string) {
	paths = make([]string, len(elem))
	for i, e := range elem {
		paths[i] = pathJoin(prefix, e)
	}
	return paths
}

// string concat alternative to s1 + s2 with low overhead.
func concat(ss ...string) string {
	length := len(ss)
	if length == 0 {
		return ""
	}
	// create & allocate the memory in advance.
	n := 0
	for i := range length {
		n += len(ss[i])
	}
	b := make([]byte, 0, n)
	for i := range length {
		b = append(b, ss[i]...)
	}
	return unsafe.String(unsafe.SliceData(b), n)
}

// pathJoin - like path.Join() but retains trailing SlashSeparator of the last element
func pathJoin(elem ...string) string {
	sb := bytebufferpool.Get()
	defer func() {
		sb.Reset()
		bytebufferpool.Put(sb)
	}()

	return pathJoinBuf(sb, elem...)
}

// pathJoinBuf - like path.Join() but retains trailing SlashSeparator of the last element.
// Provide a string builder to reduce allocation.
func pathJoinBuf(dst *bytebufferpool.ByteBuffer, elem ...string) string {
	trailingSlash := len(elem) > 0 && hasSuffixByte(elem[len(elem)-1], SlashSeparatorChar)
	dst.Reset()
	added := 0
	for _, e := range elem {
		if added > 0 || e != "" {
			if added > 0 {
				dst.WriteByte(SlashSeparatorChar)
			}
			dst.WriteString(e)
			added += len(e)
		}
	}

	if pathNeedsClean(dst.Bytes()) {
		s := path.Clean(dst.String())
		if trailingSlash {
			return s + SlashSeparator
		}
		return s
	}
	if trailingSlash {
		dst.WriteByte(SlashSeparatorChar)
	}
	return dst.String()
}

// hasSuffixByte returns true if the last byte of s is 'suffix'
func hasSuffixByte(s string, suffix byte) bool {
	return len(s) > 0 && s[len(s)-1] == suffix
}

// pathNeedsClean returns whether path.Clean may change the path.
// Will detect all cases that will be cleaned,
// but may produce false positives on non-trivial paths.
func pathNeedsClean(path []byte) bool {
	if len(path) == 0 {
		return true
	}

	rooted := path[0] == '/'
	n := len(path)

	r, w := 0, 0
	if rooted {
		r, w = 1, 1
	}

	for r < n {
		switch {
		case path[r] > 127:
			// Non ascii.
			return true
		case path[r] == '/':
			// multiple / elements
			return true
		case path[r] == '.' && (r+1 == n || path[r+1] == '/'):
			// . element - assume it has to be cleaned.
			return true
		case path[r] == '.' && path[r+1] == '.' && (r+2 == n || path[r+2] == '/'):
			// .. element: remove to last / - assume it has to be cleaned.
			return true
		default:
			// real path element.
			// add slash if needed
			if rooted && w != 1 || !rooted && w != 0 {
				w++
			}
			// copy element
			for ; r < n && path[r] != '/'; r++ {
				w++
			}
			// allow one slash, not at end
			if r < n-1 && path[r] == '/' {
				r++
			}
		}
	}

	// Turn empty string into "."
	if w == 0 {
		return true
	}

	return false
}

// mustGetUUID - get a random UUID.
func mustGetUUID() string {
	u, err := uuid.NewRandom()
	if err != nil {
		logger.CriticalIf(GlobalContext, err)
	}

	return u.String()
}

// mustGetUUIDBytes - get a random UUID as 16 bytes unencoded.
func mustGetUUIDBytes() []byte {
	u, err := uuid.NewRandom()
	if err != nil {
		logger.CriticalIf(GlobalContext, err)
	}
	return u[:]
}

// Create an s3 compatible MD5sum for complete multipart transaction.
func getCompleteMultipartMD5(parts []CompletePart) string {
	var finalMD5Bytes []byte
	for _, part := range parts {
		md5Bytes, err := hex.DecodeString(canonicalizeETag(part.ETag))
		if err != nil {
			finalMD5Bytes = append(finalMD5Bytes, []byte(part.ETag)...)
		} else {
			finalMD5Bytes = append(finalMD5Bytes, md5Bytes...)
		}
	}
	s3MD5 := fmt.Sprintf("%s-%d", getMD5Hash(finalMD5Bytes), len(parts))
	return s3MD5
}

// Clean unwanted fields from metadata
func cleanMetadata(metadata map[string]string) map[string]string {
	// Remove STANDARD StorageClass
	metadata = removeStandardStorageClass(metadata)
	// Clean meta etag keys 'md5Sum', 'etag', "expires", "x-amz-tagging".
	return cleanMetadataKeys(metadata, "md5Sum", "etag", "expires", xhttp.AmzObjectTagging, "last-modified", VersionPurgeStatusKey)
}

// Filter X-Amz-Storage-Class field only if it is set to STANDARD.
// This is done since AWS S3 doesn't return STANDARD Storage class as response header.
func removeStandardStorageClass(metadata map[string]string) map[string]string {
	if metadata[xhttp.AmzStorageClass] == storageclass.STANDARD {
		delete(metadata, xhttp.AmzStorageClass)
	}
	return metadata
}

// cleanMetadataKeys takes keyNames to be filtered
// and returns a new map with all the entries with keyNames removed.
func cleanMetadataKeys(metadata map[string]string, keyNames ...string) map[string]string {
	newMeta := make(map[string]string, len(metadata))
	for k, v := range metadata {
		if slices.Contains(keyNames, k) {
			continue
		}
		newMeta[k] = v
	}
	return newMeta
}

// Extracts etag value from the metadata.
func extractETag(metadata map[string]string) string {
	etag, ok := metadata["etag"]
	if !ok {
		// md5Sum tag is kept for backward compatibility.
		etag = metadata["md5Sum"]
	}
	// Success.
	return etag
}

// HasPrefix - Prefix matcher string matches prefix in a platform specific way.
// For example on windows since its case insensitive we are supposed
// to do case insensitive checks.
func HasPrefix(s string, prefix string) bool {
	if runtime.GOOS == globalWindowsOSName {
		return stringsHasPrefixFold(s, prefix)
	}
	return strings.HasPrefix(s, prefix)
}

// HasSuffix - Suffix matcher string matches suffix in a platform specific way.
// For example on windows since its case insensitive we are supposed
// to do case insensitive checks.
func HasSuffix(s string, suffix string) bool {
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
func isReservedOrInvalidBucket(bucketEntry string, strict bool) bool {
	if bucketEntry == "" {
		return true
	}

	bucketEntry = strings.TrimSuffix(bucketEntry, SlashSeparator)
	if strict {
		if err := s3utils.CheckValidBucketNameStrict(bucketEntry); err != nil {
			return true
		}
	} else {
		if err := s3utils.CheckValidBucketName(bucketEntry); err != nil {
			return true
		}
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
	hosts := make([]string, len(records))
	for i, r := range records {
		hosts[i] = net.JoinHostPort(r.Host, string(r.Port))
	}
	return hosts
}

// returns an online host (and corresponding port) from a slice of DNS records
func getHostFromSrv(records []dns.SrvRecord) (host string) {
	hosts := getHostsSlice(records)
	rng := rand.New(rand.NewSource(time.Now().UTC().UnixNano()))
	var d net.Dialer
	var retry int
	for retry < len(hosts) {
		ctx, cancel := context.WithTimeout(GlobalContext, 300*time.Millisecond)

		host = hosts[rng.Intn(len(hosts))]
		conn, err := d.DialContext(ctx, "tcp", host)
		cancel()
		if err != nil {
			retry++
			continue
		}
		conn.Close()
		break
	}

	return host
}

// IsCompressed returns true if the object is marked as compressed.
func (o *ObjectInfo) IsCompressed() bool {
	_, ok := o.UserDefined[ReservedMetadataPrefix+"compression"]
	return ok
}

// IsCompressedOK returns whether the object is compressed and can be decompressed.
func (o *ObjectInfo) IsCompressedOK() (bool, error) {
	scheme, ok := o.UserDefined[ReservedMetadataPrefix+"compression"]
	if !ok {
		return false, nil
	}
	switch scheme {
	case compressionAlgorithmV1, compressionAlgorithmV2:
		return true, nil
	}
	return true, fmt.Errorf("unknown compression scheme: %s", scheme)
}

// GetActualSize - returns the actual size of the stored object
func (o ObjectInfo) GetActualSize() (int64, error) {
	if o.ActualSize != nil {
		return *o.ActualSize, nil
	}
	if o.IsCompressed() {
		sizeStr := o.UserDefined[ReservedMetadataPrefix+"actual-size"]
		if sizeStr != "" {
			size, err := strconv.ParseInt(sizeStr, 10, 64)
			if err != nil {
				return -1, errInvalidDecompressedSize
			}
			return size, nil
		}
		var actualSize int64
		for _, part := range o.Parts {
			actualSize += part.ActualSize
		}
		if (actualSize == 0) && (actualSize != o.Size) {
			return -1, errInvalidDecompressedSize
		}
		return actualSize, nil
	}
	if _, ok := crypto.IsEncrypted(o.UserDefined); ok {
		sizeStr := o.UserDefined[ReservedMetadataPrefix+"actual-size"]
		if sizeStr != "" {
			size, err := strconv.ParseInt(sizeStr, 10, 64)
			if err != nil {
				return -1, errObjectTampered
			}
			return size, nil
		}
		actualSize, err := o.DecryptedSize()
		if err != nil {
			return -1, err
		}
		if (actualSize == 0) && (actualSize != o.Size) {
			return -1, errObjectTampered
		}
		return actualSize, nil
	}
	return o.Size, nil
}

// Disabling compression for encrypted enabled requests.
// Using compression and encryption together enables room for side channel attacks.
// Eliminate non-compressible objects by extensions/content-types.
func isCompressible(header http.Header, object string) bool {
	globalCompressConfigMu.Lock()
	cfg := globalCompressConfig
	globalCompressConfigMu.Unlock()

	return !excludeForCompression(header, object, cfg)
}

// Eliminate the non-compressible objects.
func excludeForCompression(header http.Header, object string, cfg compress.Config) bool {
	objStr := object
	contentType := header.Get(xhttp.ContentType)
	if !cfg.Enabled {
		return true
	}

	if crypto.Requested(header) && !cfg.AllowEncrypted {
		return true
	}

	// We strictly disable compression for standard extensions/content-types (`compressed`).
	if hasStringSuffixInSlice(objStr, standardExcludeCompressExtensions) || hasPattern(standardExcludeCompressContentTypes, contentType) {
		return true
	}

	// Filter compression includes.
	if len(cfg.Extensions) == 0 && len(cfg.MimeTypes) == 0 {
		// Nothing to filter, include everything.
		return false
	}

	if len(cfg.Extensions) > 0 && hasStringSuffixInSlice(objStr, cfg.Extensions) {
		// Matched an extension to compress, do not exclude.
		return false
	}

	if len(cfg.MimeTypes) > 0 && hasPattern(cfg.MimeTypes, contentType) {
		// Matched an MIME type to compress, do not exclude.
		return false
	}

	// Did not match any inclusion filters, exclude from compression.
	return true
}

// Utility which returns if a string is present in the list.
// Comparison is case insensitive. Explicit short-circuit if
// the list contains the wildcard "*".
func hasStringSuffixInSlice(str string, list []string) bool {
	str = strings.ToLower(str)
	for _, v := range list {
		if v == "*" {
			return true
		}

		if strings.HasSuffix(str, strings.ToLower(v)) {
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
func getPartFile(entriesTrie *trie.Trie, partNumber int, etag string) (partFile string) {
	for _, match := range entriesTrie.PrefixMatch(fmt.Sprintf("%.5d.%s.", partNumber, etag)) {
		partFile = match
		break
	}
	return partFile
}

func partNumberToRangeSpec(oi ObjectInfo, partNumber int) *HTTPRangeSpec {
	if oi.Size == 0 || len(oi.Parts) == 0 {
		return nil
	}

	var start int64
	end := int64(-1)
	for i := 0; i < len(oi.Parts) && i < partNumber; i++ {
		start = end + 1
		end = start + oi.Parts[i].ActualSize - 1
	}

	return &HTTPRangeSpec{Start: start, End: end}
}

// Returns the compressed offset which should be skipped.
// If encrypted offsets are adjusted for encrypted block headers/trailers.
// Since de-compression is after decryption encryption overhead is only added to compressedOffset.
func getCompressedOffsets(oi ObjectInfo, offset int64, decrypt func([]byte) ([]byte, error)) (compressedOffset int64, partSkip int64, firstPart int, decryptSkip int64, seqNum uint32) {
	var skipLength int64
	var cumulativeActualSize int64
	var firstPartIdx int
	for i, part := range oi.Parts {
		cumulativeActualSize += part.ActualSize
		if cumulativeActualSize <= offset {
			compressedOffset += part.Size
		} else {
			firstPartIdx = i
			skipLength = cumulativeActualSize - part.ActualSize
			break
		}
	}
	partSkip = offset - skipLength

	// Load index and skip more if feasible.
	if partSkip > 0 && len(oi.Parts) > firstPartIdx && len(oi.Parts[firstPartIdx].Index) > 0 {
		_, isEncrypted := crypto.IsEncrypted(oi.UserDefined)
		if isEncrypted {
			dec, err := decrypt(oi.Parts[firstPartIdx].Index)
			if err == nil {
				// Load Index
				var idx s2.Index
				_, err := idx.Load(s2.RestoreIndexHeaders(dec))

				// Find compressed/uncompressed offsets of our partskip
				compOff, uCompOff, err2 := idx.Find(partSkip)

				if err == nil && err2 == nil && compOff > 0 {
					// Encrypted.
					const sseDAREEncPackageBlockSize = SSEDAREPackageBlockSize + SSEDAREPackageMetaSize
					// Number of full blocks in skipped area
					seqNum = uint32(compOff / SSEDAREPackageBlockSize)
					// Skip this many inside a decrypted block to get to compression block start
					decryptSkip = compOff % SSEDAREPackageBlockSize
					// Skip this number of full blocks.
					skipEnc := compOff / SSEDAREPackageBlockSize
					skipEnc *= sseDAREEncPackageBlockSize
					compressedOffset += skipEnc
					// Skip this number of uncompressed bytes.
					partSkip -= uCompOff
				}
			}
		} else {
			// Not encrypted
			var idx s2.Index
			_, err := idx.Load(s2.RestoreIndexHeaders(oi.Parts[firstPartIdx].Index))

			// Find compressed/uncompressed offsets of our partskip
			compOff, uCompOff, err2 := idx.Find(partSkip)

			if err == nil && err2 == nil && compOff > 0 {
				compressedOffset += compOff
				partSkip -= uCompOff
			}
		}
	}

	return compressedOffset, partSkip, firstPartIdx, decryptSkip, seqNum
}

// GetObjectReader is a type that wraps a reader with a lock to
// provide a ReadCloser interface that unlocks on Close()
type GetObjectReader struct {
	io.Reader
	ObjInfo    ObjectInfo
	cleanUpFns []func()
	once       sync.Once
}

// WithCleanupFuncs sets additional cleanup functions to be called when closing
// the GetObjectReader.
func (g *GetObjectReader) WithCleanupFuncs(fns ...func()) *GetObjectReader {
	g.cleanUpFns = append(g.cleanUpFns, fns...)
	return g
}

// NewGetObjectReaderFromReader sets up a GetObjectReader with a given
// reader. This ignores any object properties.
func NewGetObjectReaderFromReader(r io.Reader, oi ObjectInfo, opts ObjectOptions, cleanupFns ...func()) (*GetObjectReader, error) {
	if opts.CheckPrecondFn != nil && opts.CheckPrecondFn(oi) {
		// Call the cleanup funcs
		for i := len(cleanupFns) - 1; i >= 0; i-- {
			cleanupFns[i]()
		}
		return nil, PreConditionFailed{}
	}
	return &GetObjectReader{
		ObjInfo:    oi,
		Reader:     r,
		cleanUpFns: cleanupFns,
	}, nil
}

// ObjReaderFn is a function type that takes a reader and returns
// GetObjectReader and an error. Request headers are passed to provide
// encryption parameters. cleanupFns allow cleanup funcs to be
// registered for calling after usage of the reader.
type ObjReaderFn func(inputReader io.Reader, h http.Header, cleanupFns ...func()) (r *GetObjectReader, err error)

// NewGetObjectReader creates a new GetObjectReader. The cleanUpFns
// are called on Close() in FIFO order as passed in ObjReadFn(). NOTE: It is
// assumed that clean up functions do not panic (otherwise, they may
// not all run!).
func NewGetObjectReader(rs *HTTPRangeSpec, oi ObjectInfo, opts ObjectOptions, h http.Header) (
	fn ObjReaderFn, off, length int64, err error,
) {
	if opts.CheckPrecondFn != nil && opts.CheckPrecondFn(oi) {
		return nil, 0, 0, PreConditionFailed{}
	}

	if rs == nil && opts.PartNumber > 0 {
		rs = partNumberToRangeSpec(oi, opts.PartNumber)
	}

	_, isEncrypted := crypto.IsEncrypted(oi.UserDefined)
	isCompressed, err := oi.IsCompressedOK()
	if err != nil {
		return nil, 0, 0, err
	}

	// if object is encrypted and it is a restore request or if NoDecryption
	// was requested, fetch content without decrypting.
	if opts.Transition.RestoreRequest != nil || opts.NoDecryption {
		isEncrypted = false
		isCompressed = false
	}

	// Calculate range to read (different for encrypted/compressed objects)
	switch {
	case isCompressed:
		var firstPart int
		if opts.PartNumber > 0 {
			// firstPart is an index to Parts slice,
			// make sure that PartNumber uses the
			// index value properly.
			firstPart = opts.PartNumber - 1
		}

		// If compressed, we start from the beginning of the part.
		// Read the decompressed size from the meta.json.
		actualSize, err := oi.GetActualSize()
		if err != nil {
			return nil, 0, 0, err
		}
		var decryptSkip int64
		var seqNum uint32

		off, length = int64(0), oi.Size
		decOff, decLength := int64(0), actualSize
		if rs != nil {
			off, length, err = rs.GetOffsetLength(actualSize)
			if err != nil {
				return nil, 0, 0, err
			}
			decrypt := func(b []byte) ([]byte, error) {
				return b, nil
			}
			if isEncrypted {
				decrypt = func(b []byte) ([]byte, error) {
					return oi.compressionIndexDecrypt(b, h)
				}
			}
			// In case of range based queries on multiparts, the offset and length are reduced.
			off, decOff, firstPart, decryptSkip, seqNum = getCompressedOffsets(oi, off, decrypt)
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
		fn = func(inputReader io.Reader, h http.Header, cFns ...func()) (r *GetObjectReader, err error) {
			if isEncrypted {
				copySource := h.Get(xhttp.AmzServerSideEncryptionCopyCustomerAlgorithm) != ""
				// Attach decrypter on inputReader
				inputReader, err = DecryptBlocksRequestR(inputReader, h, seqNum, firstPart, oi, copySource)
				if err != nil {
					// Call the cleanup funcs
					for i := len(cFns) - 1; i >= 0; i-- {
						cFns[i]()
					}
					return nil, err
				}
				if decryptSkip > 0 {
					inputReader = xioutil.NewSkipReader(inputReader, decryptSkip)
				}
				oi.Size = decLength
			}
			// Decompression reader.
			var dopts []s2.ReaderOption
			if off > 0 || decOff > 0 {
				// We are not starting at the beginning, so ignore stream identifiers.
				dopts = append(dopts, s2.ReaderIgnoreStreamIdentifier())
			}
			s2Reader := s2.NewReader(inputReader, dopts...)
			// Apply the skipLen and limit on the decompressed stream.
			if decOff > 0 {
				if err = s2Reader.Skip(decOff); err != nil {
					// Call the cleanup funcs
					for i := len(cFns) - 1; i >= 0; i-- {
						cFns[i]()
					}
					return nil, err
				}
			}

			decReader := io.LimitReader(s2Reader, decLength)
			if decLength > compReadAheadSize {
				rah, err := readahead.NewReaderSize(decReader, compReadAheadBuffers, compReadAheadBufSize)
				if err == nil {
					decReader = rah
					cFns = append([]func(){func() {
						rah.Close()
					}}, cFns...)
				}
			}
			oi.Size = decLength

			// Assemble the GetObjectReader
			r = &GetObjectReader{
				ObjInfo:    oi,
				Reader:     decReader,
				cleanUpFns: cFns,
			}
			return r, nil
		}

	case isEncrypted:
		var seqNumber uint32
		var partStart int
		var skipLen int64

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
			copySource := h.Get(xhttp.AmzServerSideEncryptionCopyCustomerAlgorithm) != ""

			// Attach decrypter on inputReader
			var decReader io.Reader
			decReader, err = DecryptBlocksRequestR(inputReader, h, seqNumber, partStart, oi, copySource)
			if err != nil {
				// Call the cleanup funcs
				for i := len(cFns) - 1; i >= 0; i-- {
					cFns[i]()
				}
				return nil, err
			}

			oi.ETag = getDecryptedETag(h, oi, false)

			// Apply the skipLen and limit on the
			// decrypted stream
			decReader = io.LimitReader(xioutil.NewSkipReader(decReader, skipLen), decRangeLength)

			// Assemble the GetObjectReader
			r = &GetObjectReader{
				ObjInfo:    oi,
				Reader:     decReader,
				cleanUpFns: cFns,
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
				Reader:     inputReader,
				cleanUpFns: cFns,
			}
			return r, nil
		}
	}
	return fn, off, length, nil
}

// Close - calls the cleanup actions in reverse order
func (g *GetObjectReader) Close() error {
	if g == nil {
		return nil
	}
	// sync.Once is used here to ensure that Close() is
	// idempotent.
	g.once.Do(func() {
		for i := len(g.cleanUpFns) - 1; i >= 0; i-- {
			g.cleanUpFns[i]()
		}
	})
	return nil
}

// compressionIndexEncrypter returns a function that will read data from input,
// encrypt it using the provided key and return the result.
func compressionIndexEncrypter(key crypto.ObjectKey, input func() []byte) func() []byte {
	var data []byte
	var fetched bool
	return func() []byte {
		if !fetched {
			data = input()
			fetched = true
		}
		return metadataEncrypter(key)("compression-index", data)
	}
}

// compressionIndexDecrypt reverses compressionIndexEncrypter.
func (o *ObjectInfo) compressionIndexDecrypt(input []byte, h http.Header) ([]byte, error) {
	return o.metadataDecrypter(h)("compression-index", input)
}

// SealMD5CurrFn seals md5sum with object encryption key and returns sealed
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
	var appendHyphen bool
	// md5sumcurr is not empty in two scenarios
	// - server is running in strict compatibility mode
	// - client set Content-Md5 during PUT operation
	if len(md5sumCurr) == 0 {
		// md5sumCurr is only empty when we are running
		// in non-compatibility mode.
		md5sumCurr = make([]byte, 16)
		rand.Read(md5sumCurr)
		appendHyphen = true
	}
	if p.sealMD5Fn != nil {
		md5sumCurr = p.sealMD5Fn(md5sumCurr)
	}
	if appendHyphen {
		// Make sure to return etag string upto 32 length, for SSE
		// requests ETag might be longer and the code decrypting the
		// ETag ignores ETag in multipart ETag form i.e <hex>-N
		return hex.EncodeToString(md5sumCurr)[:32] + "-1"
	}
	return hex.EncodeToString(md5sumCurr)
}

// WithEncryption sets up encrypted reader and the sealing for content md5sum
// using objEncKey. Unsealed md5sum is computed from the rawReader setup when
// NewPutObjReader was called. It returns an error if called on an uninitialized
// PutObjReader.
func (p *PutObjReader) WithEncryption(encReader *hash.Reader, objEncKey *crypto.ObjectKey) (*PutObjReader, error) {
	if p.Reader == nil {
		return nil, errors.New("put-object reader uninitialized")
	}
	p.Reader = encReader
	p.sealMD5Fn = sealETagFn(*objEncKey)
	return p, nil
}

// NewPutObjReader returns a new PutObjReader. It uses given hash.Reader's
// MD5Current method to construct md5sum when requested downstream.
func NewPutObjReader(rawReader *hash.Reader) *PutObjReader {
	return &PutObjReader{Reader: rawReader, rawReader: rawReader}
}

// RawServerSideChecksumResult returns the ServerSideChecksumResult from the
// underlying rawReader, since the PutObjReader might be encrypted data and
// thus any checksum from that would be incorrect.
func (p *PutObjReader) RawServerSideChecksumResult() *hash.Checksum {
	if p.rawReader != nil {
		return p.rawReader.ServerSideChecksumResult
	}
	return nil
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

// compressOpts are the options for writing compressed data.
var compressOpts []s2.WriterOption

func init() {
	if runtime.GOARCH == "amd64" {
		// On amd64 we have assembly and can use stronger compression.
		compressOpts = append(compressOpts, s2.WriterBetterCompression())
	}
}

// newS2CompressReader will read data from r, compress it and return the compressed data as a Reader.
// Use Close to ensure resources are released on incomplete streams.
//
// input 'on' is always recommended such that this function works
// properly, because we do not wish to create an object even if
// client closed the stream prematurely.
func newS2CompressReader(r io.Reader, on int64, encrypted bool) (rc io.ReadCloser, idx func() []byte) {
	pr, pw := io.Pipe()
	// Copy input to compressor
	opts := compressOpts
	if encrypted {
		// The values used for padding are not a security concern,
		// but we choose pseudo-random numbers instead of just zeros.
		rng := rand.New(rand.NewSource(time.Now().UnixNano()))
		opts = append([]s2.WriterOption{s2.WriterPadding(compPadEncrypted), s2.WriterPaddingSrc(rng)}, compressOpts...)
	}
	comp := s2.NewWriter(pw, opts...)
	indexCh := make(chan []byte, 1)
	go func() {
		defer xioutil.SafeClose(indexCh)
		cn, err := io.Copy(comp, r)
		if err != nil {
			comp.Close()
			pw.CloseWithError(err)
			return
		}
		if on > 0 && on != cn {
			// if client didn't sent all data
			// from the client verify here.
			comp.Close()
			pw.CloseWithError(IncompleteBody{})
			return
		}
		// Close the stream.
		// If more than compMinIndexSize was written, generate index.
		if cn > compMinIndexSize {
			idx, err := comp.CloseIndex()
			idx = s2.RemoveIndexHeaders(idx)
			indexCh <- idx
			pw.CloseWithError(err)
			return
		}
		pw.CloseWithError(comp.Close())
	}()
	var gotIdx []byte
	return pr, func() []byte {
		if gotIdx != nil {
			return gotIdx
		}
		// Will get index or nil if closed.
		gotIdx = <-indexCh
		return gotIdx
	}
}

// compressSelfTest performs a self-test to ensure that compression
// algorithms completes a roundtrip. If any algorithm
// produces an incorrect checksum it fails with a hard error.
//
// compressSelfTest tries to catch any issue in the compression implementation
// early instead of silently corrupting data.
func compressSelfTest() {
	// 4 MB block.
	// Approx runtime ~30ms
	data := make([]byte, 4<<20)
	rng := rand.New(rand.NewSource(0))
	for i := range data {
		// Generate compressible stream...
		data[i] = byte(rng.Int63() & 3)
	}
	failOnErr := func(err error) {
		if err != nil {
			logger.Fatal(errSelfTestFailure, "compress: error on self-test: %v", err)
		}
	}
	const skip = 2<<20 + 511
	r, _ := newS2CompressReader(bytes.NewBuffer(data), int64(len(data)), true)
	b, err := io.ReadAll(r)
	failOnErr(err)
	failOnErr(r.Close())
	// Decompression reader.
	s2Reader := s2.NewReader(bytes.NewBuffer(b))
	// Apply the skipLen on the decompressed stream.
	failOnErr(s2Reader.Skip(skip))
	got, err := io.ReadAll(s2Reader)
	failOnErr(err)
	if !bytes.Equal(got, data[skip:]) {
		logger.Fatal(errSelfTestFailure, "compress: self-test roundtrip mismatch.")
	}
}

// getDiskInfos returns the disk information for the provided disks.
// If a disk is nil or an error is returned the result will be nil as well.
func getDiskInfos(ctx context.Context, disks ...StorageAPI) []*DiskInfo {
	res := make([]*DiskInfo, len(disks))
	opts := DiskInfoOptions{}
	for i, disk := range disks {
		if disk == nil {
			continue
		}
		if di, err := disk.DiskInfo(ctx, opts); err == nil {
			res[i] = &di
		}
	}
	return res
}

// hasSpaceFor returns whether the disks in `di` have space for and object of a given size.
func hasSpaceFor(di []*DiskInfo, size int64) (bool, error) {
	// We multiply the size by 2 to account for erasure coding.
	size *= 2
	if size < 0 {
		// If no size, assume diskAssumeUnknownSize.
		size = diskAssumeUnknownSize
	}

	var available uint64
	var total uint64
	var nDisks int
	for _, disk := range di {
		if disk == nil || disk.Total == 0 {
			// Disk offline, no inodes or something else is wrong.
			continue
		}
		nDisks++
		total += disk.Total
		available += disk.Total - disk.Used
	}

	if nDisks < len(di)/2 || nDisks <= 0 {
		var errs []error
		for index, disk := range di {
			switch {
			case disk == nil:
				errs = append(errs, fmt.Errorf("disk[%d]: offline", index))
			case disk.Error != "":
				errs = append(errs, fmt.Errorf("disk %s: %s", disk.Endpoint, disk.Error))
			case disk.Total == 0:
				errs = append(errs, fmt.Errorf("disk %s: total is zero", disk.Endpoint))
			}
		}
		// Log disk errors.
		peersLogIf(context.Background(), errors.Join(errs...))
		return false, fmt.Errorf("not enough online disks to calculate the available space, need %d, found %d", (len(di)/2)+1, nDisks)
	}

	// Check we have enough on each disk, ignoring diskFillFraction.
	perDisk := size / int64(nDisks)
	for _, disk := range di {
		if disk == nil || disk.Total == 0 {
			continue
		}
		if !globalIsErasureSD && disk.FreeInodes < diskMinInodes && disk.UsedInodes > 0 {
			// We have an inode count, but not enough inodes.
			return false, nil
		}
		if int64(disk.Free) <= perDisk {
			return false, nil
		}
	}

	// Make sure we can fit "size" on to the disk without getting above the diskFillFraction
	if available < uint64(size) {
		return false, nil
	}

	// How much will be left after adding the file.
	available -= uint64(size)

	// wantLeft is how much space there at least must be left.
	wantLeft := uint64(float64(total) * (1.0 - diskFillFraction))
	return available > wantLeft, nil
}

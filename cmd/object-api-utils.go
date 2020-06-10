/*
 * MinIO Cloud Storage, (C) 2015-2019 MinIO, Inc.
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

	"github.com/google/uuid"
	"github.com/klauspost/compress/s2"
	"github.com/klauspost/readahead"
	"github.com/minio/minio-go/v6/pkg/s3utils"
	"github.com/minio/minio/cmd/config/compress"
	"github.com/minio/minio/cmd/config/etcd/dns"
	"github.com/minio/minio/cmd/config/storageclass"
	"github.com/minio/minio/cmd/crypto"
	xhttp "github.com/minio/minio/cmd/http"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/hash"
	"github.com/minio/minio/pkg/ioutil"
	"github.com/minio/minio/pkg/wildcard"
)

const (
	// MinIO meta bucket.
	minioMetaBucket = ".minio.sys"
	// Multipart meta prefix.
	mpartMetaPrefix = "multipart"
	// MinIO Multipart meta prefix.
	minioMetaMultipartBucket = minioMetaBucket + SlashSeparator + mpartMetaPrefix
	// MinIO Tmp meta prefix.
	minioMetaTmpBucket = minioMetaBucket + "/tmp"
	// DNS separator (period), used for bucket name validation.
	dnsDelimiter = "."
	// On compressed files bigger than this;
	compReadAheadSize = 100 << 20
	// Read this many buffers ahead.
	compReadAheadBuffers = 5
	// Size of each buffer.
	compReadAheadBufSize = 1 << 20
)

// isMinioBucket returns true if given bucket is a MinIO internal
// bucket and false otherwise.
func isMinioMetaBucketName(bucket string) bool {
	return bucket == minioMetaBucket ||
		bucket == minioMetaMultipartBucket ||
		bucket == minioMetaTmpBucket ||
		bucket == dataUsageBucket
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
	return true
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
		if strings.ContainsAny(object, `:*?"|<>`) {
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

// retainSlash - retains slash from a path.
func retainSlash(s string) string {
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

// pathJoin - like path.Join() but retains trailing SlashSeparator of the last element
func pathJoin(elem ...string) string {
	trailingSlash := ""
	if len(elem) > 0 {
		if HasSuffix(elem[len(elem)-1], SlashSeparator) {
			trailingSlash = SlashSeparator
		}
	}
	return path.Join(elem...) + trailingSlash
}

// mustGetUUID - get a random UUID.
func mustGetUUID() string {
	u, err := uuid.NewRandom()
	if err != nil {
		logger.CriticalIf(GlobalContext, err)
	}

	return u.String()
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
	return cleanMetadataKeys(metadata, "md5Sum", "etag", "expires", xhttp.AmzObjectTagging)
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

// HasPrefix - Prefix matcher string matches prefix in a platform specific way.
// For example on windows since its case insensitive we are supposed
// to do case insensitive checks.
func HasPrefix(s string, prefix string) bool {
	if runtime.GOOS == globalWindowsOSName {
		return strings.HasPrefix(strings.ToLower(s), strings.ToLower(prefix))
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
	var hosts []string
	for _, r := range records {
		hosts = append(hosts, net.JoinHostPort(r.Host, string(r.Port)))
	}
	return hosts
}

// returns a host (and corresponding port) from a slice of DNS records
func getHostFromSrv(records []dns.SrvRecord) string {
	rand.Seed(time.Now().Unix())
	srvRecord := records[rand.Intn(len(records))]
	return net.JoinHostPort(srvRecord.Host, string(srvRecord.Port))
}

// IsCompressed returns true if the object is marked as compressed.
func (o ObjectInfo) IsCompressed() bool {
	_, ok := o.UserDefined[ReservedMetadataPrefix+"compression"]
	return ok
}

// IsCompressedOK returns whether the object is compressed and can be decompressed.
func (o ObjectInfo) IsCompressedOK() (bool, error) {
	scheme, ok := o.UserDefined[ReservedMetadataPrefix+"compression"]
	if !ok {
		return false, nil
	}
	if crypto.IsEncrypted(o.UserDefined) {
		return true, fmt.Errorf("compression %q and encryption enabled on same object", scheme)
	}
	switch scheme {
	case compressionAlgorithmV1, compressionAlgorithmV2:
		return true, nil
	}
	return true, fmt.Errorf("unknown compression scheme: %s", scheme)
}

// GetActualSize - returns the actual size of the stored object
func (o ObjectInfo) GetActualSize() (int64, error) {
	if crypto.IsEncrypted(o.UserDefined) {
		return o.DecryptedSize()
	}
	if o.IsCompressed() {
		sizeStr, ok := o.UserDefined[ReservedMetadataPrefix+"actual-size"]
		if !ok {
			return -1, errInvalidDecompressedSize
		}
		size, err := strconv.ParseInt(sizeStr, 10, 64)
		if err != nil {
			return -1, errInvalidDecompressedSize
		}
		return size, nil
	}
	return o.Size, nil
}

// Disabling compression for encrypted enabled requests.
// Using compression and encryption together enables room for side channel attacks.
// Eliminate non-compressible objects by extensions/content-types.
func isCompressible(header http.Header, object string) bool {
	if crypto.IsRequested(header) || excludeForCompression(header, object, globalCompressConfig) {
		return false
	}
	return true
}

// Eliminate the non-compressible objects.
func excludeForCompression(header http.Header, object string, cfg compress.Config) bool {
	objStr := object
	contentType := header.Get(xhttp.ContentType)
	if !cfg.Enabled {
		return true
	}

	// We strictly disable compression for standard extensions/content-types (`compressed`).
	if hasStringSuffixInSlice(objStr, standardExcludeCompressExtensions) || hasPattern(standardExcludeCompressContentTypes, contentType) {
		return true
	}

	// Filter compression includes.
	if len(cfg.Extensions) == 0 || len(cfg.MimeTypes) == 0 {
		return false
	}

	extensions := cfg.Extensions
	mimeTypes := cfg.MimeTypes
	if hasStringSuffixInSlice(objStr, extensions) || hasPattern(mimeTypes, contentType) {
		return false
	}
	return true
}

// Utility which returns if a string is present in the list.
// Comparison is case insensitive.
func hasStringSuffixInSlice(str string, list []string) bool {
	str = strings.ToLower(str)
	for _, v := range list {
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
func getPartFile(entries []string, partNumber int, etag string) string {
	for _, entry := range entries {
		if strings.HasPrefix(entry, fmt.Sprintf("%.5d.%s.", partNumber, etag)) {
			return entry
		}
	}
	return ""
}

// Returns the compressed offset which should be skipped.
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
	opts       ObjectOptions
	once       sync.Once
}

// NewGetObjectReaderFromReader sets up a GetObjectReader with a given
// reader. This ignores any object properties.
func NewGetObjectReaderFromReader(r io.Reader, oi ObjectInfo, opts ObjectOptions, cleanupFns ...func()) (*GetObjectReader, error) {
	if opts.CheckCopyPrecondFn != nil {
		if ok := opts.CheckCopyPrecondFn(oi, ""); ok {
			// Call the cleanup funcs
			for i := len(cleanupFns) - 1; i >= 0; i-- {
				cleanupFns[i]()
			}
			return nil, PreConditionFailed{}
		}
	}
	return &GetObjectReader{
		ObjInfo:    oi,
		pReader:    r,
		cleanUpFns: cleanupFns,
		opts:       opts,
	}, nil
}

// ObjReaderFn is a function type that takes a reader and returns
// GetObjectReader and an error. Request headers are passed to provide
// encryption parameters. cleanupFns allow cleanup funcs to be
// registered for calling after usage of the reader.
type ObjReaderFn func(inputReader io.Reader, h http.Header, pcfn CheckCopyPreconditionFn, cleanupFns ...func()) (r *GetObjectReader, err error)

// NewGetObjectReader creates a new GetObjectReader. The cleanUpFns
// are called on Close() in reverse order as passed here. NOTE: It is
// assumed that clean up functions do not panic (otherwise, they may
// not all run!).
func NewGetObjectReader(rs *HTTPRangeSpec, oi ObjectInfo, opts ObjectOptions, cleanUpFns ...func()) (
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
	isCompressed, err := oi.IsCompressedOK()
	if err != nil {
		return nil, 0, 0, err
	}

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
		fn = func(inputReader io.Reader, h http.Header, pcfn CheckCopyPreconditionFn, cFns ...func()) (r *GetObjectReader, err error) {
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
			encETag := oi.ETag
			oi.ETag = getDecryptedETag(h, oi, copySource) // Decrypt the ETag before top layer consumes this value.

			if opts.CheckCopyPrecondFn != nil {
				if ok := opts.CheckCopyPrecondFn(oi, encETag); ok {
					// Call the cleanup funcs
					for i := len(cFns) - 1; i >= 0; i-- {
						cFns[i]()
					}
					return nil, PreConditionFailed{}
				}
			}

			// Apply the skipLen and limit on the
			// decrypted stream
			decReader = io.LimitReader(ioutil.NewSkipReader(decReader, skipLen), decRangeLength)

			// Assemble the GetObjectReader
			r = &GetObjectReader{
				ObjInfo:    oi,
				pReader:    decReader,
				cleanUpFns: cFns,
				opts:       opts,
			}
			return r, nil
		}
	case isCompressed:
		// Read the decompressed size from the meta.json.
		actualSize, err := oi.GetActualSize()
		if err != nil {
			return nil, 0, 0, err
		}
		off, length = int64(0), oi.Size
		decOff, decLength := int64(0), actualSize
		if rs != nil {
			off, length, err = rs.GetOffsetLength(actualSize)
			if err != nil {
				return nil, 0, 0, err
			}
			// In case of range based queries on multiparts, the offset and length are reduced.
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
		fn = func(inputReader io.Reader, _ http.Header, pcfn CheckCopyPreconditionFn, cFns ...func()) (r *GetObjectReader, err error) {
			cFns = append(cleanUpFns, cFns...)
			if opts.CheckCopyPrecondFn != nil {
				if ok := opts.CheckCopyPrecondFn(oi, ""); ok {
					// Call the cleanup funcs
					for i := len(cFns) - 1; i >= 0; i-- {
						cFns[i]()
					}
					return nil, PreConditionFailed{}
				}
			}
			// Decompression reader.
			s2Reader := s2.NewReader(inputReader)
			// Apply the skipLen and limit on the decompressed stream.
			err = s2Reader.Skip(decOff)
			if err != nil {
				// Call the cleanup funcs
				for i := len(cFns) - 1; i >= 0; i-- {
					cFns[i]()
				}
				return nil, err
			}

			decReader := io.LimitReader(s2Reader, decLength)
			if decLength > compReadAheadSize {
				rah, err := readahead.NewReaderSize(decReader, compReadAheadBuffers, compReadAheadBufSize)
				if err == nil {
					decReader = rah
					cFns = append(cFns, func() {
						rah.Close()
					})
				}
			}
			oi.Size = decLength

			// Assemble the GetObjectReader
			r = &GetObjectReader{
				ObjInfo:    oi,
				pReader:    decReader,
				cleanUpFns: cFns,
				opts:       opts,
			}
			return r, nil
		}

	default:
		off, length, err = rs.GetOffsetLength(oi.Size)
		if err != nil {
			return nil, 0, 0, err
		}
		fn = func(inputReader io.Reader, _ http.Header, pcfn CheckCopyPreconditionFn, cFns ...func()) (r *GetObjectReader, err error) {
			cFns = append(cleanUpFns, cFns...)
			if opts.CheckCopyPrecondFn != nil {
				if ok := opts.CheckCopyPrecondFn(oi, ""); ok {
					// Call the cleanup funcs
					for i := len(cFns) - 1; i >= 0; i-- {
						cFns[i]()
					}
					return nil, PreConditionFailed{}
				}
			}
			r = &GetObjectReader{
				ObjInfo:    oi,
				pReader:    inputReader,
				cleanUpFns: cFns,
				opts:       opts,
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

// NewPutObjReader returns a new PutObjReader and holds
// reference to underlying data stream from client and the encrypted
// data reader
func NewPutObjReader(rawReader *hash.Reader, encReader *hash.Reader, key *crypto.ObjectKey) *PutObjReader {
	p := PutObjReader{Reader: rawReader, rawReader: rawReader}

	if key != nil && encReader != nil {
		p.sealMD5Fn = sealETagFn(*key)
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

// CleanMinioInternalMetadataKeys removes X-Amz-Meta- prefix from minio internal
// encryption metadata that was sent by minio gateway
func CleanMinioInternalMetadataKeys(metadata map[string]string) map[string]string {
	var newMeta = make(map[string]string, len(metadata))
	for k, v := range metadata {
		if strings.HasPrefix(k, "X-Amz-Meta-X-Minio-Internal-") {
			newMeta[strings.TrimPrefix(k, "X-Amz-Meta-")] = v
		} else {
			newMeta[k] = v
		}
	}
	return newMeta
}

// newS2CompressReader will read data from r, compress it and return the compressed data as a Reader.
// Use Close to ensure resources are released on incomplete streams.
func newS2CompressReader(r io.Reader) io.ReadCloser {
	pr, pw := io.Pipe()
	comp := s2.NewWriter(pw)
	// Copy input to compressor
	go func() {
		_, err := io.Copy(comp, r)
		if err != nil {
			comp.Close()
			pw.CloseWithError(err)
			return
		}
		// Close the stream.
		err = comp.Close()
		if err != nil {
			pw.CloseWithError(err)
			return
		}
		// Everything ok, do regular close.
		pw.Close()
	}()
	return pr
}

// Returns error if the context is canceled, indicating
// either client has disconnected
type contextReader struct {
	io.ReadCloser
	ctx context.Context
}

func (d *contextReader) Read(p []byte) (int, error) {
	select {
	case <-d.ctx.Done():
		return 0, d.ctx.Err()
	default:
		return d.ReadCloser.Read(p)
	}
}

/*
 * Minio Cloud Storage, (C) 2016, 2017, 2017 Minio, Inc.
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
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"path"
	"sort"
	"sync"
	"time"

	"github.com/minio/highwayhash"
	"github.com/minio/minio/cmd/logger"
	sha256 "github.com/minio/sha256-simd"
	"golang.org/x/crypto/blake2b"
)

const erasureAlgorithmKlauspost = "klauspost/reedsolomon/vandermonde"

// magic HH-256 key as HH-256 hash of the first 100 decimals of π as utf-8 string with a zero key.
var magicHighwayHash256Key = []byte("\x4b\xe7\x34\xfa\x8e\x23\x8a\xcd\x26\x3e\x83\xe6\xbb\x96\x85\x52\x04\x0f\x93\x5d\xa3\x9f\x44\x14\x97\xe0\x9d\x13\x22\xde\x36\xa0")

// BitrotAlgorithm specifies a algorithm used for bitrot protection.
type BitrotAlgorithm uint

const (
	// SHA256 represents the SHA-256 hash function
	SHA256 BitrotAlgorithm = 1 + iota
	// HighwayHash256 represents the HighwayHash-256 hash function
	HighwayHash256
	// BLAKE2b512 represents the BLAKE2b-256 hash function
	BLAKE2b512
)

// DefaultBitrotAlgorithm is the default algorithm used for bitrot protection.
var DefaultBitrotAlgorithm = HighwayHash256

var bitrotAlgorithms = map[BitrotAlgorithm]string{
	SHA256:         "sha256",
	BLAKE2b512:     "blake2b",
	HighwayHash256: "highwayhash256",
}

// New returns a new hash.Hash calculating the given bitrot algorithm.
// New logs error and exits if the algorithm is not supported or not
// linked into the binary.
func (a BitrotAlgorithm) New() hash.Hash {
	switch a {
	case SHA256:
		return sha256.New()
	case BLAKE2b512:
		b2, _ := blake2b.New512(nil) // New512 never returns an error if the key is nil
		return b2
	case HighwayHash256:
		hh, _ := highwayhash.New(magicHighwayHash256Key) // New will never return error since key is 256 bit
		return hh
	}
	logger.CriticalIf(context.Background(), errors.New("Unsupported bitrot algorithm"))
	return nil
}

// Available reports whether the given algorihm is a supported and linked into the binary.
func (a BitrotAlgorithm) Available() bool {
	_, ok := bitrotAlgorithms[a]
	return ok
}

// String returns the string identifier for a given bitrot algorithm.
// If the algorithm is not supported String panics.
func (a BitrotAlgorithm) String() string {
	name, ok := bitrotAlgorithms[a]
	if !ok {
		logger.CriticalIf(context.Background(), errors.New("Unsupported bitrot algorithm"))
	}
	return name
}

// BitrotAlgorithmFromString returns a bitrot algorithm from the given string representation.
// It returns 0 if the string representation does not match any supported algorithm.
// The zero value of a bitrot algorithm is never supported.
func BitrotAlgorithmFromString(s string) (a BitrotAlgorithm) {
	for alg, name := range bitrotAlgorithms {
		if name == s {
			return alg
		}
	}
	return
}

// objectPartInfo Info of each part kept in the multipart metadata
// file after CompleteMultipartUpload() is called.
type objectPartInfo struct {
	Number int    `json:"number"`
	Name   string `json:"name"`
	ETag   string `json:"etag"`
	Size   int64  `json:"size"`
}

// byObjectPartNumber is a collection satisfying sort.Interface.
type byObjectPartNumber []objectPartInfo

func (t byObjectPartNumber) Len() int           { return len(t) }
func (t byObjectPartNumber) Swap(i, j int)      { t[i], t[j] = t[j], t[i] }
func (t byObjectPartNumber) Less(i, j int) bool { return t[i].Number < t[j].Number }

// ChecksumInfo - carries checksums of individual scattered parts per disk.
type ChecksumInfo struct {
	Name      string
	Algorithm BitrotAlgorithm
	Hash      []byte
}

// MarshalJSON marshals the ChecksumInfo struct
func (c ChecksumInfo) MarshalJSON() ([]byte, error) {
	type checksuminfo struct {
		Name      string `json:"name"`
		Algorithm string `json:"algorithm"`
		Hash      string `json:"hash"`
	}

	info := checksuminfo{
		Name:      c.Name,
		Algorithm: c.Algorithm.String(),
		Hash:      hex.EncodeToString(c.Hash),
	}
	return json.Marshal(info)
}

// UnmarshalJSON unmarshals the the given data into the ChecksumInfo struct
func (c *ChecksumInfo) UnmarshalJSON(data []byte) error {
	type checksuminfo struct {
		Name      string `json:"name"`
		Algorithm string `json:"algorithm"`
		Hash      string `json:"hash"`
	}

	var info checksuminfo
	err := json.Unmarshal(data, &info)
	if err != nil {
		return err
	}
	c.Algorithm = BitrotAlgorithmFromString(info.Algorithm)
	if !c.Algorithm.Available() {
		return errBitrotHashAlgoInvalid
	}
	c.Hash, err = hex.DecodeString(info.Hash)
	if err != nil {
		return err
	}
	c.Name = info.Name
	return nil
}

// ErasureInfo holds erasure coding and bitrot related information.
type ErasureInfo struct {
	// Algorithm is the string representation of erasure-coding-algorithm
	Algorithm string `json:"algorithm"`
	// DataBlocks is the number of data blocks for erasure-coding
	DataBlocks int `json:"data"`
	// ParityBlocks is the number of parity blocks for erasure-coding
	ParityBlocks int `json:"parity"`
	// BlockSize is the size of one erasure-coded block
	BlockSize int64 `json:"blockSize"`
	// Index is the index of the current disk
	Index int `json:"index"`
	// Distribution is the distribution of the data and parity blocks
	Distribution []int `json:"distribution"`
	// Checksums holds all bitrot checksums of all erasure encoded blocks
	Checksums []ChecksumInfo `json:"checksum,omitempty"`
}

// AddChecksumInfo adds a checksum of a part.
func (e *ErasureInfo) AddChecksumInfo(ckSumInfo ChecksumInfo) {
	for i, sum := range e.Checksums {
		if sum.Name == ckSumInfo.Name {
			e.Checksums[i] = ckSumInfo
			return
		}
	}
	e.Checksums = append(e.Checksums, ckSumInfo)
}

// GetChecksumInfo - get checksum of a part.
func (e ErasureInfo) GetChecksumInfo(partName string) (ckSum ChecksumInfo) {
	// Return the checksum.
	for _, sum := range e.Checksums {
		if sum.Name == partName {
			return sum
		}
	}
	return ChecksumInfo{}
}

// statInfo - carries stat information of the object.
type statInfo struct {
	Size    int64     `json:"size"`    // Size of the object `xl.json`.
	ModTime time.Time `json:"modTime"` // ModTime of the object `xl.json`.
}

// A xlMetaV1 represents `xl.json` metadata header.
type xlMetaV1 struct {
	Version string   `json:"version"` // Version of the current `xl.json`.
	Format  string   `json:"format"`  // Format of the current `xl.json`.
	Stat    statInfo `json:"stat"`    // Stat of the current object `xl.json`.
	// Erasure coded info for the current object `xl.json`.
	Erasure ErasureInfo `json:"erasure"`
	// Minio release tag for current object `xl.json`.
	Minio struct {
		Release string `json:"release"`
	} `json:"minio"`
	// Metadata map for current object `xl.json`.
	Meta map[string]string `json:"meta,omitempty"`
	// Captures all the individual object `xl.json`.
	Parts []objectPartInfo `json:"parts,omitempty"`
}

// XL metadata constants.
const (
	// XL meta version.
	xlMetaVersion = "1.0.1"

	// XL meta version.
	xlMetaVersion100 = "1.0.0"

	// XL meta format string.
	xlMetaFormat = "xl"

	// Add new constants here.
)

// newXLMetaV1 - initializes new xlMetaV1, adds version, allocates a fresh erasure info.
func newXLMetaV1(object string, dataBlocks, parityBlocks int) (xlMeta xlMetaV1) {
	xlMeta = xlMetaV1{}
	xlMeta.Version = xlMetaVersion
	xlMeta.Format = xlMetaFormat
	xlMeta.Minio.Release = ReleaseTag
	xlMeta.Erasure = ErasureInfo{
		Algorithm:    erasureAlgorithmKlauspost,
		DataBlocks:   dataBlocks,
		ParityBlocks: parityBlocks,
		BlockSize:    blockSizeV1,
		Distribution: hashOrder(object, dataBlocks+parityBlocks),
	}
	return xlMeta
}

// IsValid - tells if the format is sane by validating the version
// string, format and erasure info fields.
func (m xlMetaV1) IsValid() bool {
	return isXLMetaFormatValid(m.Version, m.Format) &&
		isXLMetaErasureInfoValid(m.Erasure.DataBlocks, m.Erasure.ParityBlocks)
}

// Verifies if the backend format metadata is sane by validating
// the version string and format style.
func isXLMetaFormatValid(version, format string) bool {
	return ((version == xlMetaVersion || version == xlMetaVersion100) &&
		format == xlMetaFormat)
}

// Verifies if the backend format metadata is sane by validating
// the ErasureInfo, i.e. data and parity blocks.
func isXLMetaErasureInfoValid(data, parity int) bool {
	return ((data >= parity) && (data != 0) && (parity != 0))
}

// Converts metadata to object info.
func (m xlMetaV1) ToObjectInfo(bucket, object string) ObjectInfo {
	objInfo := ObjectInfo{
		IsDir:           false,
		Bucket:          bucket,
		Name:            object,
		Size:            m.Stat.Size,
		ModTime:         m.Stat.ModTime,
		ContentType:     m.Meta["content-type"],
		ContentEncoding: m.Meta["content-encoding"],
	}

	// Extract etag from metadata.
	objInfo.ETag = extractETag(m.Meta)

	// etag/md5Sum has already been extracted. We need to
	// remove to avoid it from appearing as part of
	// response headers. e.g, X-Minio-* or X-Amz-*.
	objInfo.UserDefined = cleanMetadata(m.Meta)

	// All the parts per object.
	objInfo.Parts = m.Parts

	// Update storage class
	if sc, ok := m.Meta[amzStorageClass]; ok {
		objInfo.StorageClass = sc
	} else {
		objInfo.StorageClass = globalMinioDefaultStorageClass
	}

	// Success.
	return objInfo
}

// objectPartIndex - returns the index of matching object part number.
func objectPartIndex(parts []objectPartInfo, partNumber int) int {
	for i, part := range parts {
		if partNumber == part.Number {
			return i
		}
	}
	return -1
}

// AddObjectPart - add a new object part in order.
func (m *xlMetaV1) AddObjectPart(partNumber int, partName string, partETag string, partSize int64) {
	partInfo := objectPartInfo{
		Number: partNumber,
		Name:   partName,
		ETag:   partETag,
		Size:   partSize,
	}

	// Update part info if it already exists.
	for i, part := range m.Parts {
		if partNumber == part.Number {
			m.Parts[i] = partInfo
			return
		}
	}

	// Proceed to include new part info.
	m.Parts = append(m.Parts, partInfo)

	// Parts in xlMeta should be in sorted order by part number.
	sort.Sort(byObjectPartNumber(m.Parts))
}

// ObjectToPartOffset - translate offset of an object to offset of its individual part.
func (m xlMetaV1) ObjectToPartOffset(ctx context.Context, offset int64) (partIndex int, partOffset int64, err error) {
	if offset == 0 {
		// Special case - if offset is 0, then partIndex and partOffset are always 0.
		return 0, 0, nil
	}
	partOffset = offset
	// Seek until object offset maps to a particular part offset.
	for i, part := range m.Parts {
		partIndex = i
		// Offset is smaller than size we have reached the proper part offset.
		if partOffset < part.Size {
			return partIndex, partOffset, nil
		}
		// Continue to towards the next part.
		partOffset -= part.Size
	}
	logger.LogIf(ctx, InvalidRange{})
	// Offset beyond the size of the object return InvalidRange.
	return 0, 0, InvalidRange{}
}

// pickValidXLMeta - picks one valid xlMeta content and returns from a
// slice of xlmeta content.
func pickValidXLMeta(ctx context.Context, metaArr []xlMetaV1, modTime time.Time) (xmv xlMetaV1, e error) {
	// Pick latest valid metadata.
	for _, meta := range metaArr {
		if meta.IsValid() && meta.Stat.ModTime.Equal(modTime) {
			return meta, nil
		}
	}
	err := fmt.Errorf("No valid xl.json present")
	logger.LogIf(ctx, err)
	return xmv, err
}

// list of all errors that can be ignored in a metadata operation.
var objMetadataOpIgnoredErrs = append(baseIgnoredErrs, errDiskAccessDenied, errVolumeNotFound, errFileNotFound, errFileAccessDenied, errCorruptedFormat)

// readXLMetaParts - returns the XL Metadata Parts from xl.json of one of the disks picked at random.
func (xl xlObjects) readXLMetaParts(ctx context.Context, bucket, object string) (xlMetaParts []objectPartInfo, xlMeta map[string]string, err error) {
	var ignoredErrs []error
	for _, disk := range xl.getLoadBalancedDisks() {
		if disk == nil {
			ignoredErrs = append(ignoredErrs, errDiskNotFound)
			continue
		}
		xlMetaParts, xlMeta, err = readXLMetaParts(ctx, disk, bucket, object)
		if err == nil {
			return xlMetaParts, xlMeta, nil
		}
		// For any reason disk or bucket is not available continue
		// and read from other disks.
		if IsErrIgnored(err, objMetadataOpIgnoredErrs...) {
			ignoredErrs = append(ignoredErrs, err)
			continue
		}
		// Error is not ignored, return right here.
		return nil, nil, err
	}
	// If all errors were ignored, reduce to maximal occurrence
	// based on the read quorum.
	readQuorum := len(xl.getDisks()) / 2
	return nil, nil, reduceReadQuorumErrs(ctx, ignoredErrs, nil, readQuorum)
}

// readXLMetaStat - return xlMetaV1.Stat and xlMetaV1.Meta from  one of the disks picked at random.
func (xl xlObjects) readXLMetaStat(ctx context.Context, bucket, object string) (xlStat statInfo, xlMeta map[string]string, err error) {
	var ignoredErrs []error
	for _, disk := range xl.getLoadBalancedDisks() {
		if disk == nil {
			ignoredErrs = append(ignoredErrs, errDiskNotFound)
			continue
		}
		// parses only xlMetaV1.Meta and xlMeta.Stat
		xlStat, xlMeta, err = readXLMetaStat(ctx, disk, bucket, object)
		if err == nil {
			return xlStat, xlMeta, nil
		}
		// For any reason disk or bucket is not available continue
		// and read from other disks.
		if IsErrIgnored(err, objMetadataOpIgnoredErrs...) {
			ignoredErrs = append(ignoredErrs, err)
			continue
		}
		// Error is not ignored, return right here.
		return statInfo{}, nil, err
	}
	// If all errors were ignored, reduce to maximal occurrence
	// based on the read quorum.
	readQuorum := len(xl.getDisks()) / 2
	return statInfo{}, nil, reduceReadQuorumErrs(ctx, ignoredErrs, nil, readQuorum)
}

// deleteXLMetadata - deletes `xl.json` on a single disk.
func deleteXLMetdata(ctx context.Context, disk StorageAPI, bucket, prefix string) error {
	jsonFile := path.Join(prefix, xlMetaJSONFile)
	err := disk.DeleteFile(bucket, jsonFile)
	logger.LogIf(ctx, err)
	return err
}

// writeXLMetadata - writes `xl.json` to a single disk.
func writeXLMetadata(ctx context.Context, disk StorageAPI, bucket, prefix string, xlMeta xlMetaV1) error {
	jsonFile := path.Join(prefix, xlMetaJSONFile)

	// Marshal json.
	metadataBytes, err := json.Marshal(&xlMeta)
	if err != nil {
		logger.LogIf(ctx, err)
		return err
	}
	// Persist marshalled data.
	err = disk.AppendFile(bucket, jsonFile, metadataBytes)
	logger.LogIf(ctx, err)
	return err
}

// deleteAllXLMetadata - deletes all partially written `xl.json` depending on errs.
func deleteAllXLMetadata(ctx context.Context, disks []StorageAPI, bucket, prefix string, errs []error) {
	var wg = &sync.WaitGroup{}
	// Delete all the `xl.json` left over.
	for index, disk := range disks {
		if disk == nil {
			continue
		}
		// Undo rename object in parallel.
		wg.Add(1)
		go func(index int, disk StorageAPI) {
			defer wg.Done()
			if errs[index] != nil {
				return
			}
			_ = deleteXLMetdata(ctx, disk, bucket, prefix)
		}(index, disk)
	}
	wg.Wait()
}

// Rename `xl.json` content to destination location for each disk in order.
func renameXLMetadata(ctx context.Context, disks []StorageAPI, srcBucket, srcEntry, dstBucket, dstEntry string, quorum int) ([]StorageAPI, error) {
	isDir := false
	srcXLJSON := path.Join(srcEntry, xlMetaJSONFile)
	dstXLJSON := path.Join(dstEntry, xlMetaJSONFile)
	return rename(ctx, disks, srcBucket, srcXLJSON, dstBucket, dstXLJSON, isDir, quorum, []error{errFileNotFound})
}

// writeUniqueXLMetadata - writes unique `xl.json` content for each disk in order.
func writeUniqueXLMetadata(ctx context.Context, disks []StorageAPI, bucket, prefix string, xlMetas []xlMetaV1, quorum int) ([]StorageAPI, error) {
	var wg = &sync.WaitGroup{}
	var mErrs = make([]error, len(disks))

	// Start writing `xl.json` to all disks in parallel.
	for index, disk := range disks {
		if disk == nil {
			logger.LogIf(ctx, errDiskNotFound)
			mErrs[index] = errDiskNotFound
			continue
		}
		wg.Add(1)
		// Write `xl.json` in a routine.
		go func(index int, disk StorageAPI) {
			defer wg.Done()

			// Pick one xlMeta for a disk at index.
			xlMetas[index].Erasure.Index = index + 1

			// Write unique `xl.json` for a disk at index.
			err := writeXLMetadata(ctx, disk, bucket, prefix, xlMetas[index])
			if err != nil {
				mErrs[index] = err
			}
		}(index, disk)
	}

	// Wait for all the routines.
	wg.Wait()

	err := reduceWriteQuorumErrs(ctx, mErrs, objectOpIgnoredErrs, quorum)
	if err == errXLWriteQuorum {
		// Delete all `xl.json` successfully renamed.
		deleteAllXLMetadata(ctx, disks, bucket, prefix, mErrs)
	}
	return evalDisks(disks, mErrs), err
}

// writeSameXLMetadata - write `xl.json` on all disks in order.
func writeSameXLMetadata(ctx context.Context, disks []StorageAPI, bucket, prefix string, xlMeta xlMetaV1, writeQuorum int) ([]StorageAPI, error) {
	var wg = &sync.WaitGroup{}
	var mErrs = make([]error, len(disks))

	// Start writing `xl.json` to all disks in parallel.
	for index, disk := range disks {
		if disk == nil {
			logger.LogIf(ctx, errDiskNotFound)
			mErrs[index] = errDiskNotFound
			continue
		}
		wg.Add(1)
		// Write `xl.json` in a routine.
		go func(index int, disk StorageAPI, metadata xlMetaV1) {
			defer wg.Done()

			// Save the disk order index.
			metadata.Erasure.Index = index + 1

			// Write xl metadata.
			err := writeXLMetadata(ctx, disk, bucket, prefix, metadata)
			if err != nil {
				mErrs[index] = err
			}
		}(index, disk, xlMeta)
	}

	// Wait for all the routines.
	wg.Wait()

	err := reduceWriteQuorumErrs(ctx, mErrs, objectOpIgnoredErrs, writeQuorum)
	if err == errXLWriteQuorum {
		// Delete all `xl.json` successfully renamed.
		deleteAllXLMetadata(ctx, disks, bucket, prefix, mErrs)
	}
	return evalDisks(disks, mErrs), err
}

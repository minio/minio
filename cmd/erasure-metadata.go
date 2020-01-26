/*
 * MinIO Cloud Storage, (C) 2016-2019 MinIO, Inc.
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
	"net/http"
	"sort"
	"time"

	jsoniter "github.com/json-iterator/go"
	xhttp "github.com/minio/minio/cmd/http"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/sync/errgroup"
	"github.com/minio/sha256-simd"
)

const erasureAlgorithmKlauspost = "klauspost/reedsolomon/vandermonde"

// ObjectPartInfo Info of each part kept in the multipart metadata
// file after CompleteMultipartUpload() is called.
type ObjectPartInfo struct {
	Number     int    `json:"number"`
	Name       string `json:"name"`
	ETag       string `json:"etag,omitempty"`
	Size       int64  `json:"size"`
	ActualSize int64  `json:"actualSize"`
}

// byObjectPartNumber is a collection satisfying sort.Interface.
type byObjectPartNumber []ObjectPartInfo

func (t byObjectPartNumber) Len() int           { return len(t) }
func (t byObjectPartNumber) Swap(i, j int)      { t[i], t[j] = t[j], t[i] }
func (t byObjectPartNumber) Less(i, j int) bool { return t[i].Number < t[j].Number }

// ChecksumInfo - carries checksums of individual scattered parts per disk.
type ChecksumInfo struct {
	Name      string
	Algorithm BitrotAlgorithm
	Hash      []byte
}

type checksumInfoJSON struct {
	Name      string `json:"name"`
	Algorithm string `json:"algorithm"`
	Hash      string `json:"hash,omitempty"`
}

// MarshalJSON marshals the ChecksumInfo struct
func (c ChecksumInfo) MarshalJSON() ([]byte, error) {
	info := checksumInfoJSON{
		Name:      c.Name,
		Algorithm: c.Algorithm.String(),
		Hash:      hex.EncodeToString(c.Hash),
	}
	return json.Marshal(info)
}

// UnmarshalJSON - custom checksum info unmarshaller
func (c *ChecksumInfo) UnmarshalJSON(data []byte) error {
	var info checksumInfoJSON
	var json = jsoniter.ConfigCompatibleWithStandardLibrary
	if err := json.Unmarshal(data, &info); err != nil {
		return err
	}
	sum, err := hex.DecodeString(info.Hash)
	if err != nil {
		return err
	}
	c.Name = info.Name
	c.Algorithm = BitrotAlgorithmFromString(info.Algorithm)
	c.Hash = sum

	if !c.Algorithm.Available() {
		logger.LogIf(context.Background(), errBitrotHashAlgoInvalid)
		return errBitrotHashAlgoInvalid
	}
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

// StatInfo - carries stat information of the object.
type StatInfo struct {
	Size    int64     `json:"size"`    // Size of the object `xl.json`.
	ModTime time.Time `json:"modTime"` // ModTime of the object `xl.json`.
}

// Return a new FileInfo initialized using the given FileInfo. Used in healing
// to make sure that we do not copy over any part's checksum info which will
// differ for different disks.
func cloneFileInfo(fi FileInfo) FileInfo {
	nfi := fi
	nfi.Erasure.Checksums = nil
	nfi.Parts = nil
	return nfi
}

// IsValid - tells if erasure info fields are valid.
func (fi FileInfo) IsValid() bool {
	data := fi.Erasure.DataBlocks
	parity := fi.Erasure.ParityBlocks
	return ((data >= parity) && (data != 0) && (parity != 0))
}

// ToObjectInfo - Converts metadata to object info.
func (fi FileInfo) ToObjectInfo(bucket, object string) ObjectInfo {
	objInfo := ObjectInfo{
		IsDir:           false,
		Bucket:          bucket,
		Name:            object,
		Size:            fi.Size,
		ModTime:         fi.ModTime,
		ContentType:     fi.Metadata["content-type"],
		ContentEncoding: fi.Metadata["content-encoding"],
	}
	// Update expires
	var (
		t time.Time
		e error
	)
	if exp, ok := fi.Metadata["expires"]; ok {
		if t, e = time.Parse(http.TimeFormat, exp); e == nil {
			objInfo.Expires = t.UTC()
		}
	}
	objInfo.backendType = BackendErasure

	// Extract etag from metadata.
	objInfo.ETag = extractETag(fi.Metadata)

	// Add user tags to the object info
	objInfo.UserTags = fi.Metadata[xhttp.AmzObjectTagging]

	// etag/md5Sum has already been extracted. We need to
	// remove to avoid it from appearing as part of
	// response headers. e.g, X-Minio-* or X-Amz-*.
	// Tags have also been extracted, we remove that as well.
	objInfo.UserDefined = cleanMetadata(fi.Metadata)

	// All the parts per object.
	objInfo.Parts = fi.Parts

	// Update storage class
	if sc, ok := fi.Metadata[xhttp.AmzStorageClass]; ok {
		objInfo.StorageClass = sc
	} else {
		objInfo.StorageClass = globalMinioDefaultStorageClass
	}

	// Success.
	return objInfo
}

// objectPartIndex - returns the index of matching object part number.
func objectPartIndex(parts []ObjectPartInfo, partNumber int) int {
	for i, part := range parts {
		if partNumber == part.Number {
			return i
		}
	}
	return -1
}

// AddObjectPart - add a new object part in order.
func (fi *FileInfo) AddObjectPart(partNumber int, partName string, partETag string, partSize int64, actualSize int64) {
	partInfo := ObjectPartInfo{
		Number:     partNumber,
		Name:       partName,
		ETag:       partETag,
		Size:       partSize,
		ActualSize: actualSize,
	}

	// Update part info if it already exists.
	for i, part := range fi.Parts {
		if partNumber == part.Number {
			fi.Parts[i] = partInfo
			return
		}
	}

	// Proceed to include new part info.
	fi.Parts = append(fi.Parts, partInfo)

	// Parts in FileInfo should be in sorted order by part number.
	sort.Sort(byObjectPartNumber(fi.Parts))
}

// ObjectToPartOffset - translate offset of an object to offset of its individual part.
func (fi FileInfo) ObjectToPartOffset(ctx context.Context, offset int64) (partIndex int, partOffset int64, err error) {
	if offset == 0 {
		// Special case - if offset is 0, then partIndex and partOffset are always 0.
		return 0, 0, nil
	}
	partOffset = offset
	// Seek until object offset maps to a particular part offset.
	for i, part := range fi.Parts {
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

func getFileInfoInQuorum(ctx context.Context, metaArr []FileInfo, modTime time.Time, quorum int) (xmv FileInfo, e error) {
	metaHashes := make([]string, len(metaArr))
	for i, meta := range metaArr {
		if meta.IsValid() && meta.ModTime.Equal(modTime) {
			h := sha256.New()
			for _, p := range meta.Parts {
				h.Write([]byte(p.Name))
			}
			metaHashes[i] = hex.EncodeToString(h.Sum(nil))
		}
	}

	metaHashCountMap := make(map[string]int)
	for _, hash := range metaHashes {
		if hash == "" {
			continue
		}
		metaHashCountMap[hash]++
	}

	maxHash := ""
	maxCount := 0
	for hash, count := range metaHashCountMap {
		if count > maxCount {
			maxCount = count
			maxHash = hash
		}
	}

	if maxCount < quorum {
		return FileInfo{}, errXLReadQuorum
	}

	for i, hash := range metaHashes {
		if hash == maxHash {
			return metaArr[i], nil
		}
	}

	return FileInfo{}, errXLReadQuorum
}

// pickValidFileInfo - picks one valid FileInfo content and returns from a
// slice of xlmeta content.
func pickValidFileInfo(ctx context.Context, metaArr []FileInfo, modTime time.Time, quorum int) (xmv FileInfo, e error) {
	return getFileInfoInQuorum(ctx, metaArr, modTime, quorum)
}

// list of all errors that can be ignored in a metadata operation.
var objMetadataOpIgnoredErrs = append(baseIgnoredErrs, errDiskAccessDenied, errVolumeNotFound, errFileNotFound, errFileAccessDenied, errCorruptedFormat)

// Rename metadata content to destination location for each disk in order.
func renameFileInfo(ctx context.Context, disks []StorageAPI, srcBucket, srcEntry, dstBucket, dstEntry string, quorum int) ([]StorageAPI, error) {
	ignoredErr := []error{errFileNotFound}

	g := errgroup.WithNErrs(len(disks))

	// Rename file on all underlying storage disks.
	for index := range disks {
		index := index
		g.Go(func() error {
			if disks[index] == nil {
				return errDiskNotFound
			}
			if err := disks[index].RenameMetadata(srcBucket, srcEntry, dstBucket, dstEntry); err != nil {
				if !IsErrIgnored(err, ignoredErr...) {
					return err
				}
			}
			return nil
		}, index)
	}

	// Wait for all renames to finish.
	errs := g.Wait()

	// We can safely allow RenameFile errors up to len(xl.getDisks()) - writeQuorum
	// otherwise return failure. Cleanup successful renames.
	err := reduceWriteQuorumErrs(ctx, errs, objectOpIgnoredErrs, quorum)
	if err == errXLWriteQuorum {
		g = errgroup.WithNErrs(len(disks))
		for index, disk := range disks {
			if disk == nil {
				continue
			}
			index := index
			g.Go(func() error {
				if errs[index] == nil {
					// Undo all the partial rename operations.
					_ = disks[index].RenameMetadata(dstBucket, dstEntry, srcBucket, srcEntry)
				}
				return nil
			}, index)
		}
		g.Wait()
	}
	return evalDisks(disks, errs), err
}

// writeUniqueFileInfo - writes unique `xl.json` content for each disk in order.
func writeUniqueFileInfo(ctx context.Context, disks []StorageAPI, bucket, prefix string, files []FileInfo, quorum int) ([]StorageAPI, error) {
	g := errgroup.WithNErrs(len(disks))

	// Start writing `xl.json` to all disks in parallel.
	for index := range disks {
		index := index
		g.Go(func() error {
			if disks[index] == nil {
				return errDiskNotFound
			}
			// Pick one FileInfo for a disk at index.
			files[index].Erasure.Index = index + 1
			return disks[index].WriteMetadata(bucket, prefix,
				nullVersionID, files[index])
		}, index)
	}

	// Wait for all the routines.
	mErrs := g.Wait()

	err := reduceWriteQuorumErrs(ctx, mErrs, objectOpIgnoredErrs, quorum)
	return evalDisks(disks, mErrs), err
}

// Returns per object readQuorum and writeQuorum
// readQuorum is the min required disks to read data.
// writeQuorum is the min required disks to write data.
func objectQuorumFromMeta(ctx context.Context, xl xlObjects, partsMetaData []FileInfo, errs []error) (objectReadQuorum, objectWriteQuorum int, err error) {
	// get the latest updated Metadata and a count of all the latest updated FileInfo(s)
	latestFileInfo, err := getLatestFileInfo(ctx, partsMetaData, errs)
	if err != nil {
		return 0, 0, err
	}

	// Since all the valid erasure code meta updated at the same time are equivalent, pass dataBlocks
	// from latestFileInfo to get the quorum
	return latestFileInfo.Erasure.DataBlocks, latestFileInfo.Erasure.DataBlocks + 1, nil
}

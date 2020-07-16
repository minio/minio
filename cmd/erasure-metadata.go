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
	"fmt"
	"net/http"
	"sort"
	"time"

	xhttp "github.com/minio/minio/cmd/http"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/sync/errgroup"
	"github.com/minio/sha256-simd"
)

const erasureAlgorithm = "rs-vandermonde"

// byObjectPartNumber is a collection satisfying sort.Interface.
type byObjectPartNumber []ObjectPartInfo

func (t byObjectPartNumber) Len() int           { return len(t) }
func (t byObjectPartNumber) Swap(i, j int)      { t[i], t[j] = t[j], t[i] }
func (t byObjectPartNumber) Less(i, j int) bool { return t[i].Number < t[j].Number }

// AddChecksumInfo adds a checksum of a part.
func (e *ErasureInfo) AddChecksumInfo(ckSumInfo ChecksumInfo) {
	for i, sum := range e.Checksums {
		if sum.PartNumber == ckSumInfo.PartNumber {
			e.Checksums[i] = ckSumInfo
			return
		}
	}
	e.Checksums = append(e.Checksums, ckSumInfo)
}

// GetChecksumInfo - get checksum of a part.
func (e ErasureInfo) GetChecksumInfo(partNumber int) (ckSum ChecksumInfo) {
	for _, sum := range e.Checksums {
		if sum.PartNumber == partNumber {
			// Return the checksum
			return sum
		}
	}
	return ChecksumInfo{}
}

// ShardFileSize - returns final erasure size from original size.
func (e ErasureInfo) ShardFileSize(totalLength int64) int64 {
	if totalLength == 0 {
		return 0
	}
	if totalLength == -1 {
		return -1
	}
	numShards := totalLength / e.BlockSize
	lastBlockSize := totalLength % e.BlockSize
	lastShardSize := ceilFrac(lastBlockSize, int64(e.DataBlocks))
	return numShards*e.ShardSize() + lastShardSize
}

// ShardSize - returns actual shared size from erasure blockSize.
func (e ErasureInfo) ShardSize() int64 {
	return ceilFrac(e.BlockSize, int64(e.DataBlocks))
}

// IsValid - tells if erasure info fields are valid.
func (fi FileInfo) IsValid() bool {
	if fi.Deleted {
		// Delete marker has no data, no need to check
		// for erasure coding information
		return true
	}
	data := fi.Erasure.DataBlocks
	parity := fi.Erasure.ParityBlocks
	return ((data >= parity) && (data != 0) && (parity != 0))
}

// ToObjectInfo - Converts metadata to object info.
func (fi FileInfo) ToObjectInfo(bucket, object string) ObjectInfo {
	if HasSuffix(object, SlashSeparator) {
		return ObjectInfo{
			Bucket: bucket,
			Name:   object,
			IsDir:  true,
		}
	}
	objInfo := ObjectInfo{
		IsDir:           false,
		Bucket:          bucket,
		Name:            object,
		VersionID:       fi.VersionID,
		IsLatest:        fi.IsLatest,
		DeleteMarker:    fi.Deleted,
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
func (fi *FileInfo) AddObjectPart(partNumber int, partETag string, partSize int64, actualSize int64) {
	partInfo := ObjectPartInfo{
		Number:     partNumber,
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

func findFileInfoInQuorum(ctx context.Context, metaArr []FileInfo, modTime time.Time, quorum int) (xmv FileInfo, e error) {
	metaHashes := make([]string, len(metaArr))
	for i, meta := range metaArr {
		if meta.IsValid() && meta.ModTime.Equal(modTime) {
			h := sha256.New()
			for _, part := range meta.Parts {
				h.Write([]byte(fmt.Sprintf("part.%d", part.Number)))
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
		return FileInfo{}, errErasureReadQuorum
	}

	for i, hash := range metaHashes {
		if hash == maxHash {
			return metaArr[i], nil
		}
	}

	return FileInfo{}, errErasureReadQuorum
}

// pickValidFileInfo - picks one valid FileInfo content and returns from a
// slice of FileInfo.
func pickValidFileInfo(ctx context.Context, metaArr []FileInfo, modTime time.Time, quorum int) (xmv FileInfo, e error) {
	return findFileInfoInQuorum(ctx, metaArr, modTime, quorum)
}

// Rename metadata content to destination location for each disk concurrently.
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
			if err := disks[index].RenameData(srcBucket, srcEntry, "", dstBucket, dstEntry); err != nil {
				if !IsErrIgnored(err, ignoredErr...) {
					return err
				}
			}
			return nil
		}, index)
	}

	// Wait for all renames to finish.
	errs := g.Wait()

	// We can safely allow RenameData errors up to len(er.getDisks()) - writeQuorum
	// otherwise return failure. Cleanup successful renames.
	err := reduceWriteQuorumErrs(ctx, errs, objectOpIgnoredErrs, quorum)
	return evalDisks(disks, errs), err
}

// writeUniqueFileInfo - writes unique `xl.meta` content for each disk concurrently.
func writeUniqueFileInfo(ctx context.Context, disks []StorageAPI, bucket, prefix string, files []FileInfo, quorum int) ([]StorageAPI, error) {
	g := errgroup.WithNErrs(len(disks))

	// Start writing `xl.meta` to all disks in parallel.
	for index := range disks {
		index := index
		g.Go(func() error {
			if disks[index] == nil {
				return errDiskNotFound
			}
			// Pick one FileInfo for a disk at index.
			files[index].Erasure.Index = index + 1
			return disks[index].WriteMetadata(bucket, prefix, files[index])
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
func objectQuorumFromMeta(ctx context.Context, er erasureObjects, partsMetaData []FileInfo, errs []error) (objectReadQuorum, objectWriteQuorum int, err error) {
	// get the latest updated Metadata and a count of all the latest updated FileInfo(s)
	latestFileInfo, err := getLatestFileInfo(ctx, partsMetaData, errs)
	if err != nil {
		return 0, 0, err
	}

	// Since all the valid erasure code meta updated at the same time are equivalent, pass dataBlocks
	// from latestFileInfo to get the quorum
	return latestFileInfo.Erasure.DataBlocks, latestFileInfo.Erasure.DataBlocks + 1, nil
}

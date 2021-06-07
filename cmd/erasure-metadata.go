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

package cmd

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"sort"
	"time"

	"github.com/minio/minio/internal/bucket/replication"
	xhttp "github.com/minio/minio/internal/http"
	"github.com/minio/minio/internal/logger"
	"github.com/minio/minio/internal/sync/errgroup"
)

// Object was stored with additional erasure codes due to degraded system at upload time
const minIOErasureUpgraded = "x-minio-internal-erasure-upgraded"

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
	dataBlocks := fi.Erasure.DataBlocks
	parityBlocks := fi.Erasure.ParityBlocks
	correctIndexes := (fi.Erasure.Index > 0 &&
		fi.Erasure.Index <= dataBlocks+parityBlocks &&
		len(fi.Erasure.Distribution) == (dataBlocks+parityBlocks))
	return ((dataBlocks >= parityBlocks) &&
		(dataBlocks != 0) && (parityBlocks != 0) &&
		correctIndexes)
}

// ToObjectInfo - Converts metadata to object info.
func (fi FileInfo) ToObjectInfo(bucket, object string) ObjectInfo {
	object = decodeDirObject(object)
	versionID := fi.VersionID
	if (globalBucketVersioningSys.Enabled(bucket) || globalBucketVersioningSys.Suspended(bucket)) && versionID == "" {
		versionID = nullVersionID
	}

	objInfo := ObjectInfo{
		IsDir:            HasSuffix(object, SlashSeparator),
		Bucket:           bucket,
		Name:             object,
		VersionID:        versionID,
		IsLatest:         fi.IsLatest,
		DeleteMarker:     fi.Deleted,
		Size:             fi.Size,
		ModTime:          fi.ModTime,
		Legacy:           fi.XLV1,
		ContentType:      fi.Metadata["content-type"],
		ContentEncoding:  fi.Metadata["content-encoding"],
		NumVersions:      fi.NumVersions,
		SuccessorModTime: fi.SuccessorModTime,
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
	tags := fi.Metadata[xhttp.AmzObjectTagging]
	if len(tags) != 0 {
		objInfo.UserTags = tags
	}

	// Add replication status to the object info
	objInfo.ReplicationStatus = replication.StatusType(fi.Metadata[xhttp.AmzBucketReplicationStatus])
	if fi.Deleted {
		objInfo.ReplicationStatus = replication.StatusType(fi.DeleteMarkerReplicationStatus)
	}

	objInfo.TransitionStatus = fi.TransitionStatus
	objInfo.transitionedObjName = fi.TransitionedObjName
	objInfo.transitionVersionID = fi.TransitionVersionID
	objInfo.TransitionTier = fi.TransitionTier

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

	objInfo.VersionPurgeStatus = fi.VersionPurgeStatus
	// set restore status for transitioned object
	restoreHdr, ok := fi.Metadata[xhttp.AmzRestore]
	if ok {
		if restoreStatus, err := parseRestoreObjStatus(restoreHdr); err == nil {
			objInfo.RestoreOngoing = restoreStatus.Ongoing()
			objInfo.RestoreExpires, _ = restoreStatus.Expiry()
		}
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

func findFileInfoInQuorum(ctx context.Context, metaArr []FileInfo, modTime time.Time, dataDir string, quorum int) (FileInfo, error) {
	// with less quorum return error.
	if quorum < 2 {
		return FileInfo{}, errErasureReadQuorum
	}
	metaHashes := make([]string, len(metaArr))
	h := sha256.New()
	for i, meta := range metaArr {
		if meta.IsValid() && meta.ModTime.Equal(modTime) && meta.DataDir == dataDir {
			for _, part := range meta.Parts {
				h.Write([]byte(fmt.Sprintf("part.%d", part.Number)))
			}
			h.Write([]byte(fmt.Sprintf("%v", meta.Erasure.Distribution)))
			// make sure that length of Data is same
			h.Write([]byte(fmt.Sprintf("%v", len(meta.Data))))
			metaHashes[i] = hex.EncodeToString(h.Sum(nil))
			h.Reset()
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
			if metaArr[i].IsValid() {
				return metaArr[i], nil
			}
		}
	}

	return FileInfo{}, errErasureReadQuorum
}

// pickValidFileInfo - picks one valid FileInfo content and returns from a
// slice of FileInfo.
func pickValidFileInfo(ctx context.Context, metaArr []FileInfo, modTime time.Time, dataDir string, quorum int) (FileInfo, error) {
	return findFileInfoInQuorum(ctx, metaArr, modTime, dataDir, quorum)
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
			fi := files[index]
			fi.Erasure.Index = index + 1
			if fi.IsValid() {
				return disks[index].WriteMetadata(ctx, bucket, prefix, fi)
			}
			return errCorruptedFormat
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
func objectQuorumFromMeta(ctx context.Context, partsMetaData []FileInfo, errs []error, defaultParityCount int) (objectReadQuorum, objectWriteQuorum int, err error) {
	// get the latest updated Metadata and a count of all the latest updated FileInfo(s)
	latestFileInfo, err := getLatestFileInfo(ctx, partsMetaData, errs)
	if err != nil {
		return 0, 0, err
	}

	if !latestFileInfo.IsValid() {
		return 0, 0, errErasureReadQuorum
	}

	parityBlocks := globalStorageClass.GetParityForSC(latestFileInfo.Metadata[xhttp.AmzStorageClass])
	if parityBlocks <= 0 {
		parityBlocks = defaultParityCount
	}

	dataBlocks := latestFileInfo.Erasure.DataBlocks
	if dataBlocks == 0 {
		dataBlocks = len(partsMetaData) - parityBlocks
	}

	writeQuorum := dataBlocks
	if dataBlocks == parityBlocks {
		writeQuorum++
	}

	// Since all the valid erasure code meta updated at the same time are equivalent, pass dataBlocks
	// from latestFileInfo to get the quorum
	return dataBlocks, writeQuorum, nil
}

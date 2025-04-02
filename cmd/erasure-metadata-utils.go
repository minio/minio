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
	"encoding/binary"
	"errors"
	"hash/crc32"

	"github.com/minio/pkg/v3/sync/errgroup"
)

// counterMap type adds GetValueWithQuorum method to a map[T]int used to count occurrences of values of type T.
type counterMap[T comparable] map[T]int

// GetValueWithQuorum returns the first key which occurs >= quorum number of times.
func (c counterMap[T]) GetValueWithQuorum(quorum int) (T, bool) {
	var zero T
	for x, count := range c {
		if count >= quorum {
			return x, true
		}
	}
	return zero, false
}

// figure out the most commonVersions across disk that satisfies
// the 'writeQuorum' this function returns "" if quorum cannot
// be achieved and disks have too many inconsistent versions.
func reduceCommonVersions(diskVersions [][]byte, writeQuorum int) (versions []byte) {
	diskVersionsCount := make(map[uint64]int)
	for _, versions := range diskVersions {
		if len(versions) > 0 {
			diskVersionsCount[binary.BigEndian.Uint64(versions)]++
		}
	}

	var commonVersions uint64
	maxCnt := 0
	for versions, count := range diskVersionsCount {
		if maxCnt < count {
			maxCnt = count
			commonVersions = versions
		}
	}

	if maxCnt >= writeQuorum {
		for _, versions := range diskVersions {
			if binary.BigEndian.Uint64(versions) == commonVersions {
				return versions
			}
		}
	}

	return []byte{}
}

// figure out the most commonVersions across disk that satisfies
// the 'writeQuorum' this function returns '0' if quorum cannot
// be achieved and disks have too many inconsistent versions.
func reduceCommonDataDir(dataDirs []string, writeQuorum int) (dataDir string) {
	dataDirsCount := make(map[string]int)
	for _, ddir := range dataDirs {
		dataDirsCount[ddir]++
	}

	maxCnt := 0
	for ddir, count := range dataDirsCount {
		if maxCnt < count {
			maxCnt = count
			dataDir = ddir
		}
	}

	if maxCnt >= writeQuorum {
		return dataDir
	}

	return ""
}

// Returns number of errors that occurred the most (incl. nil) and the
// corresponding error value. NB When there is more than one error value that
// occurs maximum number of times, the error value returned depends on how
// golang's map orders keys. This doesn't affect correctness as long as quorum
// value is greater than or equal to simple majority, since none of the equally
// maximal values would occur quorum or more number of times.
func reduceErrs(errs []error, ignoredErrs []error) (maxCount int, maxErr error) {
	errorCounts := make(map[error]int)
	for _, err := range errs {
		if IsErrIgnored(err, ignoredErrs...) {
			continue
		}
		// Errors due to context cancellation may be wrapped - group them by context.Canceled.
		if errors.Is(err, context.Canceled) {
			errorCounts[context.Canceled]++
			continue
		}
		errorCounts[err]++
	}

	maxCnt := 0
	for err, count := range errorCounts {
		switch {
		case maxCnt < count:
			maxCnt = count
			maxErr = err

		// Prefer `nil` over other error values with the same
		// number of occurrences.
		case maxCnt == count && err == nil:
			maxErr = err
		}
	}
	return maxCnt, maxErr
}

// reduceQuorumErrs behaves like reduceErrs by only for returning
// values of maximally occurring errors validated against a generic
// quorum number that can be read or write quorum depending on usage.
func reduceQuorumErrs(ctx context.Context, errs []error, ignoredErrs []error, quorum int, quorumErr error) error {
	if contextCanceled(ctx) {
		return context.Canceled
	}
	maxCount, maxErr := reduceErrs(errs, ignoredErrs)
	if maxCount >= quorum {
		return maxErr
	}
	return quorumErr
}

// reduceReadQuorumErrs behaves like reduceErrs but only for returning
// values of maximally occurring errors validated against readQuorum.
func reduceReadQuorumErrs(ctx context.Context, errs []error, ignoredErrs []error, readQuorum int) (maxErr error) {
	return reduceQuorumErrs(ctx, errs, ignoredErrs, readQuorum, errErasureReadQuorum)
}

// reduceWriteQuorumErrs behaves like reduceErrs but only for returning
// values of maximally occurring errors validated against writeQuorum.
func reduceWriteQuorumErrs(ctx context.Context, errs []error, ignoredErrs []error, writeQuorum int) (maxErr error) {
	return reduceQuorumErrs(ctx, errs, ignoredErrs, writeQuorum, errErasureWriteQuorum)
}

// Similar to 'len(slice)' but returns the actual elements count
// skipping the unallocated elements.
func diskCount(disks []StorageAPI) int {
	diskCount := 0
	for _, disk := range disks {
		if disk == nil {
			continue
		}
		diskCount++
	}
	return diskCount
}

// hashOrder - hashes input key to return consistent
// hashed integer slice. Returned integer order is salted
// with an input key. This results in consistent order.
// NOTE: collisions are fine, we are not looking for uniqueness
// in the slices returned.
func hashOrder(key string, cardinality int) []int {
	if cardinality <= 0 {
		// Returns an empty int slice for cardinality < 0.
		return nil
	}

	nums := make([]int, cardinality)
	keyCrc := crc32.Checksum([]byte(key), crc32.IEEETable)

	start := int(keyCrc % uint32(cardinality))
	for i := 1; i <= cardinality; i++ {
		nums[i-1] = 1 + ((start + i) % cardinality)
	}
	return nums
}

// Reads all `xl.meta` metadata as a FileInfo slice.
// Returns error slice indicating the failed metadata reads.
func readAllFileInfo(ctx context.Context, disks []StorageAPI, origbucket string, bucket, object, versionID string, readData, healing bool) ([]FileInfo, []error) {
	metadataArray := make([]FileInfo, len(disks))

	opts := ReadOptions{
		ReadData: readData,
		Healing:  healing,
	}

	g := errgroup.WithNErrs(len(disks))
	// Read `xl.meta` in parallel across disks.
	for index := range disks {
		index := index
		g.Go(func() (err error) {
			if disks[index] == nil {
				return errDiskNotFound
			}
			metadataArray[index], err = disks[index].ReadVersion(ctx, origbucket, bucket, object, versionID, opts)
			return err
		}, index)
	}

	return metadataArray, g.Wait()
}

// shuffleDisksAndPartsMetadataByIndex this function should be always used by GetObjectNInfo()
// and CompleteMultipartUpload code path, it is not meant to be used with PutObject,
// NewMultipartUpload metadata shuffling.
func shuffleDisksAndPartsMetadataByIndex(disks []StorageAPI, metaArr []FileInfo, fi FileInfo) (shuffledDisks []StorageAPI, shuffledPartsMetadata []FileInfo) {
	shuffledDisks = make([]StorageAPI, len(disks))
	shuffledPartsMetadata = make([]FileInfo, len(disks))
	distribution := fi.Erasure.Distribution

	var inconsistent int
	for i, meta := range metaArr {
		if disks[i] == nil {
			// Assuming offline drives as inconsistent,
			// to be safe and fallback to original
			// distribution order.
			inconsistent++
			continue
		}
		if !meta.IsValid() {
			inconsistent++
			continue
		}
		if meta.XLV1 != fi.XLV1 {
			inconsistent++
			continue
		}
		// check if erasure distribution order matches the index
		// position if this is not correct we discard the disk
		// and move to collect others
		if distribution[i] != meta.Erasure.Index {
			inconsistent++ // keep track of inconsistent entries
			continue
		}
		shuffledDisks[meta.Erasure.Index-1] = disks[i]
		shuffledPartsMetadata[meta.Erasure.Index-1] = metaArr[i]
	}

	// Inconsistent meta info is with in the limit of
	// expected quorum, proceed with EcIndex based
	// disk order.
	if inconsistent < fi.Erasure.ParityBlocks {
		return shuffledDisks, shuffledPartsMetadata
	}

	// fall back to original distribution based order.
	return shuffleDisksAndPartsMetadata(disks, metaArr, fi)
}

// Return shuffled partsMetadata depending on fi.Distribution.
// additional validation is attempted and invalid metadata is
// automatically skipped only when fi.ModTime is non-zero
// indicating that this is called during read-phase
func shuffleDisksAndPartsMetadata(disks []StorageAPI, partsMetadata []FileInfo, fi FileInfo) (shuffledDisks []StorageAPI, shuffledPartsMetadata []FileInfo) {
	shuffledDisks = make([]StorageAPI, len(disks))
	shuffledPartsMetadata = make([]FileInfo, len(partsMetadata))
	distribution := fi.Erasure.Distribution

	init := fi.ModTime.IsZero()
	// Shuffle slice xl metadata for expected distribution.
	for index := range partsMetadata {
		if disks[index] == nil {
			continue
		}
		if !init && !partsMetadata[index].IsValid() {
			// Check for parts metadata validity for only
			// fi.ModTime is not empty - ModTime is always set,
			// if object was ever written previously.
			continue
		}
		if !init && fi.XLV1 != partsMetadata[index].XLV1 {
			continue
		}
		blockIndex := distribution[index]
		shuffledPartsMetadata[blockIndex-1] = partsMetadata[index]
		shuffledDisks[blockIndex-1] = disks[index]
	}
	return shuffledDisks, shuffledPartsMetadata
}

func shuffleWithDist[T any](input []T, distribution []int) []T {
	if distribution == nil {
		return input
	}
	shuffled := make([]T, len(input))
	for index := range input {
		blockIndex := distribution[index]
		shuffled[blockIndex-1] = input[index]
	}
	return shuffled
}

// Return shuffled partsMetadata depending on distribution.
func shufflePartsMetadata(partsMetadata []FileInfo, distribution []int) []FileInfo {
	return shuffleWithDist[FileInfo](partsMetadata, distribution)
}

// shuffleCheckParts - shuffle CheckParts slice depending on the
// erasure distribution.
func shuffleCheckParts(parts []int, distribution []int) []int {
	return shuffleWithDist[int](parts, distribution)
}

// shuffleDisks - shuffle input disks slice depending on the
// erasure distribution. Return shuffled slice of disks with
// their expected distribution.
func shuffleDisks(disks []StorageAPI, distribution []int) []StorageAPI {
	return shuffleWithDist[StorageAPI](disks, distribution)
}

// evalDisks - returns a new slice of disks where nil is set if
// the corresponding error in errs slice is not nil
func evalDisks(disks []StorageAPI, errs []error) []StorageAPI {
	if len(errs) != len(disks) {
		bugLogIf(GlobalContext, errors.New("unexpected drives/errors slice length"))
		return nil
	}
	newDisks := make([]StorageAPI, len(disks))
	for index := range errs {
		if errs[index] == nil {
			newDisks[index] = disks[index]
		} else {
			newDisks[index] = nil
		}
	}
	return newDisks
}

// Errors specifically generated by calculatePartSizeFromIdx function.
var (
	errPartSizeZero  = errors.New("Part size cannot be zero")
	errPartSizeIndex = errors.New("Part index cannot be smaller than 1")
)

// calculatePartSizeFromIdx calculates the part size according to input index.
// returns error if totalSize is -1, partSize is 0, partIndex is 0.
func calculatePartSizeFromIdx(ctx context.Context, totalSize int64, partSize int64, partIndex int) (currPartSize int64, err error) {
	if totalSize < -1 {
		return 0, errInvalidArgument
	}
	if partSize == 0 {
		return 0, errPartSizeZero
	}
	if partIndex < 1 {
		return 0, errPartSizeIndex
	}
	if totalSize == -1 {
		return -1, nil
	}
	if totalSize > 0 {
		// Compute the total count of parts
		partsCount := totalSize/partSize + 1
		// Return the part's size
		switch {
		case int64(partIndex) < partsCount:
			currPartSize = partSize
		case int64(partIndex) == partsCount:
			// Size of last part
			currPartSize = totalSize % partSize
		default:
			currPartSize = 0
		}
	}
	return currPartSize, nil
}

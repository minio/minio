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
	"errors"
	"hash/crc32"

	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/sync/errgroup"
)

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
		errorCounts[err]++
	}

	max := 0
	for err, count := range errorCounts {
		switch {
		case max < count:
			max = count
			maxErr = err

		// Prefer `nil` over other error values with the same
		// number of occurrences.
		case max == count && err == nil:
			maxErr = err
		}
	}
	return max, maxErr
}

// reduceQuorumErrs behaves like reduceErrs by only for returning
// values of maximally occurring errors validated against a generic
// quorum number that can be read or write quorum depending on usage.
func reduceQuorumErrs(ctx context.Context, errs []error, ignoredErrs []error, quorum int, quorumErr error) error {
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
func readAllFileInfo(ctx context.Context, disks []StorageAPI, bucket, object, versionID string) ([]FileInfo, []error) {
	metadataArray := make([]FileInfo, len(disks))

	g := errgroup.WithNErrs(len(disks))
	// Read `xl.meta` parallelly across disks.
	for index := range disks {
		index := index
		g.Go(func() (err error) {
			if disks[index] == nil {
				return errDiskNotFound
			}
			metadataArray[index], err = disks[index].ReadVersion(bucket, object, versionID)
			if err != nil {
				if err != errFileNotFound && err != errVolumeNotFound && err != errFileVersionNotFound {
					logger.GetReqInfo(ctx).AppendTags("disk", disks[index].String())
					logger.LogIf(ctx, err)
				}
			}
			return err
		}, index)
	}

	// Return all the metadata.
	return metadataArray, g.Wait()
}

// Return shuffled partsMetadata depending on distribution.
func shufflePartsMetadata(partsMetadata []FileInfo, distribution []int) (shuffledPartsMetadata []FileInfo) {
	if distribution == nil {
		return partsMetadata
	}
	shuffledPartsMetadata = make([]FileInfo, len(partsMetadata))
	// Shuffle slice xl metadata for expected distribution.
	for index := range partsMetadata {
		blockIndex := distribution[index]
		shuffledPartsMetadata[blockIndex-1] = partsMetadata[index]
	}
	return shuffledPartsMetadata
}

// shuffleDisks - shuffle input disks slice depending on the
// erasure distribution. Return shuffled slice of disks with
// their expected distribution.
func shuffleDisks(disks []StorageAPI, distribution []int) (shuffledDisks []StorageAPI) {
	if distribution == nil {
		return disks
	}
	shuffledDisks = make([]StorageAPI, len(disks))
	// Shuffle disks for expected distribution.
	for index := range disks {
		blockIndex := distribution[index]
		shuffledDisks[blockIndex-1] = disks[index]
	}
	return shuffledDisks
}

// evalDisks - returns a new slice of disks where nil is set if
// the corresponding error in errs slice is not nil
func evalDisks(disks []StorageAPI, errs []error) []StorageAPI {
	if len(errs) != len(disks) {
		logger.LogIf(GlobalContext, errors.New("unexpected disks/errors slice length"))
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
		logger.LogIf(ctx, errInvalidArgument)
		return 0, errInvalidArgument
	}
	if partSize == 0 {
		logger.LogIf(ctx, errPartSizeZero)
		return 0, errPartSizeZero
	}
	if partIndex < 1 {
		logger.LogIf(ctx, errPartSizeIndex)
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

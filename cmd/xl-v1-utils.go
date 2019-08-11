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
	"errors"
	"hash/crc32"
	"path"
	"sync"
	"time"

	"github.com/minio/minio/cmd/logger"
	"github.com/valyala/fastjson"
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
	return reduceQuorumErrs(ctx, errs, ignoredErrs, readQuorum, errXLReadQuorum)
}

// reduceWriteQuorumErrs behaves like reduceErrs but only for returning
// values of maximally occurring errors validated against writeQuorum.
func reduceWriteQuorumErrs(ctx context.Context, errs []error, ignoredErrs []error, writeQuorum int) (maxErr error) {
	return reduceQuorumErrs(ctx, errs, ignoredErrs, writeQuorum, errXLWriteQuorum)
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

func parseXLStat(v *fastjson.Value) (si statInfo, err error) {
	// obtain stat info.
	st := v.GetObject("stat")
	var mb []byte
	mb, err = st.Get("modTime").StringBytes()
	if err != nil {
		return si, err
	}
	// fetching modTime.
	si.ModTime, err = time.Parse(time.RFC3339, string(mb))
	if err != nil {
		return si, err
	}
	// obtain Stat.Size .
	si.Size, err = st.Get("size").Int64()
	if err != nil {
		return si, err
	}
	return si, nil
}

func parseXLVersion(v *fastjson.Value) string {
	return string(v.GetStringBytes("version"))
}

func parseXLFormat(v *fastjson.Value) string {
	return string(v.GetStringBytes("format"))
}

func parseXLRelease(v *fastjson.Value) string {
	return string(v.GetStringBytes("minio", "release"))
}

func parseXLErasureInfo(ctx context.Context, v *fastjson.Value) (ErasureInfo, error) {
	erasure := ErasureInfo{}
	// parse the xlV1Meta.Erasure.Distribution.
	er := v.GetObject("erasure")
	disResult := er.Get("distribution").GetArray()
	distribution := make([]int, len(disResult))
	var err error
	for i, dis := range disResult {
		distribution[i], err = dis.Int()
		if err != nil {
			return erasure, err
		}
	}
	erasure.Distribution = distribution

	erasure.Algorithm = string(er.Get("algorithm").GetStringBytes())
	erasure.DataBlocks = er.Get("data").GetInt()
	erasure.ParityBlocks = er.Get("parity").GetInt()
	erasure.BlockSize = er.Get("blockSize").GetInt64()
	erasure.Index = er.Get("index").GetInt()
	checkSumsResult := er.Get("checksum").GetArray()

	// Parse xlMetaV1.Erasure.Checksum array.
	checkSums := make([]ChecksumInfo, len(checkSumsResult))
	for i, ck := range checkSumsResult {
		algorithm := BitrotAlgorithmFromString(string(ck.GetStringBytes("algorithm")))
		if !algorithm.Available() {
			logger.LogIf(ctx, errBitrotHashAlgoInvalid)
			return erasure, errBitrotHashAlgoInvalid
		}
		srcHash := ck.GetStringBytes("hash")
		n, err := hex.Decode(srcHash, srcHash)
		if err != nil {
			logger.LogIf(ctx, err)
			return erasure, err
		}
		nmb := ck.GetStringBytes("name")
		if nmb == nil {
			return erasure, errCorruptedFormat
		}
		checkSums[i] = ChecksumInfo{
			Name:      string(nmb),
			Algorithm: algorithm,
			Hash:      srcHash[:n],
		}
	}
	erasure.Checksums = checkSums
	return erasure, nil
}

func parseXLParts(partsResult []*fastjson.Value) []ObjectPartInfo {
	// Parse the XL Parts.
	partInfo := make([]ObjectPartInfo, len(partsResult))
	for i, p := range partsResult {
		partInfo[i] = ObjectPartInfo{
			Number:     p.GetInt("number"),
			Name:       string(p.GetStringBytes("name")),
			ETag:       string(p.GetStringBytes("etag")),
			Size:       p.GetInt64("size"),
			ActualSize: p.GetInt64("actualSize"),
		}
	}
	return partInfo
}

func parseXLMetaMap(v *fastjson.Value) map[string]string {
	metaMap := make(map[string]string)
	// Get xlMetaV1.Meta map.
	v.GetObject("meta").Visit(func(k []byte, kv *fastjson.Value) {
		metaMap[string(k)] = string(kv.GetStringBytes())
	})
	return metaMap
}

// xl.json Parser pool
var xlParserPool fastjson.ParserPool

// Constructs XLMetaV1 using `fastjson` lib to retrieve each field.
func xlMetaV1UnmarshalJSON(ctx context.Context, xlMetaBuf []byte) (xlMeta xlMetaV1, err error) {
	parser := xlParserPool.Get()
	defer xlParserPool.Put(parser)

	var v *fastjson.Value
	v, err = parser.ParseBytes(xlMetaBuf)
	if err != nil {
		return xlMeta, err
	}

	// obtain version.
	xlMeta.Version = parseXLVersion(v)
	// obtain format.
	xlMeta.Format = parseXLFormat(v)

	// Validate if the xl.json we read is sane, return corrupted format.
	if !isXLMetaFormatValid(xlMeta.Version, xlMeta.Format) {
		// For version mismatchs and unrecognized format, return corrupted format.
		logger.LogIf(ctx, errCorruptedFormat)
		return xlMeta, errCorruptedFormat
	}

	// Parse xlMetaV1.Stat .
	stat, err := parseXLStat(v)
	if err != nil {
		logger.LogIf(ctx, err)
		return xlMeta, err
	}

	xlMeta.Stat = stat
	// parse the xlV1Meta.Erasure fields.
	xlMeta.Erasure, err = parseXLErasureInfo(ctx, v)
	if err != nil {
		return xlMeta, err
	}

	// Check for scenario where checksum information missing for some parts.
	partsResult := v.Get("parts").GetArray()
	if len(xlMeta.Erasure.Checksums) != len(partsResult) {
		return xlMeta, errCorruptedFormat
	}

	// Parse the XL Parts.
	xlMeta.Parts = parseXLParts(partsResult)
	// Get the xlMetaV1.Realse field.
	xlMeta.Minio.Release = parseXLRelease(v)
	// parse xlMetaV1.
	xlMeta.Meta = parseXLMetaMap(v)

	return xlMeta, nil
}

// read xl.json from the given disk, parse and return xlV1MetaV1.Parts.
func readXLMetaParts(ctx context.Context, disk StorageAPI, bucket string, object string) ([]ObjectPartInfo, map[string]string, error) {
	// Reads entire `xl.json`.
	xlMetaBuf, err := disk.ReadAll(bucket, path.Join(object, xlMetaJSONFile))
	if err != nil {
		logger.LogIf(ctx, err)
		return nil, nil, err
	}

	var xlMeta xlMetaV1
	xlMeta, err = xlMetaV1UnmarshalJSON(ctx, xlMetaBuf)
	if err != nil {
		return nil, nil, err
	}

	return xlMeta.Parts, xlMeta.Meta, nil
}

// read xl.json from the given disk and parse xlV1Meta.Stat and xlV1Meta.Meta using fastjson.
func readXLMetaStat(ctx context.Context, disk StorageAPI, bucket string, object string) (si statInfo,
	mp map[string]string, e error) {
	// Reads entire `xl.json`.
	xlMetaBuf, err := disk.ReadAll(bucket, path.Join(object, xlMetaJSONFile))
	if err != nil {
		logger.LogIf(ctx, err)
		return si, nil, err
	}

	var xlMeta xlMetaV1
	xlMeta, err = xlMetaV1UnmarshalJSON(ctx, xlMetaBuf)
	if err != nil {
		return si, mp, err
	}

	// Return structured `xl.json`.
	return xlMeta.Stat, xlMeta.Meta, nil
}

// readXLMeta reads `xl.json` and returns back XL metadata structure.
func readXLMeta(ctx context.Context, disk StorageAPI, bucket string, object string) (xlMeta xlMetaV1, err error) {
	// Reads entire `xl.json`.
	xlMetaBuf, err := disk.ReadAll(bucket, path.Join(object, xlMetaJSONFile))
	if err != nil {
		if err != errFileNotFound && err != errVolumeNotFound {
			logger.GetReqInfo(ctx).AppendTags("disk", disk.String())
			logger.LogIf(ctx, err)
		}
		return xlMetaV1{}, err
	}
	if len(xlMetaBuf) == 0 {
		return xlMetaV1{}, errFileNotFound
	}
	xlMeta, err = xlMetaV1UnmarshalJSON(ctx, xlMetaBuf)
	if err != nil {
		logger.GetReqInfo(ctx).AppendTags("disk", disk.String())
		logger.LogIf(ctx, err)
		return xlMetaV1{}, err
	}
	// Return structured `xl.json`.
	return xlMeta, nil
}

// Reads all `xl.json` metadata as a xlMetaV1 slice.
// Returns error slice indicating the failed metadata reads.
func readAllXLMetadata(ctx context.Context, disks []StorageAPI, bucket, object string) ([]xlMetaV1, []error) {
	errs := make([]error, len(disks))
	metadataArray := make([]xlMetaV1, len(disks))
	var wg = &sync.WaitGroup{}
	// Read `xl.json` parallelly across disks.
	for index, disk := range disks {
		if disk == nil {
			errs[index] = errDiskNotFound
			continue
		}
		wg.Add(1)
		// Read `xl.json` in routine.
		go func(index int, disk StorageAPI) {
			defer wg.Done()
			var err error
			metadataArray[index], err = readXLMeta(ctx, disk, bucket, object)
			if err != nil {
				errs[index] = err
				return
			}
		}(index, disk)
	}

	// Wait for all the routines to finish.
	wg.Wait()

	// Return all the metadata.
	return metadataArray, errs
}

// Return shuffled partsMetadata depending on distribution.
func shufflePartsMetadata(partsMetadata []xlMetaV1, distribution []int) (shuffledPartsMetadata []xlMetaV1) {
	if distribution == nil {
		return partsMetadata
	}
	shuffledPartsMetadata = make([]xlMetaV1, len(partsMetadata))
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
		logger.LogIf(context.Background(), errors.New("unexpected disks/errors slice length"))
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

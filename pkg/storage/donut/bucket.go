/*
 * Minimalist Object Storage, (C) 2015 Minio, Inc.
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

package donut

import (
	"bytes"
	"fmt"
	"hash"
	"io"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"crypto/md5"
	"encoding/hex"
	"encoding/json"

	"github.com/minio/minio/pkg/iodine"
	"github.com/minio/minio/pkg/utils/crypto/sha512"
	"github.com/minio/minio/pkg/utils/split"
)

// internal struct carrying bucket specific information
type bucket struct {
	name      string
	acl       string
	time      time.Time
	donutName string
	nodes     map[string]node
	objects   map[string]object
	lock      *sync.RWMutex
}

// newBucket - instantiate a new bucket
func newBucket(bucketName, aclType, donutName string, nodes map[string]node) (bucket, map[string]string, error) {
	errParams := map[string]string{
		"bucketName": bucketName,
		"donutName":  donutName,
		"aclType":    aclType,
	}
	if strings.TrimSpace(bucketName) == "" || strings.TrimSpace(donutName) == "" {
		return bucket{}, nil, iodine.New(InvalidArgument{}, errParams)
	}
	bucketMetadata := make(map[string]string)
	bucketMetadata["acl"] = aclType
	t := time.Now().UTC()
	bucketMetadata["created"] = t.Format(time.RFC3339Nano)
	b := bucket{}
	b.name = bucketName
	b.acl = aclType
	b.time = t
	b.donutName = donutName
	b.nodes = nodes
	b.objects = make(map[string]object)
	b.lock = new(sync.RWMutex)
	return b, bucketMetadata, nil
}

func (b bucket) getBucketName() string {
	return b.name
}

func (b bucket) getObjectName(fileName, diskPath, bucketPath string) (string, error) {
	newObject, err := newObject(fileName, filepath.Join(diskPath, bucketPath))
	if err != nil {
		return "", iodine.New(err, nil)
	}
	newObjectMetadata, err := newObject.GetObjectMetadata()
	if err != nil {
		return "", iodine.New(err, nil)
	}
	if newObjectMetadata.Object == "" {
		return "", iodine.New(ObjectCorrupted{Object: newObject.name}, nil)
	}
	b.objects[newObjectMetadata.Object] = newObject
	return newObjectMetadata.Object, nil
}

func (b bucket) GetObjectMetadata(objectName string) (*objectMetadata, error) {
	return b.objects[objectName].GetObjectMetadata()
}

// ListObjects - list all objects
func (b bucket) ListObjects(prefix, marker, delimiter string, maxkeys int) ([]string, []string, bool, error) {
	b.lock.RLock()
	defer b.lock.RUnlock()

	if maxkeys <= 0 {
		maxkeys = 1000
	}
	var isTruncated bool
	nodeSlice := 0
	var objects []string
	for _, node := range b.nodes {
		disks, err := node.ListDisks()
		if err != nil {
			return nil, nil, false, iodine.New(err, nil)
		}
		for order, disk := range disks {
			bucketSlice := fmt.Sprintf("%s$%d$%d", b.name, nodeSlice, order)
			bucketPath := filepath.Join(b.donutName, bucketSlice)
			files, err := disk.ListDir(bucketPath)
			if err != nil {
				return nil, nil, false, iodine.New(err, nil)
			}
			for _, file := range files {
				objectName, err := b.getObjectName(file.Name(), disk.GetPath(), bucketPath)
				if err != nil {
					return nil, nil, false, iodine.New(err, nil)
				}
				if strings.HasPrefix(objectName, strings.TrimSpace(prefix)) {
					if objectName > marker {
						objects = appendUniq(objects, objectName)
					}
				}
			}
		}
		nodeSlice = nodeSlice + 1
	}
	{
		if strings.TrimSpace(prefix) != "" {
			objects = removePrefix(objects, prefix)
		}
		var prefixes []string
		var filteredObjects []string
		if strings.TrimSpace(delimiter) != "" {
			filteredObjects = filterDelimited(objects, delimiter)
			prefixes = filterNotDelimited(objects, delimiter)
			prefixes = extractDelimited(prefixes, delimiter)
			prefixes = uniqueObjects(prefixes)
		} else {
			filteredObjects = objects
		}
		var results []string
		var commonPrefixes []string

		sort.Strings(filteredObjects)
		for _, objectName := range filteredObjects {
			if len(results) >= maxkeys {
				isTruncated = true
				break
			}
			results = appendUniq(results, prefix+objectName)
		}
		for _, commonPrefix := range prefixes {
			commonPrefixes = appendUniq(commonPrefixes, prefix+commonPrefix)
		}
		sort.Strings(results)
		sort.Strings(commonPrefixes)
		return results, commonPrefixes, isTruncated, nil
	}
}

// ReadObject - open an object to read
func (b bucket) ReadObject(objectName string) (reader io.ReadCloser, size int64, err error) {
	b.lock.RLock()
	defer b.lock.RUnlock()
	reader, writer := io.Pipe()
	// get list of objects
	_, _, _, err = b.ListObjects(objectName, "", "", 1)
	if err != nil {
		return nil, 0, iodine.New(err, nil)
	}
	// check if object exists
	object, ok := b.objects[objectName]
	if !ok {
		return nil, 0, iodine.New(ObjectNotFound{Object: objectName}, nil)
	}
	// verify if donutObjectMetadata is readable, before we server the request
	donutObjMetadata, err := object.GetDonutObjectMetadata()
	if err != nil {
		return nil, 0, iodine.New(err, nil)
	}
	// read and reply back to GetObject() request in a go-routine
	go b.readEncodedData(b.normalizeObjectName(objectName), writer, donutObjMetadata)
	return reader, donutObjMetadata.Size, nil
}

// WriteObject - write a new object into bucket
func (b bucket) WriteObject(objectName string, objectData io.Reader, expectedMD5Sum string, metadata map[string]string) (string, error) {
	b.lock.Lock()
	defer b.lock.Unlock()
	if objectName == "" || objectData == nil {
		return "", iodine.New(InvalidArgument{}, nil)
	}
	writers, err := b.getDiskWriters(b.normalizeObjectName(objectName), "data")
	if err != nil {
		return "", iodine.New(err, nil)
	}
	sumMD5 := md5.New()
	sum512 := sha512.New()

	objectMetadata := new(objectMetadata)
	donutObjectMetadata := new(donutObjectMetadata)
	objectMetadata.Version = objectMetadataVersion
	donutObjectMetadata.Version = donutObjectMetadataVersion
	size := metadata["contentLength"]
	sizeInt, err := strconv.ParseInt(size, 10, 64)
	if err != nil {
		return "", iodine.New(err, nil)
	}

	// if total writers are only '1' do not compute erasure
	switch len(writers) == 1 {
	case true:
		mw := io.MultiWriter(writers[0], sumMD5, sum512)
		totalLength, err := io.CopyN(mw, objectData, sizeInt)
		if err != nil {
			return "", iodine.New(err, nil)
		}
		donutObjectMetadata.Size = totalLength
		objectMetadata.Size = totalLength
	case false:
		// calculate data and parity dictated by total number of writers
		k, m, err := b.getDataAndParity(len(writers))
		if err != nil {
			return "", iodine.New(err, nil)
		}
		// encoded data with k, m and write
		chunkCount, totalLength, err := b.writeEncodedData(k, m, writers, objectData, sumMD5, sum512)
		if err != nil {
			return "", iodine.New(err, nil)
		}
		/// donutMetadata section
		donutObjectMetadata.BlockSize = 10 * 1024 * 1024
		donutObjectMetadata.ChunkCount = chunkCount
		donutObjectMetadata.DataDisks = k
		donutObjectMetadata.ParityDisks = m
		donutObjectMetadata.ErasureTechnique = "Cauchy"
		donutObjectMetadata.Size = int64(totalLength)
		// keep size inside objectMetadata as well for Object API requests
		objectMetadata.Size = int64(totalLength)
	}
	objectMetadata.Bucket = b.getBucketName()
	objectMetadata.Object = objectName
	objectMetadata.Metadata = metadata
	dataMD5sum := sumMD5.Sum(nil)
	dataSHA512sum := sum512.Sum(nil)
	objectMetadata.Created = time.Now().UTC()

	// keeping md5sum for the object in two different places
	// one for object storage and another is for internal use
	hexMD5Sum := hex.EncodeToString(dataMD5sum)
	hex512Sum := hex.EncodeToString(dataSHA512sum)
	objectMetadata.MD5Sum = hexMD5Sum
	objectMetadata.SHA512Sum = hex512Sum
	donutObjectMetadata.MD5Sum = hexMD5Sum
	donutObjectMetadata.SHA512Sum = hex512Sum

	// Verify if the written object is equal to what is expected, only if it is requested as such
	if strings.TrimSpace(expectedMD5Sum) != "" {
		if err := b.isMD5SumEqual(strings.TrimSpace(expectedMD5Sum), objectMetadata.MD5Sum); err != nil {
			return "", iodine.New(err, nil)
		}
	}
	// write donut specific metadata
	if err := b.writeDonutObjectMetadata(b.normalizeObjectName(objectName), donutObjectMetadata); err != nil {
		return "", iodine.New(err, nil)
	}
	// write object specific metadata
	if err := b.writeObjectMetadata(b.normalizeObjectName(objectName), objectMetadata); err != nil {
		return "", iodine.New(err, nil)
	}
	// close all writers, when control flow reaches here
	for _, writer := range writers {
		writer.Close()
	}
	return objectMetadata.MD5Sum, nil
}

// isMD5SumEqual - returns error if md5sum mismatches, other its `nil`
func (b bucket) isMD5SumEqual(expectedMD5Sum, actualMD5Sum string) error {
	if strings.TrimSpace(expectedMD5Sum) != "" && strings.TrimSpace(actualMD5Sum) != "" {
		expectedMD5SumBytes, err := hex.DecodeString(expectedMD5Sum)
		if err != nil {
			return iodine.New(err, nil)
		}
		actualMD5SumBytes, err := hex.DecodeString(actualMD5Sum)
		if err != nil {
			return iodine.New(err, nil)
		}
		if !bytes.Equal(expectedMD5SumBytes, actualMD5SumBytes) {
			return iodine.New(BadDigest{}, nil)
		}
		return nil
	}
	return iodine.New(InvalidArgument{}, nil)
}

// writeObjectMetadata - write additional object metadata
func (b bucket) writeObjectMetadata(objectName string, objectMetadata *objectMetadata) error {
	if objectMetadata == nil {
		return iodine.New(InvalidArgument{}, nil)
	}
	objectMetadataWriters, err := b.getDiskWriters(objectName, objectMetadataConfig)
	if err != nil {
		return iodine.New(err, nil)
	}
	for _, objectMetadataWriter := range objectMetadataWriters {
		defer objectMetadataWriter.Close()
	}
	for _, objectMetadataWriter := range objectMetadataWriters {
		jenc := json.NewEncoder(objectMetadataWriter)
		if err := jenc.Encode(objectMetadata); err != nil {
			return iodine.New(err, nil)
		}
	}
	return nil
}

// writeDonutObjectMetadata - write donut related object metadata
func (b bucket) writeDonutObjectMetadata(objectName string, donutObjectMetadata *donutObjectMetadata) error {
	if donutObjectMetadata == nil {
		return iodine.New(InvalidArgument{}, nil)
	}
	donutObjectMetadataWriters, err := b.getDiskWriters(objectName, donutObjectMetadataConfig)
	if err != nil {
		return iodine.New(err, nil)
	}
	for _, donutObjectMetadataWriter := range donutObjectMetadataWriters {
		defer donutObjectMetadataWriter.Close()
	}
	for _, donutObjectMetadataWriter := range donutObjectMetadataWriters {
		jenc := json.NewEncoder(donutObjectMetadataWriter)
		if err := jenc.Encode(donutObjectMetadata); err != nil {
			return iodine.New(err, nil)
		}
	}
	return nil
}

// TODO - This a temporary normalization of objectNames, need to find a better way
//
// normalizedObjectName - all objectNames with "/" get normalized to a simple objectName
//
// example:
// user provided value - "this/is/my/deep/directory/structure"
// donut normalized value - "this-is-my-deep-directory-structure"
//
func (b bucket) normalizeObjectName(objectName string) string {
	// replace every '/' with '-'
	return strings.Replace(objectName, "/", "-", -1)
}

// getDataAndParity - calculate k, m (data and parity) values from number of disks
func (b bucket) getDataAndParity(totalWriters int) (k uint8, m uint8, err error) {
	if totalWriters <= 1 {
		return 0, 0, iodine.New(InvalidArgument{}, nil)
	}
	quotient := totalWriters / 2 // not using float or abs to let integer round off to lower value
	// quotient cannot be bigger than (255 / 2) = 127
	if quotient > 127 {
		return 0, 0, iodine.New(ParityOverflow{}, nil)
	}
	remainder := totalWriters % 2 // will be 1 for odd and 0 for even numbers
	k = uint8(quotient + remainder)
	m = uint8(quotient)
	return k, m, nil
}

// writeEncodedData -
func (b bucket) writeEncodedData(k, m uint8, writers []io.WriteCloser, objectData io.Reader, sumMD5, sum512 hash.Hash) (int, int, error) {
	chunks := split.Stream(objectData, 10*1024*1024)
	encoder, err := newEncoder(k, m, "Cauchy")
	if err != nil {
		return 0, 0, iodine.New(err, nil)
	}
	chunkCount := 0
	totalLength := 0
	for chunk := range chunks {
		if chunk.Err == nil {
			totalLength = totalLength + len(chunk.Data)
			encodedBlocks, _ := encoder.Encode(chunk.Data)
			sumMD5.Write(chunk.Data)
			sum512.Write(chunk.Data)
			for blockIndex, block := range encodedBlocks {
				_, err := io.Copy(writers[blockIndex], bytes.NewBuffer(block))
				if err != nil {
					return 0, 0, iodine.New(err, nil)
				}
			}
		}
		chunkCount = chunkCount + 1
	}
	return chunkCount, totalLength, nil
}

// readEncodedData -
func (b bucket) readEncodedData(objectName string, writer *io.PipeWriter, donutObjMetadata *donutObjectMetadata) {
	expectedMd5sum, err := hex.DecodeString(donutObjMetadata.MD5Sum)
	if err != nil {
		writer.CloseWithError(iodine.New(err, nil))
		return
	}
	readers, err := b.getDiskReaders(objectName, "data")
	if err != nil {
		writer.CloseWithError(iodine.New(err, nil))
		return
	}
	for _, reader := range readers {
		defer reader.Close()
	}
	hasher := md5.New()
	mwriter := io.MultiWriter(writer, hasher)
	switch len(readers) == 1 {
	case false:
		if donutObjMetadata.ErasureTechnique == "" {
			writer.CloseWithError(iodine.New(MissingErasureTechnique{}, nil))
			return
		}
		encoder, err := newEncoder(donutObjMetadata.DataDisks, donutObjMetadata.ParityDisks, donutObjMetadata.ErasureTechnique)
		if err != nil {
			writer.CloseWithError(iodine.New(err, nil))
			return
		}
		totalLeft := donutObjMetadata.Size
		for i := 0; i < donutObjMetadata.ChunkCount; i++ {
			decodedData, err := b.decodeEncodedData(totalLeft, int64(donutObjMetadata.BlockSize), readers, encoder, writer)
			if err != nil {
				writer.CloseWithError(iodine.New(err, nil))
				return
			}
			_, err = io.Copy(mwriter, bytes.NewBuffer(decodedData))
			if err != nil {
				writer.CloseWithError(iodine.New(err, nil))
				return
			}
			totalLeft = totalLeft - int64(donutObjMetadata.BlockSize)
		}
	case true:
		_, err := io.Copy(writer, readers[0])
		if err != nil {
			writer.CloseWithError(iodine.New(err, nil))
			return
		}
	}
	// check if decodedData md5sum matches
	if !bytes.Equal(expectedMd5sum, hasher.Sum(nil)) {
		writer.CloseWithError(iodine.New(ChecksumMismatch{}, nil))
		return
	}
	writer.Close()
	return
}

// decodeEncodedData -
func (b bucket) decodeEncodedData(totalLeft, blockSize int64, readers []io.ReadCloser, encoder encoder, writer *io.PipeWriter) ([]byte, error) {
	var curBlockSize int64
	if blockSize < totalLeft {
		curBlockSize = blockSize
	} else {
		curBlockSize = totalLeft // cast is safe, blockSize in if protects
	}
	curChunkSize, err := encoder.GetEncodedBlockLen(int(curBlockSize))
	if err != nil {
		return nil, iodine.New(err, nil)
	}
	encodedBytes := make([][]byte, len(readers))
	for i, reader := range readers {
		var bytesBuffer bytes.Buffer
		_, err := io.CopyN(&bytesBuffer, reader, int64(curChunkSize))
		if err != nil {
			return nil, iodine.New(err, nil)
		}
		encodedBytes[i] = bytesBuffer.Bytes()
	}
	decodedData, err := encoder.Decode(encodedBytes, int(curBlockSize))
	if err != nil {
		return nil, iodine.New(err, nil)
	}
	return decodedData, nil
}

// getDiskReaders -
func (b bucket) getDiskReaders(objectName, objectMeta string) ([]io.ReadCloser, error) {
	var readers []io.ReadCloser
	nodeSlice := 0
	for _, node := range b.nodes {
		disks, err := node.ListDisks()
		if err != nil {
			return nil, iodine.New(err, nil)
		}
		readers = make([]io.ReadCloser, len(disks))
		for order, disk := range disks {
			bucketSlice := fmt.Sprintf("%s$%d$%d", b.name, nodeSlice, order)
			objectPath := filepath.Join(b.donutName, bucketSlice, objectName, objectMeta)
			objectSlice, err := disk.OpenFile(objectPath)
			if err != nil {
				return nil, iodine.New(err, nil)
			}
			readers[order] = objectSlice
		}
		nodeSlice = nodeSlice + 1
	}
	return readers, nil
}

// getDiskWriters -
func (b bucket) getDiskWriters(objectName, objectMeta string) ([]io.WriteCloser, error) {
	var writers []io.WriteCloser
	nodeSlice := 0
	for _, node := range b.nodes {
		disks, err := node.ListDisks()
		if err != nil {
			return nil, iodine.New(err, nil)
		}
		writers = make([]io.WriteCloser, len(disks))
		for order, disk := range disks {
			bucketSlice := fmt.Sprintf("%s$%d$%d", b.name, nodeSlice, order)
			objectPath := filepath.Join(b.donutName, bucketSlice, objectName, objectMeta)
			objectSlice, err := disk.CreateFile(objectPath)
			if err != nil {
				return nil, iodine.New(err, nil)
			}
			writers[order] = objectSlice
		}
		nodeSlice = nodeSlice + 1
	}
	return writers, nil
}

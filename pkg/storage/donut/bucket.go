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
	"strconv"
	"strings"
	"sync"
	"time"

	"crypto/md5"
	"encoding/hex"
	"encoding/json"

	"github.com/minio/minio/pkg/iodine"
	"github.com/minio/minio/pkg/utils/split"
)

// internal struct carrying bucket specific information
type bucket struct {
	name      string
	acl       string
	time      time.Time
	donutName string
	nodes     map[string]Node
	lock      *sync.RWMutex
}

// newBucket - instantiate a new bucket
func newBucket(bucketName, aclType, donutName string, nodes map[string]Node) (bucket, map[string]string, error) {
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
	b.lock = new(sync.RWMutex)
	return b, bucketMetadata, nil
}

// ListObjects - list all objects
func (b bucket) ListObjects() (map[string]object, error) {
	b.lock.RLock()
	defer b.lock.RUnlock()
	nodeSlice := 0
	objects := make(map[string]object)
	for _, node := range b.nodes {
		disks, err := node.ListDisks()
		if err != nil {
			return nil, iodine.New(err, nil)
		}
		for order, disk := range disks {
			bucketSlice := fmt.Sprintf("%s$%d$%d", b.name, nodeSlice, order)
			bucketPath := filepath.Join(b.donutName, bucketSlice)
			files, err := disk.ListDir(bucketPath)
			if err != nil {
				return nil, iodine.New(err, nil)
			}
			for _, file := range files {
				newObject, err := newObject(file.Name(), filepath.Join(disk.GetPath(), bucketPath))
				if err != nil {
					return nil, iodine.New(err, nil)
				}
				newObjectMetadata, err := newObject.GetObjectMetadata()
				if err != nil {
					return nil, iodine.New(err, nil)
				}
				objectName, ok := newObjectMetadata["object"]
				if !ok {
					return nil, iodine.New(ObjectCorrupted{Object: newObject.name}, nil)
				}
				objects[objectName] = newObject
			}
		}
		nodeSlice = nodeSlice + 1
	}
	return objects, nil
}

// ReadObject - open an object to read
func (b bucket) ReadObject(objectName string) (reader io.ReadCloser, size int64, err error) {
	b.lock.RLock()
	defer b.lock.RUnlock()
	reader, writer := io.Pipe()
	// get list of objects
	objects, err := b.ListObjects()
	if err != nil {
		return nil, 0, iodine.New(err, nil)
	}
	// check if object exists
	object, ok := objects[objectName]
	if !ok {
		return nil, 0, iodine.New(ObjectNotFound{Object: objectName}, nil)
	}
	// verify if objectMetadata is readable, before we serve the request
	objectMetadata, err := object.GetObjectMetadata()
	if err != nil {
		return nil, 0, iodine.New(err, nil)
	}
	if objectName == "" || writer == nil || len(objectMetadata) == 0 {
		return nil, 0, iodine.New(InvalidArgument{}, nil)
	}
	size, err = strconv.ParseInt(objectMetadata["size"], 10, 64)
	if err != nil {
		return nil, 0, iodine.New(err, nil)
	}
	// verify if donutObjectMetadata is readable, before we server the request
	donutObjectMetadata, err := object.GetDonutObjectMetadata()
	if err != nil {
		return nil, 0, iodine.New(err, nil)
	}
	// read and reply back to GetObject() request in a go-routine
	go b.readEncodedData(b.normalizeObjectName(objectName), writer, donutObjectMetadata)
	return reader, size, nil
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
	summer := md5.New()
	objectMetadata := make(map[string]string)
	donutObjectMetadata := make(map[string]string)
	objectMetadata["version"] = objectMetadataVersion
	donutObjectMetadata["version"] = donutObjectMetadataVersion
	size := metadata["contentLength"]
	sizeInt, err := strconv.ParseInt(size, 10, 64)
	if err != nil {
		return "", iodine.New(err, nil)
	}

	// if total writers are only '1' do not compute erasure
	switch len(writers) == 1 {
	case true:
		mw := io.MultiWriter(writers[0], summer)
		totalLength, err := io.CopyN(mw, objectData, sizeInt)
		if err != nil {
			return "", iodine.New(err, nil)
		}
		donutObjectMetadata["sys.size"] = strconv.FormatInt(totalLength, 10)
		objectMetadata["size"] = strconv.FormatInt(totalLength, 10)
	case false:
		// calculate data and parity dictated by total number of writers
		k, m, err := b.getDataAndParity(len(writers))
		if err != nil {
			return "", iodine.New(err, nil)
		}
		// encoded data with k, m and write
		chunkCount, totalLength, err := b.writeEncodedData(k, m, writers, objectData, summer)
		if err != nil {
			return "", iodine.New(err, nil)
		}
		/// donutMetadata section
		donutObjectMetadata["sys.blockSize"] = strconv.Itoa(10 * 1024 * 1024)
		donutObjectMetadata["sys.chunkCount"] = strconv.Itoa(chunkCount)
		donutObjectMetadata["sys.erasureK"] = strconv.FormatUint(uint64(k), 10)
		donutObjectMetadata["sys.erasureM"] = strconv.FormatUint(uint64(m), 10)
		donutObjectMetadata["sys.erasureTechnique"] = "Cauchy"
		donutObjectMetadata["sys.size"] = strconv.Itoa(totalLength)
		// keep size inside objectMetadata as well for Object API requests
		objectMetadata["size"] = strconv.Itoa(totalLength)
	}
	objectMetadata["bucket"] = b.name
	objectMetadata["object"] = objectName
	// store all user provided metadata
	for k, v := range metadata {
		objectMetadata[k] = v
	}
	dataMd5sum := summer.Sum(nil)
	objectMetadata["created"] = time.Now().UTC().Format(time.RFC3339Nano)

	// keeping md5sum for the object in two different places
	// one for object storage and another is for internal use
	objectMetadata["md5"] = hex.EncodeToString(dataMd5sum)
	donutObjectMetadata["sys.md5"] = hex.EncodeToString(dataMd5sum)

	// Verify if the written object is equal to what is expected, only if it is requested as such
	if strings.TrimSpace(expectedMD5Sum) != "" {
		if err := b.isMD5SumEqual(strings.TrimSpace(expectedMD5Sum), objectMetadata["md5"]); err != nil {
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
	return objectMetadata["md5"], nil
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
func (b bucket) writeObjectMetadata(objectName string, objectMetadata map[string]string) error {
	if len(objectMetadata) == 0 {
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
func (b bucket) writeDonutObjectMetadata(objectName string, objectMetadata map[string]string) error {
	if len(objectMetadata) == 0 {
		return iodine.New(InvalidArgument{}, nil)
	}
	objectMetadataWriters, err := b.getDiskWriters(objectName, donutObjectMetadataConfig)
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
func (b bucket) writeEncodedData(k, m uint8, writers []io.WriteCloser, objectData io.Reader, summer hash.Hash) (int, int, error) {
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
			summer.Write(chunk.Data)
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
func (b bucket) readEncodedData(objectName string, writer *io.PipeWriter, donutObjectMetadata map[string]string) {
	expectedMd5sum, err := hex.DecodeString(donutObjectMetadata["sys.md5"])
	if err != nil {
		writer.CloseWithError(iodine.New(err, nil))
		return
	}
	readers, err := b.getDiskReaders(objectName, "data")
	if err != nil {
		writer.CloseWithError(iodine.New(err, nil))
		return
	}
	hasher := md5.New()
	mwriter := io.MultiWriter(writer, hasher)
	switch len(readers) == 1 {
	case false:
		totalChunks, totalLeft, blockSize, k, m, err := b.donutMetadata2Values(donutObjectMetadata)
		if err != nil {
			writer.CloseWithError(iodine.New(err, nil))
			return
		}
		technique, ok := donutObjectMetadata["sys.erasureTechnique"]
		if !ok {
			writer.CloseWithError(iodine.New(MissingErasureTechnique{}, nil))
			return
		}
		encoder, err := newEncoder(uint8(k), uint8(m), technique)
		if err != nil {
			writer.CloseWithError(iodine.New(err, nil))
			return
		}
		for i := 0; i < totalChunks; i++ {
			decodedData, err := b.decodeEncodedData(totalLeft, blockSize, readers, encoder, writer)
			if err != nil {
				writer.CloseWithError(iodine.New(err, nil))
				return
			}
			_, err = io.Copy(mwriter, bytes.NewBuffer(decodedData))
			if err != nil {
				writer.CloseWithError(iodine.New(err, nil))
				return
			}
			totalLeft = totalLeft - int64(blockSize)
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

// donutMetadata2Values -
func (b bucket) donutMetadata2Values(donutObjectMetadata map[string]string) (totalChunks int, totalLeft, blockSize int64, k, m uint64, err error) {
	totalChunks, err = strconv.Atoi(donutObjectMetadata["sys.chunkCount"])
	if err != nil {
		return 0, 0, 0, 0, 0, iodine.New(err, nil)
	}
	totalLeft, err = strconv.ParseInt(donutObjectMetadata["sys.size"], 10, 64)
	if err != nil {
		return 0, 0, 0, 0, 0, iodine.New(err, nil)
	}
	blockSize, err = strconv.ParseInt(donutObjectMetadata["sys.blockSize"], 10, 64)
	if err != nil {
		return 0, 0, 0, 0, 0, iodine.New(err, nil)
	}
	k, err = strconv.ParseUint(donutObjectMetadata["sys.erasureK"], 10, 8)
	if err != nil {
		return 0, 0, 0, 0, 0, iodine.New(err, nil)
	}
	m, err = strconv.ParseUint(donutObjectMetadata["sys.erasureM"], 10, 8)
	if err != nil {
		return 0, 0, 0, 0, 0, iodine.New(err, nil)
	}
	return totalChunks, totalLeft, blockSize, k, m, nil
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

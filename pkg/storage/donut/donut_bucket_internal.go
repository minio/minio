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
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"path"
	"strconv"
	"strings"

	"github.com/minio-io/iodine"
	"github.com/minio-io/minio/pkg/utils/split"
)

/// This file contains all the internal functions used by Bucket interface

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
			return iodine.New(errors.New("bad digest, md5sum mismatch"), nil)
		}
		return nil
	}
	return iodine.New(errors.New("invalid argument"), nil)
}

// writeObjectMetadata - write additional object metadata
func (b bucket) writeObjectMetadata(objectName string, objectMetadata map[string]string) error {
	if len(objectMetadata) == 0 {
		return iodine.New(errors.New("invalid argument"), nil)
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
		return iodine.New(errors.New("invalid argument"), nil)
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
		return 0, 0, iodine.New(errors.New("invalid argument"), nil)
	}
	quotient := totalWriters / 2 // not using float or abs to let integer round off to lower value
	// quotient cannot be bigger than (255 / 2) = 127
	if quotient > 127 {
		return 0, 0, iodine.New(errors.New("parity over flow"), nil)
	}
	remainder := totalWriters % 2 // will be 1 for odd and 0 for even numbers
	k = uint8(quotient + remainder)
	m = uint8(quotient)
	return k, m, nil
}

// writeEncodedData -
func (b bucket) writeEncodedData(k, m uint8, writers []io.WriteCloser, objectData io.Reader, summer hash.Hash) (int, int, error) {
	chunks := split.Stream(objectData, 10*1024*1024)
	encoder, err := NewEncoder(k, m, "Cauchy")
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
			err := errors.New("missing erasure Technique")
			writer.CloseWithError(iodine.New(err, nil))
			return
		}
		encoder, err := NewEncoder(uint8(k), uint8(m), technique)
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
		err := errors.New("checksum mismatch")
		writer.CloseWithError(iodine.New(err, nil))
		return
	}
	writer.Close()
	return
}

// decodeEncodedData -
func (b bucket) decodeEncodedData(totalLeft, blockSize int64, readers []io.ReadCloser, encoder Encoder, writer *io.PipeWriter) ([]byte, error) {
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
		for _, disk := range disks {
			bucketSlice := fmt.Sprintf("%s$%d$%d", b.name, nodeSlice, disk.GetOrder())
			objectPath := path.Join(b.donutName, bucketSlice, objectName, objectMeta)
			objectSlice, err := disk.OpenFile(objectPath)
			if err != nil {
				return nil, iodine.New(err, nil)
			}
			readers[disk.GetOrder()] = objectSlice
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
		for _, disk := range disks {
			bucketSlice := fmt.Sprintf("%s$%d$%d", b.name, nodeSlice, disk.GetOrder())
			objectPath := path.Join(b.donutName, bucketSlice, objectName, objectMeta)
			objectSlice, err := disk.MakeFile(objectPath)
			if err != nil {
				return nil, iodine.New(err, nil)
			}
			writers[disk.GetOrder()] = objectSlice
		}
		nodeSlice = nodeSlice + 1
	}
	return writers, nil
}

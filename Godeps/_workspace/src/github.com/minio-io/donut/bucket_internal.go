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
	"errors"
	"fmt"
	"io"
	"path"
	"strconv"
)

func (b bucket) getDataAndParity(totalWriters int) (k uint8, m uint8, err error) {
	if totalWriters <= 1 {
		return 0, 0, errors.New("invalid argument")
	}
	quotient := totalWriters / 2 // not using float or abs to let integer round off to lower value
	// quotient cannot be bigger than (255 / 2) = 127
	if quotient > 127 {
		return 0, 0, errors.New("parity over flow")
	}
	remainder := totalWriters % 2 // will be 1 for odd and 0 for even numbers
	k = uint8(quotient + remainder)
	m = uint8(quotient)
	return k, m, nil
}

func (b bucket) getObject(objectName string, writer *io.PipeWriter, donutObjectMetadata map[string]string) {
	expectedMd5sum, err := hex.DecodeString(donutObjectMetadata["md5"])
	if err != nil {
		writer.CloseWithError(err)
		return
	}
	readers, err := b.getDiskReaders(objectName, "data")
	if err != nil {
		writer.CloseWithError(err)
		return
	}
	hasher := md5.New()
	mwriter := io.MultiWriter(writer, hasher)
	switch len(readers) == 1 {
	case false:
		totalChunks, totalLeft, blockSize, k, m, err := b.metadata2Values(donutObjectMetadata)
		if err != nil {
			writer.CloseWithError(err)
			return
		}
		technique, ok := donutObjectMetadata["erasureTechnique"]
		if !ok {
			writer.CloseWithError(errors.New("missing erasure Technique"))
			return
		}
		encoder, err := NewEncoder(uint8(k), uint8(m), technique)
		if err != nil {
			writer.CloseWithError(err)
			return
		}
		for i := 0; i < totalChunks; i++ {
			decodedData, err := b.decodeData(totalLeft, blockSize, readers, encoder, writer)
			if err != nil {
				writer.CloseWithError(err)
				return
			}
			_, err = io.Copy(mwriter, bytes.NewBuffer(decodedData))
			if err != nil {
				writer.CloseWithError(err)
				return
			}
			totalLeft = totalLeft - int64(blockSize)
		}
	case true:
		_, err := io.Copy(writer, readers[0])
		if err != nil {
			writer.CloseWithError(err)
			return
		}
	}
	// check if decodedData md5sum matches
	if !bytes.Equal(expectedMd5sum, hasher.Sum(nil)) {
		writer.CloseWithError(errors.New("checksum mismatch"))
		return
	}
	writer.Close()
	return
}

func (b bucket) decodeData(totalLeft, blockSize int64, readers []io.ReadCloser, encoder Encoder, writer *io.PipeWriter) ([]byte, error) {
	var curBlockSize int64
	if blockSize < totalLeft {
		curBlockSize = blockSize
	} else {
		curBlockSize = totalLeft // cast is safe, blockSize in if protects
	}
	curChunkSize, err := encoder.GetEncodedBlockLen(int(curBlockSize))
	if err != nil {
		return nil, err
	}
	encodedBytes := make([][]byte, len(readers))
	for i, reader := range readers {
		var bytesBuffer bytes.Buffer
		_, err := io.CopyN(&bytesBuffer, reader, int64(curChunkSize))
		if err != nil {
			return nil, err
		}
		encodedBytes[i] = bytesBuffer.Bytes()
	}
	decodedData, err := encoder.Decode(encodedBytes, int(curBlockSize))
	if err != nil {
		return nil, err
	}
	return decodedData, nil
}

func (b bucket) metadata2Values(donutObjectMetadata map[string]string) (totalChunks int, totalLeft, blockSize int64, k, m uint64, err error) {
	totalChunks, err = strconv.Atoi(donutObjectMetadata["chunkCount"])
	totalLeft, err = strconv.ParseInt(donutObjectMetadata["size"], 10, 64)
	blockSize, err = strconv.ParseInt(donutObjectMetadata["blockSize"], 10, 64)
	k, err = strconv.ParseUint(donutObjectMetadata["erasureK"], 10, 8)
	m, err = strconv.ParseUint(donutObjectMetadata["erasureM"], 10, 8)
	return
}

func (b bucket) getDiskReaders(objectName, objectMeta string) ([]io.ReadCloser, error) {
	var readers []io.ReadCloser
	nodeSlice := 0
	for _, node := range b.nodes {
		disks, err := node.ListDisks()
		if err != nil {
			return nil, err
		}
		readers = make([]io.ReadCloser, len(disks))
		for _, disk := range disks {
			bucketSlice := fmt.Sprintf("%s$%d$%d", b.name, nodeSlice, disk.GetOrder())
			objectPath := path.Join(b.donutName, bucketSlice, objectName, objectMeta)
			objectSlice, err := disk.OpenFile(objectPath)
			if err != nil {
				return nil, err
			}
			readers[disk.GetOrder()] = objectSlice
		}
		nodeSlice = nodeSlice + 1
	}
	return readers, nil
}

func (b bucket) getDiskWriters(objectName, objectMeta string) ([]io.WriteCloser, error) {
	var writers []io.WriteCloser
	nodeSlice := 0
	for _, node := range b.nodes {
		disks, err := node.ListDisks()
		if err != nil {
			return nil, err
		}
		writers = make([]io.WriteCloser, len(disks))
		for _, disk := range disks {
			bucketSlice := fmt.Sprintf("%s$%d$%d", b.name, nodeSlice, disk.GetOrder())
			objectPath := path.Join(b.donutName, bucketSlice, objectName, objectMeta)
			objectSlice, err := disk.MakeFile(objectPath)
			if err != nil {
				return nil, err
			}
			writers[disk.GetOrder()] = objectSlice
		}
		nodeSlice = nodeSlice + 1
	}
	return writers, nil
}

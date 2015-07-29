/*
 * Minio Cloud Storage, (C) 2015 Minio, Inc.
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
	"io"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
	"hash"

	"crypto/md5"
	"encoding/hex"
	"encoding/json"

	"github.com/minio/minio/pkg/crypto/sha256"
	"github.com/minio/minio/pkg/crypto/sha512"
	"github.com/minio/minio/pkg/donut/disk"
	"github.com/minio/minio/pkg/iodine"
)

const (
	blockSize = 10 * 1024 * 1024
)

// internal struct carrying bucket specific information
type bucket struct {
	name      string
	acl       string
	time      time.Time
	donutName string
	nodes     map[string]node
	lock      *sync.Mutex
}

// newBucket - instantiate a new bucket
func newBucket(bucketName, aclType, donutName string, nodes map[string]node) (bucket, BucketMetadata, error) {
	errParams := map[string]string{
		"bucketName": bucketName,
		"donutName":  donutName,
		"aclType":    aclType,
	}

	if strings.TrimSpace(bucketName) == "" || strings.TrimSpace(donutName) == "" {
		return bucket{}, BucketMetadata{}, iodine.New(InvalidArgument{}, errParams)
	}

	b := bucket{}
	t := time.Now().UTC()
	b.name = bucketName
	b.acl = aclType
	b.time = t
	b.donutName = donutName
	b.nodes = nodes
	b.lock = new(sync.Mutex)

	metadata := BucketMetadata{}
	metadata.Version = bucketMetadataVersion
	metadata.Name = bucketName
	metadata.ACL = BucketACL(aclType)
	metadata.Created = t
	metadata.Metadata = make(map[string]string)
	metadata.BucketObjects = make(map[string]struct{})

	return b, metadata, nil
}

// getBucketName -
func (b bucket) getBucketName() string {
	return b.name
}

// getBucketMetadataReaders -
func (b bucket) getBucketMetadataReaders() (map[int]io.ReadCloser, error) {
	readers := make(map[int]io.ReadCloser)
	var disks map[int]disk.Disk
	var err error
	for _, node := range b.nodes {
		disks, err = node.ListDisks()
		if err != nil {
			return nil, iodine.New(err, nil)
		}
	}
	var bucketMetaDataReader io.ReadCloser
	for order, disk := range disks {
		bucketMetaDataReader, err = disk.OpenFile(filepath.Join(b.donutName, bucketMetadataConfig))
		if err != nil {
			continue
		}
		readers[order] = bucketMetaDataReader
	}
	if err != nil {
		return nil, iodine.New(err, nil)
	}
	return readers, nil
}

// getBucketMetadata -
func (b bucket) getBucketMetadata() (*AllBuckets, error) {
	var err error
	metadata := new(AllBuckets)
	readers, err := b.getBucketMetadataReaders()
	if err != nil {
		return nil, iodine.New(err, nil)
	}
	for _, reader := range readers {
		defer reader.Close()
	}
	for _, reader := range readers {
		jenc := json.NewDecoder(reader)
		if err = jenc.Decode(metadata); err == nil {
			return metadata, nil
		}
	}
	return nil, iodine.New(err, nil)
}

// GetObjectMetadata - get metadata for an object
func (b bucket) GetObjectMetadata(objectName string) (ObjectMetadata, error) {
	b.lock.Lock()
	defer b.lock.Unlock()
	return b.readObjectMetadata(objectName)
}

// ListObjects - list all objects
func (b bucket) ListObjects(prefix, marker, delimiter string, maxkeys int) (ListObjectsResults, error) {
	b.lock.Lock()
	defer b.lock.Unlock()
	if maxkeys <= 0 {
		maxkeys = 1000
	}
	var isTruncated bool
	var objects []string
	bucketMetadata, err := b.getBucketMetadata()
	if err != nil {
		return ListObjectsResults{}, iodine.New(err, nil)
	}
	for objectName := range bucketMetadata.Buckets[b.getBucketName()].Multiparts {
		if strings.HasPrefix(objectName, strings.TrimSpace(prefix)) {
			if objectName > marker {
				objects = append(objects, objectName)
			}
		}
	}
	for objectName := range bucketMetadata.Buckets[b.getBucketName()].BucketObjects {
		if strings.HasPrefix(objectName, strings.TrimSpace(prefix)) {
			if objectName > marker {
				objects = append(objects, objectName)
			}
		}
	}
	if strings.TrimSpace(prefix) != "" {
		objects = TrimPrefix(objects, prefix)
	}
	var prefixes []string
	var filteredObjects []string
	filteredObjects = objects
	if strings.TrimSpace(delimiter) != "" {
		filteredObjects = HasNoDelimiter(objects, delimiter)
		prefixes = HasDelimiter(objects, delimiter)
		prefixes = SplitDelimiter(prefixes, delimiter)
		prefixes = SortUnique(prefixes)
	}
	var results []string
	var commonPrefixes []string

	for _, commonPrefix := range prefixes {
		commonPrefixes = append(commonPrefixes, prefix+commonPrefix)
	}
	filteredObjects = RemoveDuplicates(filteredObjects)
	sort.Strings(filteredObjects)
	for _, objectName := range filteredObjects {
		if len(results) >= maxkeys {
			isTruncated = true
			break
		}
		results = append(results, prefix+objectName)
	}
	results = RemoveDuplicates(results)
	commonPrefixes = RemoveDuplicates(commonPrefixes)
	sort.Strings(commonPrefixes)

	listObjects := ListObjectsResults{}
	listObjects.Objects = make(map[string]ObjectMetadata)
	listObjects.CommonPrefixes = commonPrefixes
	listObjects.IsTruncated = isTruncated

	for _, objectName := range results {
		objMetadata, err := b.readObjectMetadata(normalizeObjectName(objectName))
		if err != nil {
			return ListObjectsResults{}, iodine.New(err, nil)
		}
		listObjects.Objects[objectName] = objMetadata
	}
	return listObjects, nil
}

// ReadObject - open an object to read
func (b bucket) ReadObject(objectName string) (reader io.ReadCloser, size int64, err error) {
	b.lock.Lock()
	defer b.lock.Unlock()
	reader, writer := io.Pipe()
	// get list of objects
	bucketMetadata, err := b.getBucketMetadata()
	if err != nil {
		return nil, 0, iodine.New(err, nil)
	}
	// check if object exists
	if _, ok := bucketMetadata.Buckets[b.getBucketName()].BucketObjects[objectName]; !ok {
		return nil, 0, iodine.New(ObjectNotFound{Object: objectName}, nil)
	}
	objMetadata, err := b.readObjectMetadata(normalizeObjectName(objectName))
	if err != nil {
		return nil, 0, iodine.New(err, nil)
	}
	// read and reply back to GetObject() request in a go-routine
	go b.readObjectData(normalizeObjectName(objectName), writer, objMetadata)
	return reader, objMetadata.Size, nil
}

// WriteObject - write a new object into bucket
func (b bucket) WriteObject(objectName string, objectData io.Reader, size int64, expectedMD5Sum string, metadata map[string]string, signature *Signature) (ObjectMetadata, error) {
	b.lock.Lock()
	defer b.lock.Unlock()
	if objectName == "" || objectData == nil {
		return ObjectMetadata{}, iodine.New(InvalidArgument{}, nil)
	}
	writers, err := b.getObjectWriters(normalizeObjectName(objectName), "data")
	if err != nil {
		return ObjectMetadata{}, iodine.New(err, nil)
	}
	sumMD5 := md5.New()
	sum512 := sha512.New()
	var sum256 hash.Hash
	var mwriter io.Writer

	if signature != nil {
		sum256 = sha256.New()
		mwriter = io.MultiWriter(sumMD5, sum256, sum512)
	} else {
		mwriter = io.MultiWriter(sumMD5, sum512)
	}
	objMetadata := ObjectMetadata{}
	objMetadata.Version = objectMetadataVersion
	objMetadata.Created = time.Now().UTC()
	// if total writers are only '1' do not compute erasure
	switch len(writers) == 1 {
	case true:
		mw := io.MultiWriter(writers[0], mwriter)
		totalLength, err := io.Copy(mw, objectData)
		if err != nil {
			CleanupWritersOnError(writers)
			return ObjectMetadata{}, iodine.New(err, nil)
		}
		objMetadata.Size = totalLength
	case false:
		// calculate data and parity dictated by total number of writers
		k, m, err := b.getDataAndParity(len(writers))
		if err != nil {
			CleanupWritersOnError(writers)
			return ObjectMetadata{}, iodine.New(err, nil)
		}
		// write encoded data with k, m and writers
		chunkCount, totalLength, err := b.writeObjectData(k, m, writers, objectData, size, mwriter)
		if err != nil {
			CleanupWritersOnError(writers)
			return ObjectMetadata{}, iodine.New(err, nil)
		}
		/// donutMetadata section
		objMetadata.BlockSize = blockSize
		objMetadata.ChunkCount = chunkCount
		objMetadata.DataDisks = k
		objMetadata.ParityDisks = m
		objMetadata.ErasureTechnique = "Cauchy"
		objMetadata.Size = int64(totalLength)
	}
	objMetadata.Bucket = b.getBucketName()
	objMetadata.Object = objectName
	dataMD5sum := sumMD5.Sum(nil)
	dataSHA512sum := sum512.Sum(nil)
	if signature != nil {
		ok, err := signature.DoesSignatureMatch(hex.EncodeToString(sum256.Sum(nil)))
		if err != nil {
			// error occurred while doing signature calculation, we return and also cleanup any temporary writers.
			CleanupWritersOnError(writers)
			return ObjectMetadata{}, iodine.New(err, nil)
		}
		if !ok {
			// purge all writers, when control flow reaches here
			//
			// Signature mismatch occurred all temp files to be removed and all data purged.
			CleanupWritersOnError(writers)
			return ObjectMetadata{}, iodine.New(SignatureDoesNotMatch{}, nil)
		}
	}
	objMetadata.MD5Sum = hex.EncodeToString(dataMD5sum)
	objMetadata.SHA512Sum = hex.EncodeToString(dataSHA512sum)

	// Verify if the written object is equal to what is expected, only if it is requested as such
	if strings.TrimSpace(expectedMD5Sum) != "" {
		if err := b.isMD5SumEqual(strings.TrimSpace(expectedMD5Sum), objMetadata.MD5Sum); err != nil {
			return ObjectMetadata{}, iodine.New(err, nil)
		}
	}
	objMetadata.Metadata = metadata
	// write object specific metadata
	if err := b.writeObjectMetadata(normalizeObjectName(objectName), objMetadata); err != nil {
		// purge all writers, when control flow reaches here
		CleanupWritersOnError(writers)
		return ObjectMetadata{}, iodine.New(err, nil)
	}
	// close all writers, when control flow reaches here
	for _, writer := range writers {
		writer.Close()
	}
	return objMetadata, nil
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
func (b bucket) writeObjectMetadata(objectName string, objMetadata ObjectMetadata) error {
	if objMetadata.Object == "" {
		return iodine.New(InvalidArgument{}, nil)
	}
	objMetadataWriters, err := b.getObjectWriters(objectName, objectMetadataConfig)
	if err != nil {
		return iodine.New(err, nil)
	}
	for _, objMetadataWriter := range objMetadataWriters {
		jenc := json.NewEncoder(objMetadataWriter)
		if err := jenc.Encode(&objMetadata); err != nil {
			// Close writers and purge all temporary entries
			CleanupWritersOnError(objMetadataWriters)
			return iodine.New(err, nil)
		}
	}
	for _, objMetadataWriter := range objMetadataWriters {
		objMetadataWriter.Close()
	}
	return nil
}

// readObjectMetadata - read object metadata
func (b bucket) readObjectMetadata(objectName string) (ObjectMetadata, error) {
	if objectName == "" {
		return ObjectMetadata{}, iodine.New(InvalidArgument{}, nil)
	}
	var err error
	objMetadata := ObjectMetadata{}
	objMetadataReaders, err := b.getObjectReaders(objectName, objectMetadataConfig)
	if err != nil {
		return ObjectMetadata{}, iodine.New(err, nil)
	}
	for _, objMetadataReader := range objMetadataReaders {
		defer objMetadataReader.Close()
	}
	for _, objMetadataReader := range objMetadataReaders {
		jdec := json.NewDecoder(objMetadataReader)
		if err = jdec.Decode(&objMetadata); err == nil {
			return objMetadata, nil
		}
	}
	return ObjectMetadata{}, iodine.New(err, nil)
}

// TODO - This a temporary normalization of objectNames, need to find a better way
//
// normalizedObjectName - all objectNames with "/" get normalized to a simple objectName
//
// example:
// user provided value - "this/is/my/deep/directory/structure"
// donut normalized value - "this-is-my-deep-directory-structure"
//
func normalizeObjectName(objectName string) string {
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

// writeObjectData -
func (b bucket) writeObjectData(k, m uint8, writers []io.WriteCloser, objectData io.Reader, size int64, writer io.Writer) (int, int, error) {
	encoder, err := newEncoder(k, m, "Cauchy")
	chunkSize := int64(10 * 1024 * 1024)
	if err != nil {
		return 0, 0, iodine.New(err, nil)
	}
	chunkCount := 0
	totalLength := 0
	remaining := size
	for remaining > 0 {
		readSize := chunkSize
		if remaining < chunkSize {
			readSize = remaining
		}
		remaining = remaining - readSize
		totalLength = totalLength + int(readSize)
		encodedBlocks, inputData, err := encoder.EncodeStream(objectData, readSize)
		if err != nil {
			return 0, 0, iodine.New(err, nil)
		}
		_, err = writer.Write(inputData)
		if err != nil {
			return 0, 0, iodine.New(err, nil)
		}
		for blockIndex, block := range encodedBlocks {
			errCh := make(chan error, 1)
			go func(writer io.Writer, reader io.Reader, errCh chan<- error) {
				defer close(errCh)
				_, err := io.Copy(writer, reader)
				errCh <- err
			}(writers[blockIndex], bytes.NewReader(block), errCh)
			if err := <-errCh; err != nil {
				// Returning error is fine here CleanupErrors() would cleanup writers
				return 0, 0, iodine.New(err, nil)
			}
		}
		chunkCount = chunkCount + 1
	}
	return chunkCount, totalLength, nil
}

// readObjectData -
func (b bucket) readObjectData(objectName string, writer *io.PipeWriter, objMetadata ObjectMetadata) {
	readers, err := b.getObjectReaders(objectName, "data")
	if err != nil {
		writer.CloseWithError(iodine.New(err, nil))
		return
	}
	for _, reader := range readers {
		defer reader.Close()
	}
	expectedMd5sum, err := hex.DecodeString(objMetadata.MD5Sum)
	if err != nil {
		writer.CloseWithError(iodine.New(err, nil))
		return
	}
	expected512Sum, err := hex.DecodeString(objMetadata.SHA512Sum)
	if err != nil {
		writer.CloseWithError(iodine.New(err, nil))
		return
	}
	hasher := md5.New()
	sum512hasher := sha256.New()
	mwriter := io.MultiWriter(writer, hasher, sum512hasher)
	switch len(readers) > 1 {
	case true:
		if objMetadata.ErasureTechnique == "" {
			writer.CloseWithError(iodine.New(MissingErasureTechnique{}, nil))
			return
		}
		encoder, err := newEncoder(objMetadata.DataDisks, objMetadata.ParityDisks, objMetadata.ErasureTechnique)
		if err != nil {
			writer.CloseWithError(iodine.New(err, nil))
			return
		}
		totalLeft := objMetadata.Size
		for i := 0; i < objMetadata.ChunkCount; i++ {
			decodedData, err := b.decodeEncodedData(totalLeft, int64(objMetadata.BlockSize), readers, encoder, writer)
			if err != nil {
				writer.CloseWithError(iodine.New(err, nil))
				return
			}
			_, err = io.Copy(mwriter, bytes.NewReader(decodedData))
			if err != nil {
				writer.CloseWithError(iodine.New(err, nil))
				return
			}
			totalLeft = totalLeft - int64(objMetadata.BlockSize)
		}
	case false:
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
	if !bytes.Equal(expected512Sum, sum512hasher.Sum(nil)) {
		writer.CloseWithError(iodine.New(ChecksumMismatch{}, nil))
		return
	}
	writer.Close()
	return
}

// decodeEncodedData -
func (b bucket) decodeEncodedData(totalLeft, blockSize int64, readers map[int]io.ReadCloser, encoder encoder, writer *io.PipeWriter) ([]byte, error) {
	var curBlockSize int64
	if blockSize < totalLeft {
		curBlockSize = blockSize
	} else {
		curBlockSize = totalLeft
	}
	curChunkSize, err := encoder.GetEncodedBlockLen(int(curBlockSize))
	if err != nil {
		return nil, iodine.New(err, nil)
	}
	encodedBytes := make([][]byte, encoder.k+encoder.m)
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

// getObjectReaders -
func (b bucket) getObjectReaders(objectName, objectMeta string) (map[int]io.ReadCloser, error) {
	readers := make(map[int]io.ReadCloser)
	var disks map[int]disk.Disk
	var err error
	nodeSlice := 0
	for _, node := range b.nodes {
		disks, err = node.ListDisks()
		if err != nil {
			return nil, iodine.New(err, nil)
		}
		for order, disk := range disks {
			var objectSlice io.ReadCloser
			bucketSlice := fmt.Sprintf("%s$%d$%d", b.name, nodeSlice, order)
			objectPath := filepath.Join(b.donutName, bucketSlice, objectName, objectMeta)
			objectSlice, err = disk.OpenFile(objectPath)
			if err == nil {
				readers[order] = objectSlice
			}
		}
		nodeSlice = nodeSlice + 1
	}
	if err != nil {
		return nil, iodine.New(err, nil)
	}
	return readers, nil
}

// getObjectWriters -
func (b bucket) getObjectWriters(objectName, objectMeta string) ([]io.WriteCloser, error) {
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

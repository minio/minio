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
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	"crypto/md5"
	"encoding/hex"

	"github.com/minio-io/iodine"
)

// internal struct carrying bucket specific information
type bucket struct {
	name      string
	donutName string
	nodes     map[string]Node
	objects   map[string]Object
}

// NewBucket - instantiate a new bucket
func NewBucket(bucketName, donutName string, nodes map[string]Node) (Bucket, error) {
	errParams := map[string]string{
		"bucketName": bucketName,
		"donutName":  donutName,
	}
	if strings.TrimSpace(bucketName) == "" || strings.TrimSpace(donutName) == "" {
		return nil, iodine.New(errors.New("invalid argument"), errParams)
	}
	b := bucket{}
	b.name = bucketName
	b.donutName = donutName
	b.objects = make(map[string]Object)
	b.nodes = nodes
	return b, nil
}

// ListObjects - list all objects
func (b bucket) ListObjects() (map[string]Object, error) {
	nodeSlice := 0
	for _, node := range b.nodes {
		disks, err := node.ListDisks()
		if err != nil {
			return nil, iodine.New(err, nil)
		}
		for _, disk := range disks {
			bucketSlice := fmt.Sprintf("%s$%d$%d", b.name, nodeSlice, disk.GetOrder())
			bucketPath := path.Join(b.donutName, bucketSlice)
			objects, err := disk.ListDir(bucketPath)
			if err != nil {
				return nil, iodine.New(err, nil)
			}
			for _, object := range objects {
				newObject, err := NewObject(object.Name(), path.Join(disk.GetPath(), bucketPath))
				if err != nil {
					return nil, iodine.New(err, nil)
				}
				newObjectMetadata, err := newObject.GetObjectMetadata()
				if err != nil {
					return nil, iodine.New(err, nil)
				}
				objectName, ok := newObjectMetadata["object"]
				if !ok {
					return nil, iodine.New(errors.New("object corrupted"), nil)
				}
				b.objects[objectName] = newObject
			}
		}
		nodeSlice = nodeSlice + 1
	}
	return b.objects, nil
}

// GetObject - get object
func (b bucket) GetObject(objectName string) (reader io.ReadCloser, size int64, err error) {
	reader, writer := io.Pipe()
	// get list of objects
	objects, err := b.ListObjects()
	if err != nil {
		return nil, 0, iodine.New(err, nil)
	}
	// check if object exists
	object, ok := objects[objectName]
	if !ok {
		return nil, 0, iodine.New(os.ErrNotExist, nil)
	}
	// verify if objectMetadata is readable, before we serve the request
	objectMetadata, err := object.GetObjectMetadata()
	if err != nil {
		return nil, 0, iodine.New(err, nil)
	}
	if objectName == "" || writer == nil || len(objectMetadata) == 0 {
		return nil, 0, iodine.New(errors.New("invalid argument"), nil)
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

// PutObject - put a new object
func (b bucket) PutObject(objectName string, objectData io.Reader, expectedMD5Sum string, metadata map[string]string) error {
	if objectName == "" || objectData == nil {
		return iodine.New(errors.New("invalid argument"), nil)
	}
	writers, err := b.getDiskWriters(b.normalizeObjectName(objectName), "data")
	if err != nil {
		return iodine.New(err, nil)
	}
	summer := md5.New()
	objectMetadata := make(map[string]string)
	donutObjectMetadata := make(map[string]string)
	objectMetadata["version"] = "1.0"
	donutObjectMetadata["version"] = "1.0"
	// if total writers are only '1' do not compute erasure
	switch len(writers) == 1 {
	case true:
		mw := io.MultiWriter(writers[0], summer)
		totalLength, err := io.Copy(mw, objectData)
		if err != nil {
			return iodine.New(err, nil)
		}
		donutObjectMetadata["sys.size"] = strconv.FormatInt(totalLength, 10)
		objectMetadata["size"] = strconv.FormatInt(totalLength, 10)
	case false:
		// calculate data and parity dictated by total number of writers
		k, m, err := b.getDataAndParity(len(writers))
		if err != nil {
			return iodine.New(err, nil)
		}
		// encoded data with k, m and write
		chunkCount, totalLength, err := b.writeEncodedData(k, m, writers, objectData, summer)
		if err != nil {
			return iodine.New(err, nil)
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
	objectMetadata["created"] = time.Now().Format(time.RFC3339Nano)

	// keeping md5sum for the object in two different places
	// one for object storage and another is for internal use
	objectMetadata["md5"] = hex.EncodeToString(dataMd5sum)
	donutObjectMetadata["sys.md5"] = hex.EncodeToString(dataMd5sum)

	// Verify if the written object is equal to what is expected, only if it is requested as such
	if strings.TrimSpace(expectedMD5Sum) != "" {
		if err := b.isMD5SumEqual(strings.TrimSpace(expectedMD5Sum), objectMetadata["md5"]); err != nil {
			return iodine.New(err, nil)
		}
	}
	// write donut specific metadata
	if err := b.writeDonutObjectMetadata(b.normalizeObjectName(objectName), donutObjectMetadata); err != nil {
		return iodine.New(err, nil)
	}
	// write object specific metadata
	if err := b.writeObjectMetadata(b.normalizeObjectName(objectName), objectMetadata); err != nil {
		return iodine.New(err, nil)
	}
	// close all writers, when control flow reaches here
	for _, writer := range writers {
		writer.Close()
	}
	return nil
}

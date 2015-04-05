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
	"encoding/json"

	"github.com/minio-io/minio/pkg/utils/split"
)

type bucket struct {
	name      string
	donutName string
	nodes     map[string]Node
	objects   map[string]Object
}

// NewBucket - instantiate a new bucket
func NewBucket(bucketName, donutName string, nodes map[string]Node) (Bucket, error) {
	if bucketName == "" {
		return nil, errors.New("invalid argument")
	}
	b := bucket{}
	b.name = bucketName
	b.donutName = donutName
	b.objects = make(map[string]Object)
	b.nodes = nodes
	return b, nil
}

func (b bucket) ListNodes() (map[string]Node, error) {
	return b.nodes, nil
}

func (b bucket) ListObjects() (map[string]Object, error) {
	nodeSlice := 0
	for _, node := range b.nodes {
		disks, err := node.ListDisks()
		if err != nil {
			return nil, err
		}
		for _, disk := range disks {
			bucketSlice := fmt.Sprintf("%s$%d$%d", b.name, nodeSlice, disk.GetOrder())
			bucketPath := path.Join(b.donutName, bucketSlice)
			objects, err := disk.ListDir(bucketPath)
			if err != nil {
				return nil, err
			}
			for _, object := range objects {
				newObject, err := NewObject(object.Name(), path.Join(disk.GetPath(), bucketPath))
				if err != nil {
					return nil, err
				}
				newObjectMetadata, err := newObject.GetObjectMetadata()
				if err != nil {
					return nil, err
				}
				objectName, ok := newObjectMetadata["object"]
				if !ok {
					return nil, errors.New("object corrupted")
				}
				b.objects[objectName] = newObject
			}
		}
		nodeSlice = nodeSlice + 1
	}
	return b.objects, nil
}

func (b bucket) GetObject(objectName string) (reader io.ReadCloser, size int64, err error) {
	reader, writer := io.Pipe()
	// get list of objects
	objects, err := b.ListObjects()
	if err != nil {
		return nil, 0, err
	}
	// check if object exists
	object, ok := objects[objectName]
	if !ok {
		return nil, 0, os.ErrNotExist
	}
	donutObjectMetadata, err := object.GetDonutObjectMetadata()
	if err != nil {
		return nil, 0, err
	}
	if objectName == "" || writer == nil || len(donutObjectMetadata) == 0 {
		return nil, 0, errors.New("invalid argument")
	}
	size, err = strconv.ParseInt(donutObjectMetadata["size"], 10, 64)
	if err != nil {
		return nil, 0, err
	}
	go b.getObject(b.normalizeObjectName(objectName), writer, donutObjectMetadata)
	return reader, size, nil
}

func (b bucket) WriteObjectMetadata(objectName string, objectMetadata map[string]string) error {
	if len(objectMetadata) == 0 {
		return errors.New("invalid argument")
	}
	objectMetadataWriters, err := b.getDiskWriters(objectName, objectMetadataConfig)
	if err != nil {
		return err
	}
	for _, objectMetadataWriter := range objectMetadataWriters {
		defer objectMetadataWriter.Close()
	}
	for _, objectMetadataWriter := range objectMetadataWriters {
		jenc := json.NewEncoder(objectMetadataWriter)
		if err := jenc.Encode(objectMetadata); err != nil {
			return err
		}
	}
	return nil
}

func (b bucket) WriteDonutObjectMetadata(objectName string, donutObjectMetadata map[string]string) error {
	if len(donutObjectMetadata) == 0 {
		return errors.New("invalid argument")
	}
	donutObjectMetadataWriters, err := b.getDiskWriters(objectName, donutObjectMetadataConfig)
	if err != nil {
		return err
	}
	for _, donutObjectMetadataWriter := range donutObjectMetadataWriters {
		defer donutObjectMetadataWriter.Close()
	}
	for _, donutObjectMetadataWriter := range donutObjectMetadataWriters {
		jenc := json.NewEncoder(donutObjectMetadataWriter)
		if err := jenc.Encode(donutObjectMetadata); err != nil {
			return err
		}
	}
	return nil
}

// This a temporary normalization of object path, need to find a better way
func (b bucket) normalizeObjectName(objectName string) string {
	// replace every '/' with '-'
	return strings.Replace(objectName, "/", "-", -1)
}

func (b bucket) PutObject(objectName, contentType string, objectData io.Reader) error {
	if objectName == "" {
		return errors.New("invalid argument")
	}
	if objectData == nil {
		return errors.New("invalid argument")
	}
	if contentType == "" || strings.TrimSpace(contentType) == "" {
		contentType = "application/octet-stream"
	}
	writers, err := b.getDiskWriters(b.normalizeObjectName(objectName), "data")
	if err != nil {
		return err
	}
	for _, writer := range writers {
		defer writer.Close()
	}
	summer := md5.New()
	donutObjectMetadata := make(map[string]string)
	switch len(writers) == 1 {
	case true:
		mw := io.MultiWriter(writers[0], summer)
		totalLength, err := io.Copy(mw, objectData)
		if err != nil {
			return err
		}
		donutObjectMetadata["size"] = strconv.FormatInt(totalLength, 10)
	case false:
		k, m, err := b.getDataAndParity(len(writers))
		if err != nil {
			return err
		}
		chunks := split.Stream(objectData, 10*1024*1024)
		encoder, err := NewEncoder(k, m, "Cauchy")
		if err != nil {
			return err
		}
		chunkCount := 0
		totalLength := 0
		for chunk := range chunks {
			if chunk.Err == nil {
				totalLength = totalLength + len(chunk.Data)
				encodedBlocks, _ := encoder.Encode(chunk.Data)
				summer.Write(chunk.Data)
				for blockIndex, block := range encodedBlocks {
					io.Copy(writers[blockIndex], bytes.NewBuffer(block))
				}
			}
			chunkCount = chunkCount + 1
		}
		donutObjectMetadata["blockSize"] = strconv.Itoa(10 * 1024 * 1024)
		donutObjectMetadata["chunkCount"] = strconv.Itoa(chunkCount)
		donutObjectMetadata["erasureK"] = strconv.FormatUint(uint64(k), 10)
		donutObjectMetadata["erasureM"] = strconv.FormatUint(uint64(m), 10)
		donutObjectMetadata["erasureTechnique"] = "Cauchy"
		donutObjectMetadata["size"] = strconv.Itoa(totalLength)
	}
	dataMd5sum := summer.Sum(nil)
	donutObjectMetadata["created"] = time.Now().Format(time.RFC3339Nano)
	donutObjectMetadata["md5"] = hex.EncodeToString(dataMd5sum)
	if err := b.WriteDonutObjectMetadata(b.normalizeObjectName(objectName), donutObjectMetadata); err != nil {
		return err
	}
	objectMetadata := make(map[string]string)
	objectMetadata["bucket"] = b.name
	objectMetadata["object"] = objectName
	objectMetadata["contentType"] = strings.TrimSpace(contentType)
	if err := b.WriteObjectMetadata(b.normalizeObjectName(objectName), objectMetadata); err != nil {
		return err
	}
	return nil
}

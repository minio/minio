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
	objectMetata, err := object.GetObjectMetadata()
	if err != nil {
		return nil, 0, err
	}
	if objectName == "" || writer == nil || len(objectMetata) == 0 {
		return nil, 0, errors.New("invalid argument")
	}
	size, err = strconv.ParseInt(objectMetata["size"], 10, 64)
	if err != nil {
		return nil, 0, err
	}
	go b.readEncodedData(b.normalizeObjectName(objectName), writer, objectMetata)
	return reader, size, nil
}

func (b bucket) PutObject(objectName string, objectData io.Reader, metadata map[string]string) error {
	if objectName == "" || objectData == nil {
		return errors.New("invalid argument")
	}
	contentType, ok := metadata["contentType"]
	if !ok || strings.TrimSpace(contentType) == "" {
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
	objectMetata := make(map[string]string)
	switch len(writers) == 1 {
	case true:
		mw := io.MultiWriter(writers[0], summer)
		totalLength, err := io.Copy(mw, objectData)
		if err != nil {
			return err
		}
		objectMetata["size"] = strconv.FormatInt(totalLength, 10)
	case false:
		k, m, err := b.getDataAndParity(len(writers))
		if err != nil {
			return err
		}
		chunkCount, totalLength, err := b.writeEncodedData(k, m, writers, objectData, summer)
		if err != nil {
			return err
		}
		objectMetata["blockSize"] = strconv.Itoa(10 * 1024 * 1024)
		objectMetata["chunkCount"] = strconv.Itoa(chunkCount)
		objectMetata["erasureK"] = strconv.FormatUint(uint64(k), 10)
		objectMetata["erasureM"] = strconv.FormatUint(uint64(m), 10)
		objectMetata["erasureTechnique"] = "Cauchy"
		objectMetata["size"] = strconv.Itoa(totalLength)
	}
	dataMd5sum := summer.Sum(nil)
	objectMetata["created"] = time.Now().Format(time.RFC3339Nano)
	objectMetata["md5"] = hex.EncodeToString(dataMd5sum)
	objectMetata["bucket"] = b.name
	objectMetata["object"] = objectName
	objectMetata["contentType"] = strings.TrimSpace(contentType)
	if err := b.writeDonutObjectMetadata(b.normalizeObjectName(objectName), objectMetata); err != nil {
		return err
	}
	return nil
}

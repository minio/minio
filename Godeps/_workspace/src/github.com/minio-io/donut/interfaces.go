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
	"io"
	"os"
)

// Collection of Donut specification interfaces

// Donut interface
type Donut interface {
	ObjectStorage
	Management
}

// ObjectStorage interface
type ObjectStorage interface {
	// Storage service Operations
	GetBucketMetadata(bucket string) (map[string]string, error)
	SetBucketMetadata(bucket string, metadata map[string]string) error
	ListBuckets() ([]string, error)
	MakeBucket(bucket string) error

	// Bucket Operations
	ListObjects(bucket, prefix, marker, delim string, maxKeys int) (result []string, prefixes []string, isTruncated bool, err error)

	// Object Operations
	GetObject(bucket, object string) (io.ReadCloser, int64, error)
	GetObjectMetadata(bucket, object string) (map[string]string, error)
	PutObject(bucket, object string, reader io.ReadCloser, metadata map[string]string) error
}

// Management is a donut management system interface
type Management interface {
	Heal() error
	Rebalance() error
	Info() (map[string][]string, error)

	AttachNode(node Node) error
	DetachNode(node Node) error

	SaveConfig() error
	LoadConfig() error
}

// Encoder interface
type Encoder interface {
	GetEncodedBlockLen(dataLength int) (int, error)
	Encode(data []byte) (encodedData [][]byte, err error)
	Decode(encodedData [][]byte, dataLength int) (data []byte, err error)
}

// Bucket interface
type Bucket interface {
	ListObjects() (map[string]Object, error)

	GetObject(object string) (io.ReadCloser, int64, error)
	PutObject(object string, contents io.Reader, metadata map[string]string) error
}

// Object interface
type Object interface {
	GetObjectMetadata() (map[string]string, error)
}

// Node interface
type Node interface {
	ListDisks() (map[string]Disk, error)
	AttachDisk(disk Disk) error
	DetachDisk(disk Disk) error

	GetNodeName() string
	SaveConfig() error
	LoadConfig() error
}

// Disk interface
type Disk interface {
	MakeDir(dirname string) error

	ListDir(dirname string) ([]os.FileInfo, error)
	ListFiles(dirname string) ([]os.FileInfo, error)

	MakeFile(path string) (*os.File, error)
	OpenFile(path string) (*os.File, error)

	GetPath() string
	GetOrder() int
	GetFSInfo() map[string]string
}

const (
	objectMetadataConfig = "objectMetadata.json"
	donutConfig          = "donutMetadata.json"
)

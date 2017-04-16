package main

import (
	"io"
	"time"
)

type BucketInfo struct {
	Name         string
	ModifiedTime time.Time
}

type ObjectInfo struct {
	Name         string
	ModifiedTime time.Time
	Checksum     string
	Size         int64
	IsDir        bool
	Err          error
}

func (oi ObjectInfo) String() string {
	return oi.Name
}

type UploadInfo struct {
	UploadID      string
	ObjectName    string
	InitiatedTime time.Time
	Err           error
}

type PartInfo struct {
	PartNumber   int
	Checksum     string
	ModifiedTime time.Time
	Size         int
	Err          error
}

type Backend interface {
	ListBuckets() ([]BucketInfo, error)
	IsBucketExist(bucket string) (bool, error)
	MakeBucket(bucket string) error
	RemoveBucket(bucket string) error
	ListObjects(bucket, prefix string) (<-chan ObjectInfo, error)
	ListIncompleteUploads(bucket, prefix string) (<-chan UploadInfo, error)
	ListParts(bucket, object, uploadID string) (<-chan PartInfo, error)
	PutObject(bucket, object string, reader io.Reader) error
	GetObject(bucket, object string, offset, length int64) (io.Reader, error)
	RemoveObject(bucket, object string) error
	RecoverObject(bucket, object string) error
	StatObject(bucket, object string) (ObjectInfo, error)
}

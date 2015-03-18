package donut

import (
	"errors"
	"io"
)

type donutDriver struct {
	buckets map[string]Bucket
	disks   map[string]Disk
}

// NewDonutDriver instantiates a donut driver for use in object storage
func NewDonutDriver() Donut {
	return donutDriver{
		buckets: make(map[string]Bucket),
		disks:   make(map[string]Disk),
	}
}

func notImplemented() error {
	return errors.New("Not Implemented")
}

// CreateBucket creates a bucket
func (driver donutDriver) CreateBucket(bucket string) error {
	return notImplemented()
}

// GetBuckets returns a list of buckets
func (driver donutDriver) GetBuckets() ([]string, error) {
	return nil, notImplemented()
}

// GetObject returns an object
func (driver donutDriver) GetObject(bucket, object string) (io.ReadCloser, error) {
	return nil, notImplemented()
}

// GetObjectMetadata returns object metadata
func (driver donutDriver) GetObjectMetadata(bucket, object string) (map[string]string, error) {
	return nil, notImplemented()
}

// GetObjectWriter returns a writer for creating a new object.
func (driver donutDriver) GetObjectWriter(bucket, object string) (ObjectWriter, error) {
	return nil, notImplemented()
}

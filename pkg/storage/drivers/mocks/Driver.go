package mocks

import (
	"bytes"
	"io"

	"github.com/minio/minio/pkg/iodine"
	"github.com/minio/minio/pkg/storage/drivers"
	"github.com/stretchr/testify/mock"
)

// Driver is a mock
type Driver struct {
	mock.Mock

	ObjectWriterData map[string][]byte
}

// ListBuckets is a mock
func (m *Driver) ListBuckets() ([]drivers.BucketMetadata, error) {
	ret := m.Called()

	r0 := ret.Get(0).([]drivers.BucketMetadata)
	r1 := ret.Error(1)

	return r0, r1
}

// CreateBucket is a mock
func (m *Driver) CreateBucket(bucket, acl string) error {
	ret := m.Called(bucket, acl)

	r0 := ret.Error(0)

	return r0
}

// GetBucketMetadata is a mock
func (m *Driver) GetBucketMetadata(bucket string) (drivers.BucketMetadata, error) {
	ret := m.Called(bucket)
	r0 := ret.Get(0).(drivers.BucketMetadata)
	r1 := ret.Error(1)

	return r0, r1
}

// SetBucketMetadata is a mock
func (m *Driver) SetBucketMetadata(bucket, acl string) error {
	ret := m.Called(bucket, acl)

	r0 := ret.Error(0)

	return r0
}

// SetGetObjectWriter is a mock
func (m *Driver) SetGetObjectWriter(bucket, object string, data []byte) {
	m.ObjectWriterData[bucket+":"+object] = data
}

// GetObject is a mock
func (m *Driver) GetObject(w io.Writer, bucket, object string) (int64, error) {
	ret := m.Called(w, bucket, object)
	r0 := ret.Get(0).(int64)
	r1 := ret.Error(1)
	if r1 == nil {
		if obj, ok := m.ObjectWriterData[bucket+":"+object]; ok {
			n, err := io.Copy(w, bytes.NewBuffer(obj))
			if err != nil {
				panic(err)
			}
			r0 = n
		}
	}
	return r0, r1
}

// GetPartialObject is a mock
func (m *Driver) GetPartialObject(w io.Writer, bucket, object string, start int64, length int64) (int64, error) {
	ret := m.Called(w, bucket, object, start, length)

	r0 := ret.Get(0).(int64)
	r1 := ret.Error(1)

	if r1 == nil {
		if obj, ok := m.ObjectWriterData[bucket+":"+object]; ok {
			source := bytes.NewBuffer(obj)
			var nilSink bytes.Buffer
			io.CopyN(&nilSink, source, start)
			n, _ := io.CopyN(w, source, length)
			r0 = n
		}
	}
	r1 = iodine.New(r1, nil)

	return r0, r1
}

// GetObjectMetadata is a mock
func (m *Driver) GetObjectMetadata(bucket, object, prefix string) (drivers.ObjectMetadata, error) {
	ret := m.Called(bucket, object, prefix)

	r0 := ret.Get(0).(drivers.ObjectMetadata)
	r1 := ret.Error(1)

	return r0, r1
}

// ListObjects is a mock
func (m *Driver) ListObjects(bucket string, resources drivers.BucketResourcesMetadata) ([]drivers.ObjectMetadata, drivers.BucketResourcesMetadata, error) {
	ret := m.Called(bucket, resources)

	r0 := ret.Get(0).([]drivers.ObjectMetadata)
	r1 := ret.Get(1).(drivers.BucketResourcesMetadata)
	r2 := ret.Error(2)

	return r0, r1, r2
}

// CreateObject is a mock
func (m *Driver) CreateObject(bucket, key, contentType, md5sum string, size int64, data io.Reader) (string, error) {
	ret := m.Called(bucket, key, contentType, md5sum, size, data)

	r0 := ret.Get(0).(string)
	r1 := ret.Error(1)

	return r0, r1
}

// NewMultipartUpload is a mock
func (m *Driver) NewMultipartUpload(bucket, key, contentType string) (string, error) {
	ret := m.Called(bucket, key, contentType)

	r0 := ret.Get(0).(string)
	r1 := ret.Error(1)

	return r0, r1
}

// CreateObjectPart is a mock
func (m *Driver) CreateObjectPart(bucket, key, uploadID string, partID int, contentType string, md5sum string, size int64, data io.Reader) (string, error) {
	ret := m.Called(bucket, key, uploadID, partID, contentType, md5sum, size, data)

	r0 := ret.Get(0).(string)
	r1 := ret.Error(1)

	return r0, r1
}

// CompleteMultipartUpload is a mock
func (m *Driver) CompleteMultipartUpload(bucket, key, uploadID string, parts map[int]string) (string, error) {
	ret := m.Called(bucket, key, uploadID, parts)

	r0 := ret.Get(0).(string)
	r1 := ret.Error(1)

	return r0, r1
}

// ListObjectParts is a mock
func (m *Driver) ListObjectParts(bucket, key string, resources drivers.ObjectResourcesMetadata) (drivers.ObjectResourcesMetadata, error) {
	ret := m.Called(bucket, key, resources)

	r0 := ret.Get(0).(drivers.ObjectResourcesMetadata)
	r1 := ret.Error(1)

	return r0, r1
}

// ListMultipartUploads is a mock
func (m *Driver) ListMultipartUploads(bucket string, resources drivers.BucketMultipartResourcesMetadata) (drivers.BucketMultipartResourcesMetadata, error) {
	ret := m.Called(bucket, resources)

	r0 := ret.Get(0).(drivers.BucketMultipartResourcesMetadata)
	r1 := ret.Error(1)

	return r0, r1
}

// AbortMultipartUpload is a mock
func (m *Driver) AbortMultipartUpload(bucket, key, uploadID string) error {
	ret := m.Called(bucket, key, uploadID)

	r0 := ret.Error(0)

	return r0
}

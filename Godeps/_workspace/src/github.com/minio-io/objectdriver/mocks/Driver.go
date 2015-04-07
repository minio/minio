package mocks

import (
	"bytes"
	"io"

	"github.com/fkautz/testify/mock"
	"github.com/minio-io/iodine"
	"github.com/minio-io/objectdriver"
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
func (m *Driver) CreateBucket(bucket string) error {
	ret := m.Called(bucket)

	r0 := ret.Error(0)

	return r0
}

// CreateBucketPolicy is a mock
func (m *Driver) CreateBucketPolicy(bucket string, p drivers.BucketPolicy) error {
	ret := m.Called(bucket, p)

	r0 := ret.Error(0)

	return r0
}

// GetBucketPolicy is a mock
func (m *Driver) GetBucketPolicy(bucket string) (drivers.BucketPolicy, error) {
	ret := m.Called(bucket)

	r0 := ret.Get(0).(drivers.BucketPolicy)
	r1 := ret.Error(1)

	return r0, r1
}

// SetGetObjectWriter is a mock
func (m *Driver) SetGetObjectWriter(bucket, object string, data []byte) {
	m.ObjectWriterData[bucket+":"+object] = data
	//	println(string(m.ObjectWriterData["bucket:object"]))
}

// GetObject is a mock
func (m *Driver) GetObject(w io.Writer, bucket string, object string) (int64, error) {
	ret := m.Called(w, bucket, object)
	r0 := ret.Get(0).(int64)
	r1 := ret.Error(1)
	if r1 == nil {
		if obj, ok := m.ObjectWriterData[bucket+":"+object]; ok {
			n, _ := io.Copy(w, bytes.NewBuffer(obj))
			r0 = n
		}
	}

	return r0, r1
}

// GetPartialObject is a mock
func (m *Driver) GetPartialObject(w io.Writer, bucket string, object string, start int64, length int64) (int64, error) {
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
func (m *Driver) GetObjectMetadata(bucket string, object string, prefix string) (drivers.ObjectMetadata, error) {
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
func (m *Driver) CreateObject(bucket string, key string, contentType string, md5sum string, data io.Reader) error {
	ret := m.Called(bucket, key, contentType, md5sum, data)

	r0 := ret.Error(0)

	return r0
}

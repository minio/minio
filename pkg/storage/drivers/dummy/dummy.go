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

package dummy

import (
	"io"

	"github.com/minio/minio/pkg/storage/drivers"
)

// dummyDriver
type dummyDriver struct {
	driver drivers.Driver
}

// NewDriver provides a new dummy driver
func NewDriver(driver drivers.Driver) drivers.Driver {
	return dummyDriver{driver: driver}
}

// ListBuckets
func (dummy dummyDriver) ListBuckets() ([]drivers.BucketMetadata, error) {
	return dummy.driver.ListBuckets()
}

// CreateBucket
func (dummy dummyDriver) CreateBucket(bucket, acl string) error {
	return dummy.driver.CreateBucket(bucket, acl)
}

// GetBucketMetadata
func (dummy dummyDriver) GetBucketMetadata(bucket string) (drivers.BucketMetadata, error) {
	return dummy.driver.GetBucketMetadata(bucket)
}

// SetBucketMetadata
func (dummy dummyDriver) SetBucketMetadata(bucket, acl string) error {
	return dummy.driver.SetBucketMetadata(bucket, acl)
}

// GetObject
func (dummy dummyDriver) GetObject(w io.Writer, bucket, object string) (int64, error) {
	return dummy.driver.GetObject(w, bucket, object)
}

// GetPartialObject
func (dummy dummyDriver) GetPartialObject(w io.Writer, bucket, object string, start int64, length int64) (int64, error) {
	return dummy.driver.GetPartialObject(w, bucket, object, start, length)
}

// GetObjectMetadata
func (dummy dummyDriver) GetObjectMetadata(bucket, object string) (drivers.ObjectMetadata, error) {
	return dummy.driver.GetObjectMetadata(bucket, object)
}

// ListObjects
func (dummy dummyDriver) ListObjects(bucket string, resources drivers.BucketResourcesMetadata) ([]drivers.ObjectMetadata, drivers.BucketResourcesMetadata, error) {
	return dummy.driver.ListObjects(bucket, resources)
}

// CreateObject
func (dummy dummyDriver) CreateObject(bucket, key, contentType, md5sum string, size int64, data io.Reader) (string, error) {
	return dummy.driver.CreateObject(bucket, key, contentType, md5sum, size, data)
}

// NewMultipartUpload
func (dummy dummyDriver) NewMultipartUpload(bucket, key, contentType string) (string, error) {
	return dummy.driver.NewMultipartUpload(bucket, key, contentType)
}

// CreateObjectPart
func (dummy dummyDriver) CreateObjectPart(bucket, key, uploadID string, partID int, contentType string, md5sum string, size int64, data io.Reader) (string, error) {
	return dummy.driver.CreateObjectPart(bucket, key, uploadID, partID, contentType, md5sum, size, data)
}

// CompleteMultipartUpload
func (dummy dummyDriver) CompleteMultipartUpload(bucket, key, uploadID string, parts map[int]string) (string, error) {
	return dummy.driver.CompleteMultipartUpload(bucket, key, uploadID, parts)
}

// ListObjectParts
func (dummy dummyDriver) ListObjectParts(bucket, key string, resources drivers.ObjectResourcesMetadata) (drivers.ObjectResourcesMetadata, error) {
	return dummy.driver.ListObjectParts(bucket, key, resources)
}

// ListMultipartUploads
func (dummy dummyDriver) ListMultipartUploads(bucket string, resources drivers.BucketMultipartResourcesMetadata) (drivers.BucketMultipartResourcesMetadata, error) {
	return dummy.driver.ListMultipartUploads(bucket, resources)
}

// AbortMultipartUpload
func (dummy dummyDriver) AbortMultipartUpload(bucket, key, uploadID string) error {
	return dummy.driver.AbortMultipartUpload(bucket, key, uploadID)
}

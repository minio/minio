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

	"github.com/minio/minio/pkg/iodine"
	"github.com/minio/minio/pkg/storage/drivers"
)

func (d donutDriver) NewMultipartUpload(bucketName, objectName, contentType string) (string, error) {
	return "", iodine.New(drivers.APINotImplemented{API: "NewMultipartUpload"}, nil)
}

func (d donutDriver) AbortMultipartUpload(bucketName, objectName, uploadID string) error {
	return iodine.New(drivers.APINotImplemented{API: "AbortMultipartUpload"}, nil)
}

func (d donutDriver) CreateObjectPart(bucketName, objectName, uploadID string, partID int, contentType, expectedMD5Sum string, size int64, data io.Reader) (string, error) {
	return "", iodine.New(drivers.APINotImplemented{API: "CreateObjectPart"}, nil)
}

func (d donutDriver) CompleteMultipartUpload(bucketName, objectName, uploadID string, parts map[int]string) (string, error) {
	return "", iodine.New(drivers.APINotImplemented{API: "CompleteMultipartUpload"}, nil)
}

func (d donutDriver) ListMultipartUploads(bucketName string, resources drivers.BucketMultipartResourcesMetadata) (drivers.BucketMultipartResourcesMetadata, error) {
	return drivers.BucketMultipartResourcesMetadata{}, iodine.New(drivers.APINotImplemented{API: "ListMultipartUploads"}, nil)
}
func (d donutDriver) ListObjectParts(bucketName, objectName string, resources drivers.ObjectResourcesMetadata) (drivers.ObjectResourcesMetadata, error) {
	return drivers.ObjectResourcesMetadata{}, iodine.New(drivers.APINotImplemented{API: "ListObjectParts"}, nil)
}

/*
 * Minio Cloud Storage, (C) 2017 Minio, Inc.
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

package oss

import (
	"io"

	minio "github.com/minio/minio/cmd"
	"github.com/minio/minio/pkg/hash"
)

// AnonPutObject creates a new object anonymously with the incoming data,
func (l *ossObjects) AnonPutObject(bucket, object string, data *hash.Reader, metadata map[string]string) (objInfo minio.ObjectInfo, err error) {
	return ossPutObject(l.anonClient, bucket, object, data, metadata)
}

// AnonGetObject - Get object anonymously
func (l *ossObjects) AnonGetObject(bucket, key string, startOffset, length int64, writer io.Writer) error {
	return ossGetObject(l.anonClient, bucket, key, startOffset, length, writer)
}

// AnonGetObjectInfo - Get object info anonymously
func (l *ossObjects) AnonGetObjectInfo(bucket, object string) (objInfo minio.ObjectInfo, err error) {
	return ossGetObjectInfo(l.anonClient, bucket, object)
}

// AnonListObjects lists objects anonymously.
func (l *ossObjects) AnonListObjects(bucket, prefix, marker, delimiter string, maxKeys int) (loi minio.ListObjectsInfo, err error) {
	return ossListObjects(l.anonClient, bucket, prefix, marker, delimiter, maxKeys)
}

// AnonListObjectsV2 lists objects in V2 mode, anonymously.
func (l *ossObjects) AnonListObjectsV2(bucket, prefix, continuationToken, delimiter string, maxKeys int, fetchOwner bool, startAfter string) (loi minio.ListObjectsV2Info, err error) {
	return ossListObjectsV2(l.anonClient, bucket, prefix, continuationToken, delimiter, maxKeys, fetchOwner, startAfter)
}

// AnonGetBucketInfo gets bucket metadata anonymously.
func (l *ossObjects) AnonGetBucketInfo(bucket string) (bi minio.BucketInfo, err error) {
	return ossGeBucketInfo(l.anonClient, bucket)
}

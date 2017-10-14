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

package cmd

import (
	"io"

	minio "github.com/minio/minio-go"
	"github.com/minio/minio/pkg/hash"
)

// AnonPutObject creates a new object anonymously with the incoming data,
func (l *s3Objects) AnonPutObject(bucket string, object string, data *hash.Reader, metadata map[string]string) (objInfo ObjectInfo, e error) {
	oi, err := l.anonClient.PutObject(bucket, object, data, data.Size(), data.MD5(), data.SHA256(), toMinioClientMetadata(metadata))
	if err != nil {
		return objInfo, s3ToObjectError(traceError(err), bucket, object)
	}

	return fromMinioClientObjectInfo(bucket, oi), nil
}

// AnonGetObject - Get object anonymously
func (l *s3Objects) AnonGetObject(bucket string, key string, startOffset int64, length int64, writer io.Writer) error {
	opts := minio.GetObjectOptions{}
	if err := opts.SetRange(startOffset, startOffset+length-1); err != nil {
		return s3ToObjectError(traceError(err), bucket, key)
	}
	object, _, err := l.anonClient.GetObject(bucket, key, opts)
	if err != nil {
		return s3ToObjectError(traceError(err), bucket, key)
	}

	defer object.Close()

	if _, err := io.CopyN(writer, object, length); err != nil {
		return s3ToObjectError(traceError(err), bucket, key)
	}

	return nil
}

// AnonGetObjectInfo - Get object info anonymously
func (l *s3Objects) AnonGetObjectInfo(bucket string, object string) (objInfo ObjectInfo, e error) {
	oi, err := l.anonClient.StatObject(bucket, object, minio.StatObjectOptions{})
	if err != nil {
		return objInfo, s3ToObjectError(traceError(err), bucket, object)
	}

	return fromMinioClientObjectInfo(bucket, oi), nil
}

// AnonListObjects - List objects anonymously
func (l *s3Objects) AnonListObjects(bucket string, prefix string, marker string, delimiter string, maxKeys int) (loi ListObjectsInfo, e error) {
	result, err := l.anonClient.ListObjects(bucket, prefix, marker, delimiter, maxKeys)
	if err != nil {
		return loi, s3ToObjectError(traceError(err), bucket)
	}

	return fromMinioClientListBucketResult(bucket, result), nil
}

// AnonListObjectsV2 - List objects in V2 mode, anonymously
func (l *s3Objects) AnonListObjectsV2(bucket, prefix, continuationToken, delimiter string, maxKeys int, fetchOwner bool, startAfter string) (loi ListObjectsV2Info, e error) {
	result, err := l.anonClient.ListObjectsV2(bucket, prefix, continuationToken, fetchOwner, delimiter, maxKeys)
	if err != nil {
		return loi, s3ToObjectError(traceError(err), bucket)
	}

	return fromMinioClientListBucketV2Result(bucket, result), nil
}

// AnonGetBucketInfo - Get bucket metadata anonymously.
func (l *s3Objects) AnonGetBucketInfo(bucket string) (bi BucketInfo, e error) {
	if exists, err := l.anonClient.BucketExists(bucket); err != nil {
		return bi, s3ToObjectError(traceError(err), bucket)
	} else if !exists {
		return bi, traceError(BucketNotFound{Bucket: bucket})
	}

	buckets, err := l.anonClient.ListBuckets()
	if err != nil {
		return bi, s3ToObjectError(traceError(err), bucket)
	}

	for _, bi := range buckets {
		if bi.Name != bucket {
			continue
		}

		return BucketInfo{
			Name:    bi.Name,
			Created: bi.CreationDate,
		}, nil
	}

	return bi, traceError(BucketNotFound{Bucket: bucket})
}

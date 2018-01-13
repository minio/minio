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

package s3

import (
	"io"

	miniogo "github.com/minio/minio-go"
	"github.com/minio/minio/pkg/errors"
	"github.com/minio/minio/pkg/hash"

	minio "github.com/minio/minio/cmd"
)

// AnonPutObject creates a new object anonymously with the incoming data,
func (l *s3Objects) AnonPutObject(bucket string, object string, data *hash.Reader, metadata map[string]string) (objInfo minio.ObjectInfo, e error) {
	oi, err := l.anonClient.PutObject(bucket, object, data, data.Size(), data.MD5Base64String(), data.SHA256HexString(), minio.ToMinioClientMetadata(metadata))
	if err != nil {
		return objInfo, minio.ErrorRespToObjectError(errors.Trace(err), bucket, object)
	}

	return minio.FromMinioClientObjectInfo(bucket, oi), nil
}

// AnonGetObject - Get object anonymously
func (l *s3Objects) AnonGetObject(bucket string, key string, startOffset int64, length int64, writer io.Writer, etag string) error {
	opts := miniogo.GetObjectOptions{}
	if err := opts.SetRange(startOffset, startOffset+length-1); err != nil {
		return minio.ErrorRespToObjectError(errors.Trace(err), bucket, key)
	}
	object, _, err := l.anonClient.GetObject(bucket, key, opts)
	if err != nil {
		return minio.ErrorRespToObjectError(errors.Trace(err), bucket, key)
	}

	defer object.Close()

	if _, err := io.CopyN(writer, object, length); err != nil {
		return minio.ErrorRespToObjectError(errors.Trace(err), bucket, key)
	}

	return nil
}

// AnonGetObjectInfo - Get object info anonymously
func (l *s3Objects) AnonGetObjectInfo(bucket string, object string) (objInfo minio.ObjectInfo, e error) {
	oi, err := l.anonClient.StatObject(bucket, object, miniogo.StatObjectOptions{})
	if err != nil {
		return objInfo, minio.ErrorRespToObjectError(errors.Trace(err), bucket, object)
	}

	return minio.FromMinioClientObjectInfo(bucket, oi), nil
}

// AnonListObjects - List objects anonymously
func (l *s3Objects) AnonListObjects(bucket string, prefix string, marker string, delimiter string, maxKeys int) (loi minio.ListObjectsInfo, e error) {
	result, err := l.anonClient.ListObjects(bucket, prefix, marker, delimiter, maxKeys)
	if err != nil {
		return loi, minio.ErrorRespToObjectError(errors.Trace(err), bucket)
	}

	return minio.FromMinioClientListBucketResult(bucket, result), nil
}

// AnonListObjectsV2 - List objects in V2 mode, anonymously
func (l *s3Objects) AnonListObjectsV2(bucket, prefix, continuationToken, delimiter string, maxKeys int, fetchOwner bool, startAfter string) (loi minio.ListObjectsV2Info, e error) {
	result, err := l.anonClient.ListObjectsV2(bucket, prefix, continuationToken, fetchOwner, delimiter, maxKeys)
	if err != nil {
		return loi, minio.ErrorRespToObjectError(errors.Trace(err), bucket)
	}

	return minio.FromMinioClientListBucketV2Result(bucket, result), nil
}

// AnonGetBucketInfo - Get bucket metadata anonymously.
func (l *s3Objects) AnonGetBucketInfo(bucket string) (bi minio.BucketInfo, e error) {
	if exists, err := l.anonClient.BucketExists(bucket); err != nil {
		return bi, minio.ErrorRespToObjectError(errors.Trace(err), bucket)
	} else if !exists {
		return bi, errors.Trace(minio.BucketNotFound{Bucket: bucket})
	}

	buckets, err := l.anonClient.ListBuckets()
	if err != nil {
		return bi, minio.ErrorRespToObjectError(errors.Trace(err), bucket)
	}

	for _, bi := range buckets {
		if bi.Name != bucket {
			continue
		}

		return minio.BucketInfo{
			Name:    bi.Name,
			Created: bi.CreationDate,
		}, nil
	}

	return bi, errors.Trace(minio.BucketNotFound{Bucket: bucket})
}

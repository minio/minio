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

package gcs

import (
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/minio/minio/pkg/errors"

	minio "github.com/minio/minio/cmd"
)

func toGCSPublicURL(bucket, object string) string {
	return fmt.Sprintf("https://storage.googleapis.com/%s/%s", bucket, object)
}

// AnonGetObject - Get object anonymously
func (l *gcsGateway) AnonGetObject(bucket string, object string, startOffset int64, length int64, writer io.Writer, etag string) error {
	req, err := http.NewRequest("GET", toGCSPublicURL(bucket, object), nil)
	if err != nil {
		return gcsToObjectError(errors.Trace(err), bucket, object)
	}

	if length > 0 && startOffset > 0 {
		req.Header.Add("Range", fmt.Sprintf("bytes=%d-%d", startOffset, startOffset+length-1))
	} else if startOffset > 0 {
		req.Header.Add("Range", fmt.Sprintf("bytes=%d-", startOffset))
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return gcsToObjectError(errors.Trace(err), bucket, object)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusPartialContent && resp.StatusCode != http.StatusOK {
		return gcsToObjectError(errors.Trace(minio.AnonErrToObjectErr(resp.StatusCode, bucket, object)), bucket, object)
	}

	_, err = io.Copy(writer, resp.Body)
	return gcsToObjectError(errors.Trace(err), bucket, object)
}

// AnonGetObjectInfo - Get object info anonymously
func (l *gcsGateway) AnonGetObjectInfo(bucket string, object string) (objInfo minio.ObjectInfo, err error) {
	resp, err := http.Head(toGCSPublicURL(bucket, object))
	if err != nil {
		return objInfo, gcsToObjectError(errors.Trace(err), bucket, object)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return objInfo, gcsToObjectError(errors.Trace(minio.AnonErrToObjectErr(resp.StatusCode, bucket, object)), bucket, object)
	}

	var contentLength int64
	contentLengthStr := resp.Header.Get("Content-Length")
	if contentLengthStr != "" {
		contentLength, err = strconv.ParseInt(contentLengthStr, 0, 64)
		if err != nil {
			return objInfo, gcsToObjectError(errors.Trace(fmt.Errorf("Unexpected error")), bucket, object)
		}
	}

	t, err := time.Parse(time.RFC1123, resp.Header.Get("Last-Modified"))
	if err != nil {
		return objInfo, errors.Trace(err)
	}

	objInfo.ModTime = t
	objInfo.Bucket = bucket
	objInfo.UserDefined = make(map[string]string)
	if resp.Header.Get("Content-Encoding") != "" {
		objInfo.UserDefined["Content-Encoding"] = resp.Header.Get("Content-Encoding")
	}
	objInfo.UserDefined["Content-Type"] = resp.Header.Get("Content-Type")
	objInfo.ETag = resp.Header.Get("Etag")
	objInfo.ModTime = t
	objInfo.Name = object
	objInfo.Size = contentLength
	return
}

// AnonListObjects - List objects anonymously
func (l *gcsGateway) AnonListObjects(bucket string, prefix string, marker string, delimiter string, maxKeys int) (minio.ListObjectsInfo, error) {
	result, err := l.anonClient.ListObjects(bucket, prefix, marker, delimiter, maxKeys)
	if err != nil {
		return minio.ListObjectsInfo{}, minio.ErrorRespToObjectError(errors.Trace(err), bucket)
	}

	return minio.FromMinioClientListBucketResult(bucket, result), nil
}

// AnonListObjectsV2 - List objects in V2 mode, anonymously
func (l *gcsGateway) AnonListObjectsV2(bucket, prefix, continuationToken, delimiter string, maxKeys int, fetchOwner bool, startAfter string) (minio.ListObjectsV2Info, error) {
	// Request V1 List Object to the backend
	result, err := l.anonClient.ListObjects(bucket, prefix, continuationToken, delimiter, maxKeys)
	if err != nil {
		return minio.ListObjectsV2Info{}, minio.ErrorRespToObjectError(errors.Trace(err), bucket)
	}
	// translate V1 Result to V2Info
	return minio.FromMinioClientListBucketResultToV2Info(bucket, result), nil
}

// AnonGetBucketInfo - Get bucket metadata anonymously.
func (l *gcsGateway) AnonGetBucketInfo(bucket string) (bucketInfo minio.BucketInfo, err error) {
	resp, err := http.Head(toGCSPublicURL(bucket, ""))
	if err != nil {
		return bucketInfo, gcsToObjectError(errors.Trace(err))
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return bucketInfo, gcsToObjectError(errors.Trace(minio.AnonErrToObjectErr(resp.StatusCode, bucket)), bucket)
	}

	t, err := time.Parse(time.RFC1123, resp.Header.Get("Last-Modified"))
	if err != nil {
		return bucketInfo, errors.Trace(err)
	}

	// Last-Modified date being returned by GCS
	return minio.BucketInfo{
		Name:    bucket,
		Created: t,
	}, nil
}

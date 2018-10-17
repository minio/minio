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
	"net/http"

	"github.com/minio/minio/pkg/hash"

	minio "github.com/minio/minio-go"
)

var (
	// CanonicalizeETag provides canonicalizeETag function alias.
	CanonicalizeETag = canonicalizeETag

	// MustGetUUID function alias.
	MustGetUUID = mustGetUUID
)

// AnonErrToObjectErr - converts standard http codes into meaningful object layer errors.
func AnonErrToObjectErr(statusCode int, params ...string) error {
	bucket := ""
	object := ""
	if len(params) >= 1 {
		bucket = params[0]
	}
	if len(params) == 2 {
		object = params[1]
	}

	switch statusCode {
	case http.StatusNotFound:
		if object != "" {
			return ObjectNotFound{bucket, object}
		}
		return BucketNotFound{Bucket: bucket}
	case http.StatusBadRequest:
		if object != "" {
			return ObjectNameInvalid{bucket, object}
		}
		return BucketNameInvalid{Bucket: bucket}
	case http.StatusForbidden:
		fallthrough
	case http.StatusUnauthorized:
		return AllAccessDisabled{bucket, object}
	}

	return errUnexpected
}

// FromMinioClientMetadata converts minio metadata to map[string]string
func FromMinioClientMetadata(metadata map[string][]string) map[string]string {
	mm := map[string]string{}
	for k, v := range metadata {
		mm[http.CanonicalHeaderKey(k)] = v[0]
	}
	return mm
}

// FromMinioClientObjectPart converts minio ObjectPart to PartInfo
func FromMinioClientObjectPart(op minio.ObjectPart) PartInfo {
	return PartInfo{
		Size:         op.Size,
		ETag:         canonicalizeETag(op.ETag),
		LastModified: op.LastModified,
		PartNumber:   op.PartNumber,
	}
}

// FromMinioClientListPartsInfo converts minio ListObjectPartsResult to ListPartsInfo
func FromMinioClientListPartsInfo(lopr minio.ListObjectPartsResult) ListPartsInfo {
	// Convert minio ObjectPart to PartInfo
	fromMinioClientObjectParts := func(parts []minio.ObjectPart) []PartInfo {
		toParts := make([]PartInfo, len(parts))
		for i, part := range parts {
			toParts[i] = FromMinioClientObjectPart(part)
		}
		return toParts
	}

	return ListPartsInfo{
		UploadID:             lopr.UploadID,
		Bucket:               lopr.Bucket,
		Object:               lopr.Key,
		StorageClass:         "",
		PartNumberMarker:     lopr.PartNumberMarker,
		NextPartNumberMarker: lopr.NextPartNumberMarker,
		MaxParts:             lopr.MaxParts,
		IsTruncated:          lopr.IsTruncated,
		EncodingType:         lopr.EncodingType,
		Parts:                fromMinioClientObjectParts(lopr.ObjectParts),
	}
}

// FromMinioClientListMultipartsInfo converts minio ListMultipartUploadsResult to ListMultipartsInfo
func FromMinioClientListMultipartsInfo(lmur minio.ListMultipartUploadsResult) ListMultipartsInfo {
	uploads := make([]MultipartInfo, len(lmur.Uploads))

	for i, um := range lmur.Uploads {
		uploads[i] = MultipartInfo{
			Object:    um.Key,
			UploadID:  um.UploadID,
			Initiated: um.Initiated,
		}
	}

	commonPrefixes := make([]string, len(lmur.CommonPrefixes))
	for i, cp := range lmur.CommonPrefixes {
		commonPrefixes[i] = cp.Prefix
	}

	return ListMultipartsInfo{
		KeyMarker:          lmur.KeyMarker,
		UploadIDMarker:     lmur.UploadIDMarker,
		NextKeyMarker:      lmur.NextKeyMarker,
		NextUploadIDMarker: lmur.NextUploadIDMarker,
		MaxUploads:         int(lmur.MaxUploads),
		IsTruncated:        lmur.IsTruncated,
		Uploads:            uploads,
		Prefix:             lmur.Prefix,
		Delimiter:          lmur.Delimiter,
		CommonPrefixes:     commonPrefixes,
		EncodingType:       lmur.EncodingType,
	}

}

// FromMinioClientObjectInfo converts minio ObjectInfo to gateway ObjectInfo
func FromMinioClientObjectInfo(bucket string, oi minio.ObjectInfo) ObjectInfo {
	userDefined := FromMinioClientMetadata(oi.Metadata)
	userDefined["Content-Type"] = oi.ContentType

	return ObjectInfo{
		Bucket:          bucket,
		Name:            oi.Key,
		ModTime:         oi.LastModified,
		Size:            oi.Size,
		ETag:            canonicalizeETag(oi.ETag),
		UserDefined:     userDefined,
		ContentType:     oi.ContentType,
		ContentEncoding: oi.Metadata.Get("Content-Encoding"),
		StorageClass:    oi.StorageClass,
	}
}

// FromMinioClientListBucketV2Result converts minio ListBucketResult to ListObjectsInfo
func FromMinioClientListBucketV2Result(bucket string, result minio.ListBucketV2Result) ListObjectsV2Info {
	objects := make([]ObjectInfo, len(result.Contents))

	for i, oi := range result.Contents {
		objects[i] = FromMinioClientObjectInfo(bucket, oi)
	}

	prefixes := make([]string, len(result.CommonPrefixes))
	for i, p := range result.CommonPrefixes {
		prefixes[i] = p.Prefix
	}

	return ListObjectsV2Info{
		IsTruncated: result.IsTruncated,
		Prefixes:    prefixes,
		Objects:     objects,

		ContinuationToken:     result.ContinuationToken,
		NextContinuationToken: result.NextContinuationToken,
	}
}

// FromMinioClientListBucketResult converts minio ListBucketResult to ListObjectsInfo
func FromMinioClientListBucketResult(bucket string, result minio.ListBucketResult) ListObjectsInfo {
	objects := make([]ObjectInfo, len(result.Contents))

	for i, oi := range result.Contents {
		objects[i] = FromMinioClientObjectInfo(bucket, oi)
	}

	prefixes := make([]string, len(result.CommonPrefixes))
	for i, p := range result.CommonPrefixes {
		prefixes[i] = p.Prefix
	}

	return ListObjectsInfo{
		IsTruncated: result.IsTruncated,
		NextMarker:  result.NextMarker,
		Prefixes:    prefixes,
		Objects:     objects,
	}
}

// FromMinioClientListBucketResultToV2Info converts minio ListBucketResult to ListObjectsV2Info
func FromMinioClientListBucketResultToV2Info(bucket string, result minio.ListBucketResult) ListObjectsV2Info {
	objects := make([]ObjectInfo, len(result.Contents))

	for i, oi := range result.Contents {
		objects[i] = FromMinioClientObjectInfo(bucket, oi)
	}

	prefixes := make([]string, len(result.CommonPrefixes))
	for i, p := range result.CommonPrefixes {
		prefixes[i] = p.Prefix
	}

	return ListObjectsV2Info{
		IsTruncated:           result.IsTruncated,
		Prefixes:              prefixes,
		Objects:               objects,
		ContinuationToken:     result.Marker,
		NextContinuationToken: result.NextMarker,
	}
}

// ToMinioClientObjectInfoMetadata convertes metadata to map[string][]string
func ToMinioClientObjectInfoMetadata(metadata map[string]string) map[string][]string {
	mm := make(map[string][]string, len(metadata))
	for k, v := range metadata {
		mm[http.CanonicalHeaderKey(k)] = []string{v}
	}
	return mm
}

// ToMinioClientMetadata converts metadata to map[string]string
func ToMinioClientMetadata(metadata map[string]string) map[string]string {
	mm := make(map[string]string)
	for k, v := range metadata {
		mm[http.CanonicalHeaderKey(k)] = v
	}
	return mm
}

// ToMinioClientCompletePart converts CompletePart to minio CompletePart
func ToMinioClientCompletePart(part CompletePart) minio.CompletePart {
	return minio.CompletePart{
		ETag:       part.ETag,
		PartNumber: part.PartNumber,
	}
}

// ToMinioClientCompleteParts converts []CompletePart to minio []CompletePart
func ToMinioClientCompleteParts(parts []CompletePart) []minio.CompletePart {
	mparts := make([]minio.CompletePart, len(parts))
	for i, part := range parts {
		mparts[i] = ToMinioClientCompletePart(part)
	}
	return mparts
}

// ErrorRespToObjectError converts Minio errors to minio object layer errors.
func ErrorRespToObjectError(err error, params ...string) error {
	if err == nil {
		return nil
	}

	bucket := ""
	object := ""
	if len(params) >= 1 {
		bucket = params[0]
	}
	if len(params) == 2 {
		object = params[1]
	}

	if isNetworkOrHostDown(err) {
		return BackendDown{}
	}

	minioErr, ok := err.(minio.ErrorResponse)
	if !ok {
		// We don't interpret non Minio errors. As minio errors will
		// have StatusCode to help to convert to object errors.
		return err
	}

	switch minioErr.Code {
	case "BucketAlreadyOwnedByYou":
		err = BucketAlreadyOwnedByYou{}
	case "BucketNotEmpty":
		err = BucketNotEmpty{}
	case "NoSuchBucketPolicy":
		err = BucketPolicyNotFound{}
	case "InvalidBucketName":
		err = BucketNameInvalid{Bucket: bucket}
	case "InvalidPart":
		err = InvalidPart{}
	case "NoSuchBucket":
		err = BucketNotFound{Bucket: bucket}
	case "NoSuchKey":
		if object != "" {
			err = ObjectNotFound{Bucket: bucket, Object: object}
		} else {
			err = BucketNotFound{Bucket: bucket}
		}
	case "XMinioInvalidObjectName":
		err = ObjectNameInvalid{}
	case "AccessDenied":
		err = PrefixAccessDenied{
			Bucket: bucket,
			Object: object,
		}
	case "XAmzContentSHA256Mismatch":
		err = hash.SHA256Mismatch{}
	case "NoSuchUpload":
		err = InvalidUploadID{}
	case "EntityTooSmall":
		err = PartTooSmall{}
	}

	return err
}

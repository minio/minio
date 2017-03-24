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
	"crypto/sha256"
	"fmt"
	"hash"
	"io"
	"net/http"

	"encoding/hex"

	minio "github.com/minio/minio-go"
)

// Convert Minio errors to minio object layer errors.
func s3ToObjectError(err error, params ...string) error {
	if err == nil {
		return nil
	}

	e, ok := err.(*Error)
	if !ok {
		// Code should be fixed if this function is called without doing traceError()
		// Else handling different situations in this function makes this function complicated.
		errorIf(err, "Expected type *Error")
		return err
	}

	err = e.e

	bucket := ""
	object := ""
	if len(params) >= 1 {
		bucket = params[0]
	}
	if len(params) == 2 {
		object = params[1]
	}

	minioErr, ok := err.(minio.ErrorResponse)
	if !ok {
		// We don't interpret non Minio errors. As minio errors will
		// have StatusCode to help to convert to object errors.
		return e
	}

	switch minioErr.Code {
	case "BucketAlreadyOwnedByYou":
		err = BucketAlreadyOwnedByYou{}
	case "BucketNotEmpty":
		err = BucketNotEmpty{}
	case "InvalidBucketName":
		err = BucketNameInvalid{Bucket: bucket}
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
	default:
		fmt.Println("Undefined s3 object error:", bucket, object, minioErr.Code)
	}

	e.e = err
	return e
}

type s3LayerConfig struct {
	Location        string
	Endpoint        string
	AccessKeyID     string
	SecretAccessKey string
	UseSSL          bool
}

// S3Layer - Implements Object layer for S3 and Minio blob storage.
type S3Layer struct {
	s3LayerConfig

	Client *minio.Core
}

func newS3Layer(endpoint, location, accessKey, secretKey string) (GatewayLayer, error) {
	config := s3LayerConfig{
		Endpoint:        endpoint,
		Location:        location,
		AccessKeyID:     accessKey,
		SecretAccessKey: secretKey,
		UseSSL:          true,
	}

	// Initialize minio client object.
	client, err := minio.NewCore(config.Endpoint, config.AccessKeyID, config.SecretAccessKey, config.UseSSL)
	if err != nil {
		return nil, err
	}

	return &S3Layer{
		s3LayerConfig: config,
		Client:        client,
	}, nil
}

// Shutdown - save any gateway metadata to disk
// if necessary and reload upon next restart.
func (l *S3Layer) Shutdown() error {
	// TODO
	return nil
}

// StorageInfo - Not relevant to S3 backend.
func (l *S3Layer) StorageInfo() StorageInfo {
	return StorageInfo{}
}

// MakeBucket - Create a new container on S3 backend.
func (l *S3Layer) MakeBucket(bucket string) error {
	err := l.Client.MakeBucket(bucket, l.Location)
	if err != nil {
		return s3ToObjectError(traceError(err), bucket)
	}
	return err
}

// GetBucketInfo - Get bucket metadata..
func (l *S3Layer) GetBucketInfo(bucket string) (BucketInfo, error) {
	buckets, err := l.Client.ListBuckets()
	if err != nil {
		return BucketInfo{}, s3ToObjectError(traceError(err), bucket)
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

	return BucketInfo{}, traceError(BucketNotFound{Bucket: bucket})
}

// ListBuckets - Lists all S3 buckets
func (l *S3Layer) ListBuckets() ([]BucketInfo, error) {
	buckets, err := l.Client.ListBuckets()
	if err != nil {
		return nil, err
	}

	b := make([]BucketInfo, len(buckets))
	for i, bi := range buckets {
		b[i] = BucketInfo{
			Name:    bi.Name,
			Created: bi.CreationDate,
		}
	}

	return b, err
}

// DeleteBucket - delete a bucket on S3
func (l *S3Layer) DeleteBucket(bucket string) error {
	err := l.Client.RemoveBucket(bucket)
	if err != nil {
		return s3ToObjectError(traceError(err), bucket)
	}
	return nil
}

// ListObjects - lists all blobs in S3 bucket filtered by prefix
func (l *S3Layer) ListObjects(bucket string, prefix string, marker string, delimiter string, maxKeys int) (result ListObjectsInfo, err error) {
	doneCh := make(chan struct{})
	defer close(doneCh)

	loi := ListObjectsInfo{}

	loi.Objects = make([]ObjectInfo, maxKeys)

	i := 0

	for message := range l.Client.ListObjectsV2(bucket, prefix, true, doneCh) {
		if message.Err != nil {
			return ListObjectsInfo{}, s3ToObjectError(traceError(message.Err), bucket)
		}

		loi.Objects[i] = fromMinioClientObjectInfo(bucket, message)

		if maxKeys == i {
			loi.IsTruncated = true
			loi.NextMarker = message.Key
			return loi, nil
		}

		i++
	}

	loi.Objects = loi.Objects[0:maxKeys]
	return loi, nil
}

// GetObject - reads an object from S3. Supports additional
// parameters like offset and length which are synonymous with
// HTTP Range requests.
//
// startOffset indicates the starting read location of the object.
// length indicates the total length of the object.
func (l *S3Layer) GetObject(bucket string, key string, startOffset int64, length int64, writer io.Writer) error {
	object, err := l.Client.GetObject(bucket, key)
	if err != nil {
		return s3ToObjectError(traceError(err), bucket, key)
	}

	object.Seek(startOffset, io.SeekStart)
	if _, err := io.CopyN(writer, object, length); err != nil {
		return s3ToObjectError(traceError(err), bucket, key)
	}

	object.Close()

	return nil
}

func fromMinioClientObjectInfo(bucket string, oi minio.ObjectInfo) ObjectInfo {
	userDefined := fromMinioClientMetadata(oi.Metadata)
	userDefined["Content-Type"] = oi.ContentType

	return ObjectInfo{
		Bucket:          bucket,
		Name:            oi.Key,
		ModTime:         oi.LastModified,
		Size:            oi.Size,
		MD5Sum:          oi.ETag,
		UserDefined:     userDefined,
		ContentType:     oi.ContentType,
		ContentEncoding: oi.Metadata.Get("Content-Encoding"),
	}
}

// GetObjectInfo - reads blob metadata properties and replies back ObjectInfo,
// uses zure equivalent GetBlobProperties.
func (l *S3Layer) GetObjectInfo(bucket string, object string) (objInfo ObjectInfo, err error) {
	oi, err := l.Client.StatObject(bucket, object)
	if err != nil {
		return ObjectInfo{}, s3ToObjectError(traceError(err), bucket, object)
	}

	return fromMinioClientObjectInfo(bucket, oi), nil
}

// PutObject - Create a new blob with the incoming data,
func (l *S3Layer) PutObject(bucket string, object string, size int64, data io.Reader, metadata map[string]string, sha256sum string) (ObjectInfo, error) {
	var sha256Writer hash.Hash

	teeReader := data
	if sha256sum != "" {
		sha256Writer = sha256.New()
		teeReader = io.TeeReader(data, sha256Writer)
	}

	delete(metadata, "md5Sum")

	err := l.Client.PutObject(bucket, object, size, teeReader, toMinioClientMetadata(metadata))
	if err != nil {
		return ObjectInfo{}, s3ToObjectError(traceError(err), bucket, object)
	}

	if sha256sum != "" {
		newSHA256sum := hex.EncodeToString(sha256Writer.Sum(nil))
		if newSHA256sum != sha256sum {
			l.Client.RemoveObject(bucket, object)
			return ObjectInfo{}, traceError(SHA256Mismatch{})
		}
	}

	oi, err := l.GetObjectInfo(bucket, object)
	if err != nil {
		return ObjectInfo{}, s3ToObjectError(traceError(err), bucket, object)
	}

	return oi, nil
}

// CopyObject - Copies a blob from source container to destination container.
func (l *S3Layer) CopyObject(srcBucket string, srcObject string, destBucket string, destObject string, metadata map[string]string) (ObjectInfo, error) {
	err := l.Client.CopyObject(destBucket, destObject, fmt.Sprintf("%s/%s", srcBucket, srcObject), minio.CopyConditions{})
	if err != nil {
		return ObjectInfo{}, s3ToObjectError(traceError(err), srcBucket, srcObject)
	}

	oi, err := l.GetObjectInfo(destBucket, destObject)
	if err != nil {
		return ObjectInfo{}, s3ToObjectError(traceError(err), destBucket, destObject)
	}

	return oi, nil
}

// DeleteObject - Deletes a blob in bucket
func (l *S3Layer) DeleteObject(bucket string, object string) error {
	err := l.Client.RemoveObject(bucket, object)
	if err != nil {
		return s3ToObjectError(traceError(err), bucket, object)
	}

	return nil
}

func fromMinioClientUploadMetadata(omi minio.ObjectMultipartInfo) uploadMetadata {
	return uploadMetadata{
		Object:    omi.Key,
		UploadID:  omi.UploadID,
		Initiated: omi.Initiated,
	}
}

func fromMinioClientListMultipartsInfo(lmur minio.ListMultipartUploadsResult) ListMultipartsInfo {
	uploads := make([]uploadMetadata, len(lmur.Uploads))

	for i, um := range lmur.Uploads {
		uploads[i] = fromMinioClientUploadMetadata(um)
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

// ListMultipartUploads - lists all multipart uploads.
func (l *S3Layer) ListMultipartUploads(bucket string, prefix string, keyMarker string, uploadIDMarker string, delimiter string, maxUploads int) (ListMultipartsInfo, error) {
	result, err := l.Client.ListMultipartUploads(bucket, prefix, keyMarker, uploadIDMarker, delimiter, maxUploads)
	if err != nil {
		return ListMultipartsInfo{}, err
	}

	return fromMinioClientListMultipartsInfo(result), nil
}

func fromMinioClientMetadata(metadata map[string][]string) map[string]string {
	mm := map[string]string{}
	for k, v := range metadata {
		mm[http.CanonicalHeaderKey(k)] = v[0]
	}
	return mm
}

func toMinioClientMetadata(metadata map[string]string) map[string][]string {
	mm := map[string][]string{}
	for k, v := range metadata {
		mm[http.CanonicalHeaderKey(k)] = []string{v}
	}
	return mm
}

// NewMultipartUpload - upload object in multiple parts
func (l *S3Layer) NewMultipartUpload(bucket string, object string, metadata map[string]string) (uploadID string, err error) {
	return l.Client.NewMultipartUpload(bucket, object, toMinioClientMetadata(metadata))
}

// CopyObjectPart - copy part of object to other bucket and object
func (l *S3Layer) CopyObjectPart(srcBucket string, srcObject string, destBucket string, destObject string, uploadID string, partID int, startOffset int64, length int64) (info PartInfo, err error) {
	panic("CopyObjectPart: not implemented")
}

func fromMinioClientObjectPart(op minio.ObjectPart) PartInfo {
	return PartInfo{
		Size:         op.Size,
		ETag:         op.ETag,
		LastModified: op.LastModified,
		PartNumber:   op.PartNumber,
	}
}

// PutObjectPart puts a part of object in bucket
func (l *S3Layer) PutObjectPart(bucket string, object string, uploadID string, partID int, size int64, data io.Reader, md5Hex string, sha256sum string) (PartInfo, error) {
	md5HexBytes, err := hex.DecodeString(md5Hex)
	if err != nil {
		return PartInfo{}, err
	}

	sha256sumBytes, err := hex.DecodeString(sha256sum)
	if err != nil {
		return PartInfo{}, err
	}

	info, err := l.Client.PutObjectPart(bucket, object, uploadID, partID, size, data, md5HexBytes, sha256sumBytes)
	if err != nil {
		return PartInfo{}, err
	}

	return fromMinioClientObjectPart(info), nil
}

func fromMinioClientObjectParts(parts []minio.ObjectPart) []PartInfo {
	toParts := make([]PartInfo, len(parts))
	for i, part := range parts {
		toParts[i] = fromMinioClientObjectPart(part)
	}
	return toParts
}

func fromMinioClientListPartsInfo(lopr minio.ListObjectPartsResult) ListPartsInfo {
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

// ListObjectParts returns all object parts for specified object in specified bucket
func (l *S3Layer) ListObjectParts(bucket string, object string, uploadID string, partNumberMarker int, maxParts int) (ListPartsInfo, error) {
	result, err := l.Client.ListObjectParts(bucket, object, uploadID, partNumberMarker, maxParts)
	if err != nil {
		return ListPartsInfo{}, err
	}

	return fromMinioClientListPartsInfo(result), nil
}

// AbortMultipartUpload aborts a ongoing multipart upload
func (l *S3Layer) AbortMultipartUpload(bucket string, object string, uploadID string) error {
	return l.Client.AbortMultipartUpload(bucket, object, uploadID)
}

func toMinioClientCompletePart(part completePart) minio.CompletePart {
	return minio.CompletePart{
		ETag:       part.ETag,
		PartNumber: part.PartNumber,
	}
}

func toMinioClientCompleteParts(parts []completePart) []minio.CompletePart {
	mparts := make([]minio.CompletePart, len(parts))
	for i, part := range parts {
		mparts[i] = toMinioClientCompletePart(part)
	}
	return mparts
}

// CompleteMultipartUpload completes ongoing multipart upload and finalizes object
func (l *S3Layer) CompleteMultipartUpload(bucket string, object string, uploadID string, uploadedParts []completePart) (ObjectInfo, error) {
	err := l.Client.CompleteMultipartUpload(bucket, object, uploadID, toMinioClientCompleteParts(uploadedParts))
	if err != nil {
		return ObjectInfo{}, nil
	}

	return l.GetObjectInfo(bucket, object)
}

// SetBucketPolicies - Set policy on bucket
func (l *S3Layer) SetBucketPolicies(string, []BucketAccessPolicy) error {
	return traceError(NotImplemented{})
}

// GetBucketPolicies - Get policy on bucket
func (l *S3Layer) GetBucketPolicies(string) ([]BucketAccessPolicy, error) {
	return []BucketAccessPolicy{}, traceError(NotImplemented{})
}

// DeleteBucketPolicies - Delete all policies on bucket
func (l *S3Layer) DeleteBucketPolicies(string) error {
	return traceError(NotImplemented{})
}

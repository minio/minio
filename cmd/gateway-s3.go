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
	"net/http"

	minio "github.com/minio/minio-go"
	"github.com/minio/minio-go/pkg/policy"
	"github.com/minio/minio-go/pkg/s3utils"
	"github.com/minio/minio/pkg/hash"
)

// s3ToObjectError converts Minio errors to minio object layer errors.
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
	case "NoSuchBucketPolicy":
		err = PolicyNotFound{}
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

	e.e = err
	return e
}

// s3Objects implements gateway for Minio and S3 compatible object storage servers.
type s3Objects struct {
	gatewayUnsupported
	Client     *minio.Core
	anonClient *minio.Core
}

// newS3Gateway returns s3 gatewaylayer
func newS3Gateway(host string) (GatewayLayer, error) {
	var err error
	var endpoint string
	var secure = true

	// Validate host parameters.
	if host != "" {
		// Override default params if the host is provided
		endpoint, secure, err = parseGatewayEndpoint(host)
		if err != nil {
			return nil, err
		}
	}

	// Default endpoint parameters
	if endpoint == "" {
		endpoint = "s3.amazonaws.com"
	}

	creds := serverConfig.GetCredential()

	// Initialize minio client object.
	client, err := minio.NewCore(endpoint, creds.AccessKey, creds.SecretKey, secure)
	if err != nil {
		return nil, err
	}

	anonClient, err := minio.NewCore(endpoint, "", "", secure)
	if err != nil {
		return nil, err
	}
	anonClient.SetCustomTransport(newCustomHTTPTransport())

	return &s3Objects{
		Client:     client,
		anonClient: anonClient,
	}, nil
}

// Shutdown saves any gateway metadata to disk
// if necessary and reload upon next restart.
func (l *s3Objects) Shutdown() error {
	// TODO
	return nil
}

// StorageInfo is not relevant to S3 backend.
func (l *s3Objects) StorageInfo() (si StorageInfo) {
	return si
}

// MakeBucket creates a new container on S3 backend.
func (l *s3Objects) MakeBucketWithLocation(bucket, location string) error {
	err := l.Client.MakeBucket(bucket, location)
	if err != nil {
		return s3ToObjectError(traceError(err), bucket)
	}
	return err
}

// GetBucketInfo gets bucket metadata..
func (l *s3Objects) GetBucketInfo(bucket string) (bi BucketInfo, e error) {
	// Verify if bucket name is valid.
	// We are using a separate helper function here to validate bucket
	// names instead of IsValidBucketName() because there is a possibility
	// that certains users might have buckets which are non-DNS compliant
	// in us-east-1 and we might severely restrict them by not allowing
	// access to these buckets.
	// Ref - http://docs.aws.amazon.com/AmazonS3/latest/dev/BucketRestrictions.html
	if s3utils.CheckValidBucketName(bucket) != nil {
		return bi, traceError(BucketNameInvalid{Bucket: bucket})
	}

	buckets, err := l.Client.ListBuckets()
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

// ListBuckets lists all S3 buckets
func (l *s3Objects) ListBuckets() ([]BucketInfo, error) {
	buckets, err := l.Client.ListBuckets()
	if err != nil {
		return nil, s3ToObjectError(traceError(err))
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

// DeleteBucket deletes a bucket on S3
func (l *s3Objects) DeleteBucket(bucket string) error {
	err := l.Client.RemoveBucket(bucket)
	if err != nil {
		return s3ToObjectError(traceError(err), bucket)
	}
	return nil
}

// ListObjects lists all blobs in S3 bucket filtered by prefix
func (l *s3Objects) ListObjects(bucket string, prefix string, marker string, delimiter string, maxKeys int) (loi ListObjectsInfo, e error) {
	result, err := l.Client.ListObjects(bucket, prefix, marker, delimiter, maxKeys)
	if err != nil {
		return loi, s3ToObjectError(traceError(err), bucket)
	}

	return fromMinioClientListBucketResult(bucket, result), nil
}

// ListObjectsV2 lists all blobs in S3 bucket filtered by prefix
func (l *s3Objects) ListObjectsV2(bucket, prefix, continuationToken, delimiter string, maxKeys int, fetchOwner bool, startAfter string) (loi ListObjectsV2Info, e error) {
	result, err := l.Client.ListObjectsV2(bucket, prefix, continuationToken, fetchOwner, delimiter, maxKeys)
	if err != nil {
		return loi, s3ToObjectError(traceError(err), bucket)
	}

	return fromMinioClientListBucketV2Result(bucket, result), nil
}

// fromMinioClientListBucketV2Result converts minio ListBucketResult to ListObjectsInfo
func fromMinioClientListBucketV2Result(bucket string, result minio.ListBucketV2Result) ListObjectsV2Info {
	objects := make([]ObjectInfo, len(result.Contents))

	for i, oi := range result.Contents {
		objects[i] = fromMinioClientObjectInfo(bucket, oi)
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

// fromMinioClientListBucketResult converts minio ListBucketResult to ListObjectsInfo
func fromMinioClientListBucketResult(bucket string, result minio.ListBucketResult) ListObjectsInfo {
	objects := make([]ObjectInfo, len(result.Contents))

	for i, oi := range result.Contents {
		objects[i] = fromMinioClientObjectInfo(bucket, oi)
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

// GetObject reads an object from S3. Supports additional
// parameters like offset and length which are synonymous with
// HTTP Range requests.
//
// startOffset indicates the starting read location of the object.
// length indicates the total length of the object.
func (l *s3Objects) GetObject(bucket string, key string, startOffset int64, length int64, writer io.Writer) error {
	if length < 0 && length != -1 {
		return s3ToObjectError(traceError(errInvalidArgument), bucket, key)
	}

	opts := minio.GetObjectOptions{}
	if startOffset >= 0 && length >= 0 {
		if err := opts.SetRange(startOffset, startOffset+length-1); err != nil {
			return s3ToObjectError(traceError(err), bucket, key)
		}
	}
	object, _, err := l.Client.GetObject(bucket, key, opts)
	if err != nil {
		return s3ToObjectError(traceError(err), bucket, key)
	}
	defer object.Close()

	if _, err := io.Copy(writer, object); err != nil {
		return s3ToObjectError(traceError(err), bucket, key)
	}
	return nil
}

// fromMinioClientObjectInfo converts minio ObjectInfo to gateway ObjectInfo
func fromMinioClientObjectInfo(bucket string, oi minio.ObjectInfo) ObjectInfo {
	userDefined := fromMinioClientMetadata(oi.Metadata)
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
	}
}

// GetObjectInfo reads object info and replies back ObjectInfo
func (l *s3Objects) GetObjectInfo(bucket string, object string) (objInfo ObjectInfo, err error) {
	oi, err := l.Client.StatObject(bucket, object, minio.StatObjectOptions{})
	if err != nil {
		return ObjectInfo{}, s3ToObjectError(traceError(err), bucket, object)
	}

	return fromMinioClientObjectInfo(bucket, oi), nil
}

// PutObject creates a new object with the incoming data,
func (l *s3Objects) PutObject(bucket string, object string, data *hash.Reader, metadata map[string]string) (objInfo ObjectInfo, err error) {
	oi, err := l.Client.PutObject(bucket, object, data, data.Size(), data.MD5(), data.SHA256(), toMinioClientMetadata(metadata))
	if err != nil {
		return objInfo, s3ToObjectError(traceError(err), bucket, object)
	}

	return fromMinioClientObjectInfo(bucket, oi), nil
}

// CopyObject copies an object from source bucket to a destination bucket.
func (l *s3Objects) CopyObject(srcBucket string, srcObject string, dstBucket string, dstObject string, metadata map[string]string) (objInfo ObjectInfo, err error) {
	// Set this header such that following CopyObject() always sets the right metadata on the destination.
	// metadata input is already a trickled down value from interpreting x-amz-metadata-directive at
	// handler layer. So what we have right now is supposed to be applied on the destination object anyways.
	// So preserve it by adding "REPLACE" directive to save all the metadata set by CopyObject API.
	metadata["x-amz-metadata-directive"] = "REPLACE"
	if _, err = l.Client.CopyObject(srcBucket, srcObject, dstBucket, dstObject, metadata); err != nil {
		return objInfo, s3ToObjectError(traceError(err), srcBucket, srcObject)
	}
	return l.GetObjectInfo(dstBucket, dstObject)
}

// DeleteObject deletes a blob in bucket
func (l *s3Objects) DeleteObject(bucket string, object string) error {
	err := l.Client.RemoveObject(bucket, object)
	if err != nil {
		return s3ToObjectError(traceError(err), bucket, object)
	}

	return nil
}

// fromMinioClientUploadMetadata converts ObjectMultipartInfo to uploadMetadata
func fromMinioClientUploadMetadata(omi minio.ObjectMultipartInfo) uploadMetadata {
	return uploadMetadata{
		Object:    omi.Key,
		UploadID:  omi.UploadID,
		Initiated: omi.Initiated,
	}
}

// fromMinioClientListMultipartsInfo converts minio ListMultipartUploadsResult to ListMultipartsInfo
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

// ListMultipartUploads lists all multipart uploads.
func (l *s3Objects) ListMultipartUploads(bucket string, prefix string, keyMarker string, uploadIDMarker string, delimiter string, maxUploads int) (lmi ListMultipartsInfo, e error) {
	result, err := l.Client.ListMultipartUploads(bucket, prefix, keyMarker, uploadIDMarker, delimiter, maxUploads)
	if err != nil {
		return lmi, err
	}

	return fromMinioClientListMultipartsInfo(result), nil
}

// fromMinioClientMetadata converts minio metadata to map[string]string
func fromMinioClientMetadata(metadata map[string][]string) map[string]string {
	mm := map[string]string{}
	for k, v := range metadata {
		mm[http.CanonicalHeaderKey(k)] = v[0]
	}
	return mm
}

// toMinioClientMetadata converts metadata to map[string][]string
func toMinioClientMetadata(metadata map[string]string) map[string]string {
	mm := map[string]string{}
	for k, v := range metadata {
		mm[http.CanonicalHeaderKey(k)] = v
	}
	return mm
}

// NewMultipartUpload upload object in multiple parts
func (l *s3Objects) NewMultipartUpload(bucket string, object string, metadata map[string]string) (uploadID string, err error) {
	// Create PutObject options
	opts := minio.PutObjectOptions{UserMetadata: metadata}
	uploadID, err = l.Client.NewMultipartUpload(bucket, object, opts)
	if err != nil {
		return uploadID, s3ToObjectError(traceError(err), bucket, object)
	}
	return uploadID, nil
}

// fromMinioClientObjectPart converts minio ObjectPart to PartInfo
func fromMinioClientObjectPart(op minio.ObjectPart) PartInfo {
	return PartInfo{
		Size:         op.Size,
		ETag:         canonicalizeETag(op.ETag),
		LastModified: op.LastModified,
		PartNumber:   op.PartNumber,
	}
}

// PutObjectPart puts a part of object in bucket
func (l *s3Objects) PutObjectPart(bucket string, object string, uploadID string, partID int, data *hash.Reader) (pi PartInfo, e error) {
	info, err := l.Client.PutObjectPart(bucket, object, uploadID, partID, data, data.Size(), data.MD5(), data.SHA256())
	if err != nil {
		return pi, s3ToObjectError(traceError(err), bucket, object)
	}

	return fromMinioClientObjectPart(info), nil
}

// fromMinioClientObjectParts converts minio ObjectPart to PartInfo
func fromMinioClientObjectParts(parts []minio.ObjectPart) []PartInfo {
	toParts := make([]PartInfo, len(parts))
	for i, part := range parts {
		toParts[i] = fromMinioClientObjectPart(part)
	}
	return toParts
}

// fromMinioClientListPartsInfo converts minio ListObjectPartsResult to ListPartsInfo
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
func (l *s3Objects) ListObjectParts(bucket string, object string, uploadID string, partNumberMarker int, maxParts int) (lpi ListPartsInfo, e error) {
	result, err := l.Client.ListObjectParts(bucket, object, uploadID, partNumberMarker, maxParts)
	if err != nil {
		return lpi, err
	}

	return fromMinioClientListPartsInfo(result), nil
}

// AbortMultipartUpload aborts a ongoing multipart upload
func (l *s3Objects) AbortMultipartUpload(bucket string, object string, uploadID string) error {
	err := l.Client.AbortMultipartUpload(bucket, object, uploadID)
	return s3ToObjectError(traceError(err), bucket, object)
}

// toMinioClientCompletePart converts completePart to minio CompletePart
func toMinioClientCompletePart(part completePart) minio.CompletePart {
	return minio.CompletePart{
		ETag:       part.ETag,
		PartNumber: part.PartNumber,
	}
}

// toMinioClientCompleteParts converts []completePart to minio []CompletePart
func toMinioClientCompleteParts(parts []completePart) []minio.CompletePart {
	mparts := make([]minio.CompletePart, len(parts))
	for i, part := range parts {
		mparts[i] = toMinioClientCompletePart(part)
	}
	return mparts
}

// CompleteMultipartUpload completes ongoing multipart upload and finalizes object
func (l *s3Objects) CompleteMultipartUpload(bucket string, object string, uploadID string, uploadedParts []completePart) (oi ObjectInfo, e error) {
	err := l.Client.CompleteMultipartUpload(bucket, object, uploadID, toMinioClientCompleteParts(uploadedParts))
	if err != nil {
		return oi, s3ToObjectError(traceError(err), bucket, object)
	}

	return l.GetObjectInfo(bucket, object)
}

// SetBucketPolicies sets policy on bucket
func (l *s3Objects) SetBucketPolicies(bucket string, policyInfo policy.BucketAccessPolicy) error {
	if err := l.Client.PutBucketPolicy(bucket, policyInfo); err != nil {
		return s3ToObjectError(traceError(err), bucket, "")
	}

	return nil
}

// GetBucketPolicies will get policy on bucket
func (l *s3Objects) GetBucketPolicies(bucket string) (policy.BucketAccessPolicy, error) {
	policyInfo, err := l.Client.GetBucketPolicy(bucket)
	if err != nil {
		return policy.BucketAccessPolicy{}, s3ToObjectError(traceError(err), bucket, "")
	}
	return policyInfo, nil
}

// DeleteBucketPolicies deletes all policies on bucket
func (l *s3Objects) DeleteBucketPolicies(bucket string) error {
	if err := l.Client.PutBucketPolicy(bucket, policy.BucketAccessPolicy{}); err != nil {
		return s3ToObjectError(traceError(err), bucket, "")
	}
	return nil
}

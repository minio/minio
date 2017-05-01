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
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"hash"
	"io"
	"strconv"
	"strings"

	"encoding/hex"

	"cloud.google.com/go/storage"

	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"

	minio "github.com/minio/minio-go"
	"github.com/minio/minio-go/pkg/policy"
)

// Convert Minio errors to minio object layer errors.
func gcsToObjectError(err error, params ...string) error {
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

	fmt.Printf("%+v\n ok=%v code=%v, message=%v body=%v reason=%v bucket=%v object=%v\n", err.Error(), ok, "", "", "", "", bucket, object)

	// in some cases just a plain error is being returned
	switch err.Error() {
	case "storage: bucket doesn't exist":
		err = BucketNotFound{
			Bucket: bucket,
		}
		e.e = err
		return e
	case "storage: object doesn't exist":
		err = ObjectNotFound{
			Bucket: bucket,
			Object: object,
		}
		e.e = err
		return e
	}

	googleApiErr, ok := err.(*googleapi.Error)
	if !ok {
		// We don't interpret non Minio errors. As minio errors will
		// have StatusCode to help to convert to object errors.
		return e
	}

	fmt.Printf("%+v\n ok=%v code=%v, message=%v body=%v reason=%v bucket=%v object=%v\n", googleApiErr, ok, googleApiErr.Code, googleApiErr.Message, googleApiErr.Body, googleApiErr.Errors[0].Reason, bucket, object)

	reason := googleApiErr.Errors[0].Reason
	message := googleApiErr.Errors[0].Message

	switch reason {
	case "forbidden":
		err = PrefixAccessDenied{
			Bucket: bucket,
			Object: object,
		}
	case "invalid":
		err = BucketNameInvalid{
			Bucket: bucket,
		}
	case "notFound":
		fmt.Println(object, bucket)
		if object != "" {
			err = ObjectNotFound{
				Bucket: bucket,
				Object: object,
			}
		} else {
			err = BucketNotFound{
				Bucket: bucket,
			}
		}
	case "conflict":
		if message == "You already own this bucket. Please select another name." {
			err = BucketAlreadyOwnedByYou{
				Bucket: bucket,
			}

		} else {
			err = BucketNotEmpty{
				Bucket: bucket,
			}
		}
	default:
		err = fmt.Errorf("Unsupported error reason: %s", reason)

	}

	_ = bucket
	_ = object

	e.e = err
	return e
}

// gcsGateway - Implements gateway for Minio and GCS compatible object storage servers.
type gcsGateway struct {
	client     *storage.Client
	Client     *minio.Core
	anonClient *minio.Core
	projectID  string
	ctx        context.Context
}

// newGCSGateway returns gcs gatewaylayer
func newGCSGateway(endpoint string, projectID, secretKey string, secure bool) (GatewayLayer, error) {
	fmt.Println(secretKey)

	ctx := context.Background()

	// Creates a client.
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, err
	}

	return &gcsGateway{
		client:     client,
		projectID:  "minio-166400",
		ctx:        ctx,
		Client:     nil,
		anonClient: nil,
	}, nil
}

// Shutdown - save any gateway metadata to disk
// if necessary and reload upon next restart.
func (l *gcsGateway) Shutdown() error {
	// TODO
	return nil
}

// StorageInfo - Not relevant to GCS backend.
func (l *gcsGateway) StorageInfo() StorageInfo {
	return StorageInfo{}
}

// MakeBucket - Create a new container on GCS backend.
func (l *gcsGateway) MakeBucket(bucket string) error {
	// will never be called, only satisfy ObjectLayer interface
	return traceError(NotImplemented{})
}

// MakeBucket - Create a new container on GCS backend.
func (l *gcsGateway) MakeBucketWithLocation(bucket, location string) error {
	bkt := l.client.Bucket(bucket)

	// TODO: location
	_ = location
	if err := bkt.Create(l.ctx, l.projectID, &storage.BucketAttrs{
		Location: "US",
	}); err != nil {
		return gcsToObjectError(traceError(err), bucket)
	}

	return nil
}

// GetBucketInfo - Get bucket metadata..
func (l *gcsGateway) GetBucketInfo(bucket string) (BucketInfo, error) {
	attrs, err := l.client.Bucket(bucket).Attrs(l.ctx)
	if err != nil {
		return BucketInfo{}, gcsToObjectError(traceError(err), bucket)
	}

	return BucketInfo{
		Name:    attrs.Name,
		Created: attrs.Created,
	}, nil
}

// ListBuckets lists all GCS buckets
func (l *gcsGateway) ListBuckets() ([]BucketInfo, error) {
	it := l.client.Buckets(l.ctx, l.projectID)

	b := []BucketInfo{}
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}

		if err != nil {
			return []BucketInfo{}, err
		}

		b = append(b, BucketInfo{
			Name:    attrs.Name,
			Created: attrs.Created,
		})
	}

	return b, nil
}

// DeleteBucket delete a bucket on GCS
func (l *gcsGateway) DeleteBucket(bucket string) error {
	err := l.client.Bucket(bucket).Delete(l.ctx)
	if err != nil {
		return gcsToObjectError(traceError(err), bucket)
	}

	return nil
}

const (
	// ZZZZ_MINIO is used for metadata and multiparts. The prefix is being filtered out,
	// hence the naming of ZZZZ (last prefix)
	ZZZZ_MINIO_PREFIX = "ZZZZ-Minio"
)

// ListObjects - lists all blobs in GCS bucket filtered by prefix
func (l *gcsGateway) ListObjects(bucket string, prefix string, marker string, delimiter string, maxKeys int) (ListObjectsInfo, error) {
	it := l.client.Bucket(bucket).Objects(l.ctx, &storage.Query{Delimiter: delimiter, Prefix: prefix, Versions: false})

	isTruncated := false
	nextMarker := ""
	prefixes := []string{}

	objects := []ObjectInfo{}
	for {
		if len(objects) >= maxKeys {
			// check if there is one next object and
			// if that one next object is our hidden
			// metadata folder, then just break
			// otherwise we've truncated the output

			m := it.PageInfo().Token

			attrs, _ := it.Next()
			if attrs == nil {
			} else if attrs.Prefix == ZZZZ_MINIO_PREFIX {
				break
			}

			isTruncated = true
			nextMarker = m
			break
		}

		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}

		if err != nil {
			return ListObjectsInfo{}, gcsToObjectError(traceError(err), bucket, prefix)
		}

		if attrs.Prefix == ZZZZ_MINIO_PREFIX {
			// we don't return our metadata prefix
			continue
		} else if attrs.Prefix != "" {
			prefixes = append(prefixes, attrs.Prefix)
			continue
		}

		objects = append(objects, ObjectInfo{
			Name:            attrs.Name,
			Bucket:          attrs.Bucket,
			ModTime:         attrs.Updated,
			Size:            attrs.Size,
			MD5Sum:          hex.EncodeToString(attrs.MD5),
			UserDefined:     attrs.Metadata,
			ContentType:     attrs.ContentType,
			ContentEncoding: attrs.ContentEncoding,
		})
	}

	return ListObjectsInfo{
		IsTruncated: isTruncated,
		NextMarker:  nextMarker,
		Prefixes:    prefixes,
		Objects:     objects,
	}, nil
}

// ListObjectsV2 - lists all blobs in GCS bucket filtered by prefix
func (l *gcsGateway) ListObjectsV2(bucket, prefix, continuationToken string, fetchOwner bool, delimiter string, maxKeys int) (ListObjectsV2Info, error) {
	it := l.client.Bucket(bucket).Objects(l.ctx, &storage.Query{Delimiter: delimiter, Prefix: prefix, Versions: false})

	isTruncated := false
	nextMarker := ""
	prefixes := []string{}

	objects := []ObjectInfo{}
	for {
		if maxKeys < len(objects) {
			isTruncated = true
			nextMarker = it.PageInfo().Token
			break
		}

		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}

		if err != nil {
			return ListObjectsV2Info{}, gcsToObjectError(traceError(err), bucket, prefix)
		}

		if attrs.Prefix != "" {
			prefixes = append(prefixes, attrs.Prefix)
			continue
		}

		objects = append(objects, fromGCSObjectInfo(attrs))
	}

	return ListObjectsV2Info{
		IsTruncated:           isTruncated,
		ContinuationToken:     continuationToken,
		NextContinuationToken: nextMarker,
		Prefixes:              prefixes,
		Objects:               objects,
	}, nil
}

// GetObject - reads an object from GCS. Supports additional
// parameters like offset and length which are synonymous with
// HTTP Range requests.
//
// startOffset indicates the starting read location of the object.
// length indicates the total length of the object.
func (l *gcsGateway) GetObject(bucket string, key string, startOffset int64, length int64, writer io.Writer) error {
	// if we want to mimic S3 behavior exactly, we need to verify if bucket exists first,
	// otherwise gcs will just return object not exist in case of non-existing bucket
	if _, err := l.client.Bucket(bucket).Attrs(l.ctx); err != nil {
		return gcsToObjectError(traceError(err), bucket)
	}

	object := l.client.Bucket(bucket).Object(key)
	r, err := object.NewRangeReader(l.ctx, startOffset, length)
	if err != nil {
		return gcsToObjectError(traceError(err), bucket, key)
	}

	defer r.Close()

	if _, err := io.Copy(writer, r); err != nil {
		return gcsToObjectError(traceError(err), bucket, key)
	}

	return nil
}

// fromGCSObjectInfo converts GCS BucketAttrs to gateway ObjectInfo
func fromGCSObjectInfo(attrs *storage.ObjectAttrs) ObjectInfo {
	return ObjectInfo{
		Name:            attrs.Name,
		Bucket:          attrs.Bucket,
		ModTime:         attrs.Updated,
		Size:            attrs.Size,
		MD5Sum:          hex.EncodeToString(attrs.MD5),
		UserDefined:     attrs.Metadata,
		ContentType:     attrs.ContentType,
		ContentEncoding: attrs.ContentEncoding,
	}
}

// GetObjectInfo - reads object info and replies back ObjectInfo
func (l *gcsGateway) GetObjectInfo(bucket string, object string) (objInfo ObjectInfo, err error) {
	// if we want to mimic S3 behavior exactly, we need to verify if bucket exists first,
	// otherwise gcs will just return object not exist in case of non-existing bucket
	if _, err := l.client.Bucket(bucket).Attrs(l.ctx); err != nil {
		return ObjectInfo{}, gcsToObjectError(traceError(err), bucket)
	}

	attrs, err := l.client.Bucket(bucket).Object(object).Attrs(l.ctx)
	if err != nil {
		return ObjectInfo{}, gcsToObjectError(traceError(err), bucket, object)
	}

	return fromGCSObjectInfo(attrs), nil
}

// PutObject - Create a new object with the incoming data,
func (l *gcsGateway) PutObject(bucket string, key string, size int64, data io.Reader, metadata map[string]string, sha256sum string) (ObjectInfo, error) {
	// if we want to mimic S3 behavior exactly, we need to verify if bucket exists first,
	// otherwise gcs will just return object not exist in case of non-existing bucket
	if _, err := l.client.Bucket(bucket).Attrs(l.ctx); err != nil {
		return ObjectInfo{}, gcsToObjectError(traceError(err), bucket)
	}

	var sha256Writer hash.Hash

	teeReader := data
	if sha256sum == "" {
	} else if _, err := hex.DecodeString(sha256sum); err != nil {
		return ObjectInfo{}, gcsToObjectError(traceError(err), bucket, key)
	} else {
		sha256Writer = sha256.New()
		teeReader = io.TeeReader(data, sha256Writer)
	}

	delete(metadata, "md5Sum")

	object := l.client.Bucket(bucket).Object(key)

	w := object.NewWriter(l.ctx)

	w.ContentType = metadata["content-type"]
	w.ContentEncoding = metadata["content-encoding"]
	w.Metadata = metadata

	_, err := io.Copy(w, teeReader)
	if err != nil {
		return ObjectInfo{}, gcsToObjectError(traceError(err), bucket, key)
	}

	err = w.Close()
	if err != nil {
		return ObjectInfo{}, gcsToObjectError(traceError(err), bucket, key)
	}

	attrs, err := object.Attrs(l.ctx)
	if err != nil {
		return ObjectInfo{}, gcsToObjectError(traceError(err), bucket, key)
	}
	if sha256sum != "" {
		newSHA256sum := hex.EncodeToString(sha256Writer.Sum(nil))
		if newSHA256sum != sha256sum {
			//l.Client.RemoveObject(bucket, object)
			return ObjectInfo{}, traceError(SHA256Mismatch{})
		}
	}

	return fromGCSObjectInfo(attrs), nil
}

// CopyObject - Copies a blob from source container to destination container.
func (l *gcsGateway) CopyObject(srcBucket string, srcObject string, destBucket string, destObject string, metadata map[string]string) (ObjectInfo, error) {
	src := l.client.Bucket(srcBucket).Object(srcObject)
	dst := l.client.Bucket(destBucket).Object(destObject)

	attrs, err := dst.CopierFrom(src).Run(l.ctx)
	if err != nil {
		return ObjectInfo{}, gcsToObjectError(traceError(err), destBucket, destObject)
	}

	return fromGCSObjectInfo(attrs), nil
}

// DeleteObject - Deletes a blob in bucket
func (l *gcsGateway) DeleteObject(bucket string, object string) error {
	err := l.client.Bucket(bucket).Object(object).Delete(l.ctx)
	if err != nil {
		return gcsToObjectError(traceError(err), bucket, object)
	}

	return nil
}

// ListMultipartUploads - lists all multipart uploads.
func (l *gcsGateway) ListMultipartUploads(bucket string, prefix string, keyMarker string, uploadIDMarker string, delimiter string, maxUploads int) (ListMultipartsInfo, error) {
	// TODO: implement prefix and prefixes, how does this work for Multiparts??
	prefix = fmt.Sprintf("%s/multipart-", ZZZZ_MINIO_PREFIX)

	it := l.client.Bucket(bucket).Objects(l.ctx, &storage.Query{Delimiter: delimiter, Prefix: prefix, Versions: false})

	isTruncated := false
	// prefixes := []string{}
	nextMarker := ""

	uploads := []uploadMetadata{}
	for {
		if maxUploads <= len(uploads) {
			isTruncated = true
			nextMarker = it.PageInfo().Token
			break
		}

		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}

		if err != nil {
			return ListMultipartsInfo{}, gcsToObjectError(traceError(err), bucket)
		}

		if attrs.Prefix != "" {
			continue
		}

		objectKey, uploadID, partID, err := fromGCSMultipartKey(attrs.Name)
		if err != nil {
			continue
		} else if partID != 0 {
			continue
		}

		// we count only partID == 0
		uploads = append(uploads, uploadMetadata{
			Object:    objectKey,
			UploadID:  uploadID,
			Initiated: attrs.Created,
		})

	}

	return ListMultipartsInfo{
		Uploads:     uploads,
		IsTruncated: isTruncated,

		KeyMarker:          nextMarker,
		UploadIDMarker:     nextMarker,
		NextKeyMarker:      nextMarker,
		NextUploadIDMarker: nextMarker,
		MaxUploads:         maxUploads,
	}, nil

}

func fromGCSMultipartKey(s string) (key, uploadID string, partID int, err error) {
	parts := strings.Split(s, "-")
	if parts[0] != "multipart" {
		return "", "", 0, errors.New("Not a valid multipart identifier.")
	}

	if len(parts) != 4 {
		return "", "", 0, errors.New("Not a valid multipart identifier.")
	}

	key = parts[1]
	uploadID = parts[3]

	partID, err = strconv.Atoi(parts[3])
	if err != nil {
		return "", "", 0, err
	}

	return
}

func toGCSMultipartKey(key string, uploadID string, partID int) string {
	// we don't use the etag within the key, because the amazon spec
	// explicitly notes that uploaded parts with same number are being overwritten

	// parts are allowed to be numbered from 1 to 10,000 (inclusive)
	return fmt.Sprintf("%s/multipart-%s-%s-%05d", ZZZZ_MINIO_PREFIX, key, uploadID, partID)
}

// NewMultipartUpload - upload object in multiple parts
func (l *gcsGateway) NewMultipartUpload(bucket string, key string, metadata map[string]string) (uploadID string, err error) {
	// generate new uploadid
	uploadID = mustGetUUID()

	// generate name for part zero
	partZeroKey := toGCSMultipartKey(key, uploadID, 0)

	// we are writing a 0 sized object to hold the metadata
	w := l.client.Bucket(bucket).Object(partZeroKey).NewWriter(l.ctx)
	w.ContentType = metadata["content-type"]
	w.ContentEncoding = metadata["content-encoding"]
	w.Metadata = metadata
	if err = w.Close(); err != nil {
		return "", gcsToObjectError(traceError(err), bucket, key)
	}

	return uploadID, nil
}

// CopyObjectPart - copy part of object to other bucket and object
func (l *gcsGateway) CopyObjectPart(srcBucket string, srcObject string, destBucket string, destObject string, uploadID string, partID int, startOffset int64, length int64) (info PartInfo, err error) {
	return PartInfo{}, traceError(NotSupported{})
}

// PutObjectPart puts a part of object in bucket
func (l *gcsGateway) PutObjectPart(bucket string, key string, uploadID string, partID int, size int64, data io.Reader, md5Hex string, sha256sum string) (PartInfo, error) {
	multipartKey := toGCSMultipartKey(key, uploadID, partID)

	info, err := l.PutObject(bucket, multipartKey, size, data, map[string]string{}, sha256sum)
	return PartInfo{
		PartNumber:   partID,
		LastModified: info.ModTime,
		ETag:         info.MD5Sum,
		Size:         info.Size,
	}, err
}

// ListObjectParts returns all object parts for specified object in specified bucket
func (l *gcsGateway) ListObjectParts(bucket string, key string, uploadID string, partNumberMarker int, maxParts int) (ListPartsInfo, error) {
	// TODO: support partNumberMarker

	prefix := fmt.Sprintf("%s/multipart-%s-%s", ZZZZ_MINIO_PREFIX, key, uploadID)
	delimiter := "/"

	it := l.client.Bucket(bucket).Objects(l.ctx, &storage.Query{Delimiter: delimiter, Prefix: prefix, Versions: false})

	isTruncated := false

	parts := []PartInfo{}
	for {
		if maxParts <= len(parts) {
			isTruncated = true
			// nextMarker = it.PageInfo().Token
			break
		}

		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}

		if err != nil {
			return ListPartsInfo{}, gcsToObjectError(traceError(err), bucket, prefix)
		}

		if attrs.Prefix != "" {
			continue
		}

		_, _, partID, err := fromGCSMultipartKey(attrs.Name)
		if err != nil {
			continue
		} else if partID == 0 {
			// we'll ignore partID 0, it is our zero object, containing
			// metadata
			continue
		}

		parts = append(parts, PartInfo{
			PartNumber:   partID,
			LastModified: attrs.Updated,
			ETag:         hex.EncodeToString(attrs.MD5),
			Size:         attrs.Size,
		})
	}

	return ListPartsInfo{
		IsTruncated: isTruncated,
		Parts:       parts,
	}, nil
}

// AbortMultipartUpload aborts a ongoing multipart upload
func (l *gcsGateway) AbortMultipartUpload(bucket string, key string, uploadID string) error {
	prefix := fmt.Sprintf("%s/multipart-%s-%s", ZZZZ_MINIO_PREFIX, key, uploadID)
	delimiter := "/"

	// delete part zero, ignoring errors here, we want to clean up all remains
	_ = l.client.Bucket(bucket).Object(toGCSMultipartKey(key, uploadID, 0)).Delete(l.ctx)

	// iterate through all parts and delete them
	it := l.client.Bucket(bucket).Objects(l.ctx, &storage.Query{Delimiter: delimiter, Prefix: prefix, Versions: false})
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}

		if err != nil {
			return gcsToObjectError(traceError(err), bucket, prefix)
		}

		// on error continue deleting other parts
		l.client.Bucket(bucket).Object(attrs.Name).Delete(l.ctx)
	}

	return nil
}

// CompleteMultipartUpload completes ongoing multipart upload and finalizes object

// Note that there is a limit (currently 32) to the number of components that can be composed in a single operation.

// There is a limit (currently 1024) to the total number of components for a given composite object. This means you can append to each object at most 1023 times.

// There is a per-project rate limit (currently 200) to the number of components you can compose per second. This rate counts both the components being appended to a composite object as well as the components being copied when the composite object of which they are a part is copied.
func (l *gcsGateway) CompleteMultipartUpload(bucket string, key string, uploadID string, uploadedParts []completePart) (ObjectInfo, error) {
	partZero := l.client.Bucket(bucket).Object(toGCSMultipartKey(key, uploadID, 0))

	partZeroAttrs, err := partZero.Attrs(l.ctx)
	if err != nil {
		return ObjectInfo{}, gcsToObjectError(traceError(err), bucket, key)
	}

	parts := make([]*storage.ObjectHandle, len(uploadedParts))
	for i, uploadedPart := range uploadedParts {
		// TODO: verify attrs / ETag
		parts[i] = l.client.Bucket(bucket).Object(toGCSMultipartKey(key, uploadID, uploadedPart.PartNumber))
	}

	if len(parts) > 32 {
		// we need to split up the compose of more than 32 parts
		// into subcomposes. This means that the first 32 parts will
		// compose to a composed-object-0, next parts to composed-object-1,
		// the final compose will compose composed-object* to 1.
		return ObjectInfo{}, NotSupported{}
	}

	dst := l.client.Bucket(bucket).Object(key)

	composer := dst.ComposerFrom(parts...)

	composer.ContentType = partZeroAttrs.ContentType
	composer.Metadata = partZeroAttrs.Metadata

	attrs, err := composer.Run(l.ctx)

	// cleanup, delete all parts
	for _, uploadedPart := range uploadedParts {
		l.client.Bucket(bucket).Object(toGCSMultipartKey(key, uploadID, uploadedPart.PartNumber)).Delete(l.ctx)
	}

	partZero.Delete(l.ctx)

	return fromGCSObjectInfo(attrs), gcsToObjectError(traceError(err), bucket, key)
}

// SetBucketPolicies - Set policy on bucket
func (l *gcsGateway) SetBucketPolicies(bucket string, policyInfo policy.BucketAccessPolicy) error {
	return NotSupported{}
}

// GetBucketPolicies - Get policy on bucket
func (l *gcsGateway) GetBucketPolicies(bucket string) (policy.BucketAccessPolicy, error) {
	return policy.BucketAccessPolicy{}, NotSupported{}
}

// DeleteBucketPolicies - Delete all policies on bucket
func (l *gcsGateway) DeleteBucketPolicies(bucket string) error {
	return NotSupported{}
}

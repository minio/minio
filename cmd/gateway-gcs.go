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
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash"
	"io"
	"math"
	"regexp"
	"strings"
	"time"

	"golang.org/x/oauth2/google"

	"cloud.google.com/go/storage"
	cloudresourcemanager "google.golang.org/api/cloudresourcemanager/v1"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"

	minio "github.com/minio/minio-go"
	"github.com/minio/minio-go/pkg/policy"
)

const (
	// gcsMinioMeta is used for multiparts. We have "minio.sys.temp" prefix so that
	// listing on the GCS lists this entry in the end. Also in the gateway
	// ListObjects we filter out this entry.
	gcsMinioPath = "minio.sys.temp/"
	// Path where multipart objects are saved.
	// If we change the backend format we will use a different url path like /multipart/v2
	// but we will not migrate old data.
	gcsMinioMultipartPathV1 = gcsMinioPath + "multipart/v1"
	// Multipart meta file.
	gcsMinioMultipartMeta = "gcs.json"
	// gcs.json version number
	gcsMultipartMetaCurrentVersion = "1"
	// token prefixed with GCS returned marker to differentiate
	// from user supplied marker.
	gcsTokenPrefix = "##minio"

	// maxComponents - maximum component object count to create a composite object.
	// Refer https://cloud.google.com/storage/docs/composite-objects
	maxComponents = 32

	// maxPartCount - maximum multipart parts GCS supports which is 32 x 32 = 1024.
	maxPartCount = 1024
)

// Stored in gcs.json - Contents of this file is not used anywhere. It can be
// used for debugging purposes.
type gcsMultipartMetaV1 struct {
	Version string `json:"version"` // Version number
	Bucket  string `json:"bucket"`  // Bucket name
	Object  string `json:"object"`  // Object name
}

// Returns name of the multipart meta object.
func gcsMultipartMetaName(uploadID string) string {
	return fmt.Sprintf("%s/%s/%s", gcsMinioMultipartPathV1, uploadID, gcsMinioMultipartMeta)
}

// Returns name of the part object.
func gcsMultipartDataName(uploadID, etag string) string {
	return fmt.Sprintf("%s/%s/%s", gcsMinioMultipartPathV1, uploadID, etag)
}

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

	googleAPIErr, ok := err.(*googleapi.Error)
	if !ok {
		// We don't interpret non Minio errors. As minio errors will
		// have StatusCode to help to convert to object errors.
		e.e = err
		return e
	}

	if len(googleAPIErr.Errors) == 0 {
		e.e = err
		return e
	}

	reason := googleAPIErr.Errors[0].Reason
	message := googleAPIErr.Errors[0].Message

	switch reason {
	case "required":
		// Anonymous users does not have storage.xyz access to project 123.
		fallthrough
	case "keyInvalid":
		fallthrough
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
		if object != "" {
			err = ObjectNotFound{
				Bucket: bucket,
				Object: object,
			}
			break
		}
		err = BucketNotFound{Bucket: bucket}
	case "conflict":
		if message == "You already own this bucket. Please select another name." {
			err = BucketAlreadyOwnedByYou{Bucket: bucket}
			break
		}
		if message == "Sorry, that name is not available. Please try a different one." {
			err = BucketAlreadyExists{Bucket: bucket}
			break
		}
		err = BucketNotEmpty{Bucket: bucket}
	default:
		err = fmt.Errorf("Unsupported error reason: %s", reason)
	}

	e.e = err
	return e
}

// gcsProjectIDRegex defines a valid gcs project id format
var gcsProjectIDRegex = regexp.MustCompile("^[a-z][a-z0-9-]{5,29}$")

// isValidGCSProjectIDFormat - checks if a given project id format is valid or not.
// Project IDs must start with a lowercase letter and can have lowercase ASCII letters,
// digits or hyphens. Project IDs must be between 6 and 30 characters.
// Ref: https://cloud.google.com/resource-manager/reference/rest/v1/projects#Project (projectId section)
func isValidGCSProjectIDFormat(projectID string) bool {
	// Checking projectID format
	return gcsProjectIDRegex.MatchString(projectID)
}

// checkGCSProjectID - checks if the project ID does really exist using resource manager API.
func checkGCSProjectID(ctx context.Context, projectID string) error {
	// Check if a project id associated to the current account does really exist
	resourceManagerClient, err := google.DefaultClient(ctx, cloudresourcemanager.CloudPlatformReadOnlyScope)
	if err != nil {
		return err
	}

	baseSvc, err := cloudresourcemanager.New(resourceManagerClient)
	if err != nil {
		return err
	}

	projectSvc := cloudresourcemanager.NewProjectsService(baseSvc)

	curPageToken := ""

	// Iterate over projects list result pages and immediately return nil when
	// the project ID is found.
	for {
		resp, err := projectSvc.List().PageToken(curPageToken).Context(ctx).Do()
		if err != nil {
			return fmt.Errorf("Error getting projects list: %s", err.Error())
		}

		for _, p := range resp.Projects {
			if p.ProjectId == projectID {
				return nil
			}
		}

		if resp.NextPageToken != "" {
			curPageToken = resp.NextPageToken
		} else {
			break
		}
	}

	return errGCSProjectIDNotFound
}

// gcsGateway - Implements gateway for Minio and GCS compatible object storage servers.
type gcsGateway struct {
	client     *storage.Client
	anonClient *minio.Core
	projectID  string
	ctx        context.Context
}

const googleStorageEndpoint = "storage.googleapis.com"

// newGCSGateway returns gcs gatewaylayer
func newGCSGateway(projectID string) (GatewayLayer, error) {
	ctx := context.Background()

	err := checkGCSProjectID(ctx, projectID)
	if err != nil {
		return nil, err
	}

	// Initialize a GCS client.
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, err
	}

	// Initialize a anonymous client with minio core APIs.
	anonClient, err := minio.NewCore(googleStorageEndpoint, "", "", true)
	if err != nil {
		return nil, err
	}

	return &gcsGateway{
		client:     client,
		projectID:  projectID,
		ctx:        ctx,
		anonClient: anonClient,
	}, nil
}

// Shutdown - save any gateway metadata to disk
// if necessary and reload upon next restart.
func (l *gcsGateway) Shutdown() error {
	return nil
}

// StorageInfo - Not relevant to GCS backend.
func (l *gcsGateway) StorageInfo() StorageInfo {
	return StorageInfo{}
}

// MakeBucketWithLocation - Create a new container on GCS backend.
func (l *gcsGateway) MakeBucketWithLocation(bucket, location string) error {
	bkt := l.client.Bucket(bucket)

	// we'll default to the us multi-region in case of us-east-1
	if location == "us-east-1" {
		location = "us"
	}

	err := bkt.Create(l.ctx, l.projectID, &storage.BucketAttrs{
		Location: location,
	})

	return gcsToObjectError(traceError(err), bucket)
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
			return []BucketInfo{}, gcsToObjectError(traceError(err))
		}

		b = append(b, BucketInfo{
			Name:    attrs.Name,
			Created: attrs.Created,
		})
	}

	return b, nil
}

// DeleteBucket delete a bucket on GCS.
func (l *gcsGateway) DeleteBucket(bucket string) error {
	itObject := l.client.Bucket(bucket).Objects(l.ctx, &storage.Query{Delimiter: slashSeparator, Versions: false})
	// We list the bucket and if we find any objects we return BucketNotEmpty error. If we
	// find only "minio.sys.temp/" then we remove it before deleting the bucket.
	gcsMinioPathFound := false
	nonGCSMinioPathFound := false
	for {
		objAttrs, err := itObject.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return gcsToObjectError(traceError(err))
		}
		if objAttrs.Prefix == gcsMinioPath {
			gcsMinioPathFound = true
			continue
		}
		nonGCSMinioPathFound = true
		break
	}
	if nonGCSMinioPathFound {
		return gcsToObjectError(traceError(BucketNotEmpty{}))
	}
	if gcsMinioPathFound {
		// Remove minio.sys.temp before deleting the bucket.
		itObject = l.client.Bucket(bucket).Objects(l.ctx, &storage.Query{Versions: false, Prefix: gcsMinioPath})
		for {
			objAttrs, err := itObject.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				return gcsToObjectError(traceError(err))
			}
			err = l.client.Bucket(bucket).Object(objAttrs.Name).Delete(l.ctx)
			if err != nil {
				return gcsToObjectError(traceError(err))
			}
		}
	}
	err := l.client.Bucket(bucket).Delete(l.ctx)
	return gcsToObjectError(traceError(err), bucket)
}

func toGCSPageToken(name string) string {
	length := uint16(len(name))

	b := []byte{
		0xa,
		byte(length & 0xFF),
	}

	length = length >> 7
	if length > 0 {
		b = append(b, byte(length&0xFF))
	}

	b = append(b, []byte(name)...)

	return base64.StdEncoding.EncodeToString(b)
}

// Returns true if marker was returned by GCS, i.e prefixed with
// ##minio by minio gcs gateway.
func isGCSMarker(marker string) bool {
	return strings.HasPrefix(marker, gcsTokenPrefix)
}

// ListObjects - lists all blobs in GCS bucket filtered by prefix
func (l *gcsGateway) ListObjects(bucket string, prefix string, marker string, delimiter string, maxKeys int) (ListObjectsInfo, error) {
	it := l.client.Bucket(bucket).Objects(l.ctx, &storage.Query{Delimiter: delimiter, Prefix: prefix, Versions: false})

	isTruncated := false
	nextMarker := ""
	prefixes := []string{}

	// To accommodate S3-compatible applications using
	// ListObjectsV1 to use object keys as markers to control the
	// listing of objects, we use the following encoding scheme to
	// distinguish between GCS continuation tokens and application
	// supplied markers.
	//
	// - NextMarker in ListObjectsV1 response is constructed by
	//   prefixing "##minio" to the GCS continuation token,
	//   e.g, "##minioCgRvYmoz"
	//
	// - Application supplied markers are used as-is to list
	//   object keys that appear after it in the lexicographical order.

	// If application is using GCS continuation token we should
	// strip the gcsTokenPrefix we added.
	gcsMarker := isGCSMarker(marker)
	if gcsMarker {
		it.PageInfo().Token = strings.TrimPrefix(marker, gcsTokenPrefix)
	}

	it.PageInfo().MaxSize = maxKeys

	objects := []ObjectInfo{}
	for {
		if len(objects) >= maxKeys {
			// check if there is one next object and
			// if that one next object is our hidden
			// metadata folder, then just break
			// otherwise we've truncated the output
			attrs, _ := it.Next()
			if attrs != nil && attrs.Prefix == gcsMinioPath {
				break
			}

			isTruncated = true
			break
		}

		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return ListObjectsInfo{}, gcsToObjectError(traceError(err), bucket, prefix)
		}

		nextMarker = toGCSPageToken(attrs.Name)

		if attrs.Prefix == gcsMinioPath {
			// We don't return our metadata prefix.
			continue
		}
		if !strings.HasPrefix(prefix, gcsMinioPath) {
			// If client lists outside gcsMinioPath then we filter out gcsMinioPath/* entries.
			// But if the client lists inside gcsMinioPath then we return the entries in gcsMinioPath/
			// which will be helpful to observe the "directory structure" for debugging purposes.
			if strings.HasPrefix(attrs.Prefix, gcsMinioPath) ||
				strings.HasPrefix(attrs.Name, gcsMinioPath) {
				continue
			}
		}
		if attrs.Prefix != "" {
			prefixes = append(prefixes, attrs.Prefix)
			continue
		}
		if !gcsMarker && attrs.Name <= marker {
			// if user supplied a marker don't append
			// objects until we reach marker (and skip it).
			continue
		}

		objects = append(objects, ObjectInfo{
			Name:            attrs.Name,
			Bucket:          attrs.Bucket,
			ModTime:         attrs.Updated,
			Size:            attrs.Size,
			ETag:            fmt.Sprintf("%d", attrs.CRC32C),
			UserDefined:     attrs.Metadata,
			ContentType:     attrs.ContentType,
			ContentEncoding: attrs.ContentEncoding,
		})
	}

	return ListObjectsInfo{
		IsTruncated: isTruncated,
		NextMarker:  gcsTokenPrefix + nextMarker,
		Prefixes:    prefixes,
		Objects:     objects,
	}, nil
}

// ListObjectsV2 - lists all blobs in GCS bucket filtered by prefix
func (l *gcsGateway) ListObjectsV2(bucket, prefix, continuationToken string, fetchOwner bool, delimiter string, maxKeys int) (ListObjectsV2Info, error) {
	it := l.client.Bucket(bucket).Objects(l.ctx, &storage.Query{Delimiter: delimiter, Prefix: prefix, Versions: false})

	isTruncated := false
	it.PageInfo().MaxSize = maxKeys

	if continuationToken != "" {
		// If client sends continuationToken, set it
		it.PageInfo().Token = continuationToken
	} else {
		// else set the continuationToken to return
		continuationToken = it.PageInfo().Token
		if continuationToken != "" {
			// If GCS SDK sets continuationToken, it means there are more than maxKeys in the current page
			// and the response will be truncated
			isTruncated = true
		}
	}

	prefixes := []string{}
	objects := []ObjectInfo{}

	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}

		if err != nil {
			return ListObjectsV2Info{}, gcsToObjectError(traceError(err), bucket, prefix)
		}

		if attrs.Prefix == gcsMinioPath {
			// We don't return our metadata prefix.
			continue
		}
		if !strings.HasPrefix(prefix, gcsMinioPath) {
			// If client lists outside gcsMinioPath then we filter out gcsMinioPath/* entries.
			// But if the client lists inside gcsMinioPath then we return the entries in gcsMinioPath/
			// which will be helpful to observe the "directory structure" for debugging purposes.
			if strings.HasPrefix(attrs.Prefix, gcsMinioPath) ||
				strings.HasPrefix(attrs.Name, gcsMinioPath) {
				continue
			}
		}

		if attrs.Prefix != "" {
			prefixes = append(prefixes, attrs.Prefix)
			continue
		}

		objects = append(objects, fromGCSAttrsToObjectInfo(attrs))
	}

	return ListObjectsV2Info{
		IsTruncated:           isTruncated,
		ContinuationToken:     continuationToken,
		NextContinuationToken: continuationToken,
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

// fromMinioClientListBucketResultToV2Info converts minio ListBucketResult to ListObjectsV2Info
func fromMinioClientListBucketResultToV2Info(bucket string, result minio.ListBucketResult) ListObjectsV2Info {
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

		ContinuationToken:     result.Marker,
		NextContinuationToken: result.NextMarker,
	}
}

// fromGCSAttrsToObjectInfo converts GCS BucketAttrs to gateway ObjectInfo
func fromGCSAttrsToObjectInfo(attrs *storage.ObjectAttrs) ObjectInfo {
	// All google cloud storage objects have a CRC32c hash, whereas composite objects may not have a MD5 hash
	// Refer https://cloud.google.com/storage/docs/hashes-etags. Use CRC32C for ETag
	return ObjectInfo{
		Name:            attrs.Name,
		Bucket:          attrs.Bucket,
		ModTime:         attrs.Updated,
		Size:            attrs.Size,
		ETag:            fmt.Sprintf("%d", attrs.CRC32C),
		UserDefined:     attrs.Metadata,
		ContentType:     attrs.ContentType,
		ContentEncoding: attrs.ContentEncoding,
	}
}

// GetObjectInfo - reads object info and replies back ObjectInfo
func (l *gcsGateway) GetObjectInfo(bucket string, object string) (ObjectInfo, error) {
	// if we want to mimic S3 behavior exactly, we need to verify if bucket exists first,
	// otherwise gcs will just return object not exist in case of non-existing bucket
	if _, err := l.client.Bucket(bucket).Attrs(l.ctx); err != nil {
		return ObjectInfo{}, gcsToObjectError(traceError(err), bucket)
	}

	attrs, err := l.client.Bucket(bucket).Object(object).Attrs(l.ctx)

	if err != nil {
		return ObjectInfo{}, gcsToObjectError(traceError(err), bucket, object)
	}
	objInfo := fromGCSAttrsToObjectInfo(attrs)
	objInfo.ETag = fmt.Sprintf("%d", attrs.CRC32C)

	return objInfo, nil
}

// PutObject - Create a new object with the incoming data,
func (l *gcsGateway) PutObject(bucket string, key string, size int64, data io.Reader, metadata map[string]string, sha256sum string) (ObjectInfo, error) {
	// if we want to mimic S3 behavior exactly, we need to verify if bucket exists first,
	// otherwise gcs will just return object not exist in case of non-existing bucket
	if _, err := l.client.Bucket(bucket).Attrs(l.ctx); err != nil {
		return ObjectInfo{}, gcsToObjectError(traceError(err), bucket)
	}

	reader := data

	var sha256Writer hash.Hash
	if sha256sum != "" {
		sha256Writer = sha256.New()
		reader = io.TeeReader(data, sha256Writer)
	}

	md5sum := metadata["etag"]
	delete(metadata, "etag")

	object := l.client.Bucket(bucket).Object(key)

	w := object.NewWriter(l.ctx)

	w.ContentType = metadata["content-type"]
	w.ContentEncoding = metadata["content-encoding"]
	if md5sum != "" {
		var err error
		w.MD5, err = hex.DecodeString(md5sum)
		if err != nil {
			return ObjectInfo{}, gcsToObjectError(traceError(err), bucket, key)
		}
	}

	w.Metadata = metadata

	_, err := io.Copy(w, reader)
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
		if hex.EncodeToString(sha256Writer.Sum(nil)) != sha256sum {
			object.Delete(l.ctx)
			return ObjectInfo{}, traceError(SHA256Mismatch{})
		}
	}

	return fromGCSAttrsToObjectInfo(attrs), nil
}

// CopyObject - Copies a blob from source container to destination container.
func (l *gcsGateway) CopyObject(srcBucket string, srcObject string, destBucket string, destObject string, metadata map[string]string) (ObjectInfo, error) {
	src := l.client.Bucket(srcBucket).Object(srcObject)
	dst := l.client.Bucket(destBucket).Object(destObject)

	attrs, err := dst.CopierFrom(src).Run(l.ctx)
	if err != nil {
		return ObjectInfo{}, gcsToObjectError(traceError(err), destBucket, destObject)
	}

	return fromGCSAttrsToObjectInfo(attrs), nil
}

// DeleteObject - Deletes a blob in bucket
func (l *gcsGateway) DeleteObject(bucket string, object string) error {
	err := l.client.Bucket(bucket).Object(object).Delete(l.ctx)
	if err != nil {
		return gcsToObjectError(traceError(err), bucket, object)
	}

	return nil
}

// NewMultipartUpload - upload object in multiple parts
func (l *gcsGateway) NewMultipartUpload(bucket string, key string, metadata map[string]string) (uploadID string, err error) {
	// generate new uploadid
	uploadID = mustGetUUID()

	// generate name for part zero
	meta := gcsMultipartMetaName(uploadID)

	w := l.client.Bucket(bucket).Object(meta).NewWriter(l.ctx)
	w.ContentType = metadata["content-type"]
	w.ContentEncoding = metadata["content-encoding"]
	w.Metadata = metadata

	content, err := json.Marshal(gcsMultipartMetaV1{gcsMultipartMetaCurrentVersion, bucket, key})
	if err != nil {
		return "", gcsToObjectError(traceError(err), bucket, key)
	}
	if _, err = w.Write(content); err != nil {
		return "", gcsToObjectError(traceError(err), bucket, key)
	}
	if err = w.Close(); err != nil {
		return "", gcsToObjectError(traceError(err), bucket, key)
	}

	return uploadID, nil
}

// ListMultipartUploads - lists all multipart uploads.
func (l *gcsGateway) ListMultipartUploads(bucket string, prefix string, keyMarker string, uploadIDMarker string, delimiter string, maxUploads int) (ListMultipartsInfo, error) {
	return ListMultipartsInfo{
		KeyMarker:      keyMarker,
		UploadIDMarker: uploadIDMarker,
		MaxUploads:     maxUploads,
		Prefix:         prefix,
		Delimiter:      delimiter,
	}, nil
}

// CopyObjectPart - copy part of object to other bucket and object
func (l *gcsGateway) CopyObjectPart(srcBucket string, srcObject string, destBucket string, destObject string, uploadID string, partID int, startOffset int64, length int64) (info PartInfo, err error) {
	return PartInfo{}, traceError(NotSupported{})
}

// PutObjectPart puts a part of object in bucket
func (l *gcsGateway) PutObjectPart(bucket string, key string, uploadID string, partID int, size int64, data io.Reader, md5Hex string, sha256sum string) (PartInfo, error) {
	meta := gcsMultipartMetaName(uploadID)
	object := l.client.Bucket(bucket).Object(meta)

	_, err := object.Attrs(l.ctx)
	if err != nil {
		return PartInfo{}, gcsToObjectError(traceError(err), bucket, key)
	}

	var sha256Writer hash.Hash

	// Generate random ETag.
	etag := getMD5Hash([]byte(mustGetUUID()))

	reader := data

	if sha256sum != "" {
		sha256Writer = sha256.New()
		reader = io.TeeReader(data, sha256Writer)
	}

	dataName := gcsMultipartDataName(uploadID, etag)

	object = l.client.Bucket(bucket).Object(dataName)

	w := object.NewWriter(l.ctx)
	// Disable "chunked" uploading in GCS client. If enabled, it can cause a corner case
	// where it tries to upload 0 bytes in the last chunk and get error from server.
	w.ChunkSize = 0
	if md5Hex != "" {
		w.MD5, err = hex.DecodeString(md5Hex)
		if err != nil {
			return PartInfo{}, gcsToObjectError(traceError(err), bucket, key)
		}
	}
	_, err = io.Copy(w, reader)
	if err != nil {
		return PartInfo{}, gcsToObjectError(traceError(err), bucket, key)
	}

	err = w.Close()
	if err != nil {
		return PartInfo{}, gcsToObjectError(traceError(err), bucket, key)
	}

	if sha256sum != "" {
		if hex.EncodeToString(sha256Writer.Sum(nil)) != sha256sum {
			object.Delete(l.ctx)
			return PartInfo{}, traceError(SHA256Mismatch{})
		}
	}

	return PartInfo{
		PartNumber:   partID,
		ETag:         etag,
		LastModified: time.Now().UTC(),
		Size:         size,
	}, nil
}

// ListObjectParts returns all object parts for specified object in specified bucket
func (l *gcsGateway) ListObjectParts(bucket string, key string, uploadID string, partNumberMarker int, maxParts int) (ListPartsInfo, error) {
	meta := gcsMultipartMetaName(uploadID)
	object := l.client.Bucket(bucket).Object(meta)

	_, err := object.Attrs(l.ctx)
	if err != nil {
		return ListPartsInfo{}, gcsToObjectError(traceError(err), bucket, key)
	}

	return ListPartsInfo{}, nil
}

// Called by AbortMultipartUpload and CompleteMultipartUpload for cleaning up.
func (l *gcsGateway) cleanupMultipartUpload(bucket, key, uploadID string) error {
	meta := gcsMultipartMetaName(uploadID)
	object := l.client.Bucket(bucket).Object(meta)

	_, err := object.Attrs(l.ctx)
	if err != nil {
		return gcsToObjectError(traceError(err), bucket, key)
	}

	prefix := fmt.Sprintf("%s/%s/", gcsMinioMultipartPathV1, uploadID)

	// iterate through all parts and delete them
	it := l.client.Bucket(bucket).Objects(l.ctx, &storage.Query{Prefix: prefix, Versions: false})

	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return gcsToObjectError(traceError(err), bucket, key)
		}

		object := l.client.Bucket(bucket).Object(attrs.Name)
		// Ignore the error as parallel AbortMultipartUpload might have deleted it.
		object.Delete(l.ctx)
	}

	return nil
}

// AbortMultipartUpload aborts a ongoing multipart upload
func (l *gcsGateway) AbortMultipartUpload(bucket string, key string, uploadID string) error {
	return l.cleanupMultipartUpload(bucket, key, uploadID)
}

// CompleteMultipartUpload completes ongoing multipart upload and finalizes object
// Note that there is a limit (currently 32) to the number of components that can be composed in a single operation.
// There is a limit (currently 1024) to the total number of components for a given composite object. This means you can append to each object at most 1023 times.
// There is a per-project rate limit (currently 200) to the number of components you can compose per second. This rate counts both the components being appended to a composite object as well as the components being copied when the composite object of which they are a part is copied.
func (l *gcsGateway) CompleteMultipartUpload(bucket string, key string, uploadID string, uploadedParts []completePart) (ObjectInfo, error) {
	meta := gcsMultipartMetaName(uploadID)
	object := l.client.Bucket(bucket).Object(meta)

	partZeroAttrs, err := object.Attrs(l.ctx)
	if err != nil {
		return ObjectInfo{}, gcsToObjectError(traceError(err), bucket, key)
	}
	r, err := object.NewReader(l.ctx)
	if err != nil {
		return ObjectInfo{}, gcsToObjectError(traceError(err), bucket, key)
	}

	// Check version compatibility of the meta file before compose()
	multipartMeta := gcsMultipartMetaV1{}
	decoder := json.NewDecoder(r)
	err = decoder.Decode(&multipartMeta)
	if err != nil {
		return ObjectInfo{}, gcsToObjectError(traceError(err), bucket, key)
	}
	if multipartMeta.Version != gcsMultipartMetaCurrentVersion {
		return ObjectInfo{}, gcsToObjectError(traceError(errFormatNotSupported), bucket, key)
	}

	parts := make([]*storage.ObjectHandle, len(uploadedParts))
	for i, uploadedPart := range uploadedParts {
		parts[i] = l.client.Bucket(bucket).Object(gcsMultipartDataName(uploadID, uploadedPart.ETag))
	}

	if len(parts) > maxPartCount {
		return ObjectInfo{}, traceError(NotSupported{})
	}

	composeCount := int(math.Ceil(float64(len(parts)) / float64(maxComponents)))
	if composeCount > 1 {
		// Create composes of every 32 parts.
		composeParts := make([]*storage.ObjectHandle, composeCount)
		for i := 0; i < composeCount; i++ {
			// Create 'composed-object-N' using next 32 parts.
			composeName := fmt.Sprintf("composed-object-%d", i)
			composeParts[i] = l.client.Bucket(bucket).Object(gcsMultipartDataName(uploadID, composeName))

			start := i * maxComponents
			end := start + maxComponents
			if end > len(parts) {
				end = len(parts)
			}

			composer := composeParts[i].ComposerFrom(parts[start:end]...)
			composer.ContentType = partZeroAttrs.ContentType
			composer.Metadata = partZeroAttrs.Metadata

			_, err = composer.Run(l.ctx)
			if err != nil {
				return ObjectInfo{}, gcsToObjectError(traceError(err), bucket, key)
			}
		}

		// As composes are successfully created, final object needs to be created using composes.
		parts = composeParts
	}

	dst := l.client.Bucket(bucket).Object(key)

	composer := dst.ComposerFrom(parts...)

	composer.ContentType = partZeroAttrs.ContentType
	composer.Metadata = partZeroAttrs.Metadata

	attrs, err := composer.Run(l.ctx)
	if err != nil {
		return ObjectInfo{}, gcsToObjectError(traceError(err), bucket, key)
	}
	if err = l.cleanupMultipartUpload(bucket, key, uploadID); err != nil {
		return ObjectInfo{}, gcsToObjectError(traceError(err), bucket, key)
	}
	return fromGCSAttrsToObjectInfo(attrs), nil
}

// SetBucketPolicies - Set policy on bucket
func (l *gcsGateway) SetBucketPolicies(bucket string, policyInfo policy.BucketAccessPolicy) error {
	var policies []BucketAccessPolicy

	for prefix, policy := range policy.GetPolicies(policyInfo.Statements, bucket) {
		policies = append(policies, BucketAccessPolicy{
			Prefix: prefix,
			Policy: policy,
		})
	}

	prefix := bucket + "/*" // For all objects inside the bucket.

	if len(policies) != 1 {
		return traceError(NotImplemented{})
	}
	if policies[0].Prefix != prefix {
		return traceError(NotImplemented{})
	}

	acl := l.client.Bucket(bucket).ACL()
	if policies[0].Policy == policy.BucketPolicyNone {
		if err := acl.Delete(l.ctx, storage.AllUsers); err != nil {
			return gcsToObjectError(traceError(err), bucket)
		}
		return nil
	}

	var role storage.ACLRole
	switch policies[0].Policy {
	case policy.BucketPolicyReadOnly:
		role = storage.RoleReader
	case policy.BucketPolicyWriteOnly:
		role = storage.RoleWriter
	default:
		return traceError(NotImplemented{})
	}

	if err := acl.Set(l.ctx, storage.AllUsers, role); err != nil {
		return gcsToObjectError(traceError(err), bucket)
	}

	return nil
}

// GetBucketPolicies - Get policy on bucket
func (l *gcsGateway) GetBucketPolicies(bucket string) (policy.BucketAccessPolicy, error) {
	acl := l.client.Bucket(bucket).ACL()

	rules, err := acl.List(l.ctx)
	if err != nil {
		return policy.BucketAccessPolicy{}, gcsToObjectError(traceError(err), bucket)
	}

	policyInfo := policy.BucketAccessPolicy{Version: "2012-10-17"}

	for _, r := range rules {
		if r.Entity != storage.AllUsers || r.Role == storage.RoleOwner {
			continue
		}
		switch r.Role {
		case storage.RoleReader:
			policyInfo.Statements = policy.SetPolicy(policyInfo.Statements, policy.BucketPolicyReadOnly, bucket, "")
		case storage.RoleWriter:
			policyInfo.Statements = policy.SetPolicy(policyInfo.Statements, policy.BucketPolicyWriteOnly, bucket, "")
		}
	}

	return policyInfo, nil
}

// DeleteBucketPolicies - Delete all policies on bucket
func (l *gcsGateway) DeleteBucketPolicies(bucket string) error {
	acl := l.client.Bucket(bucket).ACL()

	// This only removes the storage.AllUsers policies
	if err := acl.Delete(l.ctx, storage.AllUsers); err != nil {
		return gcsToObjectError(traceError(err), bucket)
	}

	return nil
}

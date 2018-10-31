/*
 * Minio Cloud Storage, (C) 2018 Minio, Inc.
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
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/minio/minio-go/pkg/encrypt"
	minio "github.com/minio/minio/cmd"
	"github.com/minio/sio"

	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/hash"
)

const (
	// name of custom multipart metadata file for s3 backend.
	gwdareMetaJSON string = "dare.meta"

	// name of temporary per part metadata file
	gwpartMetaJSON string = "part.meta"
	// custom multipart files are stored under the defaultMinioGWPrefix
	defaultMinioGWPrefix     = ".minio"
	defaultGWContentFileName = "data"
	slashSeparator           = "/"
)

// s3EncObjects is a wrapper around s3Objects and implements gateway calls for
// custom large objects encrypted at the gateway
type s3EncObjects struct {
	s3Objects
}

/*
 NOTE:
 Custom gateway encrypted objects uploaded with single PUT operation are stored on backend as follows:
	 obj/.minio/data   <= encrypted content
	 obj/.minio/dare.meta  <= metadata
 Custom gateway encrypted objects uploaded with multipart upload operation are stored on backend as follows:
		obj/.minio/dare.meta  <= metadata
		obj/.minio/uploadId/1 <= encrypted part 1
		obj/.minio/uploadId/2 ...
		obj/.minio/uploadId/3
	When a multipart upload is in progress, part metadata is stored in obj/.minio/uploadID/1/part-etag/part.meta
	where part-etag is etag of the actual part content file on s3 backend. This metadata file will be cleaned up
	after the upload completes.
*/

// ListObjects lists all blobs in S3 bucket filtered by prefix
func (l *s3EncObjects) ListObjects(ctx context.Context, bucket string, prefix string, marker string, delimiter string, maxKeys int) (loi minio.ListObjectsInfo, e error) {
	if len(minio.GlobalGatewaySSE) > 0 {
		var continuationToken, startAfter string
		res, err := l.ListObjectsV2(ctx, bucket, prefix, continuationToken, delimiter, maxKeys, false, startAfter)
		if err != nil {
			return loi, err
		}
		loi.IsTruncated = res.IsTruncated
		loi.NextMarker = res.NextContinuationToken
		loi.Objects = res.Objects
		loi.Prefixes = res.Prefixes
		return loi, nil
	}
	return l.s3Objects.ListObjects(ctx, bucket, prefix, marker, delimiter, maxKeys)
}

// ListObjectsV2 lists all blobs in S3 bucket filtered by prefix
func (l *s3EncObjects) ListObjectsV2(ctx context.Context, bucket, prefix, continuationToken, delimiter string, maxKeys int, fetchOwner bool, startAfter string) (loi minio.ListObjectsV2Info, e error) {
	if len(minio.GlobalGatewaySSE) == 0 {
		return l.s3Objects.ListObjectsV2(ctx, bucket, prefix, continuationToken, delimiter, maxKeys, fetchOwner, startAfter)
	}
	var objects []minio.ObjectInfo
	var prefixes []string
	var isTruncated bool
	// filter out objects that contain a .minio prefix, but is not a dare.meta metadata file.
	for {
		loi, e = l.s3Objects.ListObjectsV2(ctx, bucket, prefix, continuationToken, delimiter, 1000, fetchOwner, startAfter)
		if e != nil {
			return loi, minio.ErrorRespToObjectError(e, bucket)
		}
		for _, obj := range loi.Objects {
			startAfter = obj.Name
			continuationToken = loi.NextContinuationToken
			isTruncated = loi.IsTruncated
			// skip parts objects from listing
			if strings.Contains(obj.Name, defaultMinioGWPrefix) && !strings.HasSuffix(obj.Name, gwdareMetaJSON) {
				continue
			}
			if !isGWObject(obj.Name) {
				continue
			}
			// get objectname and ObjectInfo from the custom metadata file
			if strings.HasSuffix(obj.Name, gwdareMetaJSON) {
				objSlice := strings.Split(obj.Name, slashSeparator+defaultMinioGWPrefix)
				gwMeta, e := l.getGWMetadata(ctx, bucket, getDareMetaPath(objSlice[0]))
				if e != nil {
					continue
				}
				prefixSlice := strings.Split(obj.Name, slashSeparator+defaultMinioGWPrefix+slashSeparator)
				if len(prefixSlice) >= 1 {
					oInfo := gwMeta.ToObjectInfo(bucket, prefixSlice[0][:])
					objects = append(objects, oInfo)
				}
				continue
			}
			objects = append(objects, obj)
			if len(objects) > maxKeys {
				break
			}
		}
		for _, p := range loi.Prefixes {
			objName := strings.TrimSuffix(p, slashSeparator)
			gm, err := l.getGWMetadata(ctx, bucket, getDareMetaPath(objName))
			// if prefix is actually a custom multi-part object, append it to objects
			if err == nil {
				objects = append(objects, gm.ToObjectInfo(bucket, objName))
				continue
			}
			isPrefix := l.isPrefix(ctx, bucket, p, fetchOwner, startAfter)
			if isPrefix {
				prefixes = append(prefixes, p)
			}
		}
		if (len(objects) > maxKeys) || !loi.IsTruncated {
			break
		}
	}

	loi.IsTruncated = isTruncated
	loi.ContinuationToken = continuationToken
	loi.Objects = make([]minio.ObjectInfo, 0)
	loi.Prefixes = make([]string, 0)

	for _, obj := range objects {
		loi.NextContinuationToken = obj.Name
		loi.Objects = append(loi.Objects, obj)
	}
	for _, pfx := range prefixes {
		if pfx != prefix {
			loi.Prefixes = append(loi.Prefixes, pfx)
		}
	}
	return loi, nil
}

// isGWObject returns true if it is a custom object
func isGWObject(objName string) bool {
	pfxSlice := strings.Split(objName, slashSeparator)
	var i1, i2 int
	for i := len(pfxSlice) - 1; i >= 0; i-- {
		p := pfxSlice[i]
		if p == defaultMinioGWPrefix {
			i1 = i
		}
		if p == gwdareMetaJSON {
			i2 = i
		}
		if i1 > 0 && i2 > 0 {
			break
		}
	}
	// incomplete uploads would have a uploadID between defaultMinioGWPrefix and gwdareMetaJSON
	return i2 > 0 && i1 > 0 && i2-i1 == 1
}

// isPrefix returns true if prefix exists and is not an incomplete multipart upload entry
func (l *s3EncObjects) isPrefix(ctx context.Context, bucket, prefix string, fetchOwner bool, startAfter string) bool {
	var continuationToken, delimiter string

	for {
		loi, e := l.s3Objects.ListObjectsV2(ctx, bucket, prefix, continuationToken, delimiter, 1000, fetchOwner, startAfter)
		if e != nil {
			return false
		}
		for _, obj := range loi.Objects {
			if !strings.HasSuffix(obj.Name, gwdareMetaJSON) {
				continue
			}
			if isGWObject(obj.Name) {
				return true
			}
		}

		continuationToken = loi.NextContinuationToken
		if !loi.IsTruncated {
			break
		}
	}
	return false
}

// shouldSetSSEHeaders returns true if sse mode specifies
// backend encryption
func shouldSetSSEHeaders() bool {
	for _, v := range minio.GlobalGatewaySSEMode {
		if v == minio.GatewaySSEBackendEncrypt {
			return true
		}
	}
	return false
}

// GetObject reads an object from S3. Supports additional
// parameters like offset and length which are synonymous with
// HTTP Range requests.
// In the case of multi-part uploads that were encrypted at the gateway, the objects
// are stored in a custom format at the backend with each part as an individual object
// and piped to the writer.
func (l *s3EncObjects) GetObject(ctx context.Context, bucket string, key string, startOffset int64, length int64, writer io.Writer, etag string, opts minio.ObjectOptions) error {
	var o minio.ObjectOptions
	if shouldSetSSEHeaders() {
		o = opts
	}
	// pass through encryption
	if len(minio.GlobalGatewaySSE) == 0 {
		return l.s3Objects.GetObject(ctx, bucket, key, startOffset, length, writer, etag, o)
	}
	dmeta, err := l.getGWMetadata(ctx, bucket, getDareMetaPath(key))
	if err != nil {
		// unencrypted content
		return l.s3Objects.GetObject(ctx, bucket, key, startOffset, length, writer, etag, o)
	}

	if len(dmeta.Parts) == 0 {
		// custom gateway encrypted objects uploaded with single PUT operation
		return l.s3Objects.GetObject(ctx, bucket, getGWContentPath(key), startOffset, length, writer, etag, o)
	}

	// handle custom multipart gateway encrypted objects
	var partStartIndex int
	var partStartOffset = startOffset

	// Skip parts until final offset maps to a particular part offset.
	for i, part := range dmeta.Parts {
		decryptedSize, err := sio.DecryptedSize(uint64(part.Size))
		if err != nil {
			return err
		}
		partStartIndex = i

		// Offset is smaller than size we have reached the
		// proper part offset, break out we start from
		// this part index.
		if partStartOffset < int64(decryptedSize) {
			break
		}
		// Continue to look for next part.
		partStartOffset -= int64(decryptedSize)
	}
	startSeqNum := partStartOffset / minio.SSEDAREPackageBlockSize
	partEncRelOffset := int64(startSeqNum) * (minio.SSEDAREPackageBlockSize + minio.SSEDAREPackageMetaSize)

	var size int64
	// concatenate parts stored as separate objects into writer
	for i, part := range dmeta.Parts {
		//skip parts before start offset
		if i < partStartIndex {
			continue
		}
		pInfo, err := l.s3Objects.GetObjectInfo(ctx, bucket, part.Name, o)
		if err != nil {
			logger.LogIf(ctx, err)
			return minio.ObjectNotFound{
				Bucket: bucket,
				Object: key,
			}
		}
		if o.GetDecryptedETagFn != nil && pInfo.ETag != o.GetDecryptedETagFn(part.ETag) {
			logger.LogIf(ctx, err)
			return minio.ObjectNotFound{
				Bucket: bucket,
				Object: key,
			}
		}

		partLength := pInfo.Size - partEncRelOffset
		size += partLength
		if size > length {
			partLength -= (size - length)
		}

		pipeReader, pipeWriter := io.Pipe()

		var reader io.Reader = pipeReader
		pInfo.Reader, err = hash.NewReader(reader, partLength, "", "", pInfo.Size)
		pInfo.Writer = pipeWriter

		go func(pInfo minio.ObjectInfo) {
			if gerr := l.s3Objects.GetObject(ctx, bucket, pInfo.Name, partEncRelOffset, partLength, pInfo.Writer, pInfo.ETag, o); gerr != nil {
				if gerr = pInfo.Writer.Close(); gerr != nil {
					logger.LogIf(ctx, gerr)
					return
				}
			}
		}(pInfo)
		if pInfo.Reader == nil {
			return nil
		}
		_, err = io.Copy(writer, pInfo.Reader)
		if err != nil {
			logger.LogIf(ctx, err)
			return minio.ErrorRespToObjectError(err)
		}
		partStartIndex++
		partEncRelOffset = 0
	}
	return nil
}

func (l *s3EncObjects) isGWEncrypted(ctx context.Context, bucket, object string) bool {
	_, err := l.s3Objects.GetObjectInfo(ctx, bucket, getDareMetaPath(object), minio.ObjectOptions{})
	return err == nil
}

// getDaremetadata fetches dare.meta from s3 backend and marshals into a structured format.
func (l *s3EncObjects) getGWMetadata(ctx context.Context, bucket, metaFileName string) (m gwMetaV1, err error) {
	oi, err1 := l.s3Objects.GetObjectInfo(ctx, bucket, metaFileName, minio.ObjectOptions{})
	if err1 != nil {
		return m, err1
	}
	var buffer bytes.Buffer
	err = l.s3Objects.GetObject(ctx, bucket, metaFileName, 0, oi.Size, &buffer, oi.ETag, minio.ObjectOptions{})
	if err != nil {
		return m, err
	}
	return readGWMetadata(ctx, buffer)
}

// writes dare metadata to the s3 backend
func (l *s3EncObjects) writeGWMetadata(ctx context.Context, bucket, metaFileName string, m gwMetaV1, o minio.ObjectOptions) error {
	hashReader, err := getGWMetadata(ctx, bucket, metaFileName, m)
	if err != nil {
		logger.LogIf(ctx, err)
		return err
	}
	_, err = l.s3Objects.PutObject(ctx, bucket, metaFileName, minio.NewPutObjectReader(hashReader), map[string]string{}, o)
	return err
}
func getTmpDareMetaPath(object, uploadID string) string {
	return path.Join(getGWMetaPath(object), uploadID, gwdareMetaJSON)
}
func getDareMetaPath(object string) string {
	return path.Join(getGWMetaPath(object), gwdareMetaJSON)
}
func getPartMetaPath(object, uploadID string, partID int, partETag string) string {
	return path.Join(object, defaultMinioGWPrefix, uploadID, strconv.Itoa(partID), partETag, gwpartMetaJSON)
}

// deletes the custom dare metadata file saved at the backend
func (l *s3EncObjects) deleteGWMetadata(ctx context.Context, bucket, metaFileName string) error {
	return l.s3Objects.DeleteObject(ctx, bucket, metaFileName)
}

// GetObjectNInfo - returns object info and locked object ReadCloser
func (l *s3EncObjects) GetObjectNInfo(ctx context.Context, bucket, object string, rs *minio.HTTPRangeSpec, h http.Header, lockType minio.LockType, o minio.ObjectOptions) (gr *minio.GetObjectReader, err error) {
	var opts minio.ObjectOptions
	if shouldSetSSEHeaders() {
		opts = o
	}
	if len(minio.GlobalGatewaySSE) == 0 {
		return l.s3Objects.GetObjectNInfo(ctx, bucket, object, rs, h, lockType, opts)
	}

	objInfo, err := l.GetObjectInfo(ctx, bucket, object, opts)
	if err != nil {
		return l.s3Objects.GetObjectNInfo(ctx, bucket, object, rs, h, lockType, opts)
	}
	fn, off, length, err := minio.NewGetObjectReader(rs, objInfo)
	if err != nil {
		return nil, minio.ErrorRespToObjectError(err)
	}

	pr, pw := io.Pipe()
	go func() {
		err := l.GetObject(ctx, bucket, object, off, length, pw, objInfo.ETag, opts)
		pw.CloseWithError(err)
	}()
	// Setup cleanup function to cause the above go-routine to
	// exit in case of partial read
	pipeCloser := func() { pr.Close() }
	return fn(pr, h, pipeCloser)
}

// GetObjectInfo reads object info and replies back ObjectInfo
// For custom gateway encrypted large objects, the ObjectInfo is retrieved from the dare.meta file.
func (l *s3EncObjects) GetObjectInfo(ctx context.Context, bucket string, object string, o minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	var opts minio.ObjectOptions
	if shouldSetSSEHeaders() {
		opts = o
	}
	if len(minio.GlobalGatewaySSE) == 0 {
		return l.s3Objects.GetObjectInfo(ctx, bucket, object, opts)
	}
	gwMeta, err := l.getGWMetadata(ctx, bucket, getDareMetaPath(object))
	if err != nil {
		return l.s3Objects.GetObjectInfo(ctx, bucket, object, opts)
	}
	return gwMeta.ToObjectInfo(bucket, object), nil
}

// CopyObject copies an object from source bucket to a destination bucket.
func (l *s3EncObjects) CopyObject(ctx context.Context, srcBucket string, srcObject string, dstBucket string, dstObject string, srcInfo minio.ObjectInfo, s, d minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	var srcOpts, dstOpts minio.ObjectOptions
	if shouldSetSSEHeaders() {
		srcOpts = s
		dstOpts = d
	}
	if len(minio.GlobalGatewaySSE) == 0 {
		return l.s3Objects.CopyObject(ctx, srcBucket, srcObject, dstBucket, dstObject, srcInfo, srcOpts, dstOpts)
	}
	return l.PutObject(ctx, dstBucket, dstObject, srcInfo.PutObjectReader, srcInfo.UserDefined, d)
}

// DeleteObject deletes a blob in bucket
// For custom gateway encrypted large objects, cleans up individual parts and metadata files
// from the backend.
func (l *s3EncObjects) DeleteObject(ctx context.Context, bucket string, object string) error {
	if len(minio.GlobalGatewaySSE) == 0 {
		return l.s3Objects.DeleteObject(ctx, bucket, object)
	}
	// Get dare meta json
	if _, err := l.getGWMetadata(ctx, bucket, getDareMetaPath(object)); err != nil {
		return l.s3Objects.DeleteObject(ctx, bucket, object)
	}
	return l.deleteEncryptedObject(ctx, bucket, object)
}

func (l *s3EncObjects) deleteEncryptedObject(ctx context.Context, bucket string, object string) error {
	// Get dare meta json
	gwMeta, err := l.getGWMetadata(ctx, bucket, getDareMetaPath(object))
	if err != nil {
		return err
	}
	if len(gwMeta.Parts) == 0 {
		l.s3Objects.DeleteObject(ctx, bucket, getGWContentPath(object))
	}
	for _, part := range gwMeta.Parts {
		if err = l.s3Objects.DeleteObject(ctx, bucket, part.Name); err != nil {
			return err
		}
	}
	return l.deleteGWMetadata(ctx, bucket, getDareMetaPath(object))
}

// ListMultipartUploads lists all multipart uploads.
func (l *s3EncObjects) ListMultipartUploads(ctx context.Context, bucket string, prefix string, keyMarker string, uploadIDMarker string, delimiter string, maxUploads int) (lmi minio.ListMultipartsInfo, e error) {
	if len(minio.GlobalGatewaySSE) == 0 {
		return l.s3Objects.ListMultipartUploads(ctx, bucket, prefix, keyMarker, uploadIDMarker, delimiter, maxUploads)
	}
	var uploadsMap map[string]string
	var uploadIDs []string
	var continuationToken, startAfter string
	startAfter = keyMarker

	lmi.MaxUploads = maxUploads
	lmi.KeyMarker = keyMarker
	lmi.Prefix = prefix
	lmi.Delimiter = delimiter
	lmi.NextKeyMarker = prefix
	lmi.UploadIDMarker = uploadIDMarker
	uploadsMap = make(map[string]string)
	for {
		loi, err := l.s3Objects.ListObjectsV2(ctx, bucket, prefix, continuationToken, delimiter, 1000, false, startAfter)
		if err != nil {
			return lmi, minio.ErrorRespToObjectError(err, bucket)
		}
		for _, obj := range loi.Objects {
			startAfter = obj.Name
			if !strings.HasSuffix(obj.Name, gwdareMetaJSON) {
				continue
			}
			// skip completed uploads
			if strings.HasSuffix(obj.Name, path.Join(defaultMinioGWPrefix, gwdareMetaJSON)) {
				continue
			}
			// identify uploadID from object name: obj/.minio/uploadID/..
			pSlice := strings.Split(obj.Name, "/")
			idx := -1
			for i, p := range pSlice {
				if p == defaultMinioGWPrefix {
					idx = i + 1
					break
				}
			}
			if idx == -1 || (idx == len(pSlice)) {
				continue
			}
			uploadID := pSlice[idx]
			uploadsMap[uploadID] = ""
			if len(uploadsMap)+len(lmi.Uploads) > maxUploads {
				break
			}
		}
		// get uploadID's without duplicates and sort them
		for k := range uploadsMap {
			uploadIDs = append(uploadIDs, k)
		}
		sort.Strings(uploadIDs)
		for _, uploadID := range uploadIDs {
			if len(lmi.Uploads) == maxUploads {
				return lmi, nil
			}
			lmi.Uploads = append(lmi.Uploads, minio.MultipartInfo{Object: prefix, UploadID: uploadID})
		}
		continuationToken = loi.NextContinuationToken
		if !loi.IsTruncated {
			break
		}
	}
	return lmi, nil
}

// NewMultipartUpload uploads object in multiple parts
func (l *s3EncObjects) NewMultipartUpload(ctx context.Context, bucket string, object string, metadata map[string]string, o minio.ObjectOptions) (uploadID string, err error) {
	var opts minio.ObjectOptions
	if shouldSetSSEHeaders() {
		opts = o
	}
	// Create uploadID and write a temporary dare.meta object under object/uploadID prefix
	if len(minio.GlobalGatewaySSE) > 0 {
		uploadID := minio.MustGetUUID()
		gwmeta := newGWMetaV1()
		gwmeta.Meta = metadata
		gwmeta.Stat.ModTime = time.Now().UTC()
		err := l.writeGWMetadata(ctx, bucket, getTmpDareMetaPath(object, uploadID), gwmeta, minio.ObjectOptions{})
		if err != nil {
			return uploadID, minio.ErrorRespToObjectError(err)
		}
		return uploadID, nil
	}
	return l.s3Objects.NewMultipartUpload(ctx, bucket, object, metadata, opts)
}

// PutObject creates a new object with the incoming data,
func (l *s3EncObjects) PutObject(ctx context.Context, bucket string, object string, data *minio.PutObjectReader, metadata map[string]string, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	var s3Opts minio.ObjectOptions
	if shouldSetSSEHeaders() {
		s3Opts = opts
	}
	if len(minio.GlobalGatewaySSE) == 0 {
		return l.s3Objects.PutObject(ctx, bucket, object, data, metadata, s3Opts)
	}
	if opts.ServerSideEncryption == nil {
		wasEncrypted := l.isGWEncrypted(ctx, bucket, object)
		oi, err := l.s3Objects.PutObject(ctx, bucket, object, data, metadata, s3Opts)
		if err != nil {
			return objInfo, minio.ErrorRespToObjectError(err)
		}
		if wasEncrypted {
			l.deleteEncryptedObject(ctx, bucket, object)
		}
		return oi, nil
	}
	// overwrite any previous unencrypted object with same name
	defer l.s3Objects.DeleteObject(ctx, bucket, object)
	oi, err := l.s3Objects.PutObject(ctx, bucket, getGWContentPath(object), data, map[string]string{}, s3Opts)
	if err != nil {
		return objInfo, minio.ErrorRespToObjectError(err)
	}

	gwMeta := newGWMetaV1()
	gwMeta.Meta = make(map[string]string)
	for k, v := range oi.UserDefined {
		gwMeta.Meta[k] = v
	}
	for k, v := range metadata {
		gwMeta.Meta[k] = v
	}
	var encMD5 string
	if opts.CreateEncryptedETagFn != nil {
		encMD5, _, err = opts.CreateEncryptedETagFn()
		if err != nil {
			log.Fatal("shouldnt occur")
		}
	}

	gwMeta.ETag = encMD5
	gwMeta.Stat.Size = oi.Size
	gwMeta.Stat.ModTime = oi.ModTime
	if err = l.writeGWMetadata(ctx, bucket, getDareMetaPath(object), gwMeta, minio.ObjectOptions{}); err != nil {
		return objInfo, minio.ErrorRespToObjectError(err)
	}
	objInfo = gwMeta.ToObjectInfo(bucket, object)

	return objInfo, nil
}

// PutObjectPart puts a part of object in bucket
func (l *s3EncObjects) PutObjectPart(ctx context.Context, bucket string, object string, uploadID string, partID int, data *minio.PutObjectReader, opts minio.ObjectOptions) (pi minio.PartInfo, e error) {
	var s3Opts minio.ObjectOptions
	// for sse-s3 encryption options should not be passed to backend
	if opts.ServerSideEncryption != nil && opts.ServerSideEncryption.Type() == encrypt.SSEC && shouldSetSSEHeaders() {
		s3Opts = opts
	}
	if len(minio.GlobalGatewaySSE) == 0 {
		return l.s3Objects.PutObjectPart(ctx, bucket, object, uploadID, partID, data, s3Opts)
	}
	uploadPath := getTmpGWMetaPath(object, uploadID)
	tmpDareMeta := path.Join(uploadPath, gwdareMetaJSON)
	_, err := l.s3Objects.GetObjectInfo(ctx, bucket, tmpDareMeta, minio.ObjectOptions{})
	if err != nil {
		return pi, minio.InvalidUploadID{UploadID: uploadID}
	}
	partUploadName := path.Join(uploadPath, strconv.Itoa(partID))

	oi, err := l.s3Objects.PutObject(ctx, bucket, partUploadName, data, map[string]string{}, s3Opts)
	if err != nil {
		return pi, minio.ErrorRespToObjectError(err)
	}
	gwMeta := newGWMetaV1()
	gwMeta.Parts = make([]minio.ObjectPartInfo, 1)
	if opts.CreateEncryptedETagFn != nil {
		encMD5, _, err := opts.CreateEncryptedETagFn()
		if err != nil {
			return pi, minio.ErrorRespToObjectError(err)
		}

		// Add incoming part.
		gwMeta.Parts[0] = minio.ObjectPartInfo{
			Number: partID,
			ETag:   encMD5,
			Size:   oi.Size,
			Name:   strconv.Itoa(partID),
		}
		gwMeta.ETag = encMD5
		gwMeta.Stat.Size = oi.Size
		gwMeta.Stat.ModTime = oi.ModTime
	}
	if err = l.writeGWMetadata(ctx, bucket, getPartMetaPath(object, uploadID, partID, oi.ETag), gwMeta, minio.ObjectOptions{}); err != nil {
		return pi, minio.ErrorRespToObjectError(err)
	}

	return minio.PartInfo{
		Size:         gwMeta.Stat.Size,
		ETag:         minio.CanonicalizeETag(gwMeta.ETag),
		LastModified: gwMeta.Stat.ModTime,
		PartNumber:   partID,
	}, nil
}

// CopyObjectPart creates a part in a multipart upload by copying
// existing object or a part of it.
func (l *s3EncObjects) CopyObjectPart(ctx context.Context, srcBucket, srcObject, destBucket, destObject, uploadID string,
	partID int, startOffset, length int64, srcInfo minio.ObjectInfo, s, d minio.ObjectOptions) (p minio.PartInfo, err error) {
	var srcOpts, dstOpts minio.ObjectOptions
	if shouldSetSSEHeaders() {
		srcOpts = s
		dstOpts = d
	}
	// pass through encryption
	if len(minio.GlobalGatewaySSE) == 0 {
		return l.s3Objects.CopyObjectPart(ctx, srcBucket, srcObject, destBucket, destObject, uploadID, partID, startOffset, length, srcInfo, srcOpts, dstOpts)
	}
	return l.PutObjectPart(ctx, destBucket, destObject, uploadID, partID, srcInfo.PutObjectReader, dstOpts)
}

// ListObjectParts returns all object parts for specified object in specified bucket
func (l *s3EncObjects) ListObjectParts(ctx context.Context, bucket string, object string, uploadID string, partNumberMarker int, maxParts int, o minio.ObjectOptions) (lpi minio.ListPartsInfo, e error) {
	var opts minio.ObjectOptions
	if shouldSetSSEHeaders() {
		opts = o
	}
	if len(minio.GlobalGatewaySSE) == 0 {
		return l.s3Objects.ListObjectParts(ctx, bucket, object, uploadID, partNumberMarker, maxParts, opts)
	}
	// We do not store parts uploaded so far in the dare.meta. Only CompleteMultipartUpload finalizes the parts under upload prefix.Otherwise,
	// there could be situations of dare.meta getting corrupted by competing upload parts.
	uploadPrefix := getTmpGWMetaPath(object, uploadID)
	dm, err := l.getGWMetadata(ctx, bucket, getTmpDareMetaPath(object, uploadID))
	if err != nil {
		return l.s3Objects.ListObjectParts(ctx, bucket, object, uploadID, partNumberMarker, maxParts, opts)
	}

	lpi.Parts = make([]minio.PartInfo, 0)
	lpi.UserDefined = dm.Meta
	lpi.Bucket = bucket
	lpi.Object = object
	lpi.UploadID = uploadID
	lpi.MaxParts = maxParts
	lpi.PartNumberMarker = partNumberMarker

	if maxParts == 0 {
		return lpi, nil
	}

	var continuationToken, startAfter, delimiter string
	var loi minio.ListObjectsV2Info
	if partNumberMarker > 0 {
		startAfter = path.Join(uploadPrefix, strconv.Itoa(partNumberMarker))
	}
	for {
		loi, err = l.s3Objects.ListObjectsV2(ctx, bucket, uploadPrefix, continuationToken, delimiter, 1000, false, startAfter)
		if err != nil {
			return lpi, minio.ErrorRespToObjectError(err, bucket)
		}
		for _, obj := range loi.Objects {
			startAfter = obj.Name
			if !strings.HasPrefix(obj.Name, uploadPrefix) {
				return lpi, nil
			}
			if strings.HasSuffix(obj.Name, gwdareMetaJSON) || strings.HasSuffix(obj.Name, gwpartMetaJSON) {
				continue
			}
			partNumStr := strings.TrimPrefix(obj.Name, path.Join(object, defaultMinioGWPrefix, uploadID)+slashSeparator)
			partNum, _ := strconv.Atoi(partNumStr)

			if partNum < partNumberMarker {
				continue
			}
			partMeta, err := l.getGWMetadata(ctx, bucket, path.Join(obj.Name, obj.ETag, gwpartMetaJSON))
			if err != nil || len(partMeta.Parts) == 0 {
				return lpi, minio.InvalidPart{}
			}
			if partNum > 0 {
				pi := minio.PartInfo{
					PartNumber:   partNum,
					Size:         partMeta.Stat.Size,
					ETag:         partMeta.ETag,
					LastModified: partMeta.Stat.ModTime,
				}
				lpi.Parts = append(lpi.Parts, pi)
			}
			if len(lpi.Parts) == maxParts {
				break
			}
		}
		continuationToken = loi.NextContinuationToken
		if !loi.IsTruncated {
			break
		}
	}
	if len(loi.Objects) > len(lpi.Parts) && len(lpi.Parts) > 0 {
		lpi.IsTruncated = true
		lpi.NextPartNumberMarker = lpi.Parts[len(lpi.Parts)-1].PartNumber
	}
	return lpi, nil
}

// AbortMultipartUpload aborts a ongoing multipart upload
func (l *s3EncObjects) AbortMultipartUpload(ctx context.Context, bucket string, object string, uploadID string) error {
	if len(minio.GlobalGatewaySSE) == 0 {
		return l.s3Objects.AbortMultipartUpload(ctx, bucket, object, uploadID)
	}

	uploadPrefix := getTmpGWMetaPath(object, uploadID)
	var continuationToken, startAfter, delimiter string
	for {
		loi, err := l.s3Objects.ListObjectsV2(ctx, bucket, uploadPrefix, continuationToken, delimiter, 1000, false, startAfter)
		if err != nil {
			return minio.InvalidUploadID{UploadID: uploadID}
		}
		for _, obj := range loi.Objects {
			if !strings.HasPrefix(obj.Name, uploadPrefix) {
				return nil
			}
			if err := l.s3Objects.DeleteObject(ctx, bucket, obj.Name); err != nil {
				return minio.ErrorRespToObjectError(err)
			}
			startAfter = obj.Name
		}
		continuationToken = loi.NextContinuationToken
		if !loi.IsTruncated {
			break
		}
	}
	return nil
}

// CompleteMultipartUpload completes ongoing multipart upload and finalizes object
func (l *s3EncObjects) CompleteMultipartUpload(ctx context.Context, bucket, object, uploadID string, uploadedParts []minio.CompletePart, opts minio.ObjectOptions) (oi minio.ObjectInfo, e error) {
	var s3Opts minio.ObjectOptions
	if shouldSetSSEHeaders() {
		s3Opts = opts
	}
	if len(minio.GlobalGatewaySSE) == 0 {
		return l.s3Objects.CompleteMultipartUpload(ctx, bucket, object, uploadID, uploadedParts, s3Opts)
	}
	uploadPrefix := getTmpGWMetaPath(object, uploadID)
	dareMeta, err := l.getGWMetadata(ctx, bucket, getTmpDareMetaPath(object, uploadID))
	if err != nil {
		return l.s3Objects.CompleteMultipartUpload(ctx, bucket, object, uploadID, uploadedParts, opts)
	}

	// overwrite any previous unencrypted object with same name
	defer l.s3Objects.DeleteObject(ctx, bucket, object)

	// Calculate s3 compatible md5sum for complete multipart.
	s3MD5, err := minio.GetCompleteMultipartMD5(ctx, uploadedParts)
	if err != nil {
		return oi, minio.ErrorRespToObjectError(err, bucket)
	}
	gwMeta := newGWMetaV1()
	gwMeta.Meta = make(map[string]string)
	for k, v := range dareMeta.Meta {
		gwMeta.Meta[k] = v
	}
	// Allocate parts similar to incoming slice.
	gwMeta.Parts = make([]minio.ObjectPartInfo, len(uploadedParts))

	var objectSize int64
	var marker, delimiter string

	var partsMap = make(map[string]string)
	// Validate each part and then commit to disk.
	for i, part := range uploadedParts {
		obj := fmt.Sprintf("%s/%d", uploadPrefix, part.PartNumber)
		partsMap[obj] = ""

		res, rerr := l.s3Objects.ListObjects(ctx, bucket, obj, marker, delimiter, 1)
		if rerr != nil || len(res.Objects) == 0 {
			return oi, minio.InvalidPart{}
		}
		partInfo := res.Objects[0]
		partMeta, err := l.getGWMetadata(ctx, bucket, getPartMetaPath(object, uploadID, part.PartNumber, partInfo.ETag))
		if err != nil || len(partMeta.Parts) == 0 {
			return oi, minio.InvalidPart{}
		}

		var partETag string
		if opts.GetDecryptedETagFn != nil {
			partETag = opts.GetDecryptedETagFn(partMeta.ETag)
		}

		// All parts should have same ETag as previously generated.
		// we are comparing decrypted etags here
		if partETag != part.ETag {
			invp := minio.InvalidPart{
				PartNumber: part.PartNumber,
				ExpETag:    partInfo.ETag,
				GotETag:    part.ETag,
			}
			logger.LogIf(ctx, invp)
			return oi, invp
		}

		// Last part could have been uploaded as 0bytes, do not need
		// to save it in final `xl.json`.
		if (i == len(uploadedParts)-1) && partInfo.Size == 0 {
			gwMeta.Parts = gwMeta.Parts[:i] // Skip the part.
			continue
		}
		// Save for total object size.
		objectSize += partInfo.Size

		// Add incoming parts.
		gwMeta.Parts[i] = minio.ObjectPartInfo{
			Number: part.PartNumber,
			ETag:   partMeta.ETag, //save encrypted ETag
			Size:   partInfo.Size,
			Name:   partInfo.Name,
		}
	}

	// Save the final object size and modtime.
	gwMeta.Stat.Size = objectSize
	gwMeta.Stat.ModTime = time.Now().UTC()

	// Save successfully calculated md5sum.
	gwMeta.ETag = s3MD5

	// Clean up any uploaded parts that are not being committed by this CompleteMultipart operation
	var continuationToken, startAfter string
	done := false
	for {
		loi, lerr := l.s3Objects.ListObjectsV2(ctx, bucket, uploadPrefix, continuationToken, delimiter, 1000, false, startAfter)
		if lerr != nil {
			done = true
			break
		}
		for _, obj := range loi.Objects {
			if !strings.HasPrefix(obj.Name, uploadPrefix) {
				done = true
				break
			}
			startAfter = obj.Name
			// delete parts not found in uploadedParts  map
			if _, ok := partsMap[obj.Name]; !ok {
				l.s3Objects.DeleteObject(ctx, bucket, obj.Name)
			}
			// delete temporary part metadata files
			if strings.HasSuffix(obj.Name, gwpartMetaJSON) {
				l.s3Objects.DeleteObject(ctx, bucket, obj.Name)
			}
		}
		continuationToken = loi.NextContinuationToken
		if !loi.IsTruncated || done {
			break
		}
	}
	if err = l.writeGWMetadata(ctx, bucket, getDareMetaPath(object), gwMeta, minio.ObjectOptions{}); err != nil {
		return oi, minio.ErrorRespToObjectError(err)
	}
	// clean up temporary upload dare.meta file under uploadID prefix
	if err = l.deleteGWMetadata(ctx, bucket, getTmpGWMetaPath(object, uploadID)); err != nil {
		return oi, minio.ErrorRespToObjectError(err)
	}
	return gwMeta.ToObjectInfo(bucket, object), nil
}

// getTmpGWMetaPath returns the prefix under which uploads in progress are stored on backend
func getTmpGWMetaPath(object, uploadID string) string {
	return path.Join(object, defaultMinioGWPrefix, uploadID)
}

// getGWMetaPath returns the prefix under which custom large object is stored on backend after upload completes
func getGWMetaPath(object string) string {
	return path.Join(object, defaultMinioGWPrefix)
}

// getGWContentPath returns the prefix under which custom small object is stored on backend after upload completes
func getGWContentPath(object string) string {
	return path.Join(object, defaultMinioGWPrefix, defaultGWContentFileName)
}

// Clean-up the old multipart uploads. Should be run in a Go routine.
func (l *s3EncObjects) cleanupStaleMultipartUploads(ctx context.Context, cleanupInterval, expiry time.Duration, doneCh chan struct{}) {
	ticker := time.NewTicker(cleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-doneCh:
			return
		case <-ticker.C:
			l.cleanupStaleMultipartUploadsOnGW(ctx, expiry)
		}
	}
}

// cleanupStaleMultipartUploads removes old custom encryption multipart uploads on backend
func (l *s3EncObjects) cleanupStaleMultipartUploadsOnGW(ctx context.Context, expiry time.Duration) {
	for {
		buckets, err := l.s3Objects.ListBuckets(ctx)
		if err != nil {
			break
		}
		for _, b := range buckets {
			allParts, expParts := l.getStalePartsForBucket(ctx, b.Name, expiry)
			for k := range expParts {
				if _, ok := allParts[k]; !ok {
					l.s3Objects.DeleteObject(ctx, b.Name, k)
				}
			}
		}
	}
}

func (l *s3EncObjects) getStalePartsForBucket(ctx context.Context, bucket string, expiry time.Duration) (allParts, expParts map[string]string) {
	var prefix, continuationToken, delimiter, startAfter string
	allParts = make(map[string]string)
	expParts = make(map[string]string)
	now := time.Now()
	for {
		loi, err := l.s3Objects.ListObjectsV2(ctx, bucket, prefix, continuationToken, delimiter, 1000, false, startAfter)
		if err != nil {
			break
		}
		for _, obj := range loi.Objects {
			startAfter = obj.Name
			if !strings.Contains(obj.Name, defaultMinioGWPrefix) {
				continue
			}
			if strings.HasSuffix(obj.Name, path.Join(defaultMinioGWPrefix, gwdareMetaJSON)) {
				objSlice := strings.Split(obj.Name, path.Join(slashSeparator, defaultMinioGWPrefix))
				meta, err := l.getGWMetadata(ctx, bucket, getDareMetaPath(objSlice[0]))
				if err != nil {
					continue
				}
				for _, p := range meta.Parts {
					allParts[p.Name] = ""
				}
			}
			if strings.HasSuffix(obj.Name, path.Join(defaultMinioGWPrefix, defaultGWContentFileName)) {
				objSlice := strings.Split(obj.Name, path.Join(slashSeparator, defaultMinioGWPrefix))
				expParts[getGWContentPath(objSlice[0])] = ""
			}
			if now.Sub(obj.ModTime) > expiry {
				// skip parts that are part of a completed upload
				if _, ok := allParts[obj.Name]; !ok {
					expParts[obj.Name] = ""
				}
			}
		}
		continuationToken = loi.NextContinuationToken
		if !loi.IsTruncated {
			break
		}
	}
	return
}

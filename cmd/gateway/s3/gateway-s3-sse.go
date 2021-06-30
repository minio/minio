/*
 * MinIO Object Storage (c) 2021 MinIO, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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
	"io"
	"net/http"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/minio/minio-go/v7/pkg/encrypt"
	minio "github.com/minio/minio/cmd"

	"github.com/minio/minio/internal/logger"
)

const (
	// name of custom multipart metadata file for s3 backend.
	gwdareMetaJSON string = "dare.meta"

	// name of temporary per part metadata file
	gwpartMetaJSON string = "part.meta"
	// custom multipart files are stored under the defaultMinioGWPrefix
	defaultMinioGWPrefix     = ".minio"
	defaultGWContentFileName = "data"
)

// s3EncObjects is a wrapper around s3Objects and implements gateway calls for
// custom large objects encrypted at the gateway
type s3EncObjects struct {
	s3Objects
}

/*
 NOTE:
 Custom gateway encrypted objects are stored on backend as follows:
	 obj/.minio/data   <= encrypted content
	 obj/.minio/dare.meta  <= metadata

 When a multipart upload operation is in progress, the metadata set during
 NewMultipartUpload is stored in obj/.minio/uploadID/dare.meta and each
 UploadPart operation saves additional state of the part's encrypted ETag and
 encrypted size in obj/.minio/uploadID/part1/part.meta

 All the part metadata and temp dare.meta are cleaned up when upload completes
*/

// ListObjects lists all blobs in S3 bucket filtered by prefix
func (l *s3EncObjects) ListObjects(ctx context.Context, bucket string, prefix string, marker string, delimiter string, maxKeys int) (loi minio.ListObjectsInfo, e error) {
	var startAfter string
	res, err := l.ListObjectsV2(ctx, bucket, prefix, marker, delimiter, maxKeys, false, startAfter)
	if err != nil {
		return loi, err
	}
	loi.IsTruncated = res.IsTruncated
	loi.NextMarker = res.NextContinuationToken
	loi.Objects = res.Objects
	loi.Prefixes = res.Prefixes
	return loi, nil

}

// ListObjectsV2 lists all blobs in S3 bucket filtered by prefix
func (l *s3EncObjects) ListObjectsV2(ctx context.Context, bucket, prefix, continuationToken, delimiter string, maxKeys int, fetchOwner bool, startAfter string) (loi minio.ListObjectsV2Info, e error) {

	var objects []minio.ObjectInfo
	var prefixes []string
	var isTruncated bool

	// filter out objects that contain a .minio prefix, but is not a dare.meta metadata file.
	for {
		loi, e = l.s3Objects.ListObjectsV2(ctx, bucket, prefix, continuationToken, delimiter, 1000, fetchOwner, startAfter)
		if e != nil {
			return loi, minio.ErrorRespToObjectError(e, bucket)
		}

		continuationToken = loi.NextContinuationToken
		isTruncated = loi.IsTruncated

		for _, obj := range loi.Objects {
			startAfter = obj.Name

			if !isGWObject(obj.Name) {
				continue
			}
			// get objectname and ObjectInfo from the custom metadata file
			if strings.HasSuffix(obj.Name, gwdareMetaJSON) {
				objSlice := strings.Split(obj.Name, minio.SlashSeparator+defaultMinioGWPrefix)
				gwMeta, e := l.getGWMetadata(ctx, bucket, getDareMetaPath(objSlice[0]))
				if e != nil {
					continue
				}
				oInfo := gwMeta.ToObjectInfo(bucket, objSlice[0])
				objects = append(objects, oInfo)
			} else {
				objects = append(objects, obj)
			}
			if len(objects) > maxKeys {
				break
			}
		}
		for _, p := range loi.Prefixes {
			objName := strings.TrimSuffix(p, minio.SlashSeparator)
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
	loi.Objects = append(loi.Objects, objects...)

	for _, pfx := range prefixes {
		if pfx != prefix {
			loi.Prefixes = append(loi.Prefixes, pfx)
		}
	}
	// Set continuation token if s3 returned truncated list
	if isTruncated {
		if len(objects) > 0 {
			loi.NextContinuationToken = objects[len(objects)-1].Name
		}
	}
	return loi, nil
}

// isGWObject returns true if it is a custom object
func isGWObject(objName string) bool {
	isEncrypted := strings.Contains(objName, defaultMinioGWPrefix)
	if !isEncrypted {
		return true
	}
	// ignore temp part.meta files
	if strings.Contains(objName, gwpartMetaJSON) {
		return false
	}

	pfxSlice := strings.Split(objName, minio.SlashSeparator)
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

// GetObject reads an object from S3. Supports additional
// parameters like offset and length which are synonymous with
// HTTP Range requests.
func (l *s3EncObjects) GetObject(ctx context.Context, bucket string, key string, startOffset int64, length int64, writer io.Writer, etag string, opts minio.ObjectOptions) error {
	return l.getObject(ctx, bucket, key, startOffset, length, writer, etag, opts)
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
	err = l.s3Objects.getObject(ctx, bucket, metaFileName, 0, oi.Size, &buffer, oi.ETag, minio.ObjectOptions{})
	if err != nil {
		return m, err
	}
	return readGWMetadata(ctx, buffer)
}

// writes dare metadata to the s3 backend
func (l *s3EncObjects) writeGWMetadata(ctx context.Context, bucket, metaFileName string, m gwMetaV1, o minio.ObjectOptions) error {
	reader, err := getGWMetadata(ctx, bucket, metaFileName, m)
	if err != nil {
		logger.LogIf(ctx, err)
		return err
	}
	_, err = l.s3Objects.PutObject(ctx, bucket, metaFileName, reader, o)
	return err
}

// returns path of temporary metadata json file for the upload
func getTmpDareMetaPath(object, uploadID string) string {
	return path.Join(getGWMetaPath(object), uploadID, gwdareMetaJSON)
}

// returns path of metadata json file for encrypted objects
func getDareMetaPath(object string) string {
	return path.Join(getGWMetaPath(object), gwdareMetaJSON)
}

// returns path of temporary part metadata file for multipart uploads
func getPartMetaPath(object, uploadID string, partID int) string {
	return path.Join(object, defaultMinioGWPrefix, uploadID, strconv.Itoa(partID), gwpartMetaJSON)
}

// deletes the custom dare metadata file saved at the backend
func (l *s3EncObjects) deleteGWMetadata(ctx context.Context, bucket, metaFileName string) (minio.ObjectInfo, error) {
	return l.s3Objects.DeleteObject(ctx, bucket, metaFileName, minio.ObjectOptions{})
}

func (l *s3EncObjects) getObject(ctx context.Context, bucket string, key string, startOffset int64, length int64, writer io.Writer, etag string, opts minio.ObjectOptions) error {
	var o minio.ObjectOptions
	if minio.GlobalGatewaySSE.SSEC() {
		o = opts
	}
	dmeta, err := l.getGWMetadata(ctx, bucket, getDareMetaPath(key))
	if err != nil {
		// unencrypted content
		return l.s3Objects.getObject(ctx, bucket, key, startOffset, length, writer, etag, o)
	}
	if startOffset < 0 {
		logger.LogIf(ctx, minio.InvalidRange{})
	}

	// For negative length read everything.
	if length < 0 {
		length = dmeta.Stat.Size - startOffset
	}
	// Reply back invalid range if the input offset and length fall out of range.
	if startOffset > dmeta.Stat.Size || startOffset+length > dmeta.Stat.Size {
		logger.LogIf(ctx, minio.InvalidRange{OffsetBegin: startOffset, OffsetEnd: length, ResourceSize: dmeta.Stat.Size})
		return minio.InvalidRange{OffsetBegin: startOffset, OffsetEnd: length, ResourceSize: dmeta.Stat.Size}
	}
	// Get start part index and offset.
	_, partOffset, err := dmeta.ObjectToPartOffset(ctx, startOffset)
	if err != nil {
		return minio.InvalidRange{OffsetBegin: startOffset, OffsetEnd: length, ResourceSize: dmeta.Stat.Size}
	}

	// Calculate endOffset according to length
	endOffset := startOffset
	if length > 0 {
		endOffset += length - 1
	}

	// Get last part index to read given length.
	if _, _, err := dmeta.ObjectToPartOffset(ctx, endOffset); err != nil {
		return minio.InvalidRange{OffsetBegin: startOffset, OffsetEnd: length, ResourceSize: dmeta.Stat.Size}
	}
	return l.s3Objects.getObject(ctx, bucket, key, partOffset, endOffset, writer, dmeta.ETag, o)
}

// GetObjectNInfo - returns object info and locked object ReadCloser
func (l *s3EncObjects) GetObjectNInfo(ctx context.Context, bucket, object string, rs *minio.HTTPRangeSpec, h http.Header, lockType minio.LockType, o minio.ObjectOptions) (gr *minio.GetObjectReader, err error) {
	var opts minio.ObjectOptions
	if minio.GlobalGatewaySSE.SSEC() {
		opts = o
	}
	objInfo, err := l.GetObjectInfo(ctx, bucket, object, opts)
	if err != nil {
		return l.s3Objects.GetObjectNInfo(ctx, bucket, object, rs, h, lockType, opts)
	}
	fn, off, length, err := minio.NewGetObjectReader(rs, objInfo, opts)
	if err != nil {
		return nil, minio.ErrorRespToObjectError(err, bucket, object)
	}
	if l.isGWEncrypted(ctx, bucket, object) {
		object = getGWContentPath(object)
	}
	pr, pw := io.Pipe()
	go func() {
		// Do not set an `If-Match` header for the ETag when
		// the ETag is encrypted. The ETag at the backend never
		// matches an encrypted ETag and there is in any case
		// no way to make two consecutive S3 calls safe for concurrent
		// access.
		// However,  the encrypted object changes concurrently then the
		// gateway will not be able to decrypt it since the key (obtained
		// from dare.meta) will not work for any new created object. Therefore,
		// we will in any case not return invalid data to the client.
		etag := objInfo.ETag
		if len(etag) > 32 && strings.Count(etag, "-") == 0 {
			etag = ""
		}
		err := l.getObject(ctx, bucket, object, off, length, pw, etag, opts)
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
	if minio.GlobalGatewaySSE.SSEC() {
		opts = o
	}

	gwMeta, err := l.getGWMetadata(ctx, bucket, getDareMetaPath(object))
	if err != nil {
		return l.s3Objects.GetObjectInfo(ctx, bucket, object, opts)
	}
	return gwMeta.ToObjectInfo(bucket, object), nil
}

// CopyObject copies an object from source bucket to a destination bucket.
func (l *s3EncObjects) CopyObject(ctx context.Context, srcBucket string, srcObject string, dstBucket string, dstObject string, srcInfo minio.ObjectInfo, s, d minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	cpSrcDstSame := path.Join(srcBucket, srcObject) == path.Join(dstBucket, dstObject)
	if cpSrcDstSame {
		var gwMeta gwMetaV1
		if s.ServerSideEncryption != nil && d.ServerSideEncryption != nil &&
			((s.ServerSideEncryption.Type() == encrypt.SSEC && d.ServerSideEncryption.Type() == encrypt.SSEC) ||
				(s.ServerSideEncryption.Type() == encrypt.S3 && d.ServerSideEncryption.Type() == encrypt.S3)) {
			gwMeta, err = l.getGWMetadata(ctx, srcBucket, getDareMetaPath(srcObject))
			if err != nil {
				return
			}
			header := make(http.Header)
			if d.ServerSideEncryption != nil {
				d.ServerSideEncryption.Marshal(header)
			}
			for k, v := range header {
				srcInfo.UserDefined[k] = v[0]
			}
			gwMeta.Meta = srcInfo.UserDefined
			if err = l.writeGWMetadata(ctx, dstBucket, getDareMetaPath(dstObject), gwMeta, minio.ObjectOptions{}); err != nil {
				return objInfo, minio.ErrorRespToObjectError(err)
			}
			return gwMeta.ToObjectInfo(dstBucket, dstObject), nil
		}
	}
	dstOpts := minio.ObjectOptions{ServerSideEncryption: d.ServerSideEncryption, UserDefined: srcInfo.UserDefined}
	return l.PutObject(ctx, dstBucket, dstObject, srcInfo.PutObjReader, dstOpts)
}

// DeleteObject deletes a blob in bucket
// For custom gateway encrypted large objects, cleans up encrypted content and metadata files
// from the backend.
func (l *s3EncObjects) DeleteObject(ctx context.Context, bucket string, object string, opts minio.ObjectOptions) (minio.ObjectInfo, error) {
	// Get dare meta json
	if _, err := l.getGWMetadata(ctx, bucket, getDareMetaPath(object)); err != nil {
		logger.LogIf(minio.GlobalContext, err)
		return l.s3Objects.DeleteObject(ctx, bucket, object, opts)
	}
	// delete encrypted object
	l.s3Objects.DeleteObject(ctx, bucket, getGWContentPath(object), opts)
	return l.deleteGWMetadata(ctx, bucket, getDareMetaPath(object))
}

func (l *s3EncObjects) DeleteObjects(ctx context.Context, bucket string, objects []minio.ObjectToDelete, opts minio.ObjectOptions) ([]minio.DeletedObject, []error) {
	errs := make([]error, len(objects))
	dobjects := make([]minio.DeletedObject, len(objects))
	for idx, object := range objects {
		_, errs[idx] = l.DeleteObject(ctx, bucket, object.ObjectName, opts)
		if errs[idx] == nil {
			dobjects[idx] = minio.DeletedObject{
				ObjectName: object.ObjectName,
			}
		}
	}
	return dobjects, errs
}

// ListMultipartUploads lists all multipart uploads.
func (l *s3EncObjects) ListMultipartUploads(ctx context.Context, bucket string, prefix string, keyMarker string, uploadIDMarker string, delimiter string, maxUploads int) (lmi minio.ListMultipartsInfo, e error) {

	lmi, e = l.s3Objects.ListMultipartUploads(ctx, bucket, prefix, keyMarker, uploadIDMarker, delimiter, maxUploads)
	if e != nil {
		return
	}
	lmi.KeyMarker = strings.TrimSuffix(lmi.KeyMarker, getGWContentPath(minio.SlashSeparator))
	lmi.NextKeyMarker = strings.TrimSuffix(lmi.NextKeyMarker, getGWContentPath(minio.SlashSeparator))
	for i := range lmi.Uploads {
		lmi.Uploads[i].Object = strings.TrimSuffix(lmi.Uploads[i].Object, getGWContentPath(minio.SlashSeparator))
	}
	return
}

// NewMultipartUpload uploads object in multiple parts
func (l *s3EncObjects) NewMultipartUpload(ctx context.Context, bucket string, object string, o minio.ObjectOptions) (uploadID string, err error) {
	var sseOpts encrypt.ServerSide
	if o.ServerSideEncryption == nil {
		return l.s3Objects.NewMultipartUpload(ctx, bucket, object, minio.ObjectOptions{UserDefined: o.UserDefined})
	}
	// Decide if sse options needed to be passed to backend
	if (minio.GlobalGatewaySSE.SSEC() && o.ServerSideEncryption.Type() == encrypt.SSEC) ||
		(minio.GlobalGatewaySSE.SSES3() && o.ServerSideEncryption.Type() == encrypt.S3) {
		sseOpts = o.ServerSideEncryption
	}

	uploadID, err = l.s3Objects.NewMultipartUpload(ctx, bucket, getGWContentPath(object), minio.ObjectOptions{ServerSideEncryption: sseOpts})
	if err != nil {
		return
	}
	// Create uploadID and write a temporary dare.meta object under object/uploadID prefix
	gwmeta := newGWMetaV1()
	gwmeta.Meta = o.UserDefined
	gwmeta.Stat.ModTime = time.Now().UTC()
	err = l.writeGWMetadata(ctx, bucket, getTmpDareMetaPath(object, uploadID), gwmeta, minio.ObjectOptions{})
	if err != nil {
		return uploadID, minio.ErrorRespToObjectError(err)
	}
	return uploadID, nil
}

// PutObject creates a new object with the incoming data,
func (l *s3EncObjects) PutObject(ctx context.Context, bucket string, object string, data *minio.PutObjReader, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	var sseOpts encrypt.ServerSide
	// Decide if sse options needed to be passed to backend
	if opts.ServerSideEncryption != nil &&
		((minio.GlobalGatewaySSE.SSEC() && opts.ServerSideEncryption.Type() == encrypt.SSEC) ||
			(minio.GlobalGatewaySSE.SSES3() && opts.ServerSideEncryption.Type() == encrypt.S3) ||
			opts.ServerSideEncryption.Type() == encrypt.KMS) {
		sseOpts = opts.ServerSideEncryption
	}
	if opts.ServerSideEncryption == nil {
		defer l.deleteGWMetadata(ctx, bucket, getDareMetaPath(object))
		defer l.DeleteObject(ctx, bucket, getGWContentPath(object), opts)
		return l.s3Objects.PutObject(ctx, bucket, object, data, minio.ObjectOptions{UserDefined: opts.UserDefined})
	}

	oi, err := l.s3Objects.PutObject(ctx, bucket, getGWContentPath(object), data, minio.ObjectOptions{ServerSideEncryption: sseOpts})
	if err != nil {
		return objInfo, minio.ErrorRespToObjectError(err)
	}

	gwMeta := newGWMetaV1()
	gwMeta.Meta = make(map[string]string)
	for k, v := range opts.UserDefined {
		gwMeta.Meta[k] = v
	}
	encMD5 := data.MD5CurrentHexString()

	gwMeta.ETag = encMD5
	gwMeta.Stat.Size = oi.Size
	gwMeta.Stat.ModTime = time.Now().UTC()
	if err = l.writeGWMetadata(ctx, bucket, getDareMetaPath(object), gwMeta, minio.ObjectOptions{}); err != nil {
		return objInfo, minio.ErrorRespToObjectError(err)
	}
	objInfo = gwMeta.ToObjectInfo(bucket, object)
	// delete any unencrypted content of the same name created previously
	l.s3Objects.DeleteObject(ctx, bucket, object, opts)
	return objInfo, nil
}

// PutObjectPart puts a part of object in bucket
func (l *s3EncObjects) PutObjectPart(ctx context.Context, bucket string, object string, uploadID string, partID int, data *minio.PutObjReader, opts minio.ObjectOptions) (pi minio.PartInfo, e error) {

	if opts.ServerSideEncryption == nil {
		return l.s3Objects.PutObjectPart(ctx, bucket, object, uploadID, partID, data, opts)
	}

	var s3Opts minio.ObjectOptions
	// for sse-s3 encryption options should not be passed to backend
	if opts.ServerSideEncryption != nil && opts.ServerSideEncryption.Type() == encrypt.SSEC && minio.GlobalGatewaySSE.SSEC() {
		s3Opts = opts
	}

	uploadPath := getTmpGWMetaPath(object, uploadID)
	tmpDareMeta := path.Join(uploadPath, gwdareMetaJSON)
	_, err := l.s3Objects.GetObjectInfo(ctx, bucket, tmpDareMeta, minio.ObjectOptions{})
	if err != nil {
		return pi, minio.InvalidUploadID{UploadID: uploadID}
	}

	pi, e = l.s3Objects.PutObjectPart(ctx, bucket, getGWContentPath(object), uploadID, partID, data, s3Opts)
	if e != nil {
		return
	}
	gwMeta := newGWMetaV1()
	gwMeta.Parts = make([]minio.ObjectPartInfo, 1)
	// Add incoming part.
	gwMeta.Parts[0] = minio.ObjectPartInfo{
		Number: partID,
		ETag:   pi.ETag,
		Size:   pi.Size,
	}
	gwMeta.ETag = data.MD5CurrentHexString() // encrypted ETag
	gwMeta.Stat.Size = pi.Size
	gwMeta.Stat.ModTime = pi.LastModified

	if err = l.writeGWMetadata(ctx, bucket, getPartMetaPath(object, uploadID, partID), gwMeta, minio.ObjectOptions{}); err != nil {
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
	partID int, startOffset, length int64, srcInfo minio.ObjectInfo, srcOpts, dstOpts minio.ObjectOptions) (p minio.PartInfo, err error) {
	return l.PutObjectPart(ctx, destBucket, destObject, uploadID, partID, srcInfo.PutObjReader, dstOpts)
}

// GetMultipartInfo returns multipart info of the uploadId of the object
func (l *s3EncObjects) GetMultipartInfo(ctx context.Context, bucket, object, uploadID string, opts minio.ObjectOptions) (result minio.MultipartInfo, err error) {
	result.Bucket = bucket
	result.Object = object
	result.UploadID = uploadID
	// We do not store parts uploaded so far in the dare.meta. Only CompleteMultipartUpload finalizes the parts under upload prefix.Otherwise,
	// there could be situations of dare.meta getting corrupted by competing upload parts.
	dm, err := l.getGWMetadata(ctx, bucket, getTmpDareMetaPath(object, uploadID))
	if err != nil {
		return l.s3Objects.GetMultipartInfo(ctx, bucket, object, uploadID, opts)
	}
	result.UserDefined = dm.ToObjectInfo(bucket, object).UserDefined
	return result, nil
}

// ListObjectParts returns all object parts for specified object in specified bucket
func (l *s3EncObjects) ListObjectParts(ctx context.Context, bucket string, object string, uploadID string, partNumberMarker int, maxParts int, opts minio.ObjectOptions) (lpi minio.ListPartsInfo, e error) {
	// We do not store parts uploaded so far in the dare.meta. Only CompleteMultipartUpload finalizes the parts under upload prefix.Otherwise,
	// there could be situations of dare.meta getting corrupted by competing upload parts.
	dm, err := l.getGWMetadata(ctx, bucket, getTmpDareMetaPath(object, uploadID))
	if err != nil {
		return l.s3Objects.ListObjectParts(ctx, bucket, object, uploadID, partNumberMarker, maxParts, opts)
	}

	lpi, err = l.s3Objects.ListObjectParts(ctx, bucket, getGWContentPath(object), uploadID, partNumberMarker, maxParts, opts)
	if err != nil {
		return lpi, err
	}
	for i, part := range lpi.Parts {
		partMeta, err := l.getGWMetadata(ctx, bucket, getPartMetaPath(object, uploadID, part.PartNumber))
		if err != nil || len(partMeta.Parts) == 0 {
			return lpi, minio.InvalidPart{}
		}
		lpi.Parts[i].ETag = partMeta.ETag
	}
	lpi.UserDefined = dm.ToObjectInfo(bucket, object).UserDefined
	lpi.Object = object
	return lpi, nil
}

// AbortMultipartUpload aborts a ongoing multipart upload
func (l *s3EncObjects) AbortMultipartUpload(ctx context.Context, bucket string, object string, uploadID string, opts minio.ObjectOptions) error {
	if _, err := l.getGWMetadata(ctx, bucket, getTmpDareMetaPath(object, uploadID)); err != nil {
		return l.s3Objects.AbortMultipartUpload(ctx, bucket, object, uploadID, opts)
	}

	if err := l.s3Objects.AbortMultipartUpload(ctx, bucket, getGWContentPath(object), uploadID, opts); err != nil {
		return err
	}

	uploadPrefix := getTmpGWMetaPath(object, uploadID)
	var continuationToken, startAfter, delimiter string
	for {
		loi, err := l.s3Objects.ListObjectsV2(ctx, bucket, uploadPrefix, continuationToken, delimiter, 1000, false, startAfter)
		if err != nil {
			return minio.InvalidUploadID{UploadID: uploadID}
		}
		for _, obj := range loi.Objects {
			if _, err := l.s3Objects.DeleteObject(ctx, bucket, obj.Name, minio.ObjectOptions{}); err != nil {
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

	tmpMeta, err := l.getGWMetadata(ctx, bucket, getTmpDareMetaPath(object, uploadID))
	if err != nil {
		oi, e = l.s3Objects.CompleteMultipartUpload(ctx, bucket, object, uploadID, uploadedParts, opts)
		if e == nil {
			// delete any encrypted version of object that might exist
			defer l.deleteGWMetadata(ctx, bucket, getDareMetaPath(object))
			defer l.DeleteObject(ctx, bucket, getGWContentPath(object), opts)
		}
		return oi, e
	}
	gwMeta := newGWMetaV1()
	gwMeta.Meta = make(map[string]string)
	for k, v := range tmpMeta.Meta {
		gwMeta.Meta[k] = v
	}
	// Allocate parts similar to incoming slice.
	gwMeta.Parts = make([]minio.ObjectPartInfo, len(uploadedParts))

	bkUploadedParts := make([]minio.CompletePart, len(uploadedParts))
	// Calculate full object size.
	var objectSize int64

	// Validate each part and then commit to disk.
	for i, part := range uploadedParts {
		partMeta, err := l.getGWMetadata(ctx, bucket, getPartMetaPath(object, uploadID, part.PartNumber))
		if err != nil || len(partMeta.Parts) == 0 {
			return oi, minio.InvalidPart{}
		}
		bkUploadedParts[i] = minio.CompletePart{PartNumber: part.PartNumber, ETag: partMeta.Parts[0].ETag}
		gwMeta.Parts[i] = partMeta.Parts[0]
		objectSize += partMeta.Parts[0].Size
	}
	oi, e = l.s3Objects.CompleteMultipartUpload(ctx, bucket, getGWContentPath(object), uploadID, bkUploadedParts, opts)
	if e != nil {
		return oi, e
	}

	//delete any unencrypted version of object that might be on the backend
	defer l.s3Objects.DeleteObject(ctx, bucket, object, opts)

	// Save the final object size and modtime.
	gwMeta.Stat.Size = objectSize
	gwMeta.Stat.ModTime = time.Now().UTC()
	gwMeta.ETag = oi.ETag

	if err = l.writeGWMetadata(ctx, bucket, getDareMetaPath(object), gwMeta, minio.ObjectOptions{}); err != nil {
		return oi, minio.ErrorRespToObjectError(err)
	}
	// Clean up any uploaded parts that are not being committed by this CompleteMultipart operation
	var continuationToken, startAfter, delimiter string
	uploadPrefix := getTmpGWMetaPath(object, uploadID)
	done := false
	for {
		loi, lerr := l.s3Objects.ListObjectsV2(ctx, bucket, uploadPrefix, continuationToken, delimiter, 1000, false, startAfter)
		if lerr != nil {
			break
		}
		for _, obj := range loi.Objects {
			if !strings.HasPrefix(obj.Name, uploadPrefix) {
				done = true
				break
			}
			startAfter = obj.Name
			l.s3Objects.DeleteObject(ctx, bucket, obj.Name, opts)
		}
		continuationToken = loi.NextContinuationToken
		if !loi.IsTruncated || done {
			break
		}
	}

	return gwMeta.ToObjectInfo(bucket, object), nil
}

// getTmpGWMetaPath returns the prefix under which uploads in progress are stored on backend
func getTmpGWMetaPath(object, uploadID string) string {
	return path.Join(object, defaultMinioGWPrefix, uploadID)
}

// getGWMetaPath returns the prefix under which custom object metadata and object are stored on backend after upload completes
func getGWMetaPath(object string) string {
	return path.Join(object, defaultMinioGWPrefix)
}

// getGWContentPath returns the prefix under which custom object is stored on backend after upload completes
func getGWContentPath(object string) string {
	return path.Join(object, defaultMinioGWPrefix, defaultGWContentFileName)
}

// Clean-up the stale incomplete encrypted multipart uploads. Should be run in a Go routine.
func (l *s3EncObjects) cleanupStaleEncMultipartUploads(ctx context.Context, cleanupInterval, expiry time.Duration) {
	ticker := time.NewTicker(cleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			l.cleanupStaleUploads(ctx, expiry)
		}
	}
}

// cleanupStaleUploads removes old custom encryption multipart uploads on backend
func (l *s3EncObjects) cleanupStaleUploads(ctx context.Context, expiry time.Duration) {
	buckets, err := l.s3Objects.ListBuckets(ctx)
	if err != nil {
		logger.LogIf(ctx, err)
		return
	}
	for _, b := range buckets {
		expParts := l.getStalePartsForBucket(ctx, b.Name, expiry)
		for k := range expParts {
			l.s3Objects.DeleteObject(ctx, b.Name, k, minio.ObjectOptions{})
		}
	}
}

func (l *s3EncObjects) getStalePartsForBucket(ctx context.Context, bucket string, expiry time.Duration) (expParts map[string]string) {
	var prefix, continuationToken, delimiter, startAfter string
	expParts = make(map[string]string)
	now := time.Now()
	for {
		loi, err := l.s3Objects.ListObjectsV2(ctx, bucket, prefix, continuationToken, delimiter, 1000, false, startAfter)
		if err != nil {
			logger.LogIf(ctx, err)
			break
		}
		for _, obj := range loi.Objects {
			startAfter = obj.Name
			if !strings.Contains(obj.Name, defaultMinioGWPrefix) {
				continue
			}

			if isGWObject(obj.Name) {
				continue
			}

			// delete temporary part.meta or dare.meta files for incomplete uploads that are past expiry
			if (strings.HasSuffix(obj.Name, gwpartMetaJSON) || strings.HasSuffix(obj.Name, gwdareMetaJSON)) &&
				now.Sub(obj.ModTime) > expiry {
				expParts[obj.Name] = ""
			}
		}
		continuationToken = loi.NextContinuationToken
		if !loi.IsTruncated {
			break
		}
	}
	return
}

func (l *s3EncObjects) DeleteBucket(ctx context.Context, bucket string, forceDelete bool) error {
	var prefix, continuationToken, delimiter, startAfter string
	expParts := make(map[string]string)

	for {
		loi, err := l.s3Objects.ListObjectsV2(ctx, bucket, prefix, continuationToken, delimiter, 1000, false, startAfter)
		if err != nil {
			break
		}
		for _, obj := range loi.Objects {
			startAfter = obj.Name
			if !strings.Contains(obj.Name, defaultMinioGWPrefix) {
				return minio.BucketNotEmpty{}
			}
			if isGWObject(obj.Name) {
				return minio.BucketNotEmpty{}
			}
			// delete temporary part.meta or dare.meta files for incomplete uploads
			if strings.HasSuffix(obj.Name, gwpartMetaJSON) || strings.HasSuffix(obj.Name, gwdareMetaJSON) {
				expParts[obj.Name] = ""
			}
		}
		continuationToken = loi.NextContinuationToken
		if !loi.IsTruncated {
			break
		}
	}
	for k := range expParts {
		l.s3Objects.DeleteObject(ctx, bucket, k, minio.ObjectOptions{})
	}
	err := l.Client.RemoveBucket(ctx, bucket)
	if err != nil {
		return minio.ErrorRespToObjectError(err, bucket)
	}
	return nil
}

// Copyright (c) 2015-2021 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package cmd

import (
	"bytes"
	"context"
	"errors"
	"io"
	"mime"
	"net/http"
	"path/filepath"
	"sort"
	"strings"

	"github.com/minio/minio/internal/auth"
	"github.com/minio/minio/internal/crypto"
	xhttp "github.com/minio/minio/internal/http"
	xioutil "github.com/minio/minio/internal/ioutil"
	"github.com/minio/pkg/v3/policy"
	"github.com/minio/zipindex"
)

const (
	archiveType            = "zip"
	archiveTypeEnc         = "zip-enc"
	archiveExt             = "." + archiveType // ".zip"
	archiveSeparator       = "/"
	archivePattern         = archiveExt + archiveSeparator                // ".zip/"
	archiveTypeMetadataKey = ReservedMetadataPrefixLower + "archive-type" // "x-minio-internal-archive-type"
	archiveInfoMetadataKey = ReservedMetadataPrefixLower + "archive-info" // "x-minio-internal-archive-info"

	// Peek into a zip archive
	xMinIOExtract = "x-minio-extract"
)

// splitZipExtensionPath splits the S3 path to the zip file and the path inside the zip:
//
//	e.g  /path/to/archive.zip/backup-2021/myimage.png => /path/to/archive.zip, backup/myimage.png
func splitZipExtensionPath(input string) (zipPath, object string, err error) {
	idx := strings.Index(input, archivePattern)
	if idx < 0 {
		// Should never happen
		return "", "", errors.New("unable to parse zip path")
	}
	return input[:idx+len(archivePattern)-1], input[idx+len(archivePattern):], nil
}

// getObjectInArchiveFileHandler - GET Object in the archive file
func (api objectAPIHandlers) getObjectInArchiveFileHandler(ctx context.Context, objectAPI ObjectLayer, bucket, object string, w http.ResponseWriter, r *http.Request) {
	if crypto.S3.IsRequested(r.Header) || crypto.S3KMS.IsRequested(r.Header) { // If SSE-S3 or SSE-KMS present -> AWS fails with undefined error
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrBadRequest), r.URL)
		return
	}

	zipPath, object, err := splitZipExtensionPath(object)
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	opts, err := getOpts(ctx, r, bucket, zipPath)
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	getObjectInfo := objectAPI.GetObjectInfo

	// Check for auth type to return S3 compatible error.
	// type to return the correct error (NoSuchKey vs AccessDenied)
	if s3Error := checkRequestAuthType(ctx, r, policy.GetObjectAction, bucket, zipPath); s3Error != ErrNone {
		if getRequestAuthType(r) == authTypeAnonymous {
			// As per "Permission" section in
			// https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectGET.html
			// If the object you request does not exist,
			// the error Amazon S3 returns depends on
			// whether you also have the s3:ListBucket
			// permission.
			// * If you have the s3:ListBucket permission
			//   on the bucket, Amazon S3 will return an
			//   HTTP status code 404 ("no such key")
			//   error.
			// * if you don’t have the s3:ListBucket
			//   permission, Amazon S3 will return an HTTP
			//   status code 403 ("access denied") error.`
			if globalPolicySys.IsAllowed(policy.BucketPolicyArgs{
				Action:          policy.ListBucketAction,
				BucketName:      bucket,
				ConditionValues: getConditionValues(r, "", auth.AnonymousCredentials),
				IsOwner:         false,
			}) {
				_, err = getObjectInfo(ctx, bucket, zipPath, opts)
				if toAPIError(ctx, err).Code == "NoSuchKey" {
					s3Error = ErrNoSuchKey
				}
			}
		}
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(s3Error), r.URL)
		return
	}

	// We do not allow offsetting into extracted files.
	if opts.PartNumber != 0 {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrInvalidPartNumber), r.URL)
		return
	}

	if r.Header.Get(xhttp.Range) != "" {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrInvalidRange), r.URL)
		return
	}

	// Validate pre-conditions if any.
	opts.CheckPrecondFn = func(oi ObjectInfo) bool {
		if _, err := DecryptObjectInfo(&oi, r); err != nil {
			writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
			return true
		}

		return checkPreconditions(ctx, w, r, oi, opts)
	}

	zipObjInfo, err := getObjectInfo(ctx, bucket, zipPath, opts)
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	zipInfo := zipObjInfo.ArchiveInfo(r.Header)
	if len(zipInfo) == 0 {
		opts.EncryptFn, err = zipObjInfo.metadataEncryptFn(r.Header)
		if err != nil {
			writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
			return
		}

		zipInfo, err = updateObjectMetadataWithZipInfo(ctx, objectAPI, bucket, zipPath, opts)
	}
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}
	file, err := zipindex.FindSerialized(zipInfo, object)
	if err != nil {
		if err == io.EOF {
			writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrNoSuchKey), r.URL)
		} else {
			writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		}
		return
	}

	// New object info
	fileObjInfo := ObjectInfo{
		Bucket:      bucket,
		Name:        object,
		Size:        int64(file.UncompressedSize64),
		ModTime:     zipObjInfo.ModTime,
		ContentType: mime.TypeByExtension(filepath.Ext(object)),
	}

	var rc io.ReadCloser

	if file.UncompressedSize64 > 0 {
		// There may be number of header bytes before the content.
		// Reading 64K extra. This should more than cover name and any "extra" details.
		end := min(file.Offset+int64(file.CompressedSize64)+64<<10, zipObjInfo.Size)
		rs := &HTTPRangeSpec{Start: file.Offset, End: end}
		gr, err := objectAPI.GetObjectNInfo(ctx, bucket, zipPath, rs, nil, opts)
		if err != nil {
			writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
			return
		}
		defer gr.Close()
		rc, err = file.Open(gr)
		if err != nil {
			writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
			return
		}
	} else {
		rc = io.NopCloser(bytes.NewReader([]byte{}))
	}

	defer rc.Close()

	if err = setObjectHeaders(ctx, w, fileObjInfo, nil, opts); err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}
	// s3zip does not allow ranges
	w.Header().Del(xhttp.AcceptRanges)

	setHeadGetRespHeaders(w, r.Form)

	httpWriter := xioutil.WriteOnClose(w)

	// Write object content to response body
	if _, err = xioutil.Copy(httpWriter, rc); err != nil {
		if !httpWriter.HasWritten() {
			// write error response only if no data or headers has been written to client yet
			writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
			return
		}
		return
	}

	if err = httpWriter.Close(); err != nil {
		if !httpWriter.HasWritten() { // write error response only if no data or headers has been written to client yet
			writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
			return
		}
		return
	}
}

// listObjectsV2InArchive generates S3 listing result ListObjectsV2Info from zip file, all parameters are already validated by the caller.
func listObjectsV2InArchive(ctx context.Context, objectAPI ObjectLayer, bucket, prefix, token, delimiter string, maxKeys int, startAfter string, h http.Header) (ListObjectsV2Info, error) {
	zipPath, _, err := splitZipExtensionPath(prefix)
	if err != nil {
		// Return empty listing
		return ListObjectsV2Info{}, nil
	}

	zipObjInfo, err := objectAPI.GetObjectInfo(ctx, bucket, zipPath, ObjectOptions{})
	if err != nil {
		// Return empty listing
		return ListObjectsV2Info{}, nil
	}

	zipInfo := zipObjInfo.ArchiveInfo(h)
	if len(zipInfo) == 0 {
		// Always update the latest version
		zipInfo, err = updateObjectMetadataWithZipInfo(ctx, objectAPI, bucket, zipPath, ObjectOptions{})
	}
	if err != nil {
		return ListObjectsV2Info{}, err
	}

	files, err := zipindex.DeserializeFiles(zipInfo)
	if err != nil {
		return ListObjectsV2Info{}, err
	}

	sort.Slice(files, func(i, j int) bool {
		return files[i].Name < files[j].Name
	})

	var (
		count           int
		isTruncated     bool
		nextToken       string
		listObjectsInfo ListObjectsV2Info
	)

	// Always set this
	listObjectsInfo.ContinuationToken = token

	// Open and iterate through the files in the archive.
	for _, file := range files {
		objName := zipObjInfo.Name + archiveSeparator + file.Name
		if objName <= startAfter || objName <= token {
			continue
		}
		if strings.HasPrefix(objName, prefix) {
			if count == maxKeys {
				isTruncated = true
				break
			}
			if delimiter != "" {
				i := strings.Index(objName[len(prefix):], delimiter)
				if i >= 0 {
					commonPrefix := objName[:len(prefix)+i+1]
					if len(listObjectsInfo.Prefixes) == 0 || commonPrefix != listObjectsInfo.Prefixes[len(listObjectsInfo.Prefixes)-1] {
						listObjectsInfo.Prefixes = append(listObjectsInfo.Prefixes, commonPrefix)
						count++
					}
					goto next
				}
			}
			listObjectsInfo.Objects = append(listObjectsInfo.Objects, ObjectInfo{
				Bucket:  bucket,
				Name:    objName,
				Size:    int64(file.UncompressedSize64),
				ModTime: zipObjInfo.ModTime,
			})
			count++
		}
	next:
		nextToken = objName
	}

	if isTruncated {
		listObjectsInfo.IsTruncated = true
		listObjectsInfo.NextContinuationToken = nextToken
	}

	return listObjectsInfo, nil
}

// getFilesFromZIPObject reads a partial stream of a zip file to build the zipindex.Files index
func getFilesListFromZIPObject(ctx context.Context, objectAPI ObjectLayer, bucket, object string, opts ObjectOptions) (zipindex.Files, ObjectInfo, error) {
	size := 1 << 20
	var objSize int64
	for {
		rs := &HTTPRangeSpec{IsSuffixLength: true, Start: int64(-size)}
		gr, err := objectAPI.GetObjectNInfo(ctx, bucket, object, rs, nil, opts)
		if err != nil {
			return nil, ObjectInfo{}, err
		}
		b, err := io.ReadAll(gr)
		gr.Close()
		if err != nil {
			return nil, ObjectInfo{}, err
		}
		if size > len(b) {
			size = len(b)
		}

		// Calculate the object real size if encrypted
		if _, ok := crypto.IsEncrypted(gr.ObjInfo.UserDefined); ok {
			objSize, err = gr.ObjInfo.DecryptedSize()
			if err != nil {
				return nil, ObjectInfo{}, err
			}
		} else {
			objSize = gr.ObjInfo.Size
		}

		files, err := zipindex.ReadDir(b[len(b)-size:], objSize, nil)
		if err == nil {
			return files, gr.ObjInfo, nil
		}
		var terr zipindex.ErrNeedMoreData
		if errors.As(err, &terr) {
			size = int(terr.FromEnd)
			if size <= 0 || size > 100<<20 {
				return nil, ObjectInfo{}, errors.New("zip directory too large")
			}
		} else {
			return nil, ObjectInfo{}, err
		}
	}
}

// headObjectInArchiveFileHandler - HEAD Object in an archive file
func (api objectAPIHandlers) headObjectInArchiveFileHandler(ctx context.Context, objectAPI ObjectLayer, bucket, object string, w http.ResponseWriter, r *http.Request) {
	if crypto.S3.IsRequested(r.Header) || crypto.S3KMS.IsRequested(r.Header) { // If SSE-S3 or SSE-KMS present -> AWS fails with undefined error
		writeErrorResponseHeadersOnly(w, errorCodes.ToAPIErr(ErrBadRequest))
		return
	}

	zipPath, object, err := splitZipExtensionPath(object)
	if err != nil {
		writeErrorResponseHeadersOnly(w, toAPIError(ctx, err))
		return
	}

	getObjectInfo := objectAPI.GetObjectInfo

	opts, err := getOpts(ctx, r, bucket, zipPath)
	if err != nil {
		writeErrorResponseHeadersOnly(w, toAPIError(ctx, err))
		return
	}

	if s3Error := checkRequestAuthType(ctx, r, policy.GetObjectAction, bucket, zipPath); s3Error != ErrNone {
		if getRequestAuthType(r) == authTypeAnonymous {
			// As per "Permission" section in
			// https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectHEAD.html
			// If the object you request does not exist,
			// the error Amazon S3 returns depends on
			// whether you also have the s3:ListBucket
			// permission.
			// * If you have the s3:ListBucket permission
			//   on the bucket, Amazon S3 will return an
			//   HTTP status code 404 ("no such key")
			//   error.
			// * if you don’t have the s3:ListBucket
			//   permission, Amazon S3 will return an HTTP
			//   status code 403 ("access denied") error.`
			if globalPolicySys.IsAllowed(policy.BucketPolicyArgs{
				Action:          policy.ListBucketAction,
				BucketName:      bucket,
				ConditionValues: getConditionValues(r, "", auth.AnonymousCredentials),
				IsOwner:         false,
			}) {
				_, err = getObjectInfo(ctx, bucket, zipPath, opts)
				if toAPIError(ctx, err).Code == "NoSuchKey" {
					s3Error = ErrNoSuchKey
				}
			}
		}
		errCode := errorCodes.ToAPIErr(s3Error)
		w.Header().Set(xMinIOErrCodeHeader, errCode.Code)
		w.Header().Set(xMinIOErrDescHeader, "\""+errCode.Description+"\"")
		writeErrorResponseHeadersOnly(w, errCode)
		return
	}

	// Validate pre-conditions if any.
	opts.CheckPrecondFn = func(oi ObjectInfo) bool {
		return checkPreconditions(ctx, w, r, oi, opts)
	}

	// We do not allow offsetting into extracted files.
	if opts.PartNumber != 0 {
		writeErrorResponseHeadersOnly(w, errorCodes.ToAPIErr(ErrInvalidPartNumber))
		return
	}

	if r.Header.Get(xhttp.Range) != "" {
		writeErrorResponseHeadersOnly(w, errorCodes.ToAPIErr(ErrInvalidRange))
		return
	}

	zipObjInfo, err := getObjectInfo(ctx, bucket, zipPath, opts)
	if err != nil {
		writeErrorResponseHeadersOnly(w, toAPIError(ctx, err))
		return
	}

	zipInfo := zipObjInfo.ArchiveInfo(r.Header)
	if len(zipInfo) == 0 {
		opts.EncryptFn, err = zipObjInfo.metadataEncryptFn(r.Header)
		if err != nil {
			writeErrorResponseHeadersOnly(w, toAPIError(ctx, err))
			return
		}
		zipInfo, err = updateObjectMetadataWithZipInfo(ctx, objectAPI, bucket, zipPath, opts)
	}
	if err != nil {
		writeErrorResponseHeadersOnly(w, toAPIError(ctx, err))
		return
	}

	file, err := zipindex.FindSerialized(zipInfo, object)
	if err != nil {
		if err == io.EOF {
			writeErrorResponseHeadersOnly(w, errorCodes.ToAPIErr(ErrNoSuchKey))
		} else {
			writeErrorResponseHeadersOnly(w, toAPIError(ctx, err))
		}
		return
	}

	objInfo := ObjectInfo{
		Bucket:  bucket,
		Name:    file.Name,
		Size:    int64(file.UncompressedSize64),
		ModTime: zipObjInfo.ModTime,
	}

	// Set standard object headers.
	if err = setObjectHeaders(ctx, w, objInfo, nil, opts); err != nil {
		writeErrorResponseHeadersOnly(w, toAPIError(ctx, err))
		return
	}

	// s3zip does not allow ranges.
	w.Header().Del(xhttp.AcceptRanges)

	// Set any additional requested response headers.
	setHeadGetRespHeaders(w, r.Form)

	// Successful response.
	w.WriteHeader(http.StatusOK)
}

// Update the passed zip object metadata with the zip contents info, file name, modtime, size, etc.
// The returned zip index will de decrypted.
func updateObjectMetadataWithZipInfo(ctx context.Context, objectAPI ObjectLayer, bucket, object string, opts ObjectOptions) ([]byte, error) {
	files, srcInfo, err := getFilesListFromZIPObject(ctx, objectAPI, bucket, object, opts)
	if err != nil {
		return nil, err
	}
	files.OptimizeSize()
	zipInfo, err := files.Serialize()
	if err != nil {
		return nil, err
	}
	at := archiveType
	zipInfoStr := string(zipInfo)
	if opts.EncryptFn != nil {
		at = archiveTypeEnc
		zipInfoStr = string(opts.EncryptFn(archiveTypeEnc, zipInfo))
	}
	srcInfo.UserDefined[archiveTypeMetadataKey] = at
	popts := ObjectOptions{
		MTime:     srcInfo.ModTime,
		VersionID: srcInfo.VersionID,
		EvalMetadataFn: func(oi *ObjectInfo, gerr error) (dsc ReplicateDecision, err error) {
			oi.UserDefined[archiveTypeMetadataKey] = at
			oi.UserDefined[archiveInfoMetadataKey] = zipInfoStr
			return dsc, nil
		},
	}

	// For all other modes use in-place update to update metadata on a specific version.
	if _, err = objectAPI.PutObjectMetadata(ctx, bucket, object, popts); err != nil {
		return nil, err
	}

	return zipInfo, nil
}

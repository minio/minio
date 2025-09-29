// Copyright (c) 2015-2023 MinIO, Inc.
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
	"archive/tar"
	"context"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"net/textproto"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/klauspost/compress/gzhttp"
	miniogo "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/minio/minio-go/v7/pkg/encrypt"
	"github.com/minio/minio-go/v7/pkg/tags"
	"github.com/minio/minio/internal/amztime"
	"github.com/minio/minio/internal/auth"
	sse "github.com/minio/minio/internal/bucket/encryption"
	"github.com/minio/minio/internal/bucket/lifecycle"
	objectlock "github.com/minio/minio/internal/bucket/object/lock"
	"github.com/minio/minio/internal/bucket/replication"
	"github.com/minio/minio/internal/config/dns"
	"github.com/minio/minio/internal/config/storageclass"
	"github.com/minio/minio/internal/crypto"
	"github.com/minio/minio/internal/etag"
	"github.com/minio/minio/internal/event"
	"github.com/minio/minio/internal/handlers"
	"github.com/minio/minio/internal/hash"
	xhttp "github.com/minio/minio/internal/http"
	xioutil "github.com/minio/minio/internal/ioutil"
	"github.com/minio/minio/internal/kms"
	"github.com/minio/minio/internal/logger"
	"github.com/minio/minio/internal/s3select"
	"github.com/minio/mux"
	"github.com/minio/pkg/v3/policy"
)

// supportedHeadGetReqParams - supported request parameters for GET and HEAD presigned request.
var supportedHeadGetReqParams = map[string]string{
	"response-expires":             xhttp.Expires,
	"response-content-type":        xhttp.ContentType,
	"response-cache-control":       xhttp.CacheControl,
	"response-content-encoding":    xhttp.ContentEncoding,
	"response-content-language":    xhttp.ContentLanguage,
	"response-content-disposition": xhttp.ContentDisposition,
}

const (
	compressionAlgorithmV1 = "golang/snappy/LZ77"
	compressionAlgorithmV2 = "klauspost/compress/s2"

	// When an upload exceeds encryptBufferThreshold ...
	encryptBufferThreshold = 1 << 20
	// add an input buffer of this size.
	encryptBufferSize = 1 << 20

	// minCompressibleSize is the minimum size at which we enable compression.
	minCompressibleSize = 4096
)

// setHeadGetRespHeaders - set any requested parameters as response headers.
func setHeadGetRespHeaders(w http.ResponseWriter, reqParams url.Values) {
	for k, v := range reqParams {
		if header, ok := supportedHeadGetReqParams[strings.ToLower(k)]; ok {
			w.Header()[header] = []string{strings.Join(v, ",")}
		}
	}
}

// SelectObjectContentHandler - GET Object?select
// ----------
// This implementation of the GET operation retrieves object content based
// on an SQL expression. In the request, along with the sql expression, you must
// also specify a data serialization format (JSON, CSV) of the object.
func (api objectAPIHandlers) SelectObjectContentHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "SelectObject")

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	// Fetch object stat info.
	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrServerNotInitialized), r.URL)
		return
	}

	if crypto.S3.IsRequested(r.Header) || crypto.S3KMS.IsRequested(r.Header) { // If SSE-S3 or SSE-KMS present -> AWS fails with undefined error
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrBadRequest), r.URL)
		return
	}

	vars := mux.Vars(r)
	bucket := vars["bucket"]
	object, err := unescapePath(vars["object"])
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	opts, err := getOpts(ctx, r, bucket, object)
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	getObjectInfo := objectAPI.GetObjectInfo

	// Check for auth type to return S3 compatible error.
	// type to return the correct error (NoSuchKey vs AccessDenied)
	if s3Error := checkRequestAuthType(ctx, r, policy.GetObjectAction, bucket, object); s3Error != ErrNone {
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
				_, err = getObjectInfo(ctx, bucket, object, opts)
				if toAPIError(ctx, err).Code == "NoSuchKey" {
					s3Error = ErrNoSuchKey
				}
			}
		}
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(s3Error), r.URL)
		return
	}

	// Get request range.
	rangeHeader := r.Header.Get(xhttp.Range)
	if rangeHeader != "" {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrUnsupportedRangeHeader), r.URL)
		return
	}

	if r.ContentLength <= 0 {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrEmptyRequestBody), r.URL)
		return
	}

	// Take read lock on object, here so subsequent lower-level
	// calls do not need to.
	lock := objectAPI.NewNSLock(bucket, object)
	lkctx, err := lock.GetRLock(ctx, globalOperationTimeout)
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}
	ctx = lkctx.Context()
	defer lock.RUnlock(lkctx)

	getObjectNInfo := objectAPI.GetObjectNInfo

	gopts := opts
	gopts.NoLock = true // We already have a lock, we can live with it.
	objInfo, err := getObjectInfo(ctx, bucket, object, gopts)
	if err != nil {
		// Versioning enabled quite possibly object is deleted might be delete-marker
		// if present set the headers, no idea why AWS S3 sets these headers.
		if objInfo.VersionID != "" && objInfo.DeleteMarker {
			w.Header()[xhttp.AmzVersionID] = []string{objInfo.VersionID}
			w.Header()[xhttp.AmzDeleteMarker] = []string{strconv.FormatBool(objInfo.DeleteMarker)}
		}
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	// filter object lock metadata if permission does not permit
	getRetPerms := checkRequestAuthType(ctx, r, policy.GetObjectRetentionAction, bucket, object)
	legalHoldPerms := checkRequestAuthType(ctx, r, policy.GetObjectLegalHoldAction, bucket, object)

	// filter object lock metadata if permission does not permit
	objInfo.UserDefined = objectlock.FilterObjectLockMetadata(objInfo.UserDefined, getRetPerms != ErrNone, legalHoldPerms != ErrNone)

	if _, err = DecryptObjectInfo(&objInfo, r); err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	actualSize, err := objInfo.GetActualSize()
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	objectRSC := s3select.NewObjectReadSeekCloser(
		func(offset int64) (io.ReadCloser, error) {
			rs := &HTTPRangeSpec{
				IsSuffixLength: false,
				Start:          offset,
				End:            -1,
			}
			opts.NoLock = true
			return getObjectNInfo(ctx, bucket, object, rs, r.Header, opts)
		},
		actualSize,
	)
	defer objectRSC.Close()
	s3Select, err := s3select.NewS3Select(r.Body)
	if err != nil {
		if serr, ok := err.(s3select.SelectError); ok {
			encodedErrorResponse := encodeResponse(APIErrorResponse{
				Code:       serr.ErrorCode(),
				Message:    serr.ErrorMessage(),
				BucketName: bucket,
				Key:        object,
				Resource:   r.URL.Path,
				RequestID:  w.Header().Get(xhttp.AmzRequestID),
				HostID:     globalDeploymentID(),
			})
			writeResponse(w, serr.HTTPStatusCode(), encodedErrorResponse, mimeXML)
		} else {
			writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		}
		return
	}
	defer s3Select.Close()

	if err = s3Select.Open(objectRSC); err != nil {
		if serr, ok := err.(s3select.SelectError); ok {
			encodedErrorResponse := encodeResponse(APIErrorResponse{
				Code:       serr.ErrorCode(),
				Message:    serr.ErrorMessage(),
				BucketName: bucket,
				Key:        object,
				Resource:   r.URL.Path,
				RequestID:  w.Header().Get(xhttp.AmzRequestID),
				HostID:     globalDeploymentID(),
			})
			writeResponse(w, serr.HTTPStatusCode(), encodedErrorResponse, mimeXML)
		} else {
			writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		}
		return
	}

	// Set encryption response headers
	switch kind, _ := crypto.IsEncrypted(objInfo.UserDefined); kind {
	case crypto.S3:
		w.Header().Set(xhttp.AmzServerSideEncryption, xhttp.AmzEncryptionAES)
	case crypto.S3KMS:
		w.Header().Set(xhttp.AmzServerSideEncryption, xhttp.AmzEncryptionKMS)
		w.Header().Set(xhttp.AmzServerSideEncryptionKmsID, objInfo.KMSKeyID())
		if kmsCtx, ok := objInfo.UserDefined[crypto.MetaContext]; ok {
			w.Header().Set(xhttp.AmzServerSideEncryptionKmsContext, kmsCtx)
		}
	case crypto.SSEC:
		// Validate the SSE-C Key set in the header.
		if _, err = crypto.SSEC.UnsealObjectKey(r.Header, objInfo.UserDefined, bucket, object); err != nil {
			writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
			return
		}
		w.Header().Set(xhttp.AmzServerSideEncryptionCustomerAlgorithm, r.Header.Get(xhttp.AmzServerSideEncryptionCustomerAlgorithm))
		w.Header().Set(xhttp.AmzServerSideEncryptionCustomerKeyMD5, r.Header.Get(xhttp.AmzServerSideEncryptionCustomerKeyMD5))
	}

	s3Select.Evaluate(w)

	// Notify object accessed via a GET request.
	sendEvent(eventArgs{
		EventName:    event.ObjectAccessedGet,
		BucketName:   bucket,
		Object:       objInfo,
		ReqParams:    extractReqParams(r),
		RespElements: extractRespElements(w),
		UserAgent:    r.UserAgent(),
		Host:         handlers.GetSourceIP(r),
	})
}

func (api objectAPIHandlers) getObjectHandler(ctx context.Context, objectAPI ObjectLayer, bucket, object string, w http.ResponseWriter, r *http.Request) {
	if crypto.S3.IsRequested(r.Header) || crypto.S3KMS.IsRequested(r.Header) { // If SSE-S3 or SSE-KMS present -> AWS fails with undefined error
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrBadRequest), r.URL)
		return
	}

	opts, err := getOpts(ctx, r, bucket, object)
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	// Check for auth type to return S3 compatible error.
	// type to return the correct error (NoSuchKey vs AccessDenied)
	if s3Error := authenticateRequest(ctx, r, policy.GetObjectAction); s3Error != ErrNone {
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
				getObjectInfo := objectAPI.GetObjectInfo

				_, err = getObjectInfo(ctx, bucket, object, opts)
				if toAPIError(ctx, err).Code == "NoSuchKey" {
					s3Error = ErrNoSuchKey
				}
			}
		}
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(s3Error), r.URL)
		return
	}

	getObjectNInfo := objectAPI.GetObjectNInfo

	// Get request range.
	var rs *HTTPRangeSpec
	var rangeErr error
	rangeHeader := r.Header.Get(xhttp.Range)
	if rangeHeader != "" {
		// Both 'Range' and 'partNumber' cannot be specified at the same time
		if opts.PartNumber > 0 {
			writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrInvalidRangePartNumber), r.URL)
			return
		}

		rs, rangeErr = parseRequestRangeSpec(rangeHeader)
		// Handle only errInvalidRange. Ignore other
		// parse error and treat it as regular Get
		// request like Amazon S3.
		if errors.Is(rangeErr, errInvalidRange) {
			writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrInvalidRange), r.URL)
			return
		}
	}

	// Validate pre-conditions if any.
	opts.CheckPrecondFn = func(oi ObjectInfo) bool {
		if _, err := DecryptObjectInfo(&oi, r); err != nil {
			writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
			return true
		}

		if oi.UserTags != "" {
			r.Header.Set(xhttp.AmzObjectTagging, oi.UserTags)
		}

		if s3Error := authorizeRequest(ctx, r, policy.GetObjectAction); s3Error != ErrNone {
			writeErrorResponse(ctx, w, errorCodes.ToAPIErr(s3Error), r.URL)
			return true
		}

		return checkPreconditions(ctx, w, r, oi, opts)
	}

	opts.FastGetObjInfo = true

	var proxy proxyResult
	gr, err := getObjectNInfo(ctx, bucket, object, rs, r.Header, opts)
	if err != nil {
		var (
			reader *GetObjectReader
			perr   error
		)

		if (isErrObjectNotFound(err) || isErrVersionNotFound(err) || isErrReadQuorum(err)) && (gr == nil || !gr.ObjInfo.DeleteMarker) {
			proxytgts := getProxyTargets(ctx, bucket, object, opts)
			if !proxytgts.Empty() {
				globalReplicationStats.Load().incProxy(bucket, getObjectAPI, false)
				// proxy to replication target if active-active replication is in place.
				reader, proxy, perr = proxyGetToReplicationTarget(ctx, bucket, object, rs, r.Header, opts, proxytgts)
				if perr != nil {
					globalReplicationStats.Load().incProxy(bucket, getObjectAPI, true)
					proxyGetErr := ErrorRespToObjectError(perr, bucket, object)
					if !isErrBucketNotFound(proxyGetErr) && !isErrObjectNotFound(proxyGetErr) && !isErrVersionNotFound(proxyGetErr) &&
						!isErrPreconditionFailed(proxyGetErr) && !isErrInvalidRange(proxyGetErr) {
						replLogIf(ctx, fmt.Errorf("Proxying request (replication) failed for %s/%s(%s) - %w", bucket, object, opts.VersionID, perr))
					}
				}
				if reader != nil && proxy.Proxy && perr == nil {
					gr = reader
				}
			}
		}
		if reader == nil || !proxy.Proxy {
			// validate if the request indeed was authorized, if it wasn't we need to return "ErrAccessDenied"
			// instead of any namespace related error.
			if s3Error := authorizeRequest(ctx, r, policy.GetObjectAction); s3Error != ErrNone {
				writeErrorResponse(ctx, w, errorCodes.ToAPIErr(s3Error), r.URL)
				return
			}
			if isErrPreconditionFailed(err) {
				return
			}
			if proxy.Err != nil {
				writeErrorResponse(ctx, w, toAPIError(ctx, proxy.Err), r.URL)
				return
			}
			if gr != nil {
				if !gr.ObjInfo.VersionPurgeStatus.Empty() {
					// Shows the replication status of a permanent delete of a version
					w.Header()[xhttp.MinIODeleteReplicationStatus] = []string{string(gr.ObjInfo.VersionPurgeStatus)}
				}
				if !gr.ObjInfo.ReplicationStatus.Empty() && gr.ObjInfo.DeleteMarker {
					w.Header()[xhttp.MinIODeleteMarkerReplicationStatus] = []string{string(gr.ObjInfo.ReplicationStatus)}
				}

				// Versioning enabled quite possibly object is deleted might be delete-marker
				// if present set the headers, no idea why AWS S3 sets these headers.
				if gr.ObjInfo.VersionID != "" && gr.ObjInfo.DeleteMarker {
					w.Header()[xhttp.AmzVersionID] = []string{gr.ObjInfo.VersionID}
					w.Header()[xhttp.AmzDeleteMarker] = []string{strconv.FormatBool(gr.ObjInfo.DeleteMarker)}
				}
				QueueReplicationHeal(ctx, bucket, gr.ObjInfo, 0)
			}
			writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
			return
		}
	}
	defer gr.Close()

	objInfo := gr.ObjInfo

	if !proxy.Proxy { // apply lifecycle rules only for local requests
		// Automatically remove the object/version if an expiry lifecycle rule can be applied
		if lc, err := globalLifecycleSys.Get(bucket); err == nil {
			rcfg, err := globalBucketObjectLockSys.Get(bucket)
			if err != nil {
				writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
				return
			}
			replcfg, err := getReplicationConfig(ctx, bucket)
			if err != nil {
				writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
				return
			}
			event := evalActionFromLifecycle(ctx, *lc, rcfg, replcfg, objInfo)
			if event.Action.Delete() {
				// apply whatever the expiry rule is.
				applyExpiryRule(event, lcEventSrc_s3GetObject, objInfo)
				if !event.Action.DeleteRestored() {
					// If the ILM action is not on restored object return error.
					writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrNoSuchKey), r.URL)
					return
				}
			}
		}

		QueueReplicationHeal(ctx, bucket, gr.ObjInfo, 0)
	}

	// filter object lock metadata if permission does not permit
	getRetPerms := checkRequestAuthType(ctx, r, policy.GetObjectRetentionAction, bucket, object)
	legalHoldPerms := checkRequestAuthType(ctx, r, policy.GetObjectLegalHoldAction, bucket, object)

	// filter object lock metadata if permission does not permit
	objInfo.UserDefined = objectlock.FilterObjectLockMetadata(objInfo.UserDefined, getRetPerms != ErrNone, legalHoldPerms != ErrNone)

	// Set encryption response headers
	if kind, isEncrypted := crypto.IsEncrypted(objInfo.UserDefined); isEncrypted {
		switch kind {
		case crypto.S3:
			w.Header().Set(xhttp.AmzServerSideEncryption, xhttp.AmzEncryptionAES)
		case crypto.S3KMS:
			w.Header().Set(xhttp.AmzServerSideEncryption, xhttp.AmzEncryptionKMS)
			w.Header().Set(xhttp.AmzServerSideEncryptionKmsID, objInfo.KMSKeyID())
			if kmsCtx, ok := objInfo.UserDefined[crypto.MetaContext]; ok {
				w.Header().Set(xhttp.AmzServerSideEncryptionKmsContext, kmsCtx)
			}
		case crypto.SSEC:
			w.Header().Set(xhttp.AmzServerSideEncryptionCustomerAlgorithm, r.Header.Get(xhttp.AmzServerSideEncryptionCustomerAlgorithm))
			w.Header().Set(xhttp.AmzServerSideEncryptionCustomerKeyMD5, r.Header.Get(xhttp.AmzServerSideEncryptionCustomerKeyMD5))
		}
		objInfo.ETag = getDecryptedETag(r.Header, objInfo, false)
	}

	if r.Header.Get(xhttp.AmzChecksumMode) == "ENABLED" && rs == nil {
		// AWS S3 silently drops checksums on range requests.
		cs, _ := objInfo.decryptChecksums(opts.PartNumber, r.Header)
		hash.AddChecksumHeader(w, cs)
	}

	if err = setObjectHeaders(ctx, w, objInfo, rs, opts); err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	// Set Parts Count Header
	if opts.PartNumber > 0 && len(objInfo.Parts) > 0 {
		setPartsCountHeaders(w, objInfo)
	}

	setHeadGetRespHeaders(w, r.Form)

	var iw io.Writer = w

	statusCodeWritten := false
	httpWriter := xioutil.WriteOnClose(iw)
	if rs != nil || opts.PartNumber > 0 {
		statusCodeWritten = true
		w.WriteHeader(http.StatusPartialContent)
	}

	// Write object content to response body
	if _, err = xioutil.Copy(httpWriter, gr); err != nil {
		if !httpWriter.HasWritten() && !statusCodeWritten {
			// write error response only if no data or headers has been written to client yet
			writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
			return
		}
		return
	}

	if err = httpWriter.Close(); err != nil {
		if !httpWriter.HasWritten() && !statusCodeWritten { // write error response only if no data or headers has been written to client yet
			writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
			return
		}
		return
	}

	// Notify object accessed via a GET request.
	sendEvent(eventArgs{
		EventName:    event.ObjectAccessedGet,
		BucketName:   bucket,
		Object:       objInfo,
		ReqParams:    extractReqParams(r),
		RespElements: extractRespElements(w),
		UserAgent:    r.UserAgent(),
		Host:         handlers.GetSourceIP(r),
	})
}

// GetObjectAttributes ...
func (api objectAPIHandlers) getObjectAttributesHandler(ctx context.Context, objectAPI ObjectLayer, bucket, object string, w http.ResponseWriter, r *http.Request) {
	opts, valid := getAndValidateAttributesOpts(ctx, w, r, bucket, object)
	if !valid {
		return
	}

	var s3Error APIErrorCode
	if opts.VersionID != "" {
		s3Error = checkRequestAuthType(ctx, r, policy.GetObjectVersionAttributesAction, bucket, object)
		if s3Error == ErrNone {
			s3Error = checkRequestAuthType(ctx, r, policy.GetObjectVersionAction, bucket, object)
		}
	} else {
		s3Error = checkRequestAuthType(ctx, r, policy.GetObjectAttributesAction, bucket, object)
		if s3Error == ErrNone {
			s3Error = checkRequestAuthType(ctx, r, policy.GetObjectAction, bucket, object)
		}
	}

	if s3Error != ErrNone {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(s3Error), r.URL)
		return
	}

	objInfo, err := objectAPI.GetObjectInfo(ctx, bucket, object, opts)
	if err != nil {
		s3Error = checkRequestAuthType(ctx, r, policy.ListBucketAction, bucket, object)
		if s3Error == ErrNone {
			writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
			return
		}
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrAccessDenied), r.URL)
		return
	}

	if _, err = DecryptObjectInfo(&objInfo, r); err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	if checkPreconditions(ctx, w, r, objInfo, opts) {
		return
	}

	OA := new(getObjectAttributesResponse)

	if opts.Versioned {
		w.Header().Set(xhttp.AmzVersionID, objInfo.VersionID)
	}

	lastModified := objInfo.ModTime.UTC().Format(http.TimeFormat)
	w.Header().Set(xhttp.LastModified, lastModified)
	w.Header().Del(xhttp.ContentType)

	if _, ok := opts.ObjectAttributes[xhttp.Checksum]; ok {
		chkSums, _ := objInfo.decryptChecksums(0, r.Header)
		// AWS does not appear to append part number on this API call.
		if len(chkSums) > 0 {
			OA.Checksum = &objectAttributesChecksum{
				ChecksumCRC32:     strings.Split(chkSums["CRC32"], "-")[0],
				ChecksumCRC32C:    strings.Split(chkSums["CRC32C"], "-")[0],
				ChecksumSHA1:      strings.Split(chkSums["SHA1"], "-")[0],
				ChecksumSHA256:    strings.Split(chkSums["SHA256"], "-")[0],
				ChecksumCRC64NVME: strings.Split(chkSums["CRC64NVME"], "-")[0],
				ChecksumType:      chkSums[xhttp.AmzChecksumType],
			}
		}
	}

	if _, ok := opts.ObjectAttributes[xhttp.ETag]; ok {
		OA.ETag = objInfo.ETag
	}

	if _, ok := opts.ObjectAttributes[xhttp.ObjectSize]; ok {
		OA.ObjectSize, _ = objInfo.GetActualSize()
	}

	if _, ok := opts.ObjectAttributes[xhttp.StorageClass]; ok {
		OA.StorageClass = filterStorageClass(ctx, objInfo.StorageClass)
	}

	objInfo.decryptPartsChecksums(r.Header)

	if _, ok := opts.ObjectAttributes[xhttp.ObjectParts]; ok {
		OA.ObjectParts = new(objectAttributesParts)
		OA.ObjectParts.PartNumberMarker = opts.PartNumberMarker

		OA.ObjectParts.MaxParts = opts.MaxParts
		partsLength := len(objInfo.Parts)
		OA.ObjectParts.PartsCount = partsLength

		if opts.MaxParts > -1 {
			for i, v := range objInfo.Parts {
				if v.Number <= opts.PartNumberMarker {
					continue
				}

				if len(OA.ObjectParts.Parts) == opts.MaxParts {
					break
				}

				OA.ObjectParts.NextPartNumberMarker = v.Number
				OA.ObjectParts.Parts = append(OA.ObjectParts.Parts, &objectAttributesPart{
					ChecksumSHA1:      objInfo.Parts[i].Checksums["SHA1"],
					ChecksumSHA256:    objInfo.Parts[i].Checksums["SHA256"],
					ChecksumCRC32:     objInfo.Parts[i].Checksums["CRC32"],
					ChecksumCRC32C:    objInfo.Parts[i].Checksums["CRC32C"],
					ChecksumCRC64NVME: objInfo.Parts[i].Checksums["CRC64NVME"],
					PartNumber:        objInfo.Parts[i].Number,
					Size:              objInfo.Parts[i].Size,
				})
			}
		}

		if OA.ObjectParts.NextPartNumberMarker != partsLength {
			OA.ObjectParts.IsTruncated = true
		}
	}

	writeSuccessResponseXML(w, encodeResponse(OA))

	sendEvent(eventArgs{
		EventName:    event.ObjectAccessedAttributes,
		BucketName:   bucket,
		Object:       objInfo,
		ReqParams:    extractReqParams(r),
		RespElements: extractRespElements(w),
		UserAgent:    r.UserAgent(),
		Host:         handlers.GetSourceIP(r),
	})
}

// GetObjectHandler - GET Object
// ----------
// This implementation of the GET operation retrieves object. To use GET,
// you must have READ access to the object.
func (api objectAPIHandlers) GetObjectHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "GetObject")

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrServerNotInitialized), r.URL)
		return
	}

	vars := mux.Vars(r)
	bucket := vars["bucket"]
	object, err := unescapePath(vars["object"])
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}
	if !globalAPIConfig.shouldGzipObjects() {
		w.Header().Set(gzhttp.HeaderNoCompression, "true")
	}

	if r.Header.Get(xMinIOExtract) == "true" && strings.Contains(object, archivePattern) {
		api.getObjectInArchiveFileHandler(ctx, objectAPI, bucket, object, w, r)
	} else {
		api.getObjectHandler(ctx, objectAPI, bucket, object, w, r)
	}
}

func (api objectAPIHandlers) headObjectHandler(ctx context.Context, objectAPI ObjectLayer, bucket, object string, w http.ResponseWriter, r *http.Request) {
	if crypto.S3.IsRequested(r.Header) || crypto.S3KMS.IsRequested(r.Header) { // If SSE-S3 or SSE-KMS present -> AWS fails with undefined error
		writeErrorResponseHeadersOnly(w, errorCodes.ToAPIErr(ErrBadRequest))
		return
	}

	getObjectInfo := objectAPI.GetObjectInfo

	opts, err := getOpts(ctx, r, bucket, object)
	if err != nil {
		writeErrorResponseHeadersOnly(w, toAPIError(ctx, err))
		return
	}

	// Check for auth type to return S3 compatible error.
	// type to return the correct error (NoSuchKey vs AccessDenied)
	if s3Error := authenticateRequest(ctx, r, policy.GetObjectAction); s3Error != ErrNone {
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
				getObjectInfo := objectAPI.GetObjectInfo

				_, err = getObjectInfo(ctx, bucket, object, opts)
				if toAPIError(ctx, err).Code == "NoSuchKey" {
					s3Error = ErrNoSuchKey
				}
			}
		}
		writeErrorResponseHeadersOnly(w, errorCodes.ToAPIErr(s3Error))
		return
	}

	// Get request range.
	var rs *HTTPRangeSpec
	rangeHeader := r.Header.Get(xhttp.Range)
	if rangeHeader != "" {
		rs, _ = parseRequestRangeSpec(rangeHeader)
	}

	if rangeHeader != "" {
		// Both 'Range' and 'partNumber' cannot be specified at the same time
		if opts.PartNumber > 0 {
			writeErrorResponseHeadersOnly(w, errorCodes.ToAPIErr(ErrInvalidRangePartNumber))
			return
		}

		if rs, err = parseRequestRangeSpec(rangeHeader); err != nil {
			// Handle only errInvalidRange. Ignore other
			// parse error and treat it as regular Get
			// request like Amazon S3.
			if errors.Is(err, errInvalidRange) {
				writeErrorResponseHeadersOnly(w, errorCodes.ToAPIErr(ErrInvalidRange))
				return
			}
		}
	}

	opts.FastGetObjInfo = true

	objInfo, err := getObjectInfo(ctx, bucket, object, opts)
	var proxy proxyResult
	if err != nil && !objInfo.DeleteMarker && (isErrObjectNotFound(err) || isErrVersionNotFound(err) || isErrReadQuorum(err)) {
		// proxy HEAD to replication target if active-active replication configured on bucket
		proxytgts := getProxyTargets(ctx, bucket, object, opts)
		if !proxytgts.Empty() {
			globalReplicationStats.Load().incProxy(bucket, headObjectAPI, false)
			var oi ObjectInfo
			oi, proxy = proxyHeadToReplicationTarget(ctx, bucket, object, rs, opts, proxytgts)
			if proxy.Proxy {
				objInfo = oi
			}
			if proxy.Err != nil {
				globalReplicationStats.Load().incProxy(bucket, headObjectAPI, true)
				writeErrorResponseHeadersOnly(w, toAPIError(ctx, proxy.Err))
				return
			}
		}
	}

	if objInfo.UserTags != "" {
		// Set this such that authorization policies can be applied on the object tags.
		r.Header.Set(xhttp.AmzObjectTagging, objInfo.UserTags)
	}

	if s3Error := authorizeRequest(ctx, r, policy.GetObjectAction); s3Error != ErrNone {
		writeErrorResponseHeadersOnly(w, errorCodes.ToAPIErr(s3Error))
		return
	}

	if err != nil && !proxy.Proxy {
		switch {
		case !objInfo.VersionPurgeStatus.Empty():
			w.Header()[xhttp.MinIODeleteReplicationStatus] = []string{string(objInfo.VersionPurgeStatus)}
		case !objInfo.ReplicationStatus.Empty() && objInfo.DeleteMarker:
			w.Header()[xhttp.MinIODeleteMarkerReplicationStatus] = []string{string(objInfo.ReplicationStatus)}
		}
		// Versioning enabled quite possibly object is deleted might be delete-marker
		// if present set the headers, no idea why AWS S3 sets these headers.
		if objInfo.VersionID != "" && objInfo.DeleteMarker {
			w.Header()[xhttp.AmzVersionID] = []string{objInfo.VersionID}
			w.Header()[xhttp.AmzDeleteMarker] = []string{strconv.FormatBool(objInfo.DeleteMarker)}
		}

		QueueReplicationHeal(ctx, bucket, objInfo, 0)
		// do an additional verification whether object exists when object is deletemarker and request
		// is from replication
		if opts.CheckDMReplicationReady {
			topts := opts
			topts.VersionID = ""
			goi, gerr := getObjectInfo(ctx, bucket, object, topts)
			if gerr == nil || goi.VersionID != "" { // object layer returned more info because object is deleted
				w.Header().Set(xhttp.MinIOTargetReplicationReady, "true")
			}
		}

		writeErrorResponseHeadersOnly(w, toAPIError(ctx, err))
		return
	}

	if !proxy.Proxy { // apply lifecycle rules only locally not for proxied requests
		// Automatically remove the object/version if an expiry lifecycle rule can be applied
		if lc, err := globalLifecycleSys.Get(bucket); err == nil {
			rcfg, err := globalBucketObjectLockSys.Get(bucket)
			if err != nil {
				writeErrorResponseHeadersOnly(w, toAPIError(ctx, err))
				return
			}
			replcfg, err := getReplicationConfig(ctx, bucket)
			if err != nil {
				writeErrorResponseHeadersOnly(w, toAPIError(ctx, err))
				return
			}
			event := evalActionFromLifecycle(ctx, *lc, rcfg, replcfg, objInfo)
			if event.Action.Delete() {
				// apply whatever the expiry rule is.
				applyExpiryRule(event, lcEventSrc_s3HeadObject, objInfo)
				if !event.Action.DeleteRestored() {
					// If the ILM action is not on restored object return error.
					writeErrorResponseHeadersOnly(w, errorCodes.ToAPIErr(ErrNoSuchKey))
					return
				}
			}
		}
		QueueReplicationHeal(ctx, bucket, objInfo, 0)
	}

	// filter object lock metadata if permission does not permit
	getRetPerms := checkRequestAuthType(ctx, r, policy.GetObjectRetentionAction, bucket, object)
	legalHoldPerms := checkRequestAuthType(ctx, r, policy.GetObjectLegalHoldAction, bucket, object)

	// filter object lock metadata if permission does not permit
	objInfo.UserDefined = objectlock.FilterObjectLockMetadata(objInfo.UserDefined, getRetPerms != ErrNone, legalHoldPerms != ErrNone)

	if _, err = DecryptObjectInfo(&objInfo, r); err != nil {
		writeErrorResponseHeadersOnly(w, toAPIError(ctx, err))
		return
	}

	// Validate pre-conditions if any.
	if checkPreconditions(ctx, w, r, objInfo, opts) {
		return
	}

	// Set encryption response headers
	switch kind, _ := crypto.IsEncrypted(objInfo.UserDefined); kind {
	case crypto.S3:
		w.Header().Set(xhttp.AmzServerSideEncryption, xhttp.AmzEncryptionAES)
	case crypto.S3KMS:
		w.Header().Set(xhttp.AmzServerSideEncryption, xhttp.AmzEncryptionKMS)
		w.Header().Set(xhttp.AmzServerSideEncryptionKmsID, objInfo.KMSKeyID())
		if kmsCtx, ok := objInfo.UserDefined[crypto.MetaContext]; ok {
			w.Header().Set(xhttp.AmzServerSideEncryptionKmsContext, kmsCtx)
		}
	case crypto.SSEC:
		// Validate the SSE-C Key set in the header.
		if _, err = crypto.SSEC.UnsealObjectKey(r.Header, objInfo.UserDefined, bucket, object); err != nil {
			writeErrorResponseHeadersOnly(w, toAPIError(ctx, err))
			return
		}
		w.Header().Set(xhttp.AmzServerSideEncryptionCustomerAlgorithm, r.Header.Get(xhttp.AmzServerSideEncryptionCustomerAlgorithm))
		w.Header().Set(xhttp.AmzServerSideEncryptionCustomerKeyMD5, r.Header.Get(xhttp.AmzServerSideEncryptionCustomerKeyMD5))
	}

	if r.Header.Get(xhttp.AmzChecksumMode) == "ENABLED" && rs == nil {
		// AWS S3 silently drops checksums on range requests.
		cs, _ := objInfo.decryptChecksums(opts.PartNumber, r.Header)
		hash.AddChecksumHeader(w, cs)
	}

	// Set standard object headers.
	if err = setObjectHeaders(ctx, w, objInfo, rs, opts); err != nil {
		writeErrorResponseHeadersOnly(w, toAPIError(ctx, err))
		return
	}

	// Set Parts Count Header
	if opts.PartNumber > 0 && len(objInfo.Parts) > 0 {
		setPartsCountHeaders(w, objInfo)
	}

	// Set any additional requested response headers.
	setHeadGetRespHeaders(w, r.Form)

	// Successful response.
	if rs != nil || opts.PartNumber > 0 {
		w.WriteHeader(http.StatusPartialContent)
	} else {
		w.WriteHeader(http.StatusOK)
	}

	// Notify object accessed via a HEAD request.
	sendEvent(eventArgs{
		EventName:    event.ObjectAccessedHead,
		BucketName:   bucket,
		Object:       objInfo,
		ReqParams:    extractReqParams(r),
		RespElements: extractRespElements(w),
		UserAgent:    r.UserAgent(),
		Host:         handlers.GetSourceIP(r),
	})
}

// GetObjectAttributesHandles - GET Object
// -----------
// This operation retrieves metadata and part metadata from an object without returning the object itself.
func (api objectAPIHandlers) GetObjectAttributesHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "GetObjectAttributes")
	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponseHeadersOnly(w, errorCodes.ToAPIErr(ErrServerNotInitialized))
		return
	}

	vars := mux.Vars(r)
	bucket := vars["bucket"]
	object, err := unescapePath(vars["object"])
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	api.getObjectAttributesHandler(ctx, objectAPI, bucket, object, w, r)
}

// HeadObjectHandler - HEAD Object
// -----------
// The HEAD operation retrieves metadata from an object without returning the object itself.
func (api objectAPIHandlers) HeadObjectHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "HeadObject")
	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponseHeadersOnly(w, errorCodes.ToAPIErr(ErrServerNotInitialized))
		return
	}

	vars := mux.Vars(r)
	bucket := vars["bucket"]
	object, err := unescapePath(vars["object"])
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	if r.Header.Get(xMinIOExtract) == "true" && strings.Contains(object, archivePattern) {
		api.headObjectInArchiveFileHandler(ctx, objectAPI, bucket, object, w, r)
	} else {
		api.headObjectHandler(ctx, objectAPI, bucket, object, w, r)
	}
}

// Extract metadata relevant for an CopyObject operation based on conditional
// header values specified in X-Amz-Metadata-Directive.
func getCpObjMetadataFromHeader(ctx context.Context, r *http.Request, userMeta map[string]string) (map[string]string, error) {
	// Make a copy of the supplied metadata to avoid
	// to change the original one.
	defaultMeta := make(map[string]string, len(userMeta))
	for k, v := range userMeta {
		// skip tier metadata when copying metadata from source object
		switch k {
		case metaTierName, metaTierStatus, metaTierObjName, metaTierVersionID:
			continue
		}
		defaultMeta[k] = v
	}

	// remove SSE Headers from source info
	crypto.RemoveSSEHeaders(defaultMeta)

	// Storage class is special, it can be replaced regardless of the
	// metadata directive, if set should be preserved and replaced
	// to the destination metadata.
	sc := r.Header.Get(xhttp.AmzStorageClass)
	if sc == "" {
		sc = r.Form.Get(xhttp.AmzStorageClass)
	}

	// if x-amz-metadata-directive says REPLACE then
	// we extract metadata from the input headers.
	if isDirectiveReplace(r.Header.Get(xhttp.AmzMetadataDirective)) {
		emetadata, err := extractMetadataFromReq(ctx, r)
		if err != nil {
			return nil, err
		}
		if sc != "" {
			emetadata[xhttp.AmzStorageClass] = sc
		}
		return emetadata, nil
	}

	if sc != "" {
		defaultMeta[xhttp.AmzStorageClass] = sc
	}

	// if x-amz-metadata-directive says COPY then we
	// return the default metadata.
	if isDirectiveCopy(r.Header.Get(xhttp.AmzMetadataDirective)) {
		return defaultMeta, nil
	}

	// Copy is default behavior if not x-amz-metadata-directive is set.
	return defaultMeta, nil
}

// getRemoteInstanceTransport contains a roundtripper for external (not peers) servers
var remoteInstanceTransport atomic.Value

func setRemoteInstanceTransport(tr http.RoundTripper) {
	remoteInstanceTransport.Store(tr)
}

func getRemoteInstanceTransport() http.RoundTripper {
	rt, ok := remoteInstanceTransport.Load().(http.RoundTripper)
	if ok {
		return rt
	}
	return nil
}

// Returns a minio-go Client configured to access remote host described by destDNSRecord
// Applicable only in a federated deployment
var getRemoteInstanceClient = func(r *http.Request, host string) (*miniogo.Core, error) {
	cred := getReqAccessCred(r, globalSite.Region())
	// In a federated deployment, all the instances share config files
	// and hence expected to have same credentials.
	core, err := miniogo.NewCore(host, &miniogo.Options{
		Creds:     credentials.NewStaticV4(cred.AccessKey, cred.SecretKey, ""),
		Secure:    globalIsTLS,
		Transport: getRemoteInstanceTransport(),
	})
	if err != nil {
		return nil, err
	}
	core.SetAppInfo("minio-federated", ReleaseTag)
	return core, nil
}

// Check if the destination bucket is on a remote site, this code only gets executed
// when federation is enabled, ie when globalDNSConfig is non 'nil'.
//
// This function is similar to isRemoteCallRequired but specifically for COPY object API
// if destination and source are same we do not need to check for destination bucket
// to exist locally.
func isRemoteCopyRequired(ctx context.Context, srcBucket, dstBucket string, objAPI ObjectLayer) bool {
	if srcBucket == dstBucket {
		return false
	}
	return isRemoteCallRequired(ctx, dstBucket, objAPI)
}

// Check if the bucket is on a remote site, this code only gets executed when federation is enabled.
func isRemoteCallRequired(ctx context.Context, bucket string, objAPI ObjectLayer) bool {
	if globalDNSConfig == nil {
		return false
	}
	if globalBucketFederation {
		_, err := objAPI.GetBucketInfo(ctx, bucket, BucketOptions{})
		return err == toObjectErr(errVolumeNotFound, bucket)
	}
	return false
}

// CopyObjectHandler - Copy Object
// ----------
// This implementation of the PUT operation adds an object to a bucket
// while reading the object from another source.
// Notice: The S3 client can send secret keys in headers for encryption related jobs,
// the handler should ensure to remove these keys before sending them to the object layer.
// Currently these keys are:
//   - X-Amz-Server-Side-Encryption-Customer-Key
//   - X-Amz-Copy-Source-Server-Side-Encryption-Customer-Key
func (api objectAPIHandlers) CopyObjectHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "CopyObject")

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrServerNotInitialized), r.URL)
		return
	}

	vars := mux.Vars(r)
	dstBucket := vars["bucket"]
	dstObject, err := unescapePath(vars["object"])
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	if s3Error := checkRequestAuthType(ctx, r, policy.PutObjectAction, dstBucket, dstObject); s3Error != ErrNone {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(s3Error), r.URL)
		return
	}

	// Read escaped copy source path to check for parameters.
	cpSrcPath := r.Header.Get(xhttp.AmzCopySource)
	var vid string
	if u, err := url.Parse(cpSrcPath); err == nil {
		vid = strings.TrimSpace(u.Query().Get(xhttp.VersionID))
		// Note that url.Parse does the unescaping
		cpSrcPath = u.Path
	}

	srcBucket, srcObject := path2BucketObject(cpSrcPath)
	// If source object is empty or bucket is empty, reply back invalid copy source.
	if srcObject == "" || srcBucket == "" {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrInvalidCopySource), r.URL)
		return
	}

	// Sanitize the source object name similar to NewMultipart and PutObject API
	srcObject = trimLeadingSlash(srcObject)

	if vid != "" && vid != nullVersionID {
		_, err := uuid.Parse(vid)
		if err != nil {
			writeErrorResponse(ctx, w, toAPIError(ctx, VersionNotFound{
				Bucket:    srcBucket,
				Object:    srcObject,
				VersionID: vid,
			}), r.URL)
			return
		}
	}

	if s3Error := checkRequestAuthType(ctx, r, policy.GetObjectAction, srcBucket, srcObject); s3Error != ErrNone {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(s3Error), r.URL)
		return
	}

	// Check if metadata directive is valid.
	if !isDirectiveValid(r.Header.Get(xhttp.AmzMetadataDirective)) {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrInvalidMetadataDirective), r.URL)
		return
	}

	// check if tag directive is valid
	if !isDirectiveValid(r.Header.Get(xhttp.AmzTagDirective)) {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrInvalidTagDirective), r.URL)
		return
	}

	// Validate storage class metadata if present
	dstSc := r.Header.Get(xhttp.AmzStorageClass)
	if dstSc != "" && !storageclass.IsValid(dstSc) {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrInvalidStorageClass), r.URL)
		return
	}

	// Check if bucket encryption is enabled
	sseConfig, _ := globalBucketSSEConfigSys.Get(dstBucket)
	sseConfig.Apply(r.Header, sse.ApplyOptions{
		AutoEncrypt: globalAutoEncryption,
	})

	var srcOpts, dstOpts ObjectOptions
	srcOpts, err = copySrcOpts(ctx, r, srcBucket, srcObject)
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}
	srcOpts.VersionID = vid

	// convert copy src encryption options for GET calls
	getOpts := ObjectOptions{
		VersionID:          srcOpts.VersionID,
		Versioned:          srcOpts.Versioned,
		VersionSuspended:   srcOpts.VersionSuspended,
		ReplicationRequest: r.Header.Get(xhttp.MinIOSourceReplicationRequest) == "true",
	}
	getSSE := encrypt.SSE(srcOpts.ServerSideEncryption)
	if getSSE != srcOpts.ServerSideEncryption {
		getOpts.ServerSideEncryption = getSSE
	}

	dstOpts, err = copyDstOpts(ctx, r, dstBucket, dstObject, nil)
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}
	cpSrcDstSame := isStringEqual(pathJoin(srcBucket, srcObject), pathJoin(dstBucket, dstObject))

	getObjectNInfo := objectAPI.GetObjectNInfo

	checkCopyPrecondFn := func(o ObjectInfo) bool {
		if _, err := DecryptObjectInfo(&o, r); err != nil {
			writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
			return true
		}
		return checkCopyObjectPreconditions(ctx, w, r, o)
	}
	getOpts.CheckPrecondFn = checkCopyPrecondFn
	if cpSrcDstSame {
		getOpts.NoLock = true
	}

	var rs *HTTPRangeSpec
	gr, err := getObjectNInfo(ctx, srcBucket, srcObject, rs, r.Header, getOpts)
	if err != nil {
		if isErrPreconditionFailed(err) {
			return
		}

		// Versioning enabled quite possibly object is deleted might be delete-marker
		// if present set the headers, no idea why AWS S3 sets these headers.
		if gr != nil && gr.ObjInfo.VersionID != "" && gr.ObjInfo.DeleteMarker {
			w.Header()[xhttp.AmzVersionID] = []string{gr.ObjInfo.VersionID}
			w.Header()[xhttp.AmzDeleteMarker] = []string{strconv.FormatBool(gr.ObjInfo.DeleteMarker)}
		}

		// Update context bucket & object names for correct S3 XML error response
		reqInfo := logger.GetReqInfo(ctx)
		reqInfo.BucketName = srcBucket
		reqInfo.ObjectName = srcObject
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}
	defer gr.Close()
	srcInfo := gr.ObjInfo

	// maximum Upload size for object in a single CopyObject operation.
	if isMaxObjectSize(srcInfo.Size) {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrEntityTooLarge), r.URL)
		return
	}

	// We have to copy metadata only if source and destination are same.
	// this changes for encryption which can be observed below.
	if cpSrcDstSame {
		srcInfo.metadataOnly = true
	}

	var chStorageClass bool
	if dstSc != "" && dstSc != srcInfo.StorageClass {
		chStorageClass = true
		srcInfo.metadataOnly = false
	} // no changes in storage-class expected so its a metadataonly operation.

	var reader io.Reader = gr

	// Set the actual size to the compressed/decrypted size if encrypted.
	actualSize, err := srcInfo.GetActualSize()
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}
	length := actualSize

	if !cpSrcDstSame {
		if err := enforceBucketQuotaHard(ctx, dstBucket, actualSize); err != nil {
			writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
			return
		}
	}

	var compressMetadata map[string]string
	// No need to compress for remote etcd calls
	// Pass the decompressed stream to such calls.
	isDstCompressed := isCompressible(r.Header, dstObject) &&
		length > minCompressibleSize &&
		!isRemoteCopyRequired(ctx, srcBucket, dstBucket, objectAPI)
	if isDstCompressed {
		compressMetadata = make(map[string]string, 2)
		// Preserving the compression metadata.
		compressMetadata[ReservedMetadataPrefix+"compression"] = compressionAlgorithmV2
		compressMetadata[ReservedMetadataPrefix+"actual-size"] = strconv.FormatInt(actualSize, 10)

		reader = etag.NewReader(ctx, reader, nil, nil)
		wantEncryption := crypto.Requested(r.Header)
		s2c, cb := newS2CompressReader(reader, actualSize, wantEncryption)
		dstOpts.IndexCB = cb
		defer s2c.Close()
		reader = etag.Wrap(s2c, reader)
		length = -1
	} else {
		delete(srcInfo.UserDefined, ReservedMetadataPrefix+"compression")
		delete(srcInfo.UserDefined, ReservedMetadataPrefix+"actual-size")
		reader = gr
	}

	srcInfo.Reader, err = hash.NewReader(ctx, reader, length, "", "", actualSize)
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	pReader := NewPutObjReader(srcInfo.Reader)

	// Handle encryption
	encMetadata := make(map[string]string)
	// Encryption parameters not applicable for this object.
	if _, ok := crypto.IsEncrypted(srcInfo.UserDefined); !ok && crypto.SSECopy.IsRequested(r.Header) {
		writeErrorResponse(ctx, w, toAPIError(ctx, errInvalidEncryptionParameters), r.URL)
		return
	}
	// Encryption parameters not present for this object.
	if crypto.SSEC.IsEncrypted(srcInfo.UserDefined) && !crypto.SSECopy.IsRequested(r.Header) && r.Header.Get(xhttp.MinIOSourceReplicationRequest) != "true" {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrInvalidSSECustomerAlgorithm), r.URL)
		return
	}

	var oldKey, newKey []byte
	var newKeyID string
	var kmsCtx kms.Context
	var objEncKey crypto.ObjectKey
	sseCopyKMS := crypto.S3KMS.IsEncrypted(srcInfo.UserDefined)
	sseCopyS3 := crypto.S3.IsEncrypted(srcInfo.UserDefined)
	sseCopyC := crypto.SSEC.IsEncrypted(srcInfo.UserDefined) && crypto.SSECopy.IsRequested(r.Header)
	sseC := crypto.SSEC.IsRequested(r.Header)
	sseS3 := crypto.S3.IsRequested(r.Header)
	sseKMS := crypto.S3KMS.IsRequested(r.Header)

	isSourceEncrypted := sseCopyC || sseCopyS3 || sseCopyKMS
	isTargetEncrypted := sseC || sseS3 || sseKMS

	if sseC {
		newKey, err = ParseSSECustomerRequest(r)
		if err != nil {
			writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
			return
		}
	}
	if crypto.S3KMS.IsRequested(r.Header) {
		newKeyID, kmsCtx, err = crypto.S3KMS.ParseHTTP(r.Header)
		if err != nil {
			writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
			return
		}
	}

	// If src == dst and either
	// - the object is encrypted using SSE-C and two different SSE-C keys are present
	// - the object is encrypted using SSE-S3 and the SSE-S3 header is present
	// - the object storage class is not changing
	// then execute a key rotation.
	if cpSrcDstSame && (sseCopyC && sseC) && !chStorageClass {
		oldKey, err = ParseSSECopyCustomerRequest(r.Header, srcInfo.UserDefined)
		if err != nil {
			writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
			return
		}

		for k, v := range srcInfo.UserDefined {
			if stringsHasPrefixFold(k, ReservedMetadataPrefixLower) {
				encMetadata[k] = v
			}
		}

		if err = rotateKey(ctx, oldKey, newKeyID, newKey, srcBucket, srcObject, encMetadata, kmsCtx); err != nil {
			writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
			return
		}

		// Since we are rotating the keys, make sure to update the metadata.
		srcInfo.metadataOnly = true
		srcInfo.keyRotation = true
	} else {
		if isSourceEncrypted || isTargetEncrypted {
			// We are not only copying just metadata instead
			// we are creating a new object at this point, even
			// if source and destination are same objects.
			if !srcInfo.keyRotation {
				srcInfo.metadataOnly = false
			}
		}

		// Calculate the size of the target object
		var targetSize int64

		switch {
		case isDstCompressed:
			targetSize = -1
		case !isSourceEncrypted && !isTargetEncrypted:
			targetSize, _ = srcInfo.GetActualSize()
		case isSourceEncrypted && isTargetEncrypted:
			objInfo := ObjectInfo{Size: actualSize}
			targetSize = objInfo.EncryptedSize()
		case !isSourceEncrypted && isTargetEncrypted:
			targetSize = srcInfo.EncryptedSize()
		case isSourceEncrypted && !isTargetEncrypted:
			targetSize, _ = srcInfo.DecryptedSize()
		}

		// Client can request that a different type of checksum is computed server-side for the
		// destination object using the x-amz-checksum-algorithm header.
		headerChecksumType := hash.NewChecksumHeader(r.Header)
		if headerChecksumType.IsSet() {
			dstOpts.WantServerSideChecksumType = headerChecksumType.Base()
			srcInfo.Reader.AddServerSideChecksumHasher(headerChecksumType)
			dstOpts.WantChecksum = nil
		} else {
			// Check the source object for checksum.
			// If Checksum is not encrypted, decryptChecksum will be a no-op and return
			// the already unencrypted value.
			srcChecksumDecrypted, err := srcInfo.decryptChecksum(r.Header)
			if err != nil {
				encLogOnceIf(GlobalContext,
					fmt.Errorf("Unable to decryptChecksum for object: %s/%s, error: %w", srcBucket, srcObject, err),
					"copy-object-decrypt-checksums-"+srcBucket+srcObject)
			}

			// The source object has a checksum set, we need the destination to have one too.
			if srcChecksumDecrypted != nil {
				dstOpts.WantChecksum = hash.ChecksumFromBytes(srcChecksumDecrypted)

				// When an object is being copied from a source that is multipart, the destination will
				// no longer be multipart, and thus the checksum becomes full-object instead. Since
				// the CopyObject API does not require that the caller send us this final checksum, we need
				// to compute it server-side, with the same type as the source object.
				if dstOpts.WantChecksum != nil && dstOpts.WantChecksum.Type.IsMultipartComposite() {
					dstOpts.WantServerSideChecksumType = dstOpts.WantChecksum.Type.Base()
					srcInfo.Reader.AddServerSideChecksumHasher(dstOpts.WantServerSideChecksumType)
					dstOpts.WantChecksum = nil
				}
			} else {
				// S3: All copied objects without checksums and specified destination checksum algorithms
				// automatically gain a CRC-64NVME checksum algorithm.
				dstOpts.WantServerSideChecksumType = hash.ChecksumCRC64NVME
				srcInfo.Reader.AddServerSideChecksumHasher(dstOpts.WantServerSideChecksumType)
				dstOpts.WantChecksum = nil
			}
		}

		if isTargetEncrypted {
			var encReader io.Reader
			kind, _ := crypto.IsRequested(r.Header)
			encReader, objEncKey, err = newEncryptReader(ctx, srcInfo.Reader, kind, newKeyID, newKey, dstBucket, dstObject, encMetadata, kmsCtx)
			if err != nil {
				writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
				return
			}
			reader = etag.Wrap(encReader, srcInfo.Reader)
		}

		if isSourceEncrypted {
			// Remove all source encrypted related metadata to
			// avoid copying them in target object.
			crypto.RemoveInternalEntries(srcInfo.UserDefined)
		}

		// do not try to verify encrypted content
		srcInfo.Reader, err = hash.NewReader(ctx, reader, targetSize, "", "", actualSize)
		if err != nil {
			writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
			return
		}

		if isTargetEncrypted {
			pReader, err = pReader.WithEncryption(srcInfo.Reader, &objEncKey)
			if err != nil {
				writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
				return
			}
			if dstOpts.IndexCB != nil {
				dstOpts.IndexCB = compressionIndexEncrypter(objEncKey, dstOpts.IndexCB)
			}
			dstOpts.EncryptFn = metadataEncrypter(objEncKey)
		}
	}

	srcInfo.PutObjReader = pReader

	srcInfo.UserDefined, err = getCpObjMetadataFromHeader(ctx, r, srcInfo.UserDefined)
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	objTags := srcInfo.UserTags
	// If x-amz-tagging-directive header is REPLACE, get passed tags.
	if isDirectiveReplace(r.Header.Get(xhttp.AmzTagDirective)) {
		objTags = r.Header.Get(xhttp.AmzObjectTagging)
		if _, err := tags.ParseObjectTags(objTags); err != nil {
			writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
			return
		}
	}

	if objTags != "" {
		lastTaggingTimestamp := srcInfo.UserDefined[ReservedMetadataPrefixLower+TaggingTimestamp]
		if dstOpts.ReplicationRequest {
			srcTimestamp := dstOpts.ReplicationSourceTaggingTimestamp
			if !srcTimestamp.IsZero() {
				ondiskTimestamp, err := time.Parse(time.RFC3339Nano, lastTaggingTimestamp)
				// update tagging metadata only if replica  timestamp is newer than what's on disk
				if err != nil || (err == nil && !ondiskTimestamp.After(srcTimestamp)) {
					srcInfo.UserDefined[ReservedMetadataPrefixLower+TaggingTimestamp] = srcTimestamp.UTC().Format(time.RFC3339Nano)
					srcInfo.UserDefined[xhttp.AmzObjectTagging] = objTags
				}
			}
		} else {
			srcInfo.UserDefined[xhttp.AmzObjectTagging] = objTags
			srcInfo.UserDefined[ReservedMetadataPrefixLower+TaggingTimestamp] = UTCNow().Format(time.RFC3339Nano)
		}
	}

	srcInfo.UserDefined = filterReplicationStatusMetadata(srcInfo.UserDefined)
	srcInfo.UserDefined = objectlock.FilterObjectLockMetadata(srcInfo.UserDefined, true, true)
	retPerms := isPutActionAllowed(ctx, getRequestAuthType(r), dstBucket, dstObject, r, policy.PutObjectRetentionAction)
	holdPerms := isPutActionAllowed(ctx, getRequestAuthType(r), dstBucket, dstObject, r, policy.PutObjectLegalHoldAction)
	getObjectInfo := objectAPI.GetObjectInfo

	// apply default bucket configuration/governance headers for dest side.
	retentionMode, retentionDate, legalHold, s3Err := checkPutObjectLockAllowed(ctx, r, dstBucket, dstObject, getObjectInfo, retPerms, holdPerms)
	if s3Err == ErrNone && retentionMode.Valid() {
		lastretentionTimestamp := srcInfo.UserDefined[ReservedMetadataPrefixLower+ObjectLockRetentionTimestamp]
		if dstOpts.ReplicationRequest {
			srcTimestamp := dstOpts.ReplicationSourceRetentionTimestamp
			if !srcTimestamp.IsZero() {
				ondiskTimestamp, err := time.Parse(time.RFC3339Nano, lastretentionTimestamp)
				// update retention metadata only if replica  timestamp is newer than what's on disk
				if err != nil || (err == nil && ondiskTimestamp.Before(srcTimestamp)) {
					srcInfo.UserDefined[strings.ToLower(xhttp.AmzObjectLockMode)] = string(retentionMode)
					srcInfo.UserDefined[strings.ToLower(xhttp.AmzObjectLockRetainUntilDate)] = amztime.ISO8601Format(retentionDate.UTC())
					srcInfo.UserDefined[ReservedMetadataPrefixLower+ObjectLockRetentionTimestamp] = srcTimestamp.UTC().Format(time.RFC3339Nano)
				}
			}
		} else {
			srcInfo.UserDefined[strings.ToLower(xhttp.AmzObjectLockMode)] = string(retentionMode)
			srcInfo.UserDefined[strings.ToLower(xhttp.AmzObjectLockRetainUntilDate)] = amztime.ISO8601Format(retentionDate.UTC())
			srcInfo.UserDefined[ReservedMetadataPrefixLower+ObjectLockRetentionTimestamp] = UTCNow().Format(time.RFC3339Nano)
		}
	}

	if s3Err == ErrNone && legalHold.Status.Valid() {
		lastLegalHoldTimestamp := srcInfo.UserDefined[ReservedMetadataPrefixLower+ObjectLockLegalHoldTimestamp]
		if dstOpts.ReplicationRequest {
			srcTimestamp := dstOpts.ReplicationSourceLegalholdTimestamp
			if !srcTimestamp.IsZero() {
				ondiskTimestamp, err := time.Parse(time.RFC3339Nano, lastLegalHoldTimestamp)
				// update legalhold metadata only if replica timestamp is newer than what's on disk
				if err != nil || (err == nil && ondiskTimestamp.Before(srcTimestamp)) {
					srcInfo.UserDefined[strings.ToLower(xhttp.AmzObjectLockLegalHold)] = string(legalHold.Status)
					srcInfo.UserDefined[ReservedMetadataPrefixLower+ObjectLockRetentionTimestamp] = srcTimestamp.Format(time.RFC3339Nano)
				}
			}
		} else {
			srcInfo.UserDefined[strings.ToLower(xhttp.AmzObjectLockLegalHold)] = string(legalHold.Status)
		}
	}
	if s3Err != ErrNone {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(s3Err), r.URL)
		return
	}
	if rs := r.Header.Get(xhttp.AmzBucketReplicationStatus); rs != "" {
		srcInfo.UserDefined[ReservedMetadataPrefixLower+ReplicaStatus] = replication.Replica.String()
		srcInfo.UserDefined[ReservedMetadataPrefixLower+ReplicaTimestamp] = UTCNow().Format(time.RFC3339Nano)
		srcInfo.UserDefined[xhttp.AmzBucketReplicationStatus] = rs
	}

	op := replication.ObjectReplicationType
	if srcInfo.metadataOnly {
		op = replication.MetadataReplicationType
	}
	if dsc := mustReplicate(ctx, dstBucket, dstObject, srcInfo.getMustReplicateOptions(op, dstOpts)); dsc.ReplicateAny() {
		srcInfo.UserDefined[ReservedMetadataPrefixLower+ReplicationStatus] = dsc.PendingStatus()
		srcInfo.UserDefined[ReservedMetadataPrefixLower+ReplicationTimestamp] = UTCNow().Format(time.RFC3339Nano)
	}
	// Store the preserved compression metadata.
	maps.Copy(srcInfo.UserDefined, compressMetadata)

	// We need to preserve the encryption headers set in EncryptRequest,
	// so we do not want to override them, copy them instead.
	maps.Copy(srcInfo.UserDefined, encMetadata)

	// Ensure that metadata does not contain sensitive information
	crypto.RemoveSensitiveEntries(srcInfo.UserDefined)

	// If we see legacy source, metadataOnly we have to overwrite the content.
	if srcInfo.Legacy {
		srcInfo.metadataOnly = false
	}

	// Check if x-amz-metadata-directive or x-amz-tagging-directive was not set to REPLACE and source,
	// destination are same objects. Apply this restriction also when
	// metadataOnly is true indicating that we are not overwriting the object.
	// if encryption is enabled we do not need explicit "REPLACE" metadata to
	// be enabled as well - this is to allow for key-rotation.
	if !isDirectiveReplace(r.Header.Get(xhttp.AmzMetadataDirective)) && !isDirectiveReplace(r.Header.Get(xhttp.AmzTagDirective)) &&
		srcInfo.metadataOnly && srcOpts.VersionID == "" &&
		!crypto.Requested(r.Header) &&
		!crypto.IsSourceEncrypted(srcInfo.UserDefined) {
		// If x-amz-metadata-directive is not set to REPLACE then we need
		// to error out if source and destination are same.
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrInvalidCopyDest), r.URL)
		return
	}

	// After we've checked for an invalid copy (above), if a server-side checksum type
	// is requested, we need to read the source to recompute the checksum.
	if dstOpts.WantServerSideChecksumType.IsSet() {
		srcInfo.metadataOnly = false
	}

	// Federation only.
	remoteCallRequired := isRemoteCopyRequired(ctx, srcBucket, dstBucket, objectAPI)

	var objInfo ObjectInfo
	var os *objSweeper
	if remoteCallRequired {
		var dstRecords []dns.SrvRecord
		dstRecords, err = globalDNSConfig.Get(dstBucket)
		if err != nil {
			writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
			return
		}

		// Send PutObject request to appropriate instance (in federated deployment)
		core, rerr := getRemoteInstanceClient(r, getHostFromSrv(dstRecords))
		if rerr != nil {
			writeErrorResponse(ctx, w, toAPIError(ctx, rerr), r.URL)
			return
		}
		tag, err := tags.ParseObjectTags(objTags)
		if err != nil {
			writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
			return
		}
		// Remove the metadata for remote calls.
		delete(srcInfo.UserDefined, ReservedMetadataPrefix+"compression")
		delete(srcInfo.UserDefined, ReservedMetadataPrefix+"actual-size")
		opts := miniogo.PutObjectOptions{
			UserMetadata:         srcInfo.UserDefined,
			ServerSideEncryption: dstOpts.ServerSideEncryption,
			UserTags:             tag.ToMap(),
		}
		remoteObjInfo, rerr := core.PutObject(ctx, dstBucket, dstObject, srcInfo.Reader,
			srcInfo.Size, "", "", opts)
		if rerr != nil {
			writeErrorResponse(ctx, w, toAPIError(ctx, rerr), r.URL)
			return
		}
		objInfo.UserDefined = cloneMSS(opts.UserMetadata)
		objInfo.ETag = remoteObjInfo.ETag
		objInfo.ModTime = remoteObjInfo.LastModified
	} else {
		os = newObjSweeper(dstBucket, dstObject).WithVersioning(dstOpts.Versioned, dstOpts.VersionSuspended)
		// Get appropriate object info to identify the remote object to delete
		if !srcInfo.metadataOnly {
			goiOpts := os.GetOpts()
			if !globalTierConfigMgr.Empty() {
				if goi, gerr := getObjectInfo(ctx, dstBucket, dstObject, goiOpts); gerr == nil {
					os.SetTransitionState(goi.TransitionedObject)
				}
			}
		}

		copyObjectFn := objectAPI.CopyObject

		// Copy source object to destination, if source and destination
		// object is same then only metadata is updated.
		objInfo, err = copyObjectFn(ctx, srcBucket, srcObject, dstBucket, dstObject, srcInfo, srcOpts, dstOpts)
		if err != nil {
			writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
			return
		}
	}

	origETag := objInfo.ETag
	objInfo.ETag = getDecryptedETag(r.Header, objInfo, false)
	response := generateCopyObjectResponse(objInfo.ETag, objInfo.ModTime)
	encodedSuccessResponse := encodeResponse(response)

	if dsc := mustReplicate(ctx, dstBucket, dstObject, objInfo.getMustReplicateOptions(replication.ObjectReplicationType, dstOpts)); dsc.ReplicateAny() {
		scheduleReplication(ctx, objInfo, objectAPI, dsc, replication.ObjectReplicationType)
	}

	setPutObjHeaders(w, objInfo, false, r.Header)
	// We must not use the http.Header().Set method here because some (broken)
	// clients expect the x-amz-copy-source-version-id header key to be literally
	// "x-amz-copy-source-version-id"- not in canonicalized form, preserve it.
	if srcOpts.VersionID != "" {
		w.Header()[strings.ToLower(xhttp.AmzCopySourceVersionID)] = []string{srcOpts.VersionID}
	}

	// Write success response.
	writeSuccessResponseXML(w, encodedSuccessResponse)

	// Notify object created event.
	sendEvent(eventArgs{
		EventName:    event.ObjectCreatedCopy,
		BucketName:   dstBucket,
		Object:       objInfo,
		ReqParams:    extractReqParams(r),
		RespElements: extractRespElements(w),
		UserAgent:    r.UserAgent(),
		Host:         handlers.GetSourceIP(r),
	})

	if !remoteCallRequired && !globalTierConfigMgr.Empty() {
		// Schedule object for immediate transition if eligible.
		objInfo.ETag = origETag
		enqueueTransitionImmediate(objInfo, lcEventSrc_s3CopyObject)
		// Remove the transitioned object whose object version is being overwritten.
		os.Sweep()
	}
}

// PutObjectHandler - PUT Object
// ----------
// This implementation of the PUT operation adds an object to a bucket.
// Notice: The S3 client can send secret keys in headers for encryption related jobs,
// the handler should ensure to remove these keys before sending them to the object layer.
// Currently these keys are:
//   - X-Amz-Server-Side-Encryption-Customer-Key
//   - X-Amz-Copy-Source-Server-Side-Encryption-Customer-Key
func (api objectAPIHandlers) PutObjectHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "PutObject")
	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrServerNotInitialized), r.URL)
		return
	}

	vars := mux.Vars(r)
	bucket := vars["bucket"]
	object, err := unescapePath(vars["object"])
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	// X-Amz-Copy-Source shouldn't be set for this call.
	if _, ok := r.Header[xhttp.AmzCopySource]; ok {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrInvalidCopySource), r.URL)
		return
	}

	// Validate storage class metadata if present
	if sc := r.Header.Get(xhttp.AmzStorageClass); sc != "" {
		if !storageclass.IsValid(sc) {
			writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrInvalidStorageClass), r.URL)
			return
		}
	}

	clientETag, err := etag.FromContentMD5(r.Header)
	if err != nil {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrInvalidDigest), r.URL)
		return
	}

	// if Content-Length is unknown/missing, deny the request
	size := r.ContentLength
	rAuthType := getRequestAuthType(r)
	switch rAuthType {
	// Check signature types that must have content length
	case authTypeStreamingSigned, authTypeStreamingSignedTrailer, authTypeStreamingUnsignedTrailer:
		if sizeStr, ok := r.Header[xhttp.AmzDecodedContentLength]; ok {
			if sizeStr[0] == "" {
				writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrMissingContentLength), r.URL)
				return
			}
			size, err = strconv.ParseInt(sizeStr[0], 10, 64)
			if err != nil {
				writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
				return
			}
		}
	}
	if size == -1 {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrMissingContentLength), r.URL)
		return
	}

	// maximum Upload size for objects in a single operation
	if isMaxObjectSize(size) {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrEntityTooLarge), r.URL)
		return
	}

	metadata, err := extractMetadataFromReq(ctx, r)
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	if objTags := r.Header.Get(xhttp.AmzObjectTagging); objTags != "" {
		if _, err := tags.ParseObjectTags(objTags); err != nil {
			writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
			return
		}

		metadata[xhttp.AmzObjectTagging] = objTags
	}

	var (
		md5hex              = clientETag.String()
		sha256hex           = ""
		rd        io.Reader = r.Body
		s3Err     APIErrorCode
		putObject = objectAPI.PutObject
	)

	// Check if put is allowed
	if s3Err = isPutActionAllowed(ctx, rAuthType, bucket, object, r, policy.PutObjectAction); s3Err != ErrNone {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(s3Err), r.URL)
		return
	}

	switch rAuthType {
	case authTypeStreamingSigned, authTypeStreamingSignedTrailer:
		// Initialize stream signature verifier.
		rd, s3Err = newSignV4ChunkedReader(r, rAuthType == authTypeStreamingSignedTrailer)
		if s3Err != ErrNone {
			writeErrorResponse(ctx, w, errorCodes.ToAPIErr(s3Err), r.URL)
			return
		}
	case authTypeStreamingUnsignedTrailer:
		// Initialize stream chunked reader with optional trailers.
		rd, s3Err = newUnsignedV4ChunkedReader(r, true, r.Header.Get(xhttp.Authorization) != "")
		if s3Err != ErrNone {
			writeErrorResponse(ctx, w, errorCodes.ToAPIErr(s3Err), r.URL)
			return
		}
	case authTypeSignedV2, authTypePresignedV2:
		s3Err = isReqAuthenticatedV2(r)
		if s3Err != ErrNone {
			writeErrorResponse(ctx, w, errorCodes.ToAPIErr(s3Err), r.URL)
			return
		}

	case authTypePresigned, authTypeSigned:
		if s3Err = reqSignatureV4Verify(r, globalSite.Region(), serviceS3); s3Err != ErrNone {
			writeErrorResponse(ctx, w, errorCodes.ToAPIErr(s3Err), r.URL)
			return
		}
		if !skipContentSha256Cksum(r) {
			sha256hex = getContentSha256Cksum(r, serviceS3)
		}
	}

	if _, ok := r.Header[xhttp.MinIOSourceReplicationCheck]; ok {
		// requests to just validate replication settings and permissions are not allowed to write data
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrReplicationPermissionCheckError), r.URL)
		return
	}

	if err := enforceBucketQuotaHard(ctx, bucket, size); err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}
	if r.Header.Get(xhttp.AmzBucketReplicationStatus) == replication.Replica.String() {
		if s3Err = isPutActionAllowed(ctx, getRequestAuthType(r), bucket, object, r, policy.ReplicateObjectAction); s3Err != ErrNone {
			writeErrorResponse(ctx, w, errorCodes.ToAPIErr(s3Err), r.URL)
			return
		}
		metadata[ReservedMetadataPrefixLower+ReplicaStatus] = replication.Replica.String()
		metadata[ReservedMetadataPrefixLower+ReplicaTimestamp] = UTCNow().Format(time.RFC3339Nano)
		defer globalReplicationStats.Load().UpdateReplicaStat(bucket, size)
	}

	// Check if bucket encryption is enabled
	sseConfig, _ := globalBucketSSEConfigSys.Get(bucket)
	sseConfig.Apply(r.Header, sse.ApplyOptions{
		AutoEncrypt: globalAutoEncryption,
	})

	var reader io.Reader
	reader = rd

	var opts ObjectOptions
	opts, err = putOptsFromReq(ctx, r, bucket, object, metadata)
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	actualSize := size
	var idxCb func() []byte
	if isCompressible(r.Header, object) && size > minCompressibleSize {
		// Storing the compression metadata.
		metadata[ReservedMetadataPrefix+"compression"] = compressionAlgorithmV2
		metadata[ReservedMetadataPrefix+"actual-size"] = strconv.FormatInt(size, 10)

		actualReader, err := hash.NewReader(ctx, reader, size, md5hex, sha256hex, actualSize)
		if err != nil {
			writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
			return
		}
		if err = actualReader.AddChecksum(r, false); err != nil {
			writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrInvalidChecksum), r.URL)
			return
		}
		opts.WantChecksum = actualReader.Checksum()

		// Set compression metrics.
		var s2c io.ReadCloser
		wantEncryption := crypto.Requested(r.Header)
		s2c, idxCb = newS2CompressReader(actualReader, actualSize, wantEncryption)
		defer s2c.Close()

		reader = etag.Wrap(s2c, actualReader)
		size = -1   // Since compressed size is un-predictable.
		md5hex = "" // Do not try to verify the content.
		sha256hex = ""
	}

	var forceMD5 []byte
	// Optimization: If SSE-KMS and SSE-C did not request Content-Md5. Use uuid as etag. Optionally enable this also
	// for server that is started with `--no-compat`.
	if !etag.ContentMD5Requested(r.Header) && (crypto.S3KMS.IsRequested(r.Header) || crypto.SSEC.IsRequested(r.Header) || !globalServerCtxt.StrictS3Compat) {
		forceMD5 = mustGetUUIDBytes()
	}
	hashReader, err := hash.NewReaderWithOpts(ctx, reader, hash.Options{
		Size:       size,
		MD5Hex:     md5hex,
		SHA256Hex:  sha256hex,
		ActualSize: actualSize,
		DisableMD5: false,
		ForceMD5:   forceMD5,
	})
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}
	if size >= 0 {
		if err := hashReader.AddChecksum(r, false); err != nil {
			writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrInvalidChecksum), r.URL)
			return
		}
		opts.WantChecksum = hashReader.Checksum()
	}

	rawReader := hashReader
	pReader := NewPutObjReader(rawReader)
	opts.IndexCB = idxCb

	if r.Header.Get(xhttp.IfMatch) != "" {
		opts.HasIfMatch = true
	}
	if opts.PreserveETag != "" ||
		r.Header.Get(xhttp.IfMatch) != "" ||
		r.Header.Get(xhttp.IfNoneMatch) != "" {
		opts.CheckPrecondFn = func(oi ObjectInfo) bool {
			if _, err := DecryptObjectInfo(&oi, r); err != nil {
				writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
				return true
			}
			return checkPreconditionsPUT(ctx, w, r, oi, opts)
		}
	}

	retPerms := isPutActionAllowed(ctx, getRequestAuthType(r), bucket, object, r, policy.PutObjectRetentionAction)
	holdPerms := isPutActionAllowed(ctx, getRequestAuthType(r), bucket, object, r, policy.PutObjectLegalHoldAction)

	getObjectInfo := objectAPI.GetObjectInfo

	retentionMode, retentionDate, legalHold, s3Err := checkPutObjectLockAllowed(ctx, r, bucket, object, getObjectInfo, retPerms, holdPerms)
	if s3Err == ErrNone && retentionMode.Valid() {
		metadata[strings.ToLower(xhttp.AmzObjectLockMode)] = string(retentionMode)
		metadata[strings.ToLower(xhttp.AmzObjectLockRetainUntilDate)] = amztime.ISO8601Format(retentionDate.UTC())
	}
	if s3Err == ErrNone && legalHold.Status.Valid() {
		metadata[strings.ToLower(xhttp.AmzObjectLockLegalHold)] = string(legalHold.Status)
	}
	if s3Err != ErrNone {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(s3Err), r.URL)
		return
	}
	if dsc := mustReplicate(ctx, bucket, object, getMustReplicateOptions(metadata, "", "", replication.ObjectReplicationType, opts)); dsc.ReplicateAny() {
		metadata[ReservedMetadataPrefixLower+ReplicationTimestamp] = UTCNow().Format(time.RFC3339Nano)
		metadata[ReservedMetadataPrefixLower+ReplicationStatus] = dsc.PendingStatus()
	}
	var objectEncryptionKey crypto.ObjectKey
	if crypto.Requested(r.Header) {
		if crypto.SSECopy.IsRequested(r.Header) {
			writeErrorResponse(ctx, w, toAPIError(ctx, errInvalidEncryptionParameters), r.URL)
			return
		}

		if crypto.SSEC.IsRequested(r.Header) && crypto.S3.IsRequested(r.Header) {
			writeErrorResponse(ctx, w, toAPIError(ctx, crypto.ErrIncompatibleEncryptionMethod), r.URL)
			return
		}

		if crypto.SSEC.IsRequested(r.Header) && crypto.S3KMS.IsRequested(r.Header) {
			writeErrorResponse(ctx, w, toAPIError(ctx, crypto.ErrIncompatibleEncryptionMethod), r.URL)
			return
		}

		reader, objectEncryptionKey, err = EncryptRequest(hashReader, r, bucket, object, metadata)
		if err != nil {
			writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
			return
		}

		wantSize := int64(-1)
		if size >= 0 {
			info := ObjectInfo{Size: size}
			wantSize = info.EncryptedSize()
		}

		// do not try to verify encrypted content
		hashReader, err = hash.NewReader(ctx, etag.Wrap(reader, hashReader), wantSize, "", "", actualSize)
		if err != nil {
			writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
			return
		}
		pReader, err = pReader.WithEncryption(hashReader, &objectEncryptionKey)
		if err != nil {
			writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
			return
		}
		if opts.IndexCB != nil {
			opts.IndexCB = compressionIndexEncrypter(objectEncryptionKey, opts.IndexCB)
		}
		opts.EncryptFn = metadataEncrypter(objectEncryptionKey)
	}

	// Ensure that metadata does not contain sensitive information
	crypto.RemoveSensitiveEntries(metadata)

	os := newObjSweeper(bucket, object).WithVersioning(opts.Versioned, opts.VersionSuspended)
	if !globalTierConfigMgr.Empty() {
		// Get appropriate object info to identify the remote object to delete
		goiOpts := os.GetOpts()
		if goi, gerr := getObjectInfo(ctx, bucket, object, goiOpts); gerr == nil {
			os.SetTransitionState(goi.TransitionedObject)
		}
	}

	// Create the object..
	objInfo, err := putObject(ctx, bucket, object, pReader, opts)
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	if r.Header.Get(xMinIOExtract) == "true" && HasSuffix(object, archiveExt) {
		opts := ObjectOptions{VersionID: objInfo.VersionID, MTime: objInfo.ModTime}
		if _, err := updateObjectMetadataWithZipInfo(ctx, objectAPI, bucket, object, opts); err != nil {
			writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
			return
		}
	}

	origETag := objInfo.ETag
	if kind, encrypted := crypto.IsEncrypted(objInfo.UserDefined); encrypted {
		switch kind {
		case crypto.S3:
			w.Header().Set(xhttp.AmzServerSideEncryption, xhttp.AmzEncryptionAES)
			objInfo.ETag, _ = DecryptETag(objectEncryptionKey, ObjectInfo{ETag: objInfo.ETag})
		case crypto.S3KMS:
			w.Header().Set(xhttp.AmzServerSideEncryption, xhttp.AmzEncryptionKMS)
			w.Header().Set(xhttp.AmzServerSideEncryptionKmsID, objInfo.KMSKeyID())
			if kmsCtx, ok := objInfo.UserDefined[crypto.MetaContext]; ok {
				w.Header().Set(xhttp.AmzServerSideEncryptionKmsContext, kmsCtx)
			}
			if len(objInfo.ETag) >= 32 && strings.Count(objInfo.ETag, "-") != 1 {
				objInfo.ETag = objInfo.ETag[len(objInfo.ETag)-32:]
			}
		case crypto.SSEC:
			w.Header().Set(xhttp.AmzServerSideEncryptionCustomerAlgorithm, r.Header.Get(xhttp.AmzServerSideEncryptionCustomerAlgorithm))
			w.Header().Set(xhttp.AmzServerSideEncryptionCustomerKeyMD5, r.Header.Get(xhttp.AmzServerSideEncryptionCustomerKeyMD5))

			if len(objInfo.ETag) >= 32 && strings.Count(objInfo.ETag, "-") != 1 {
				objInfo.ETag = objInfo.ETag[len(objInfo.ETag)-32:]
			}
		}
	}
	if dsc := mustReplicate(ctx, bucket, object, getMustReplicateOptions(metadata, "", "", replication.ObjectReplicationType, opts)); dsc.ReplicateAny() {
		scheduleReplication(ctx, objInfo, objectAPI, dsc, replication.ObjectReplicationType)
	}

	setPutObjHeaders(w, objInfo, false, r.Header)

	// Notify object created event.
	evt := eventArgs{
		EventName:    event.ObjectCreatedPut,
		BucketName:   bucket,
		Object:       objInfo,
		ReqParams:    extractReqParams(r),
		RespElements: extractRespElements(w),
		UserAgent:    r.UserAgent(),
		Host:         handlers.GetSourceIP(r),
	}
	sendEvent(evt)
	if objInfo.NumVersions > int(scannerExcessObjectVersions.Load()) {
		evt.EventName = event.ObjectManyVersions
		sendEvent(evt)

		auditLogInternal(context.Background(), AuditLogOptions{
			Event:     "scanner:manyversions",
			APIName:   "PutObject",
			Bucket:    objInfo.Bucket,
			Object:    objInfo.Name,
			VersionID: objInfo.VersionID,
			Status:    http.StatusText(http.StatusOK),
		})
	}

	// Do not send checksums in events to avoid leaks.
	hash.TransferChecksumHeader(w, r)
	writeSuccessResponseHeadersOnly(w)

	// Remove the transitioned object whose object version is being overwritten.
	if !globalTierConfigMgr.Empty() {
		// Schedule object for immediate transition if eligible.
		objInfo.ETag = origETag
		enqueueTransitionImmediate(objInfo, lcEventSrc_s3PutObject)
		os.Sweep()
	}
}

// PutObjectExtractHandler - PUT Object extract is an extended API
// based off from AWS Snowball feature to auto extract compressed
// stream will be extracted in the same directory it is stored in
// and the folder structures will be built out accordingly.
func (api objectAPIHandlers) PutObjectExtractHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "PutObjectExtract")
	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrServerNotInitialized), r.URL)
		return
	}

	if crypto.S3KMS.IsRequested(r.Header) { // SSE-KMS is not supported
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrNotImplemented), r.URL)
		return
	}

	vars := mux.Vars(r)
	bucket := vars["bucket"]
	object, err := unescapePath(vars["object"])
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	// X-Amz-Copy-Source shouldn't be set for this call.
	if _, ok := r.Header[xhttp.AmzCopySource]; ok {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrInvalidCopySource), r.URL)
		return
	}

	// Validate storage class metadata if present
	sc := r.Header.Get(xhttp.AmzStorageClass)
	if sc != "" {
		if !storageclass.IsValid(sc) {
			writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrInvalidStorageClass), r.URL)
			return
		}
	}

	clientETag, err := etag.FromContentMD5(r.Header)
	if err != nil {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrInvalidDigest), r.URL)
		return
	}

	// if Content-Length is unknown/missing, deny the request
	size := r.ContentLength
	rAuthType := getRequestAuthType(r)
	if rAuthType == authTypeStreamingSigned || rAuthType == authTypeStreamingSignedTrailer {
		if sizeStr, ok := r.Header[xhttp.AmzDecodedContentLength]; ok {
			if sizeStr[0] == "" {
				writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrMissingContentLength), r.URL)
				return
			}
			size, err = strconv.ParseInt(sizeStr[0], 10, 64)
			if err != nil {
				writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
				return
			}
		}
	}

	if size == -1 {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrMissingContentLength), r.URL)
		return
	}

	// maximum Upload size for objects in a single operation
	if isMaxObjectSize(size) {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrEntityTooLarge), r.URL)
		return
	}

	var (
		md5hex              = clientETag.String()
		sha256hex           = ""
		reader    io.Reader = r.Body
		s3Err     APIErrorCode
		putObject = objectAPI.PutObject
	)

	var opts untarOptions
	opts.ignoreDirs = strings.EqualFold(r.Header.Get(xhttp.MinIOSnowballIgnoreDirs), "true")
	opts.ignoreErrs = strings.EqualFold(r.Header.Get(xhttp.MinIOSnowballIgnoreErrors), "true")
	opts.prefixAll = r.Header.Get(xhttp.MinIOSnowballPrefix)
	if opts.prefixAll != "" {
		opts.prefixAll = trimLeadingSlash(pathJoin(opts.prefixAll, slashSeparator))
	}
	// Check if put is allow for specified prefix.
	if s3Err = isPutActionAllowed(ctx, rAuthType, bucket, opts.prefixAll, r, policy.PutObjectAction); s3Err != ErrNone {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(s3Err), r.URL)
		return
	}

	switch rAuthType {
	case authTypeStreamingSigned, authTypeStreamingSignedTrailer:
		// Initialize stream signature verifier.
		reader, s3Err = newSignV4ChunkedReader(r, rAuthType == authTypeStreamingSignedTrailer)
		if s3Err != ErrNone {
			writeErrorResponse(ctx, w, errorCodes.ToAPIErr(s3Err), r.URL)
			return
		}
	case authTypeSignedV2, authTypePresignedV2:
		s3Err = isReqAuthenticatedV2(r)
		if s3Err != ErrNone {
			writeErrorResponse(ctx, w, errorCodes.ToAPIErr(s3Err), r.URL)
			return
		}

	case authTypePresigned, authTypeSigned:
		if s3Err = reqSignatureV4Verify(r, globalSite.Region(), serviceS3); s3Err != ErrNone {
			writeErrorResponse(ctx, w, errorCodes.ToAPIErr(s3Err), r.URL)
			return
		}
		if !skipContentSha256Cksum(r) {
			sha256hex = getContentSha256Cksum(r, serviceS3)
		}
	}

	hreader, err := hash.NewReader(ctx, reader, size, md5hex, sha256hex, size)
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}
	if err = hreader.AddChecksum(r, false); err != nil {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrInvalidChecksum), r.URL)
		return
	}

	if err := enforceBucketQuotaHard(ctx, bucket, size); err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	// Check if bucket encryption is enabled
	sseConfig, _ := globalBucketSSEConfigSys.Get(bucket)
	sseConfig.Apply(r.Header, sse.ApplyOptions{
		AutoEncrypt: globalAutoEncryption,
	})

	retPerms := isPutActionAllowed(ctx, getRequestAuthType(r), bucket, object, r, policy.PutObjectRetentionAction)
	holdPerms := isPutActionAllowed(ctx, getRequestAuthType(r), bucket, object, r, policy.PutObjectLegalHoldAction)

	getObjectInfo := objectAPI.GetObjectInfo

	// These are static for all objects extracted.
	reqParams := extractReqParams(r)
	respElements := map[string]string{
		"requestId": w.Header().Get(xhttp.AmzRequestID),
		"nodeId":    w.Header().Get(xhttp.AmzRequestHostID),
	}
	if sc == "" {
		sc = storageclass.STANDARD
	}

	putObjectTar := func(reader io.Reader, info os.FileInfo, object string) error {
		size := info.Size()
		if s3Err = isPutActionAllowed(ctx, getRequestAuthType(r), bucket, object, r, policy.PutObjectAction); s3Err != ErrNone {
			writeErrorResponse(ctx, w, errorCodes.ToAPIErr(s3Err), r.URL)
			return errors.New(errorCodes.ToAPIErr(s3Err).Code)
		}
		metadata := map[string]string{
			xhttp.AmzStorageClass: sc, // save same storage-class as incoming stream.
		}

		actualSize := size
		var idxCb func() []byte
		if isCompressible(r.Header, object) && size > minCompressibleSize {
			// Storing the compression metadata.
			metadata[ReservedMetadataPrefix+"compression"] = compressionAlgorithmV2
			metadata[ReservedMetadataPrefix+"actual-size"] = strconv.FormatInt(size, 10)

			actualReader, err := hash.NewReader(ctx, reader, size, "", "", actualSize)
			if err != nil {
				return err
			}

			// Set compression metrics.
			wantEncryption := crypto.Requested(r.Header)
			s2c, cb := newS2CompressReader(actualReader, actualSize, wantEncryption)
			defer s2c.Close()
			idxCb = cb
			reader = etag.Wrap(s2c, actualReader)
			size = -1 // Since compressed size is un-predictable.
		}

		hashReader, err := hash.NewReader(ctx, reader, size, "", "", actualSize)
		if err != nil {
			return err
		}

		rawReader := hashReader
		pReader := NewPutObjReader(rawReader)

		if r.Header.Get(xhttp.AmzBucketReplicationStatus) == replication.Replica.String() {
			if s3Err = isPutActionAllowed(ctx, getRequestAuthType(r), bucket, object, r, policy.ReplicateObjectAction); s3Err != ErrNone {
				return errors.New(errorCodes.ToAPIErr(s3Err).Code)
			}
			metadata[ReservedMetadataPrefixLower+ReplicaStatus] = replication.Replica.String()
			metadata[ReservedMetadataPrefixLower+ReplicaTimestamp] = UTCNow().Format(time.RFC3339Nano)
		}

		var (
			versionID string
			hdrs      http.Header
		)

		if tarHdrs, ok := info.Sys().(*tar.Header); ok && len(tarHdrs.PAXRecords) > 0 {
			versionID = tarHdrs.PAXRecords["minio.versionId"]
			hdrs = make(http.Header)
			for k, v := range tarHdrs.PAXRecords {
				if k == "minio.versionId" {
					continue
				}
				if after, ok0 := strings.CutPrefix(k, "minio.metadata."); ok0 {
					k = after
					hdrs.Set(k, v)
				}
			}
			m, err := extractMetadata(ctx, textproto.MIMEHeader(hdrs))
			if err != nil {
				return err
			}
			maps.Copy(metadata, m)
		} else {
			versionID = r.Form.Get(xhttp.VersionID)
			hdrs = r.Header
		}

		opts, err := putOpts(ctx, bucket, object, versionID, hdrs, metadata)
		if err != nil {
			return err
		}

		opts.MTime = info.ModTime()
		if opts.MTime.Unix() <= 0 {
			opts.MTime = UTCNow()
		}
		opts.IndexCB = idxCb

		retentionMode, retentionDate, legalHold, s3err := checkPutObjectLockAllowed(ctx, r, bucket, object, getObjectInfo, retPerms, holdPerms)
		if s3err == ErrNone && retentionMode.Valid() {
			metadata[strings.ToLower(xhttp.AmzObjectLockMode)] = string(retentionMode)
			metadata[strings.ToLower(xhttp.AmzObjectLockRetainUntilDate)] = amztime.ISO8601Format(retentionDate.UTC())
		}

		if s3err == ErrNone && legalHold.Status.Valid() {
			metadata[strings.ToLower(xhttp.AmzObjectLockLegalHold)] = string(legalHold.Status)
		}

		if s3err != ErrNone {
			s3Err = s3err
			return ObjectLocked{}
		}

		if dsc := mustReplicate(ctx, bucket, object, getMustReplicateOptions(metadata, "", "", replication.ObjectReplicationType, opts)); dsc.ReplicateAny() {
			metadata[ReservedMetadataPrefixLower+ReplicationTimestamp] = UTCNow().Format(time.RFC3339Nano)
			metadata[ReservedMetadataPrefixLower+ReplicationStatus] = dsc.PendingStatus()
		}

		var objectEncryptionKey crypto.ObjectKey
		if crypto.Requested(r.Header) {
			if crypto.SSECopy.IsRequested(r.Header) {
				return errInvalidEncryptionParameters
			}

			reader, objectEncryptionKey, err = EncryptRequest(hashReader, r, bucket, object, metadata)
			if err != nil {
				return err
			}

			wantSize := int64(-1)
			if size >= 0 {
				info := ObjectInfo{Size: size}
				wantSize = info.EncryptedSize()
			}

			// do not try to verify encrypted content
			hashReader, err = hash.NewReader(ctx, etag.Wrap(reader, hashReader), wantSize, "", "", actualSize)
			if err != nil {
				return err
			}

			pReader, err = pReader.WithEncryption(hashReader, &objectEncryptionKey)
			if err != nil {
				return err
			}
		}
		if opts.IndexCB != nil {
			opts.IndexCB = compressionIndexEncrypter(objectEncryptionKey, opts.IndexCB)
		}

		// Ensure that metadata does not contain sensitive information
		crypto.RemoveSensitiveEntries(metadata)

		os := newObjSweeper(bucket, object).WithVersioning(opts.Versioned, opts.VersionSuspended)
		if !globalTierConfigMgr.Empty() {
			// Get appropriate object info to identify the remote object to delete
			goiOpts := os.GetOpts()
			if goi, gerr := getObjectInfo(ctx, bucket, object, goiOpts); gerr == nil {
				os.SetTransitionState(goi.TransitionedObject)
			}
		}

		// Create the object..
		objInfo, err := putObject(ctx, bucket, object, pReader, opts)
		if err != nil {
			return err
		}

		origETag := objInfo.ETag
		objInfo.ETag = getDecryptedETag(r.Header, objInfo, false)

		if dsc := mustReplicate(ctx, bucket, object, getMustReplicateOptions(metadata, "", "", replication.ObjectReplicationType, opts)); dsc.ReplicateAny() {
			scheduleReplication(ctx, objInfo, objectAPI, dsc, replication.ObjectReplicationType)
		}

		// Notify object created event.
		evt := eventArgs{
			EventName:    event.ObjectCreatedPut,
			BucketName:   bucket,
			Object:       objInfo,
			ReqParams:    reqParams,
			RespElements: respElements,
			UserAgent:    r.UserAgent(),
			Host:         handlers.GetSourceIP(r),
		}
		sendEvent(evt)

		// Remove the transitioned object whose object version is being overwritten.
		if !globalTierConfigMgr.Empty() {
			objInfo.ETag = origETag
			// Schedule object for immediate transition if eligible.
			enqueueTransitionImmediate(objInfo, lcEventSrc_s3PutObject)
			os.Sweep()
		}

		return nil
	}

	if err = untar(ctx, hreader, putObjectTar, opts); err != nil {
		apiErr := errorCodes.ToAPIErr(s3Err)
		// If not set, convert or use BadRequest
		if s3Err == ErrNone {
			apiErr = toAPIError(ctx, err)
			if apiErr.Code == "InternalError" {
				// Convert generic internal errors to bad requests.
				apiErr = APIError{
					Code:           "BadRequest",
					Description:    err.Error(),
					HTTPStatusCode: http.StatusBadRequest,
				}
			}
		}
		writeErrorResponse(ctx, w, apiErr, r.URL)
		return
	}

	w.Header()[xhttp.ETag] = []string{`"` + hex.EncodeToString(hreader.MD5Current()) + `"`}
	hash.TransferChecksumHeader(w, r)
	writeSuccessResponseHeadersOnly(w)
}

// Delete objectAPIHandlers

// DeleteObjectHandler - delete an object
func (api objectAPIHandlers) DeleteObjectHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "DeleteObject")

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	vars := mux.Vars(r)
	bucket := vars["bucket"]
	object, err := unescapePath(vars["object"])
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrServerNotInitialized), r.URL)
		return
	}

	if s3Error := checkRequestAuthType(ctx, r, policy.DeleteObjectAction, bucket, object); s3Error != ErrNone {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(s3Error), r.URL)
		return
	}
	if _, ok := r.Header[xhttp.MinIOSourceReplicationCheck]; ok {
		// requests to just validate replication settings and permissions are not allowed to delete data
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrReplicationPermissionCheckError), r.URL)
		return
	}

	replica := r.Header.Get(xhttp.AmzBucketReplicationStatus) == replication.Replica.String()
	if replica {
		if s3Error := checkRequestAuthType(ctx, r, policy.ReplicateDeleteAction, bucket, object); s3Error != ErrNone {
			writeErrorResponse(ctx, w, errorCodes.ToAPIErr(s3Error), r.URL)
			return
		}
	}

	if globalDNSConfig != nil {
		_, err := globalDNSConfig.Get(bucket)
		if err != nil && err != dns.ErrNotImplemented {
			writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
			return
		}
	}

	opts, err := delOpts(ctx, r, bucket, object)
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	rcfg, _ := globalBucketObjectLockSys.Get(bucket)
	if rcfg.LockEnabled && opts.DeletePrefix {
		apiErr := toAPIError(ctx, errInvalidArgument)
		apiErr.Description = "force-delete is forbidden on Object Locking enabled buckets"
		writeErrorResponse(ctx, w, apiErr, r.URL)
		return
	}

	os := newObjSweeper(bucket, object).WithVersion(opts.VersionID).WithVersioning(opts.Versioned, opts.VersionSuspended)

	opts.SetEvalMetadataFn(func(oi *ObjectInfo, gerr error) (dsc ReplicateDecision, err error) {
		if replica { // no need to check replication on receiver
			return dsc, nil
		}
		dsc = checkReplicateDelete(ctx, bucket, ObjectToDelete{
			ObjectV: ObjectV{
				ObjectName: object,
				VersionID:  opts.VersionID,
			},
		}, *oi, opts, gerr)
		// Mutations of objects on versioning suspended buckets
		// affect its null version. Through opts below we select
		// the null version's remote object to delete if
		// transitioned.
		if gerr == nil {
			os.SetTransitionState(oi.TransitionedObject)
		}
		return dsc, nil
	})

	vID := opts.VersionID
	if replica {
		opts.SetReplicaStatus(replication.Replica)
		if opts.VersionPurgeStatus().Empty() {
			// opts.VersionID holds delete marker version ID to replicate and not yet present on disk
			vID = ""
		}
	}
	opts.SetEvalRetentionBypassFn(func(goi ObjectInfo, gerr error) (err error) {
		err = nil
		if vID != "" {
			err := enforceRetentionBypassForDelete(ctx, r, bucket, ObjectToDelete{
				ObjectV: ObjectV{
					ObjectName: object,
					VersionID:  vID,
				},
			}, goi, gerr)
			if err != nil && !isErrObjectNotFound(err) {
				return err
			}
		}
		return err
	})

	deleteObject := objectAPI.DeleteObject

	// http://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectDELETE.html
	objInfo, err := deleteObject(ctx, bucket, object, opts)
	if err != nil {
		if _, ok := err.(BucketNotFound); ok {
			// When bucket doesn't exist specially handle it.
			writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
			return
		}
		if isErrObjectNotFound(err) || isErrVersionNotFound(err) {
			// Send an event when the object is not found
			objInfo.Name = object
			objInfo.VersionID = opts.VersionID
			sendEvent(eventArgs{
				EventName:    event.ObjectRemovedNoOP,
				BucketName:   bucket,
				Object:       objInfo,
				ReqParams:    extractReqParams(r),
				RespElements: extractRespElements(w),
				UserAgent:    r.UserAgent(),
				Host:         handlers.GetSourceIP(r),
			})
			writeSuccessNoContent(w)
			return
		}
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	if objInfo.Name == "" {
		writeSuccessNoContent(w)
		return
	}

	setPutObjHeaders(w, objInfo, true, r.Header)
	writeSuccessNoContent(w)

	eventName := event.ObjectRemovedDelete
	if objInfo.DeleteMarker {
		eventName = event.ObjectRemovedDeleteMarkerCreated
	}

	// Notify object deleted event.
	sendEvent(eventArgs{
		EventName:    eventName,
		BucketName:   bucket,
		Object:       objInfo,
		ReqParams:    extractReqParams(r),
		RespElements: extractRespElements(w),
		UserAgent:    r.UserAgent(),
		Host:         handlers.GetSourceIP(r),
	})

	if objInfo.ReplicationStatus == replication.Pending || objInfo.VersionPurgeStatus == replication.VersionPurgePending {
		dmVersionID := ""
		versionID := ""
		if objInfo.DeleteMarker {
			dmVersionID = objInfo.VersionID
		} else {
			versionID = objInfo.VersionID
		}
		dobj := DeletedObjectReplicationInfo{
			DeletedObject: DeletedObject{
				ObjectName:            object,
				VersionID:             versionID,
				DeleteMarkerVersionID: dmVersionID,
				DeleteMarkerMTime:     DeleteMarkerMTime{objInfo.ModTime},
				DeleteMarker:          objInfo.DeleteMarker,
				ReplicationState:      objInfo.ReplicationState(),
			},
			Bucket:    bucket,
			EventType: ReplicateIncomingDelete,
		}
		scheduleReplicationDelete(ctx, dobj, objectAPI)
	}

	// Remove the transitioned object whose object version is being overwritten.
	if !globalTierConfigMgr.Empty() {
		os.Sweep()
	}
}

// PutObjectLegalHoldHandler - set legal hold configuration to object,
func (api objectAPIHandlers) PutObjectLegalHoldHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "PutObjectLegalHold")

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	vars := mux.Vars(r)
	bucket := vars["bucket"]
	object, err := unescapePath(vars["object"])
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrServerNotInitialized), r.URL)
		return
	}

	// Check permissions to perform this legal hold operation
	if s3Err := checkRequestAuthType(ctx, r, policy.PutObjectLegalHoldAction, bucket, object); s3Err != ErrNone {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(s3Err), r.URL)
		return
	}

	if _, err := objectAPI.GetBucketInfo(ctx, bucket, BucketOptions{}); err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}
	if !validateLengthAndChecksum(r) {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrMissingContentMD5), r.URL)
		return
	}

	if rcfg, _ := globalBucketObjectLockSys.Get(bucket); !rcfg.LockEnabled {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrInvalidBucketObjectLockConfiguration), r.URL)
		return
	}

	legalHold, err := objectlock.ParseObjectLegalHold(r.Body)
	if err != nil {
		apiErr := errorCodes.ToAPIErr(ErrMalformedXML)
		apiErr.Description = err.Error()
		writeErrorResponse(ctx, w, apiErr, r.URL)
		return
	}

	opts, err := getOpts(ctx, r, bucket, object)
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	popts := ObjectOptions{
		MTime:     opts.MTime,
		VersionID: opts.VersionID,
		EvalMetadataFn: func(oi *ObjectInfo, gerr error) (ReplicateDecision, error) {
			oi.UserDefined[strings.ToLower(xhttp.AmzObjectLockLegalHold)] = strings.ToUpper(string(legalHold.Status))
			oi.UserDefined[ReservedMetadataPrefixLower+ObjectLockLegalHoldTimestamp] = UTCNow().Format(time.RFC3339Nano)

			dsc := mustReplicate(ctx, bucket, object, oi.getMustReplicateOptions(replication.MetadataReplicationType, opts))
			if dsc.ReplicateAny() {
				oi.UserDefined[ReservedMetadataPrefixLower+ReplicationTimestamp] = UTCNow().Format(time.RFC3339Nano)
				oi.UserDefined[ReservedMetadataPrefixLower+ReplicationStatus] = dsc.PendingStatus()
			}
			return dsc, nil
		},
	}

	objInfo, err := objectAPI.PutObjectMetadata(ctx, bucket, object, popts)
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	dsc := mustReplicate(ctx, bucket, object, objInfo.getMustReplicateOptions(replication.MetadataReplicationType, opts))
	if dsc.ReplicateAny() {
		scheduleReplication(ctx, objInfo, objectAPI, dsc, replication.MetadataReplicationType)
	}

	writeSuccessResponseHeadersOnly(w)

	// Notify object event.
	sendEvent(eventArgs{
		EventName:    event.ObjectCreatedPutLegalHold,
		BucketName:   bucket,
		Object:       objInfo,
		ReqParams:    extractReqParams(r),
		RespElements: extractRespElements(w),
		UserAgent:    r.UserAgent(),
		Host:         handlers.GetSourceIP(r),
	})
}

// GetObjectLegalHoldHandler - get legal hold configuration to object,
func (api objectAPIHandlers) GetObjectLegalHoldHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "GetObjectLegalHold")

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	vars := mux.Vars(r)
	bucket := vars["bucket"]
	object, err := unescapePath(vars["object"])
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrServerNotInitialized), r.URL)
		return
	}
	if s3Error := checkRequestAuthType(ctx, r, policy.GetObjectLegalHoldAction, bucket, object); s3Error != ErrNone {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(s3Error), r.URL)
		return
	}

	getObjectInfo := objectAPI.GetObjectInfo

	if rcfg, _ := globalBucketObjectLockSys.Get(bucket); !rcfg.LockEnabled {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrInvalidBucketObjectLockConfiguration), r.URL)
		return
	}

	opts, err := getOpts(ctx, r, bucket, object)
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	objInfo, err := getObjectInfo(ctx, bucket, object, opts)
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	legalHold := objectlock.GetObjectLegalHoldMeta(objInfo.UserDefined)
	if legalHold.IsEmpty() {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrNoSuchObjectLockConfiguration), r.URL)
		return
	}

	writeSuccessResponseXML(w, encodeResponse(legalHold))
	// Notify object legal hold accessed via a GET request.
	sendEvent(eventArgs{
		EventName:    event.ObjectAccessedGetLegalHold,
		BucketName:   bucket,
		Object:       objInfo,
		ReqParams:    extractReqParams(r),
		RespElements: extractRespElements(w),
		UserAgent:    r.UserAgent(),
		Host:         handlers.GetSourceIP(r),
	})
}

// PutObjectRetentionHandler - set object hold configuration to object,
func (api objectAPIHandlers) PutObjectRetentionHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "PutObjectRetention")

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	vars := mux.Vars(r)
	bucket := vars["bucket"]
	object, err := unescapePath(vars["object"])
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrServerNotInitialized), r.URL)
		return
	}

	// Check permissions to perform this object retention operation
	if s3Error := authenticateRequest(ctx, r, policy.PutObjectRetentionAction); s3Error != ErrNone {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(s3Error), r.URL)
		return
	}

	if _, err := objectAPI.GetBucketInfo(ctx, bucket, BucketOptions{}); err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	if !validateLengthAndChecksum(r) {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrMissingContentMD5), r.URL)
		return
	}

	if rcfg, _ := globalBucketObjectLockSys.Get(bucket); !rcfg.LockEnabled {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrInvalidBucketObjectLockConfiguration), r.URL)
		return
	}

	objRetention, err := objectlock.ParseObjectRetention(r.Body)
	if err != nil {
		apiErr := errorCodes.ToAPIErr(ErrMalformedXML)
		apiErr.Description = err.Error()
		writeErrorResponse(ctx, w, apiErr, r.URL)
		return
	}

	reqInfo := logger.GetReqInfo(ctx)
	reqInfo.SetTags("retention", objRetention.String())

	opts, err := getOpts(ctx, r, bucket, object)
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	popts := ObjectOptions{
		MTime:     opts.MTime,
		VersionID: opts.VersionID,
		EvalMetadataFn: func(oi *ObjectInfo, gerr error) (dsc ReplicateDecision, err error) {
			if err := enforceRetentionBypassForPut(ctx, r, *oi, objRetention, reqInfo.Cred, reqInfo.Owner); err != nil {
				return dsc, err
			}
			if objRetention.Mode.Valid() {
				oi.UserDefined[strings.ToLower(xhttp.AmzObjectLockMode)] = string(objRetention.Mode)
				oi.UserDefined[strings.ToLower(xhttp.AmzObjectLockRetainUntilDate)] = amztime.ISO8601Format(objRetention.RetainUntilDate.UTC())
			} else {
				oi.UserDefined[strings.ToLower(xhttp.AmzObjectLockMode)] = ""
				oi.UserDefined[strings.ToLower(xhttp.AmzObjectLockRetainUntilDate)] = ""
			}
			oi.UserDefined[ReservedMetadataPrefixLower+ObjectLockRetentionTimestamp] = UTCNow().Format(time.RFC3339Nano)
			dsc = mustReplicate(ctx, bucket, object, oi.getMustReplicateOptions(replication.MetadataReplicationType, opts))
			if dsc.ReplicateAny() {
				oi.UserDefined[ReservedMetadataPrefixLower+ReplicationTimestamp] = UTCNow().Format(time.RFC3339Nano)
				oi.UserDefined[ReservedMetadataPrefixLower+ReplicationStatus] = dsc.PendingStatus()
			}
			return dsc, nil
		},
	}

	objInfo, err := objectAPI.PutObjectMetadata(ctx, bucket, object, popts)
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	dsc := mustReplicate(ctx, bucket, object, objInfo.getMustReplicateOptions(replication.MetadataReplicationType, opts))
	if dsc.ReplicateAny() {
		scheduleReplication(ctx, objInfo, objectAPI, dsc, replication.MetadataReplicationType)
	}

	writeSuccessResponseHeadersOnly(w)

	// Notify object  event.
	sendEvent(eventArgs{
		EventName:    event.ObjectCreatedPutRetention,
		BucketName:   bucket,
		Object:       objInfo,
		ReqParams:    extractReqParams(r),
		RespElements: extractRespElements(w),
		UserAgent:    r.UserAgent(),
		Host:         handlers.GetSourceIP(r),
	})
}

// GetObjectRetentionHandler - get object retention configuration of object,
func (api objectAPIHandlers) GetObjectRetentionHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "GetObjectRetention")
	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	vars := mux.Vars(r)
	bucket := vars["bucket"]
	object, err := unescapePath(vars["object"])
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrServerNotInitialized), r.URL)
		return
	}
	if s3Error := checkRequestAuthType(ctx, r, policy.GetObjectRetentionAction, bucket, object); s3Error != ErrNone {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(s3Error), r.URL)
		return
	}

	getObjectInfo := objectAPI.GetObjectInfo

	if rcfg, _ := globalBucketObjectLockSys.Get(bucket); !rcfg.LockEnabled {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrInvalidBucketObjectLockConfiguration), r.URL)
		return
	}

	opts, err := getOpts(ctx, r, bucket, object)
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	objInfo, err := getObjectInfo(ctx, bucket, object, opts)
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	retention := objectlock.GetObjectRetentionMeta(objInfo.UserDefined)
	if !retention.Mode.Valid() {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrNoSuchObjectLockConfiguration), r.URL)
		return
	}

	writeSuccessResponseXML(w, encodeResponse(retention))
	// Notify object retention accessed via a GET request.
	sendEvent(eventArgs{
		EventName:    event.ObjectAccessedGetRetention,
		BucketName:   bucket,
		Object:       objInfo,
		ReqParams:    extractReqParams(r),
		RespElements: extractRespElements(w),
		UserAgent:    r.UserAgent(),
		Host:         handlers.GetSourceIP(r),
	})
}

// ObjectTagSet key value tags
type ObjectTagSet struct {
	Tags []tags.Tag `xml:"Tag"`
}

type objectTagging struct {
	XMLName xml.Name      `xml:"Tagging"`
	TagSet  *ObjectTagSet `xml:"TagSet"`
}

// GetObjectTaggingHandler - GET object tagging
func (api objectAPIHandlers) GetObjectTaggingHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "GetObjectTagging")
	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	vars := mux.Vars(r)
	bucket := vars["bucket"]
	object, err := unescapePath(vars["object"])
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	objAPI := api.ObjectAPI()
	if objAPI == nil {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrServerNotInitialized), r.URL)
		return
	}

	if s3Error := authenticateRequest(ctx, r, policy.GetObjectTaggingAction); s3Error != ErrNone {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(s3Error), r.URL)
		return
	}

	opts, err := getOpts(ctx, r, bucket, object)
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	ot, err := objAPI.GetObjectTags(ctx, bucket, object, opts)
	if err != nil {
		// if object/version is not found locally, but exists on peer site - proxy
		// the tagging request to peer site. The response to client will
		// return tags from peer site.
		if isErrObjectNotFound(err) || isErrVersionNotFound(err) {
			proxytgts := getProxyTargets(ctx, bucket, object, opts)
			if !proxytgts.Empty() {
				globalReplicationStats.Load().incProxy(bucket, getObjectTaggingAPI, false)
				// proxy to replication target if site replication is in place.
				tags, gerr := proxyGetTaggingToRepTarget(ctx, bucket, object, opts, proxytgts)
				if gerr.Err != nil || tags == nil {
					globalReplicationStats.Load().incProxy(bucket, getObjectTaggingAPI, true)
					writeErrorResponse(ctx, w, toAPIError(ctx, gerr.Err), r.URL)
					return
				} // overlay tags from peer site.
				ot = tags
				w.Header()[xhttp.MinIOTaggingProxied] = []string{"true"} // indicate that the request was proxied.
			} else {
				writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
				return
			}
		} else {
			writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
			return
		}
	}

	// Set this such that authorization policies can be applied on the object tags.
	if tags := ot.String(); tags != "" {
		r.Header.Set(xhttp.AmzObjectTagging, tags)
	}

	if s3Error := authorizeRequest(ctx, r, policy.GetObjectTaggingAction); s3Error != ErrNone {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(s3Error), r.URL)
		return
	}

	if opts.VersionID != "" && opts.VersionID != nullVersionID {
		w.Header()[xhttp.AmzVersionID] = []string{opts.VersionID}
	}

	otags := &objectTagging{
		TagSet: &ObjectTagSet{},
	}

	var list []tags.Tag
	for k, v := range ot.ToMap() {
		list = append(list, tags.Tag{
			Key:   k,
			Value: v,
		})
	}
	// Always return in sorted order for tags.
	sort.Slice(list, func(i, j int) bool {
		return list[i].Key < list[j].Key
	})
	otags.TagSet.Tags = list

	writeSuccessResponseXML(w, encodeResponse(otags))
}

// PutObjectTaggingHandler - PUT object tagging
func (api objectAPIHandlers) PutObjectTaggingHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "PutObjectTagging")
	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	vars := mux.Vars(r)
	bucket := vars["bucket"]
	object, err := unescapePath(vars["object"])
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	objAPI := api.ObjectAPI()
	if objAPI == nil {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrServerNotInitialized), r.URL)
		return
	}

	// Tags XML will not be bigger than 1MiB in size, fail if its bigger.
	tags, err := tags.ParseObjectXML(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	// Set this such that authorization policies can be applied on the object tags.
	r.Header.Set(xhttp.AmzObjectTagging, tags.String())

	// Allow putObjectTagging if policy action is set
	if s3Error := checkRequestAuthType(ctx, r, policy.PutObjectTaggingAction, bucket, object); s3Error != ErrNone {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(s3Error), r.URL)
		return
	}

	opts, err := getOpts(ctx, r, bucket, object)
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	objInfo, err := objAPI.GetObjectInfo(ctx, bucket, object, opts)
	if err != nil {
		// if object is not found locally, but exists on peer site - proxy
		// the tagging request to peer site. The response to client will
		// be 200 with extra header indicating that the request was proxied.
		if isErrObjectNotFound(err) || isErrVersionNotFound(err) {
			proxytgts := getProxyTargets(ctx, bucket, object, opts)
			if !proxytgts.Empty() {
				globalReplicationStats.Load().incProxy(bucket, putObjectTaggingAPI, false)
				// proxy to replication target if site replication is in place.
				perr := proxyTaggingToRepTarget(ctx, bucket, object, tags, opts, proxytgts)
				if perr.Err != nil {
					globalReplicationStats.Load().incProxy(bucket, putObjectTaggingAPI, true)
					writeErrorResponse(ctx, w, toAPIError(ctx, perr.Err), r.URL)
					return
				}
				w.Header()[xhttp.MinIOTaggingProxied] = []string{"true"}
				writeSuccessResponseHeadersOnly(w)
				// when tagging is proxied, the object version is not available to return
				// as header in the response, or ObjectInfo in the notification event.
				sendEvent(eventArgs{
					EventName:    event.ObjectCreatedPutTagging,
					BucketName:   bucket,
					ReqParams:    extractReqParams(r),
					RespElements: extractRespElements(w),
					UserAgent:    r.UserAgent(),
					Host:         handlers.GetSourceIP(r),
				})
				return
			}
		}
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}
	tagsStr := tags.String()

	dsc := mustReplicate(ctx, bucket, object, getMustReplicateOptions(objInfo.UserDefined, tagsStr, objInfo.ReplicationStatus, replication.MetadataReplicationType, opts))
	if dsc.ReplicateAny() {
		opts.UserDefined = make(map[string]string)
		opts.UserDefined[ReservedMetadataPrefixLower+ReplicationTimestamp] = UTCNow().Format(time.RFC3339Nano)
		opts.UserDefined[ReservedMetadataPrefixLower+ReplicationStatus] = dsc.PendingStatus()
		opts.UserDefined[ReservedMetadataPrefixLower+TaggingTimestamp] = UTCNow().Format(time.RFC3339Nano)
	}

	// Put object tags
	objInfo, err = objAPI.PutObjectTags(ctx, bucket, object, tagsStr, opts)
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	if dsc.ReplicateAny() {
		scheduleReplication(ctx, objInfo, objAPI, dsc, replication.MetadataReplicationType)
	}

	if objInfo.VersionID != "" && objInfo.VersionID != nullVersionID {
		w.Header()[xhttp.AmzVersionID] = []string{objInfo.VersionID}
	}

	writeSuccessResponseHeadersOnly(w)

	sendEvent(eventArgs{
		EventName:    event.ObjectCreatedPutTagging,
		BucketName:   bucket,
		Object:       objInfo,
		ReqParams:    extractReqParams(r),
		RespElements: extractRespElements(w),
		UserAgent:    r.UserAgent(),
		Host:         handlers.GetSourceIP(r),
	})
}

// DeleteObjectTaggingHandler - DELETE object tagging
func (api objectAPIHandlers) DeleteObjectTaggingHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "DeleteObjectTagging")
	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	objAPI := api.ObjectAPI()
	if objAPI == nil {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrServerNotInitialized), r.URL)
		return
	}

	vars := mux.Vars(r)
	bucket := vars["bucket"]
	object, err := unescapePath(vars["object"])
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	opts, err := getOpts(ctx, r, bucket, object)
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	oi, err := objAPI.GetObjectInfo(ctx, bucket, object, opts)
	if err != nil {
		// if object is not found locally, but exists on peer site - proxy
		// the tagging request to peer site. The response to client will
		// be 200 OK with extra header indicating that the request was proxied.
		if isErrObjectNotFound(err) || isErrVersionNotFound(err) {
			proxytgts := getProxyTargets(ctx, bucket, object, opts)
			if !proxytgts.Empty() {
				globalReplicationStats.Load().incProxy(bucket, removeObjectTaggingAPI, false)
				// proxy to replication target if active-active replication is in place.
				perr := proxyTaggingToRepTarget(ctx, bucket, object, nil, opts, proxytgts)
				if perr.Err != nil {
					globalReplicationStats.Load().incProxy(bucket, removeObjectTaggingAPI, true)
					writeErrorResponse(ctx, w, toAPIError(ctx, perr.Err), r.URL)
					return
				}
				// when delete tagging is proxied, the object version/tags are not available to return
				// as header in the response, nor ObjectInfo in the notification event.
				w.Header()[xhttp.MinIOTaggingProxied] = []string{"true"}
				writeSuccessNoContent(w)
				sendEvent(eventArgs{
					EventName:    event.ObjectCreatedDeleteTagging,
					BucketName:   bucket,
					Object:       oi,
					ReqParams:    extractReqParams(r),
					RespElements: extractRespElements(w),
					UserAgent:    r.UserAgent(),
					Host:         handlers.GetSourceIP(r),
				})
				return
			}
		}
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	if userTags := oi.UserTags; userTags != "" {
		// Set this such that authorization policies can be applied on the object tags.
		r.Header.Set(xhttp.AmzObjectTagging, oi.UserTags)
	}

	// Allow deleteObjectTagging if policy action is set
	if s3Error := checkRequestAuthType(ctx, r, policy.DeleteObjectTaggingAction, bucket, object); s3Error != ErrNone {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(s3Error), r.URL)
		return
	}

	dsc := mustReplicate(ctx, bucket, object, oi.getMustReplicateOptions(replication.MetadataReplicationType, opts))
	if dsc.ReplicateAny() {
		opts.UserDefined = make(map[string]string)
		opts.UserDefined[ReservedMetadataPrefixLower+ReplicationTimestamp] = UTCNow().Format(time.RFC3339Nano)
		opts.UserDefined[ReservedMetadataPrefixLower+ReplicationStatus] = dsc.PendingStatus()
	}

	oi, err = objAPI.DeleteObjectTags(ctx, bucket, object, opts)
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	if dsc.ReplicateAny() {
		scheduleReplication(ctx, oi, objAPI, dsc, replication.MetadataReplicationType)
	}

	if oi.VersionID != "" && oi.VersionID != nullVersionID {
		w.Header()[xhttp.AmzVersionID] = []string{oi.VersionID}
	}
	writeSuccessNoContent(w)

	sendEvent(eventArgs{
		EventName:    event.ObjectCreatedDeleteTagging,
		BucketName:   bucket,
		Object:       oi,
		ReqParams:    extractReqParams(r),
		RespElements: extractRespElements(w),
		UserAgent:    r.UserAgent(),
		Host:         handlers.GetSourceIP(r),
	})
}

// RestoreObjectHandler - POST restore object handler.
// ----------
func (api objectAPIHandlers) PostRestoreObjectHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "PostRestoreObject")
	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))
	vars := mux.Vars(r)
	bucket := vars["bucket"]
	object, err := unescapePath(vars["object"])
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	// Fetch object stat info.
	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrServerNotInitialized), r.URL)
		return
	}

	getObjectInfo := objectAPI.GetObjectInfo

	// Check for auth type to return S3 compatible error.
	if s3Error := checkRequestAuthType(ctx, r, policy.RestoreObjectAction, bucket, object); s3Error != ErrNone {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(s3Error), r.URL)
		return
	}

	if r.ContentLength <= 0 {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrEmptyRequestBody), r.URL)
		return
	}
	opts, err := postRestoreOpts(ctx, r, bucket, object)
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	objInfo, err := getObjectInfo(ctx, bucket, object, opts)
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	if objInfo.TransitionedObject.Status != lifecycle.TransitionComplete {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrInvalidObjectState), r.URL)
		return
	}

	rreq, err := parseRestoreRequest(io.LimitReader(r.Body, r.ContentLength))
	if err != nil {
		apiErr := errorCodes.ToAPIErr(ErrMalformedXML)
		apiErr.Description = err.Error()
		writeErrorResponse(ctx, w, apiErr, r.URL)
		return
	}
	// validate the request
	if err := rreq.validate(ctx, objectAPI); err != nil {
		apiErr := errorCodes.ToAPIErr(ErrMalformedXML)
		apiErr.Description = err.Error()
		writeErrorResponse(ctx, w, apiErr, r.URL)
		return
	}
	statusCode := http.StatusOK
	alreadyRestored := false
	if err == nil {
		if objInfo.RestoreOngoing && rreq.Type != SelectRestoreRequest {
			writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrObjectRestoreAlreadyInProgress), r.URL)
			return
		}
		if !objInfo.RestoreOngoing && !objInfo.RestoreExpires.IsZero() {
			statusCode = http.StatusAccepted
			alreadyRestored = true
		}
	}
	// set or upgrade restore expiry
	restoreExpiry := lifecycle.ExpectedExpiryTime(time.Now().UTC(), rreq.Days)
	metadata := cloneMSS(objInfo.UserDefined)

	// update self with restore metadata
	if rreq.Type != SelectRestoreRequest {
		objInfo.metadataOnly = true // Perform only metadata updates.
		metadata[xhttp.AmzRestoreExpiryDays] = strconv.Itoa(rreq.Days)
		metadata[xhttp.AmzRestoreRequestDate] = time.Now().UTC().Format(http.TimeFormat)
		if alreadyRestored {
			metadata[xhttp.AmzRestore] = completedRestoreObj(restoreExpiry).String()
		} else {
			metadata[xhttp.AmzRestore] = ongoingRestoreObj().String()
		}
		objInfo.UserDefined = metadata
		if _, err := objectAPI.CopyObject(GlobalContext, bucket, object, bucket, object, objInfo, ObjectOptions{
			VersionID: objInfo.VersionID,
		}, ObjectOptions{
			VersionID: objInfo.VersionID,
			MTime:     objInfo.ModTime,
		}); err != nil {
			writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrInvalidObjectState), r.URL)
			return
		}
		// for previously restored object, just update the restore expiry
		if alreadyRestored {
			return
		}
	}

	restoreObject := mustGetUUID()
	if rreq.OutputLocation.S3.BucketName != "" {
		w.Header()[xhttp.AmzRestoreOutputPath] = []string{pathJoin(rreq.OutputLocation.S3.BucketName, rreq.OutputLocation.S3.Prefix, restoreObject)}
	}
	w.WriteHeader(statusCode)
	// Notify object restore started via a POST request.
	sendEvent(eventArgs{
		EventName:  event.ObjectRestorePost,
		BucketName: bucket,
		Object:     objInfo,
		ReqParams:  extractReqParams(r),
		UserAgent:  r.UserAgent(),
		Host:       handlers.GetSourceIP(r),
	})
	// now process the restore in background
	go func() {
		rctx := GlobalContext
		if !rreq.SelectParameters.IsEmpty() {
			actualSize, err := objInfo.GetActualSize()
			if err != nil {
				writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
				return
			}

			objectRSC := s3select.NewObjectReadSeekCloser(
				func(offset int64) (io.ReadCloser, error) {
					rs := &HTTPRangeSpec{
						IsSuffixLength: false,
						Start:          offset,
						End:            -1,
					}
					return getTransitionedObjectReader(rctx, bucket, object, rs, r.Header,
						objInfo, ObjectOptions{VersionID: objInfo.VersionID})
				},
				actualSize,
			)
			defer objectRSC.Close()
			if err = rreq.SelectParameters.Open(objectRSC); err != nil {
				if serr, ok := err.(s3select.SelectError); ok {
					encodedErrorResponse := encodeResponse(APIErrorResponse{
						Code:       serr.ErrorCode(),
						Message:    serr.ErrorMessage(),
						BucketName: bucket,
						Key:        object,
						Resource:   r.URL.Path,
						RequestID:  w.Header().Get(xhttp.AmzRequestID),
						HostID:     globalDeploymentID(),
					})
					writeResponse(w, serr.HTTPStatusCode(), encodedErrorResponse, mimeXML)
				} else {
					writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
				}
				return
			}
			nr := httptest.NewRecorder()
			rw := xhttp.NewResponseRecorder(nr)
			rw.LogErrBody = true
			rw.LogAllBody = true
			rreq.SelectParameters.Evaluate(rw)
			rreq.SelectParameters.Close()
			return
		}
		opts := ObjectOptions{
			Transition: TransitionOptions{
				RestoreRequest: rreq,
				RestoreExpiry:  restoreExpiry,
			},
			VersionID: objInfo.VersionID,
		}
		if err := objectAPI.RestoreTransitionedObject(rctx, bucket, object, opts); err != nil {
			s3LogIf(ctx, fmt.Errorf("Unable to restore transitioned bucket/object %s/%s: %w", bucket, object, err))
			return
		}

		// Notify object restore completed via a POST request.
		sendEvent(eventArgs{
			EventName:  event.ObjectRestoreCompleted,
			BucketName: bucket,
			Object:     objInfo,
			ReqParams:  extractReqParams(r),
			UserAgent:  r.UserAgent(),
			Host:       handlers.GetSourceIP(r),
		})
	}()
}

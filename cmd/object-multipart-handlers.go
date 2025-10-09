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
	"bufio"
	"context"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/encrypt"
	"github.com/minio/minio-go/v7/pkg/tags"
	"github.com/minio/minio/internal/amztime"
	sse "github.com/minio/minio/internal/bucket/encryption"
	objectlock "github.com/minio/minio/internal/bucket/object/lock"
	"github.com/minio/minio/internal/bucket/replication"
	"github.com/minio/minio/internal/config/dns"
	"github.com/minio/minio/internal/config/storageclass"
	"github.com/minio/minio/internal/crypto"
	"github.com/minio/minio/internal/etag"
	"github.com/minio/minio/internal/event"
	"github.com/minio/minio/internal/handlers"
	"github.com/minio/minio/internal/hash"
	"github.com/minio/minio/internal/hash/sha256"
	xhttp "github.com/minio/minio/internal/http"
	"github.com/minio/minio/internal/logger"
	"github.com/minio/mux"
	"github.com/minio/pkg/v3/policy"
	"github.com/minio/sio"
)

// Multipart objectAPIHandlers

// NewMultipartUploadHandler - New multipart upload.
// Notice: The S3 client can send secret keys in headers for encryption related jobs,
// the handler should ensure to remove these keys before sending them to the object layer.
// Currently these keys are:
//   - X-Amz-Server-Side-Encryption-Customer-Key
//   - X-Amz-Copy-Source-Server-Side-Encryption-Customer-Key
func (api objectAPIHandlers) NewMultipartUploadHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "NewMultipartUpload")

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

	if s3Error := checkRequestAuthType(ctx, r, policy.PutObjectAction, bucket, object); s3Error != ErrNone {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(s3Error), r.URL)
		return
	}

	// Check if bucket encryption is enabled
	sseConfig, _ := globalBucketSSEConfigSys.Get(bucket)
	sseConfig.Apply(r.Header, sse.ApplyOptions{
		AutoEncrypt: globalAutoEncryption,
	})

	// Validate storage class metadata if present
	if sc := r.Header.Get(xhttp.AmzStorageClass); sc != "" {
		if !storageclass.IsValid(sc) {
			writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrInvalidStorageClass), r.URL)
			return
		}
	}

	encMetadata := map[string]string{}

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

		_, sourceReplReq := r.Header[xhttp.MinIOSourceReplicationRequest]
		ssecRepHeaders := []string{
			"X-Minio-Replication-Server-Side-Encryption-Seal-Algorithm",
			"X-Minio-Replication-Server-Side-Encryption-Sealed-Key",
			"X-Minio-Replication-Server-Side-Encryption-Iv",
		}
		ssecRep := false
		for _, header := range ssecRepHeaders {
			if val := r.Header.Get(header); val != "" {
				ssecRep = true
				break
			}
		}
		if !ssecRep || !sourceReplReq {
			if err = setEncryptionMetadata(r, bucket, object, encMetadata); err != nil {
				writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
				return
			}
		}
		// Set this for multipart only operations, we need to differentiate during
		// decryption if the file was actually multipart or not.
		encMetadata[ReservedMetadataPrefix+"Encrypted-Multipart"] = ""
	}

	// Extract metadata that needs to be saved.
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
	if r.Header.Get(xhttp.AmzBucketReplicationStatus) == replication.Replica.String() {
		metadata[ReservedMetadataPrefixLower+ReplicaStatus] = replication.Replica.String()
		metadata[ReservedMetadataPrefixLower+ReplicaTimestamp] = UTCNow().Format(time.RFC3339Nano)
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
	if dsc := mustReplicate(ctx, bucket, object, getMustReplicateOptions(metadata, "", "", replication.ObjectReplicationType, ObjectOptions{})); dsc.ReplicateAny() {
		metadata[ReservedMetadataPrefixLower+ReplicationTimestamp] = UTCNow().Format(time.RFC3339Nano)
		metadata[ReservedMetadataPrefixLower+ReplicationStatus] = dsc.PendingStatus()
	}

	// We need to preserve the encryption headers set in EncryptRequest,
	// so we do not want to override them, copy them instead.
	maps.Copy(metadata, encMetadata)

	// Ensure that metadata does not contain sensitive information
	crypto.RemoveSensitiveEntries(metadata)

	if isCompressible(r.Header, object) {
		// Storing the compression metadata.
		metadata[ReservedMetadataPrefix+"compression"] = compressionAlgorithmV2
	}

	opts, err := putOptsFromReq(ctx, r, bucket, object, metadata)
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

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

	checksumType := hash.NewChecksumHeader(r.Header)
	if checksumType.Is(hash.ChecksumInvalid) {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrInvalidChecksum), r.URL)
		return
	} else if checksumType.IsSet() && !checksumType.Is(hash.ChecksumTrailing) {
		opts.WantChecksum = &hash.Checksum{Type: checksumType}
	}

	if opts.WantChecksum != nil {
		opts.WantChecksum.Type |= hash.ChecksumMultipart | hash.ChecksumIncludesMultipart
	}

	newMultipartUpload := objectAPI.NewMultipartUpload

	res, err := newMultipartUpload(ctx, bucket, object, opts)
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	response := generateInitiateMultipartUploadResponse(bucket, object, res.UploadID)
	if res.ChecksumAlgo != "" {
		w.Header().Set(xhttp.AmzChecksumAlgo, res.ChecksumAlgo)
		if res.ChecksumType != "" {
			w.Header().Set(xhttp.AmzChecksumType, res.ChecksumType)
		}
	}
	encodedSuccessResponse := encodeResponse(response)

	// Write success response.
	writeSuccessResponseXML(w, encodedSuccessResponse)
}

// CopyObjectPartHandler - uploads a part by copying data from an existing object as data source.
func (api objectAPIHandlers) CopyObjectPartHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "CopyObjectPart")

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

	uploadID := r.Form.Get(xhttp.UploadID)
	partIDString := r.Form.Get(xhttp.PartNumber)

	partID, err := strconv.Atoi(partIDString)
	if err != nil || partID <= 0 {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrInvalidPart), r.URL)
		return
	}

	// check partID with maximum part ID for multipart objects
	if isMaxPartID(partID) {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrInvalidMaxParts), r.URL)
		return
	}

	var srcOpts, dstOpts ObjectOptions
	srcOpts, err = copySrcOpts(ctx, r, srcBucket, srcObject)
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}
	srcOpts.VersionID = vid

	// convert copy src and dst encryption options for GET/PUT calls
	getOpts := ObjectOptions{VersionID: srcOpts.VersionID}
	if srcOpts.ServerSideEncryption != nil {
		getOpts.ServerSideEncryption = encrypt.SSE(srcOpts.ServerSideEncryption)
	}

	dstOpts, err = copyDstOpts(ctx, r, dstBucket, dstObject, nil)
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	getObjectNInfo := objectAPI.GetObjectNInfo

	// Get request range.
	var rs *HTTPRangeSpec
	var parseRangeErr error
	if rangeHeader := r.Header.Get(xhttp.AmzCopySourceRange); rangeHeader != "" {
		rs, parseRangeErr = parseCopyPartRangeSpec(rangeHeader)
	} else {
		// This check is to see if client specified a header but the value
		// is empty for 'x-amz-copy-source-range'
		_, ok := r.Header[xhttp.AmzCopySourceRange]
		if ok {
			parseRangeErr = errInvalidRange
		}
	}

	checkCopyPartPrecondFn := func(o ObjectInfo) bool {
		if _, err := DecryptObjectInfo(&o, r); err != nil {
			writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
			return true
		}
		if checkCopyObjectPartPreconditions(ctx, w, r, o) {
			return true
		}
		if parseRangeErr != nil {
			writeCopyPartErr(ctx, w, parseRangeErr, r.URL)
			// Range header mismatch is pre-condition like failure
			// so return true to indicate Range precondition failed.
			return true
		}
		return false
	}
	getOpts.CheckPrecondFn = checkCopyPartPrecondFn
	gr, err := getObjectNInfo(ctx, srcBucket, srcObject, rs, r.Header, getOpts)
	if err != nil {
		if isErrPreconditionFailed(err) {
			return
		}
		if globalBucketVersioningSys.PrefixEnabled(srcBucket, srcObject) && gr != nil {
			// Versioning enabled quite possibly object is deleted might be delete-marker
			// if present set the headers, no idea why AWS S3 sets these headers.
			if gr.ObjInfo.VersionID != "" && gr.ObjInfo.DeleteMarker {
				w.Header()[xhttp.AmzVersionID] = []string{gr.ObjInfo.VersionID}
				w.Header()[xhttp.AmzDeleteMarker] = []string{strconv.FormatBool(gr.ObjInfo.DeleteMarker)}
			}
		}
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}
	defer gr.Close()
	srcInfo := gr.ObjInfo

	actualPartSize, err := srcInfo.GetActualSize()
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	if err := enforceBucketQuotaHard(ctx, dstBucket, actualPartSize); err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	// Special care for CopyObjectPart
	if partRangeErr := checkCopyPartRangeWithSize(rs, actualPartSize); partRangeErr != nil {
		writeCopyPartErr(ctx, w, partRangeErr, r.URL)
		return
	}

	// Get the object offset & length
	startOffset, length, err := rs.GetOffsetLength(actualPartSize)
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	// maximum copy size for multipart objects in a single operation
	if isMaxObjectSize(length) {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrEntityTooLarge), r.URL)
		return
	}

	if isRemoteCopyRequired(ctx, srcBucket, dstBucket, objectAPI) {
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

		popts := minio.PutObjectPartOptions{
			SSE: dstOpts.ServerSideEncryption,
		}

		partInfo, err := core.PutObjectPart(ctx, dstBucket, dstObject, uploadID, partID, gr, length, popts)
		if err != nil {
			writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
			return
		}

		response := generateCopyObjectPartResponse(partInfo.ETag, partInfo.LastModified)
		encodedSuccessResponse := encodeResponse(response)

		// Write success response.
		writeSuccessResponseXML(w, encodedSuccessResponse)
		return
	}

	actualPartSize = length
	var reader io.Reader = etag.NewReader(ctx, gr, nil, nil)

	mi, err := objectAPI.GetMultipartInfo(ctx, dstBucket, dstObject, uploadID, dstOpts)
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	_, isEncrypted := crypto.IsEncrypted(mi.UserDefined)

	// Read compression metadata preserved in the init multipart for the decision.
	_, isCompressed := mi.UserDefined[ReservedMetadataPrefix+"compression"]
	// Compress only if the compression is enabled during initial multipart.
	var idxCb func() []byte
	if isCompressed {
		wantEncryption := crypto.Requested(r.Header) || isEncrypted
		s2c, cb := newS2CompressReader(reader, actualPartSize, wantEncryption)
		idxCb = cb
		defer s2c.Close()
		reader = etag.Wrap(s2c, reader)
		length = -1
	}

	srcInfo.Reader, err = hash.NewReader(ctx, reader, length, "", "", actualPartSize)
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	dstOpts, err = copyDstOpts(ctx, r, dstBucket, dstObject, mi.UserDefined)
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}
	dstOpts.IndexCB = idxCb

	rawReader := srcInfo.Reader
	pReader := NewPutObjReader(rawReader)

	var objectEncryptionKey crypto.ObjectKey
	if isEncrypted {
		if !crypto.SSEC.IsRequested(r.Header) && crypto.SSEC.IsEncrypted(mi.UserDefined) {
			writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrSSEMultipartEncrypted), r.URL)
			return
		}
		if crypto.S3.IsEncrypted(mi.UserDefined) && crypto.SSEC.IsRequested(r.Header) {
			writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrSSEMultipartEncrypted), r.URL)
			return
		}
		var key []byte
		if crypto.SSEC.IsRequested(r.Header) {
			key, err = ParseSSECustomerRequest(r)
			if err != nil {
				writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
				return
			}
		}
		key, err = decryptObjectMeta(key, dstBucket, dstObject, mi.UserDefined)
		if err != nil {
			writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
			return
		}
		copy(objectEncryptionKey[:], key)

		var nonce [12]byte
		tmp := sha256.Sum256(fmt.Append(nil, uploadID, partID))
		copy(nonce[:], tmp[:12])

		partEncryptionKey := objectEncryptionKey.DerivePartKey(uint32(partID))
		encReader, err := sio.EncryptReader(reader, sio.Config{
			Key:   partEncryptionKey[:],
			Nonce: &nonce,
		})
		if err != nil {
			writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
			return
		}
		reader = etag.Wrap(encReader, reader)

		wantSize := int64(-1)
		if length >= 0 {
			info := ObjectInfo{Size: length}
			wantSize = info.EncryptedSize()
		}

		srcInfo.Reader, err = hash.NewReader(ctx, reader, wantSize, "", "", actualPartSize)
		if err != nil {
			writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
			return
		}
		pReader, err = pReader.WithEncryption(srcInfo.Reader, &objectEncryptionKey)
		if err != nil {
			writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
			return
		}
		if dstOpts.IndexCB != nil {
			dstOpts.IndexCB = compressionIndexEncrypter(objectEncryptionKey, dstOpts.IndexCB)
		}
	}

	srcInfo.PutObjReader = pReader
	copyObjectPart := objectAPI.CopyObjectPart

	// Copy source object to destination, if source and destination
	// object is same then only metadata is updated.
	partInfo, err := copyObjectPart(ctx, srcBucket, srcObject, dstBucket, dstObject, uploadID, partID,
		startOffset, length, srcInfo, srcOpts, dstOpts)
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	if isEncrypted {
		sseS3 := crypto.S3.IsRequested(r.Header) || crypto.S3.IsEncrypted(mi.UserDefined)
		partInfo.ETag = tryDecryptETag(objectEncryptionKey[:], partInfo.ETag, sseS3)
	}

	response := generateCopyObjectPartResponse(partInfo.ETag, partInfo.LastModified)
	encodedSuccessResponse := encodeResponse(response)

	// Write success response.
	writeSuccessResponseXML(w, encodedSuccessResponse)
}

// PutObjectPartHandler - uploads an incoming part for an ongoing multipart operation.
func (api objectAPIHandlers) PutObjectPartHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "PutObjectPart")

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

	clientETag, err := etag.FromContentMD5(r.Header)
	if err != nil {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrInvalidDigest), r.URL)
		return
	}

	// if Content-Length is unknown/missing, throw away
	size := r.ContentLength

	rAuthType := getRequestAuthType(r)
	// For auth type streaming signature, we need to gather a different content length.
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

	uploadID := r.Form.Get(xhttp.UploadID)
	partIDString := r.Form.Get(xhttp.PartNumber)

	partID, err := strconv.Atoi(partIDString)
	if err != nil || partID <= 0 {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrInvalidPart), r.URL)
		return
	}

	// maximum size for multipart objects in a single operation
	if isMaxObjectSize(size) {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrEntityTooLarge), r.URL)
		return
	}

	// check partID with maximum part ID for multipart objects
	if isMaxPartID(partID) {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrInvalidMaxParts), r.URL)
		return
	}

	var (
		md5hex              = clientETag.String()
		sha256hex           = ""
		reader    io.Reader = r.Body
		s3Error   APIErrorCode
	)
	if s3Error = isPutActionAllowed(ctx, rAuthType, bucket, object, r, policy.PutObjectAction); s3Error != ErrNone {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(s3Error), r.URL)
		return
	}

	switch rAuthType {
	case authTypeStreamingSigned, authTypeStreamingSignedTrailer:
		// Initialize stream signature verifier.
		reader, s3Error = newSignV4ChunkedReader(r, rAuthType == authTypeStreamingSignedTrailer)
		if s3Error != ErrNone {
			writeErrorResponse(ctx, w, errorCodes.ToAPIErr(s3Error), r.URL)
			return
		}
	case authTypeStreamingUnsignedTrailer:
		// Initialize stream signature verifier.
		reader, s3Error = newUnsignedV4ChunkedReader(r, true, r.Header.Get(xhttp.Authorization) != "")
		if s3Error != ErrNone {
			writeErrorResponse(ctx, w, errorCodes.ToAPIErr(s3Error), r.URL)
			return
		}
	case authTypeSignedV2, authTypePresignedV2:
		if s3Error = isReqAuthenticatedV2(r); s3Error != ErrNone {
			writeErrorResponse(ctx, w, errorCodes.ToAPIErr(s3Error), r.URL)
			return
		}
	case authTypePresigned, authTypeSigned:
		if s3Error = reqSignatureV4Verify(r, globalSite.Region(), serviceS3); s3Error != ErrNone {
			writeErrorResponse(ctx, w, errorCodes.ToAPIErr(s3Error), r.URL)
			return
		}

		if !skipContentSha256Cksum(r) {
			sha256hex = getContentSha256Cksum(r, serviceS3)
		}
	}

	if err := enforceBucketQuotaHard(ctx, bucket, size); err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	actualSize := size

	// get encryption options
	var opts ObjectOptions
	if crypto.SSEC.IsRequested(r.Header) {
		opts, err = getOpts(ctx, r, bucket, object)
		if err != nil {
			writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
			return
		}
	}

	mi, err := objectAPI.GetMultipartInfo(ctx, bucket, object, uploadID, opts)
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	// Read compression metadata preserved in the init multipart for the decision.
	_, isCompressed := mi.UserDefined[ReservedMetadataPrefix+"compression"]
	var idxCb func() []byte
	if isCompressed {
		actualReader, err := hash.NewReader(ctx, reader, size, md5hex, sha256hex, actualSize)
		if err != nil {
			writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
			return
		}
		if err = actualReader.AddChecksum(r, false); err != nil {
			writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrInvalidChecksum), r.URL)
			return
		}

		// Set compression metrics.
		wantEncryption := crypto.Requested(r.Header)
		s2c, cb := newS2CompressReader(actualReader, actualSize, wantEncryption)
		idxCb = cb
		defer s2c.Close()
		reader = etag.Wrap(s2c, actualReader)
		size = -1   // Since compressed size is un-predictable.
		md5hex = "" // Do not try to verify the content.
		sha256hex = ""
	}

	var forceMD5 []byte
	// Optimization: If SSE-KMS and SSE-C did not request Content-Md5. Use uuid as etag. Optionally enable this also
	// for server that is started with `--no-compat`.
	if !etag.ContentMD5Requested(r.Header) && (crypto.S3KMS.IsEncrypted(mi.UserDefined) || crypto.SSEC.IsRequested(r.Header) || !globalServerCtxt.StrictS3Compat) {
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

	if err := hashReader.AddChecksum(r, size < 0); err != nil {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrInvalidChecksum), r.URL)
		return
	}

	pReader := NewPutObjReader(hashReader)

	_, isEncrypted := crypto.IsEncrypted(mi.UserDefined)
	_, replicationStatus := mi.UserDefined[xhttp.AmzBucketReplicationStatus]
	_, sourceReplReq := r.Header[xhttp.MinIOSourceReplicationRequest]
	var objectEncryptionKey crypto.ObjectKey
	if isEncrypted {
		if !crypto.SSEC.IsRequested(r.Header) && crypto.SSEC.IsEncrypted(mi.UserDefined) && !replicationStatus {
			writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrSSEMultipartEncrypted), r.URL)
			return
		}

		opts, err = putOptsFromReq(ctx, r, bucket, object, mi.UserDefined)
		if err != nil {
			writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
			return
		}

		var key []byte
		if crypto.SSEC.IsRequested(r.Header) {
			key, err = ParseSSECustomerRequest(r)
			if err != nil {
				writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
				return
			}
		}

		if !sourceReplReq || !crypto.SSEC.IsEncrypted(mi.UserDefined) {
			// Calculating object encryption key
			key, err = decryptObjectMeta(key, bucket, object, mi.UserDefined)
			if err != nil {
				writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
				return
			}
			copy(objectEncryptionKey[:], key)

			partEncryptionKey := objectEncryptionKey.DerivePartKey(uint32(partID))
			in := io.Reader(hashReader)
			if size > encryptBufferThreshold {
				// The encryption reads in blocks of 64KB.
				// We add a buffer on bigger files to reduce the number of syscalls upstream.
				in = bufio.NewReaderSize(hashReader, encryptBufferSize)
			}

			var nonce [12]byte
			tmp := sha256.Sum256(fmt.Append(nil, uploadID, partID))
			copy(nonce[:], tmp[:12])

			reader, err = sio.EncryptReader(in, sio.Config{
				Key:   partEncryptionKey[:],
				Nonce: &nonce,
			})
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
			if err := hashReader.AddChecksum(r, true); err != nil {
				writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrInvalidChecksum), r.URL)
				return
			}

			pReader, err = pReader.WithEncryption(hashReader, &objectEncryptionKey)
			if err != nil {
				writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
				return
			}

			if idxCb != nil {
				idxCb = compressionIndexEncrypter(objectEncryptionKey, idxCb)
			}
			opts.EncryptFn = metadataEncrypter(objectEncryptionKey)
		}
	}
	opts.IndexCB = idxCb

	opts.ReplicationRequest = sourceReplReq
	putObjectPart := objectAPI.PutObjectPart

	partInfo, err := putObjectPart(ctx, bucket, object, uploadID, partID, pReader, opts)
	if err != nil {
		// Verify if the underlying error is signature mismatch.
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	etag := partInfo.ETag
	if kind, encrypted := crypto.IsEncrypted(mi.UserDefined); encrypted {
		switch kind {
		case crypto.S3KMS:
			w.Header().Set(xhttp.AmzServerSideEncryption, xhttp.AmzEncryptionKMS)
			w.Header().Set(xhttp.AmzServerSideEncryptionKmsID, mi.KMSKeyID())
			if kmsCtx, ok := mi.UserDefined[crypto.MetaContext]; ok {
				w.Header().Set(xhttp.AmzServerSideEncryptionKmsContext, kmsCtx)
			}
			if len(etag) >= 32 && strings.Count(etag, "-") != 1 {
				etag = etag[len(etag)-32:]
			}
		case crypto.S3:
			w.Header().Set(xhttp.AmzServerSideEncryption, xhttp.AmzEncryptionAES)
			etag, _ = DecryptETag(objectEncryptionKey, ObjectInfo{ETag: etag})
		case crypto.SSEC:
			w.Header().Set(xhttp.AmzServerSideEncryptionCustomerAlgorithm, r.Header.Get(xhttp.AmzServerSideEncryptionCustomerAlgorithm))
			w.Header().Set(xhttp.AmzServerSideEncryptionCustomerKeyMD5, r.Header.Get(xhttp.AmzServerSideEncryptionCustomerKeyMD5))

			if len(etag) >= 32 && strings.Count(etag, "-") != 1 {
				etag = etag[len(etag)-32:]
			}
		}
	}

	// We must not use the http.Header().Set method here because some (broken)
	// clients expect the ETag header key to be literally "ETag" - not "Etag" (case-sensitive).
	// Therefore, we have to set the ETag directly as map entry.
	w.Header()[xhttp.ETag] = []string{"\"" + etag + "\""}
	hash.TransferChecksumHeader(w, r)

	writeSuccessResponseHeadersOnly(w)
}

// CompleteMultipartUploadHandler - Complete multipart upload.
func (api objectAPIHandlers) CompleteMultipartUploadHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "CompleteMultipartUpload")

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

	if s3Error := checkRequestAuthType(ctx, r, policy.PutObjectAction, bucket, object); s3Error != ErrNone {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(s3Error), r.URL)
		return
	}

	// Get upload id.
	uploadID, _, _, _, s3Error := getObjectResources(r.Form)
	if s3Error != ErrNone {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(s3Error), r.URL)
		return
	}

	// Content-Length is required and should be non-zero
	if r.ContentLength <= 0 {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrMissingPart), r.URL)
		return
	}

	complMultipartUpload := &CompleteMultipartUpload{}
	if err = xmlDecoder(r.Body, complMultipartUpload, r.ContentLength); err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}
	if len(complMultipartUpload.Parts) == 0 {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrMissingPart), r.URL)
		return
	}

	if !sort.SliceIsSorted(complMultipartUpload.Parts, func(i, j int) bool {
		return complMultipartUpload.Parts[i].PartNumber < complMultipartUpload.Parts[j].PartNumber
	}) {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrInvalidPartOrder), r.URL)
		return
	}

	// Reject retention or governance headers if set, CompleteMultipartUpload spec
	// does not use these headers, and should not be passed down to checkPutObjectLockAllowed
	if objectlock.IsObjectLockRequested(r.Header) || objectlock.IsObjectLockGovernanceBypassSet(r.Header) {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrInvalidRequest), r.URL)
		return
	}

	if _, _, _, s3Err := checkPutObjectLockAllowed(ctx, r, bucket, object, objectAPI.GetObjectInfo, ErrNone, ErrNone); s3Err != ErrNone {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(s3Err), r.URL)
		return
	}

	completeMultiPartUpload := objectAPI.CompleteMultipartUpload

	versioned := globalBucketVersioningSys.PrefixEnabled(bucket, object)
	suspended := globalBucketVersioningSys.PrefixSuspended(bucket, object)
	os := newObjSweeper(bucket, object).WithVersioning(versioned, suspended)
	if !globalTierConfigMgr.Empty() {
		// Get appropriate object info to identify the remote object to delete
		goiOpts := os.GetOpts()
		if goi, gerr := objectAPI.GetObjectInfo(ctx, bucket, object, goiOpts); gerr == nil {
			os.SetTransitionState(goi.TransitionedObject)
		}
	}

	opts, err := completeMultipartOpts(ctx, r, bucket, object)
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}
	opts.Versioned = versioned
	opts.VersionSuspended = suspended

	// First, we compute the ETag of the multipart object.
	// The ETag of a multi-part object is always:
	//   ETag := MD5(ETag_p1, ETag_p2, ...)+"-N"   (N being the number of parts)
	//
	// This is independent of encryption. An encrypted multipart
	// object also has an ETag that is the MD5 of its part ETags.
	// The fact the in case of encryption the ETag of a part is
	// not the MD5 of the part content does not change that.
	var completeETags []etag.ETag
	for _, part := range complMultipartUpload.Parts {
		ETag, err := etag.Parse(part.ETag)
		if err != nil {
			continue
		}
		completeETags = append(completeETags, ETag)
	}
	multipartETag := etag.Multipart(completeETags...)
	opts.UserDefined["etag"] = multipartETag.String()

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

	objInfo, err := completeMultiPartUpload(ctx, bucket, object, uploadID, complMultipartUpload.Parts, opts)
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	opts.EncryptFn, err = objInfo.metadataEncryptFn(r.Header)
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

	setPutObjHeaders(w, objInfo, false, r.Header)
	if dsc := mustReplicate(ctx, bucket, object, objInfo.getMustReplicateOptions(replication.ObjectReplicationType, opts)); dsc.ReplicateAny() {
		scheduleReplication(ctx, objInfo, objectAPI, dsc, replication.ObjectReplicationType)
	}
	if _, ok := r.Header[xhttp.MinIOSourceReplicationRequest]; ok {
		actualSize, _ := objInfo.GetActualSize()
		defer globalReplicationStats.Load().UpdateReplicaStat(bucket, actualSize)
	}

	// Get object location.
	location := getObjectLocation(r, globalDomainNames, bucket, object)
	// Generate complete multipart response.
	response := generateCompleteMultipartUploadResponse(bucket, object, location, objInfo, r.Header)
	encodedSuccessResponse := encodeResponse(response)

	// Write success response.
	writeSuccessResponseXML(w, encodedSuccessResponse)

	// Notify object created event.
	evt := eventArgs{
		EventName:    event.ObjectCreatedCompleteMultipartUpload,
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
			APIName:   "CompleteMultipartUpload",
			Bucket:    objInfo.Bucket,
			Object:    objInfo.Name,
			VersionID: objInfo.VersionID,
			Status:    http.StatusText(http.StatusOK),
		})
	}

	// Remove the transitioned object whose object version is being overwritten.
	if !globalTierConfigMgr.Empty() {
		// Schedule object for immediate transition if eligible.
		enqueueTransitionImmediate(objInfo, lcEventSrc_s3CompleteMultipartUpload)
		os.Sweep()
	}
}

// AbortMultipartUploadHandler - Abort multipart upload
func (api objectAPIHandlers) AbortMultipartUploadHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "AbortMultipartUpload")

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
	abortMultipartUpload := objectAPI.AbortMultipartUpload

	if s3Error := checkRequestAuthType(ctx, r, policy.AbortMultipartUploadAction, bucket, object); s3Error != ErrNone {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(s3Error), r.URL)
		return
	}

	uploadID, _, _, _, s3Error := getObjectResources(r.Form)
	if s3Error != ErrNone {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(s3Error), r.URL)
		return
	}
	opts := ObjectOptions{}
	if err := abortMultipartUpload(ctx, bucket, object, uploadID, opts); err != nil {
		switch err.(type) {
		case InvalidUploadID:
			// Do not have return an error for non-existent upload-id
		default:
			writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
			return
		}
	}

	writeSuccessNoContent(w)
}

// ListObjectPartsHandler - List object parts
func (api objectAPIHandlers) ListObjectPartsHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "ListObjectParts")

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

	if s3Error := checkRequestAuthType(ctx, r, policy.ListMultipartUploadPartsAction, bucket, object); s3Error != ErrNone {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(s3Error), r.URL)
		return
	}

	uploadID, partNumberMarker, maxParts, encodingType, s3Error := getObjectResources(r.Form)
	if s3Error != ErrNone {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(s3Error), r.URL)
		return
	}
	if partNumberMarker < 0 {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrInvalidPartNumberMarker), r.URL)
		return
	}
	if maxParts < 0 {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrInvalidMaxParts), r.URL)
		return
	}

	opts := ObjectOptions{}
	listPartsInfo, err := objectAPI.ListObjectParts(ctx, bucket, object, uploadID, partNumberMarker, maxParts, opts)
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	// We have to adjust the size of encrypted parts since encrypted parts
	// are slightly larger due to encryption overhead.
	// Further, we have to adjust the ETags of parts when using SSE-S3.
	// Due to AWS S3, SSE-S3 encrypted parts return the plaintext ETag
	// being the content MD5 of that particular part. This is not the
	// case for SSE-C and SSE-KMS objects.
	if kind, ok := crypto.IsEncrypted(listPartsInfo.UserDefined); ok {
		var objectEncryptionKey []byte
		if kind == crypto.S3 {
			objectEncryptionKey, err = decryptObjectMeta(nil, bucket, object, listPartsInfo.UserDefined)
			if err != nil {
				writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
				return
			}
		}
		for i, p := range listPartsInfo.Parts {
			listPartsInfo.Parts[i].ETag = tryDecryptETag(objectEncryptionKey, p.ETag, kind == crypto.S3)
			listPartsInfo.Parts[i].Size = p.ActualSize
		}
	} else if _, ok := listPartsInfo.UserDefined[ReservedMetadataPrefix+"compression"]; ok {
		for i, p := range listPartsInfo.Parts {
			listPartsInfo.Parts[i].Size = p.ActualSize
		}
	}

	response := generateListPartsResponse(listPartsInfo, encodingType)
	encodedSuccessResponse := encodeResponse(response)

	// Write success response.
	writeSuccessResponseXML(w, encodedSuccessResponse)
}

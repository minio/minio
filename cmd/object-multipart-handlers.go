// Copyright (c) 2015-2022 MinIO, Inc.
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
	"encoding/xml"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/mux"
	sse "github.com/minio/minio/internal/bucket/encryption"
	objectlock "github.com/minio/minio/internal/bucket/object/lock"
	"github.com/minio/minio/internal/bucket/replication"
	"github.com/minio/minio/internal/config/storageclass"
	"github.com/minio/minio/internal/crypto"
	"github.com/minio/minio/internal/etag"
	"github.com/minio/minio/internal/event"
	"github.com/minio/minio/internal/fips"
	"github.com/minio/minio/internal/handlers"
	"github.com/minio/minio/internal/hash"
	xhttp "github.com/minio/minio/internal/http"
	"github.com/minio/minio/internal/logger"
	"github.com/minio/pkg/bucket/policy"
	iampolicy "github.com/minio/pkg/iam/policy"
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

	if crypto.Requested(r.Header) {
		if globalIsGateway {
			if crypto.SSEC.IsRequested(r.Header) && !objectAPI.IsEncryptionSupported() {
				writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrNotImplemented), r.URL)
				return
			}
		} else {
			if !objectAPI.IsEncryptionSupported() {
				writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrNotImplemented), r.URL)
				return
			}
		}
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
		Passthrough: globalIsGateway && globalGatewayName == S3BackendGateway,
	})

	// Validate storage class metadata if present
	if sc := r.Header.Get(xhttp.AmzStorageClass); sc != "" {
		if !storageclass.IsValid(sc) {
			writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrInvalidStorageClass), r.URL)
			return
		}
	}

	encMetadata := map[string]string{}

	if objectAPI.IsEncryptionSupported() {
		if crypto.Requested(r.Header) {
			if err = setEncryptionMetadata(r, bucket, object, encMetadata); err != nil {
				writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
				return
			}
			// Set this for multipart only operations, we need to differentiate during
			// decryption if the file was actually multipart or not.
			encMetadata[ReservedMetadataPrefix+"Encrypted-Multipart"] = ""
		}
	}

	// Extract metadata that needs to be saved.
	metadata, err := extractMetadata(ctx, r)
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	retPerms := isPutActionAllowed(ctx, getRequestAuthType(r), bucket, object, r, iampolicy.PutObjectRetentionAction)
	holdPerms := isPutActionAllowed(ctx, getRequestAuthType(r), bucket, object, r, iampolicy.PutObjectLegalHoldAction)

	getObjectInfo := objectAPI.GetObjectInfo
	if api.CacheAPI() != nil {
		getObjectInfo = api.CacheAPI().GetObjectInfo
	}

	retentionMode, retentionDate, legalHold, s3Err := checkPutObjectLockAllowed(ctx, r, bucket, object, getObjectInfo, retPerms, holdPerms)
	if s3Err == ErrNone && retentionMode.Valid() {
		metadata[strings.ToLower(xhttp.AmzObjectLockMode)] = string(retentionMode)
		metadata[strings.ToLower(xhttp.AmzObjectLockRetainUntilDate)] = retentionDate.UTC().Format(iso8601TimeFormat)
	}
	if s3Err == ErrNone && legalHold.Status.Valid() {
		metadata[strings.ToLower(xhttp.AmzObjectLockLegalHold)] = string(legalHold.Status)
	}
	if s3Err != ErrNone {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(s3Err), r.URL)
		return
	}
	if dsc := mustReplicate(ctx, bucket, object, getMustReplicateOptions(ObjectInfo{
		UserDefined: metadata,
	}, replication.ObjectReplicationType, ObjectOptions{})); dsc.ReplicateAny() {
		metadata[ReservedMetadataPrefixLower+ReplicationTimestamp] = UTCNow().Format(time.RFC3339Nano)
		metadata[ReservedMetadataPrefixLower+ReplicationStatus] = dsc.PendingStatus()
	}
	// We need to preserve the encryption headers set in EncryptRequest,
	// so we do not want to override them, copy them instead.
	for k, v := range encMetadata {
		metadata[k] = v
	}

	// Ensure that metadata does not contain sensitive information
	crypto.RemoveSensitiveEntries(metadata)

	if objectAPI.IsCompressionSupported() && isCompressible(r.Header, object) {
		// Storing the compression metadata.
		metadata[ReservedMetadataPrefix+"compression"] = compressionAlgorithmV2
	}

	opts, err := putOpts(ctx, r, bucket, object, metadata)
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}
	newMultipartUpload := objectAPI.NewMultipartUpload
	if api.CacheAPI() != nil {
		newMultipartUpload = api.CacheAPI().NewMultipartUpload
	}

	uploadID, err := newMultipartUpload(ctx, bucket, object, opts)
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	response := generateInitiateMultipartUploadResponse(bucket, object, uploadID)
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

	if crypto.Requested(r.Header) {
		if globalIsGateway {
			if crypto.SSEC.IsRequested(r.Header) && !objectAPI.IsEncryptionSupported() {
				writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrNotImplemented), r.URL)
				return
			}
		} else {
			if !objectAPI.IsEncryptionSupported() {
				writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrNotImplemented), r.URL)
				return
			}
		}
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
	if rAuthType == authTypeStreamingSigned {
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

	// maximum Upload size for multipart objects in a single operation
	if isMaxAllowedPartSize(size) {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrEntityTooLarge), r.URL)
		return
	}

	uploadID := r.Form.Get(xhttp.UploadID)
	partIDString := r.Form.Get(xhttp.PartNumber)

	partID, err := strconv.Atoi(partIDString)
	if err != nil {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrInvalidPart), r.URL)
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
	if s3Error = isPutActionAllowed(ctx, rAuthType, bucket, object, r, iampolicy.PutObjectAction); s3Error != ErrNone {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(s3Error), r.URL)
		return
	}

	switch rAuthType {
	case authTypeStreamingSigned:
		// Initialize stream signature verifier.
		reader, s3Error = newSignV4ChunkedReader(r)
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
		if s3Error = reqSignatureV4Verify(r, globalSite.Region, serviceS3); s3Error != ErrNone {
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
	if objectAPI.IsCompressionSupported() && isCompressed {
		actualReader, err := hash.NewReader(reader, size, md5hex, sha256hex, actualSize)
		if err != nil {
			writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
			return
		}

		// Set compression metrics.
		wantEncryption := objectAPI.IsEncryptionSupported() && crypto.Requested(r.Header)
		s2c, cb := newS2CompressReader(actualReader, actualSize, wantEncryption)
		idxCb = cb
		defer s2c.Close()
		reader = etag.Wrap(s2c, actualReader)
		size = -1   // Since compressed size is un-predictable.
		md5hex = "" // Do not try to verify the content.
		sha256hex = ""
	}

	hashReader, err := hash.NewReader(reader, size, md5hex, sha256hex, actualSize)
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}
	rawReader := hashReader
	pReader := NewPutObjReader(rawReader)

	_, isEncrypted := crypto.IsEncrypted(mi.UserDefined)
	var objectEncryptionKey crypto.ObjectKey
	if objectAPI.IsEncryptionSupported() && isEncrypted {
		if !crypto.SSEC.IsRequested(r.Header) && crypto.SSEC.IsEncrypted(mi.UserDefined) {
			writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrSSEMultipartEncrypted), r.URL)
			return
		}

		opts, err = putOpts(ctx, r, bucket, object, mi.UserDefined)
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

		// Calculating object encryption key
		key, err = decryptObjectInfo(key, bucket, object, mi.UserDefined)
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
		reader, err = sio.EncryptReader(in, sio.Config{Key: partEncryptionKey[:], CipherSuites: fips.DARECiphers()})
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
		hashReader, err = hash.NewReader(etag.Wrap(reader, hashReader), wantSize, "", "", actualSize)
		if err != nil {
			writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
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
	}
	opts.IndexCB = idxCb

	putObjectPart := objectAPI.PutObjectPart
	if api.CacheAPI() != nil {
		putObjectPart = api.CacheAPI().PutObjectPart
	}

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

	// Content-Length is required and should be non-zero
	if r.ContentLength <= 0 {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrMissingContentLength), r.URL)
		return
	}

	// Get upload id.
	uploadID, _, _, _, s3Error := getObjectResources(r.Form)
	if s3Error != ErrNone {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(s3Error), r.URL)
		return
	}

	complMultipartUpload := &CompleteMultipartUpload{}
	if err = xmlDecoder(r.Body, complMultipartUpload, r.ContentLength); err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}
	if len(complMultipartUpload.Parts) == 0 {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrMalformedXML), r.URL)
		return
	}
	if !sort.IsSorted(CompletedParts(complMultipartUpload.Parts)) {
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
	if api.CacheAPI() != nil {
		completeMultiPartUpload = api.CacheAPI().CompleteMultipartUpload
	}
	// This code is specifically to handle the requirements for slow
	// complete multipart upload operations on FS mode.
	writeErrorResponseWithoutXMLHeader := func(ctx context.Context, w http.ResponseWriter, err APIError, reqURL *url.URL) {
		switch err.Code {
		case "SlowDown", "XMinioServerNotInitialized", "XMinioReadQuorum", "XMinioWriteQuorum":
			// Set retxry-after header to indicate user-agents to retry request after 120secs.
			// https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Retry-After
			w.Header().Set(xhttp.RetryAfter, "120")
		}

		// Generate error response.
		errorResponse := getAPIErrorResponse(ctx, err, reqURL.Path,
			w.Header().Get(xhttp.AmzRequestID), globalDeploymentID)
		encodedErrorResponse, _ := xml.Marshal(errorResponse)
		setCommonHeaders(w)
		w.Header().Set(xhttp.ContentType, string(mimeXML))
		w.Write(encodedErrorResponse)
	}

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

	setEventStreamHeaders(w)

	opts, err := completeMultipartOpts(ctx, r, bucket, object)
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

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

	// However, in case of encryption, the persisted part ETags don't match
	// what we have sent to the client during PutObjectPart. The reason is
	// that ETags are encrypted. Hence, the client will send a list of complete
	// part ETags of which non can match the ETag of any part. For example
	//   ETag (client):          30902184f4e62dd8f98f0aaff810c626
	//   ETag (server-internal): 20000f00ce5dc16e3f3b124f586ae1d88e9caa1c598415c2759bbb50e84a59f630902184f4e62dd8f98f0aaff810c626
	//
	// Therefore, we adjust all ETags sent by the client to match what is stored
	// on the backend.
	// TODO(klauspost): This should be done while object is finalized instead of fetching the data twice
	if objectAPI.IsEncryptionSupported() {
		mi, err := objectAPI.GetMultipartInfo(ctx, bucket, object, uploadID, ObjectOptions{})
		if err != nil {
			writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
			return
		}

		if _, ok := crypto.IsEncrypted(mi.UserDefined); ok {
			// Only fetch parts in between first and last.
			// We already checked if we have at least one part.
			start := complMultipartUpload.Parts[0].PartNumber
			maxParts := complMultipartUpload.Parts[len(complMultipartUpload.Parts)-1].PartNumber - start + 1
			listPartsInfo, err := objectAPI.ListObjectParts(ctx, bucket, object, uploadID, start-1, maxParts, ObjectOptions{})
			if err != nil {
				writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
				return
			}
			sort.Slice(listPartsInfo.Parts, func(i, j int) bool {
				return listPartsInfo.Parts[i].PartNumber < listPartsInfo.Parts[j].PartNumber
			})
			for i := range listPartsInfo.Parts {
				for j := range complMultipartUpload.Parts {
					if listPartsInfo.Parts[i].PartNumber == complMultipartUpload.Parts[j].PartNumber {
						complMultipartUpload.Parts[j].ETag = listPartsInfo.Parts[i].ETag
						continue
					}
				}
			}
		}
	}

	w = &whiteSpaceWriter{ResponseWriter: w, Flusher: w.(http.Flusher)}
	completeDoneCh := sendWhiteSpace(ctx, w)
	objInfo, err := completeMultiPartUpload(ctx, bucket, object, uploadID, complMultipartUpload.Parts, opts)
	// Stop writing white spaces to the client. Note that close(doneCh) style is not used as it
	// can cause white space to be written after we send XML response in a race condition.
	headerWritten := <-completeDoneCh
	if err != nil {
		if headerWritten {
			writeErrorResponseWithoutXMLHeader(ctx, w, toAPIError(ctx, err), r.URL)
		} else {
			writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		}
		return
	}

	// Get object location.
	location := getObjectLocation(r, globalDomainNames, bucket, object)
	// Generate complete multipart response.
	response := generateCompleteMultpartUploadResponse(bucket, object, location, objInfo.ETag)
	var encodedSuccessResponse []byte
	if !headerWritten {
		encodedSuccessResponse = encodeResponse(response)
	} else {
		encodedSuccessResponse, err = xml.Marshal(response)
		if err != nil {
			writeErrorResponseWithoutXMLHeader(ctx, w, toAPIError(ctx, err), r.URL)
			return
		}
	}

	if r.Header.Get(xMinIOExtract) == "true" && strings.HasSuffix(object, archiveExt) {
		opts := ObjectOptions{VersionID: objInfo.VersionID, MTime: objInfo.ModTime}
		if _, err := updateObjectMetadataWithZipInfo(ctx, objectAPI, bucket, object, opts); err != nil {
			writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
			return
		}
	}

	setPutObjHeaders(w, objInfo, false)
	if dsc := mustReplicate(ctx, bucket, object, getMustReplicateOptions(objInfo, replication.ObjectReplicationType, opts)); dsc.ReplicateAny() {
		scheduleReplication(ctx, objInfo.Clone(), objectAPI, dsc, replication.ObjectReplicationType)
	}
	if _, ok := r.Header[xhttp.MinIOSourceReplicationRequest]; ok {
		actualSize, _ := objInfo.GetActualSize()
		defer globalReplicationStats.UpdateReplicaStat(bucket, actualSize)
	}

	// Write success response.
	writeSuccessResponseXML(w, encodedSuccessResponse)

	// Notify object created event.
	sendEvent(eventArgs{
		EventName:    event.ObjectCreatedCompleteMultipartUpload,
		BucketName:   bucket,
		Object:       objInfo,
		ReqParams:    extractReqParams(r),
		RespElements: extractRespElements(w),
		UserAgent:    r.UserAgent(),
		Host:         handlers.GetSourceIP(r),
	})

	// Remove the transitioned object whose object version is being overwritten.
	if !globalTierConfigMgr.Empty() {
		// Schedule object for immediate transition if eligible.
		enqueueTransitionImmediate(objInfo)
		logger.LogIf(ctx, os.Sweep())
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
	if api.CacheAPI() != nil {
		abortMultipartUpload = api.CacheAPI().AbortMultipartUpload
	}

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
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
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
	if kind, ok := crypto.IsEncrypted(listPartsInfo.UserDefined); ok && objectAPI.IsEncryptionSupported() {
		var objectEncryptionKey []byte
		if kind == crypto.S3 {
			objectEncryptionKey, err = decryptObjectInfo(nil, bucket, object, listPartsInfo.UserDefined)
			if err != nil {
				writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
				return
			}
		}
		for i, p := range listPartsInfo.Parts {
			listPartsInfo.Parts[i].ETag = tryDecryptETag(objectEncryptionKey, p.ETag, kind != crypto.S3)
			listPartsInfo.Parts[i].Size = p.ActualSize
		}
	}

	response := generateListPartsResponse(listPartsInfo, encodingType)
	encodedSuccessResponse := encodeResponse(response)

	// Write success response.
	writeSuccessResponseXML(w, encodedSuccessResponse)
}

type whiteSpaceWriter struct {
	http.ResponseWriter
	http.Flusher
	written bool
}

func (w *whiteSpaceWriter) Write(b []byte) (n int, err error) {
	n, err = w.ResponseWriter.Write(b)
	w.written = true
	return
}

func (w *whiteSpaceWriter) WriteHeader(statusCode int) {
	if !w.written {
		w.ResponseWriter.WriteHeader(statusCode)
	}
}

// Send empty whitespaces every 10 seconds to the client till completeMultiPartUpload() is
// done so that the client does not time out. Downside is we might send 200 OK and
// then send error XML. But accoording to S3 spec the client is supposed to check
// for error XML even if it received 200 OK. But for erasure this is not a problem
// as completeMultiPartUpload() is quick. Even For FS, it would not be an issue as
// we do background append as and when the parts arrive and completeMultiPartUpload
// is quick. Only in a rare case where parts would be out of order will
// FS:completeMultiPartUpload() take a longer time.
func sendWhiteSpace(ctx context.Context, w http.ResponseWriter) <-chan bool {
	doneCh := make(chan bool)
	go func() {
		defer close(doneCh)
		ticker := time.NewTicker(time.Second * 10)
		defer ticker.Stop()
		headerWritten := false
		for {
			select {
			case <-ticker.C:
				// Write header if not written yet.
				if !headerWritten {
					_, err := w.Write([]byte(xml.Header))
					headerWritten = err == nil
				}

				// Once header is written keep writing empty spaces
				// which are ignored by client SDK XML parsers.
				// This occurs when server takes long time to completeMultiPartUpload()
				_, err := w.Write([]byte(" "))
				if err != nil {
					return
				}
				w.(http.Flusher).Flush()
			case doneCh <- headerWritten:
				return
			case <-ctx.Done():
				return
			}
		}
	}()
	return doneCh
}

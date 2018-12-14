/*
 * Minio Cloud Storage, (C) 2015, 2016, 2017, 2018 Minio, Inc.
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
	"encoding/base64"
	"encoding/xml"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"path"
	"path/filepath"
	"strings"

	"github.com/gorilla/mux"

	"github.com/minio/minio-go/pkg/set"
	"github.com/minio/minio/cmd/crypto"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/dns"
	"github.com/minio/minio/pkg/event"
	"github.com/minio/minio/pkg/handlers"
	"github.com/minio/minio/pkg/hash"
	"github.com/minio/minio/pkg/policy"
	"github.com/minio/minio/pkg/sync/errgroup"
)

// Check if there are buckets on server without corresponding entry in etcd backend and
// make entries. Here is the general flow
// - Range over all the available buckets
// - Check if a bucket has an entry in etcd backend
// -- If no, make an entry
// -- If yes, check if the IP of entry matches local IP. This means entry is for this instance.
// -- If IP of the entry doesn't match, this means entry is for another instance. Log an error to console.
func initFederatorBackend(objLayer ObjectLayer) {
	b, err := objLayer.ListBuckets(context.Background())
	if err != nil {
		logger.LogIf(context.Background(), err)
		return
	}

	g := errgroup.WithNErrs(len(b))
	for index := range b {
		index := index
		g.Go(func() error {
			r, gerr := globalDNSConfig.Get(b[index].Name)
			if gerr != nil {
				if gerr == dns.ErrNoEntriesFound {
					return globalDNSConfig.Put(b[index].Name)
				}
				return gerr
			}
			if globalDomainIPs.Intersection(set.CreateStringSet(getHostsSlice(r)...)).IsEmpty() {
				// There is already an entry for this bucket, with all IP addresses different. This indicates a bucket name collision. Log an error and continue.
				return fmt.Errorf("Unable to add bucket DNS entry for bucket %s, an entry exists for the same bucket. Use one of these IP addresses %v to access the bucket", b[index].Name, globalDomainIPs.ToSlice())
			}
			return nil
		}, index)
	}

	for _, err := range g.Wait() {
		if err != nil {
			logger.LogIf(context.Background(), err)
			return
		}
	}
}

// GetBucketLocationHandler - GET Bucket location.
// -------------------------
// This operation returns bucket location.
func (api objectAPIHandlers) GetBucketLocationHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "GetBucketLocation")

	defer logger.AuditLog(w, r, "GetBucketLocation", mustGetClaimsFromToken(r))

	vars := mux.Vars(r)
	bucket := vars["bucket"]

	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(w, ErrServerNotInitialized, r.URL, guessIsBrowserReq(r))
		return
	}

	if s3Error := checkRequestAuthType(ctx, r, policy.GetBucketLocationAction, bucket, ""); s3Error != ErrNone {
		writeErrorResponse(w, s3Error, r.URL, guessIsBrowserReq(r))
		return
	}

	getBucketInfo := objectAPI.GetBucketInfo
	if api.CacheAPI() != nil {
		getBucketInfo = api.CacheAPI().GetBucketInfo
	}
	if _, err := getBucketInfo(ctx, bucket); err != nil {
		writeErrorResponse(w, toAPIErrorCode(ctx, err), r.URL, guessIsBrowserReq(r))
		return
	}

	// Generate response.
	encodedSuccessResponse := encodeResponse(LocationResponse{})
	// Get current region.
	region := globalServerConfig.GetRegion()
	if region != globalMinioDefaultRegion {
		encodedSuccessResponse = encodeResponse(LocationResponse{
			Location: region,
		})
	}

	// Write success response.
	writeSuccessResponseXML(w, encodedSuccessResponse)
}

// ListMultipartUploadsHandler - GET Bucket (List Multipart uploads)
// -------------------------
// This operation lists in-progress multipart uploads. An in-progress
// multipart upload is a multipart upload that has been initiated,
// using the Initiate Multipart Upload request, but has not yet been
// completed or aborted. This operation returns at most 1,000 multipart
// uploads in the response.
//
func (api objectAPIHandlers) ListMultipartUploadsHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "ListMultipartUploads")

	defer logger.AuditLog(w, r, "ListMultipartUploads", mustGetClaimsFromToken(r))

	vars := mux.Vars(r)
	bucket := vars["bucket"]

	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(w, ErrServerNotInitialized, r.URL, guessIsBrowserReq(r))
		return
	}

	if s3Error := checkRequestAuthType(ctx, r, policy.ListBucketMultipartUploadsAction, bucket, ""); s3Error != ErrNone {
		writeErrorResponse(w, s3Error, r.URL, guessIsBrowserReq(r))
		return
	}

	prefix, keyMarker, uploadIDMarker, delimiter, maxUploads, _, s3Error := getBucketMultipartResources(r.URL.Query())
	if s3Error != ErrNone {
		writeErrorResponse(w, s3Error, r.URL, guessIsBrowserReq(r))
		return
	}
	if maxUploads < 0 {
		writeErrorResponse(w, ErrInvalidMaxUploads, r.URL, guessIsBrowserReq(r))
		return
	}
	if keyMarker != "" {
		// Marker not common with prefix is not implemented.
		if !hasPrefix(keyMarker, prefix) {
			writeErrorResponse(w, ErrNotImplemented, r.URL, guessIsBrowserReq(r))
			return
		}
	}

	listMultipartsInfo, err := objectAPI.ListMultipartUploads(ctx, bucket, prefix, keyMarker, uploadIDMarker, delimiter, maxUploads)
	if err != nil {
		writeErrorResponse(w, toAPIErrorCode(ctx, err), r.URL, guessIsBrowserReq(r))
		return
	}
	// generate response
	response := generateListMultipartUploadsResponse(bucket, listMultipartsInfo)
	encodedSuccessResponse := encodeResponse(response)

	// write success response.
	writeSuccessResponseXML(w, encodedSuccessResponse)
}

// ListBucketsHandler - GET Service.
// -----------
// This implementation of the GET operation returns a list of all buckets
// owned by the authenticated sender of the request.
func (api objectAPIHandlers) ListBucketsHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "ListBuckets")

	defer logger.AuditLog(w, r, "ListBuckets", mustGetClaimsFromToken(r))

	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(w, ErrServerNotInitialized, r.URL, guessIsBrowserReq(r))
		return
	}
	listBuckets := objectAPI.ListBuckets

	if api.CacheAPI() != nil {
		listBuckets = api.CacheAPI().ListBuckets
	}

	if s3Error := checkRequestAuthType(ctx, r, policy.ListAllMyBucketsAction, "", ""); s3Error != ErrNone {
		writeErrorResponse(w, s3Error, r.URL, guessIsBrowserReq(r))
		return
	}
	// If etcd, dns federation configured list buckets from etcd.
	var bucketsInfo []BucketInfo
	if globalDNSConfig != nil {
		dnsBuckets, err := globalDNSConfig.List()
		if err != nil && err != dns.ErrNoEntriesFound {
			writeErrorResponse(w, toAPIErrorCode(ctx, err), r.URL, guessIsBrowserReq(r))
			return
		}
		bucketSet := set.NewStringSet()
		for _, dnsRecord := range dnsBuckets {
			if bucketSet.Contains(dnsRecord.Key) {
				continue
			}
			bucketsInfo = append(bucketsInfo, BucketInfo{
				Name:    strings.Trim(dnsRecord.Key, slashSeparator),
				Created: dnsRecord.CreationDate,
			})
			bucketSet.Add(dnsRecord.Key)
		}
	} else {
		// Invoke the list buckets.
		var err error
		bucketsInfo, err = listBuckets(ctx)
		if err != nil {
			writeErrorResponse(w, toAPIErrorCode(ctx, err), r.URL, guessIsBrowserReq(r))
			return
		}
	}

	// Generate response.
	response := generateListBucketsResponse(bucketsInfo)
	encodedSuccessResponse := encodeResponse(response)

	// Write response.
	writeSuccessResponseXML(w, encodedSuccessResponse)
}

// DeleteMultipleObjectsHandler - deletes multiple objects.
func (api objectAPIHandlers) DeleteMultipleObjectsHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "DeleteMultipleObjects")

	defer logger.AuditLog(w, r, "DeleteMultipleObjects", mustGetClaimsFromToken(r))

	vars := mux.Vars(r)
	bucket := vars["bucket"]

	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(w, ErrServerNotInitialized, r.URL, guessIsBrowserReq(r))
		return
	}

	var s3Error APIErrorCode
	if s3Error = checkRequestAuthType(ctx, r, policy.DeleteObjectAction, bucket, ""); s3Error != ErrNone {
		// In the event access is denied, a 200 response should still be returned
		// http://docs.aws.amazon.com/AmazonS3/latest/API/multiobjectdeleteapi.html
		if s3Error != ErrAccessDenied {
			writeErrorResponse(w, s3Error, r.URL, guessIsBrowserReq(r))
			return
		}
	}

	// Content-Length is required and should be non-zero
	// http://docs.aws.amazon.com/AmazonS3/latest/API/multiobjectdeleteapi.html
	if r.ContentLength <= 0 {
		writeErrorResponse(w, ErrMissingContentLength, r.URL, guessIsBrowserReq(r))
		return
	}

	// Content-Md5 is requied should be set
	// http://docs.aws.amazon.com/AmazonS3/latest/API/multiobjectdeleteapi.html
	if _, ok := r.Header["Content-Md5"]; !ok {
		writeErrorResponse(w, ErrMissingContentMD5, r.URL, guessIsBrowserReq(r))
		return
	}

	// Allocate incoming content length bytes.
	var deleteXMLBytes []byte
	const maxBodySize = 2 * 1000 * 1024 // The max. XML contains 1000 object names (each at most 1024 bytes long) + XML overhead
	if r.ContentLength > maxBodySize {  // Only allocated memory for at most 1000 objects
		deleteXMLBytes = make([]byte, maxBodySize)
	} else {
		deleteXMLBytes = make([]byte, r.ContentLength)
	}

	// Read incoming body XML bytes.
	if _, err := io.ReadFull(r.Body, deleteXMLBytes); err != nil {
		logger.LogIf(ctx, err)
		writeErrorResponse(w, ErrInternalError, r.URL, guessIsBrowserReq(r))
		return
	}

	// Unmarshal list of keys to be deleted.
	deleteObjects := &DeleteObjectsRequest{}
	if err := xml.Unmarshal(deleteXMLBytes, deleteObjects); err != nil {
		logger.LogIf(ctx, err)
		writeErrorResponse(w, ErrMalformedXML, r.URL, guessIsBrowserReq(r))
		return
	}

	// Deny if WORM is enabled
	if globalWORMEnabled {
		// Not required to check whether given objects exist or not, because
		// DeleteMultipleObject is always successful irrespective of object existence.
		writeErrorResponse(w, ErrMethodNotAllowed, r.URL, guessIsBrowserReq(r))
		return
	}

	deleteObject := objectAPI.DeleteObject
	if api.CacheAPI() != nil {
		deleteObject = api.CacheAPI().DeleteObject
	}

	var dErrs = make([]error, len(deleteObjects.Objects))
	for index, object := range deleteObjects.Objects {
		// If the request is denied access, each item
		// should be marked as 'AccessDenied'
		if s3Error == ErrAccessDenied {
			dErrs[index] = PrefixAccessDenied{
				Bucket: bucket,
				Object: object.ObjectName,
			}
			continue
		}
		dErrs[index] = deleteObject(ctx, bucket, object.ObjectName)
	}

	// Collect deleted objects and errors if any.
	var deletedObjects []ObjectIdentifier
	var deleteErrors []DeleteError
	for index, err := range dErrs {
		object := deleteObjects.Objects[index]
		// Success deleted objects are collected separately.
		if err == nil {
			deletedObjects = append(deletedObjects, object)
			continue
		}
		if _, ok := err.(ObjectNotFound); ok {
			// If the object is not found it should be
			// accounted as deleted as per S3 spec.
			deletedObjects = append(deletedObjects, object)
			continue
		}
		// Error during delete should be collected separately.
		deleteErrors = append(deleteErrors, DeleteError{
			Code:    errorCodeResponse[toAPIErrorCode(ctx, err)].Code,
			Message: errorCodeResponse[toAPIErrorCode(ctx, err)].Description,
			Key:     object.ObjectName,
		})
	}

	// Generate response
	response := generateMultiDeleteResponse(deleteObjects.Quiet, deletedObjects, deleteErrors)
	encodedSuccessResponse := encodeResponse(response)

	// Write success response.
	writeSuccessResponseXML(w, encodedSuccessResponse)

	// Get host and port from Request.RemoteAddr failing which
	// fill them with empty strings.
	host, port, err := net.SplitHostPort(handlers.GetSourceIP(r))
	if err != nil {
		host, port = "", ""
	}

	// Notify deleted event for objects.
	for _, dobj := range deletedObjects {
		sendEvent(eventArgs{
			EventName:  event.ObjectRemovedDelete,
			BucketName: bucket,
			Object: ObjectInfo{
				Name: dobj.ObjectName,
			},
			ReqParams:    extractReqParams(r),
			RespElements: extractRespElements(w),
			UserAgent:    r.UserAgent(),
			Host:         host,
			Port:         port,
		})
	}
}

// PutBucketHandler - PUT Bucket
// ----------
// This implementation of the PUT operation creates a new bucket for authenticated request
func (api objectAPIHandlers) PutBucketHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "PutBucket")

	defer logger.AuditLog(w, r, "PutBucket", mustGetClaimsFromToken(r))

	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(w, ErrServerNotInitialized, r.URL, guessIsBrowserReq(r))
		return
	}

	vars := mux.Vars(r)
	bucket := vars["bucket"]

	if s3Error := checkRequestAuthType(ctx, r, policy.CreateBucketAction, bucket, ""); s3Error != ErrNone {
		writeErrorResponse(w, s3Error, r.URL, guessIsBrowserReq(r))
		return
	}

	// Parse incoming location constraint.
	location, s3Error := parseLocationConstraint(r)
	if s3Error != ErrNone {
		writeErrorResponse(w, s3Error, r.URL, guessIsBrowserReq(r))
		return
	}

	// Validate if location sent by the client is valid, reject
	// requests which do not follow valid region requirements.
	if !isValidLocation(location) {
		writeErrorResponse(w, ErrInvalidRegion, r.URL, guessIsBrowserReq(r))
		return
	}

	if globalDNSConfig != nil {
		if _, err := globalDNSConfig.Get(bucket); err != nil {
			if err == dns.ErrNoEntriesFound {
				// Proceed to creating a bucket.
				if err = objectAPI.MakeBucketWithLocation(ctx, bucket, location); err != nil {
					writeErrorResponse(w, toAPIErrorCode(ctx, err), r.URL, guessIsBrowserReq(r))
					return
				}
				if err = globalDNSConfig.Put(bucket); err != nil {
					objectAPI.DeleteBucket(ctx, bucket)
					writeErrorResponse(w, toAPIErrorCode(ctx, err), r.URL, guessIsBrowserReq(r))
					return
				}

				// Make sure to add Location information here only for bucket
				w.Header().Set("Location", getObjectLocation(r, globalDomainName, bucket, ""))

				writeSuccessResponseHeadersOnly(w)
				return
			}
			writeErrorResponse(w, toAPIErrorCode(ctx, err), r.URL, guessIsBrowserReq(r))
			return

		}
		writeErrorResponse(w, ErrBucketAlreadyOwnedByYou, r.URL, guessIsBrowserReq(r))
		return
	}

	// Proceed to creating a bucket.
	err := objectAPI.MakeBucketWithLocation(ctx, bucket, location)
	if err != nil {
		writeErrorResponse(w, toAPIErrorCode(ctx, err), r.URL, guessIsBrowserReq(r))
		return
	}

	// Make sure to add Location information here only for bucket
	w.Header().Set("Location", path.Clean(r.URL.Path)) // Clean any trailing slashes.

	writeSuccessResponseHeadersOnly(w)
}

// PostPolicyBucketHandler - POST policy
// ----------
// This implementation of the POST operation handles object creation with a specified
// signature policy in multipart/form-data
func (api objectAPIHandlers) PostPolicyBucketHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "PostPolicyBucket")

	defer logger.AuditLog(w, r, "PostPolicyBucket", mustGetClaimsFromToken(r))

	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(w, ErrServerNotInitialized, r.URL, guessIsBrowserReq(r))
		return
	}
	if crypto.S3KMS.IsRequested(r.Header) { // SSE-KMS is not supported
		writeErrorResponse(w, ErrNotImplemented, r.URL, guessIsBrowserReq(r))
		return
	}
	if !objectAPI.IsEncryptionSupported() && hasServerSideEncryptionHeader(r.Header) {
		writeErrorResponse(w, ErrNotImplemented, r.URL, guessIsBrowserReq(r))
		return
	}
	bucket := mux.Vars(r)["bucket"]

	// Require Content-Length to be set in the request
	size := r.ContentLength
	if size < 0 {
		writeErrorResponse(w, ErrMissingContentLength, r.URL, guessIsBrowserReq(r))
		return
	}
	resource, err := getResource(r.URL.Path, r.Host, globalDomainName)
	if err != nil {
		writeErrorResponse(w, ErrInvalidRequest, r.URL, guessIsBrowserReq(r))
		return
	}
	// Make sure that the URL  does not contain object name.
	if bucket != filepath.Clean(resource[1:]) {
		writeErrorResponse(w, ErrMethodNotAllowed, r.URL, guessIsBrowserReq(r))
		return
	}

	// Here the parameter is the size of the form data that should
	// be loaded in memory, the remaining being put in temporary files.
	reader, err := r.MultipartReader()
	if err != nil {
		logger.LogIf(ctx, err)
		writeErrorResponse(w, ErrMalformedPOSTRequest, r.URL, guessIsBrowserReq(r))
		return
	}

	// Read multipart data and save in memory and in the disk if needed
	form, err := reader.ReadForm(maxFormMemory)
	if err != nil {
		logger.LogIf(ctx, err)
		writeErrorResponse(w, ErrMalformedPOSTRequest, r.URL, guessIsBrowserReq(r))
		return
	}

	// Remove all tmp files creating during multipart upload
	defer form.RemoveAll()

	// Extract all form fields
	fileBody, fileName, fileSize, formValues, err := extractPostPolicyFormValues(ctx, form)
	if err != nil {
		logger.LogIf(ctx, err)
		writeErrorResponse(w, ErrMalformedPOSTRequest, r.URL, guessIsBrowserReq(r))
		return
	}

	// Check if file is provided, error out otherwise.
	if fileBody == nil {
		writeErrorResponse(w, ErrPOSTFileRequired, r.URL, guessIsBrowserReq(r))
		return
	}

	// Close multipart file
	defer fileBody.Close()

	formValues.Set("Bucket", bucket)

	if fileName != "" && strings.Contains(formValues.Get("Key"), "${filename}") {
		// S3 feature to replace ${filename} found in Key form field
		// by the filename attribute passed in multipart
		formValues.Set("Key", strings.Replace(formValues.Get("Key"), "${filename}", fileName, -1))
	}
	object := formValues.Get("Key")

	successRedirect := formValues.Get("success_action_redirect")
	successStatus := formValues.Get("success_action_status")
	var redirectURL *url.URL
	if successRedirect != "" {
		redirectURL, err = url.Parse(successRedirect)
		if err != nil {
			writeErrorResponse(w, ErrMalformedPOSTRequest, r.URL, guessIsBrowserReq(r))
			return
		}
	}

	// Verify policy signature.
	apiErr := doesPolicySignatureMatch(formValues)
	if apiErr != ErrNone {
		writeErrorResponse(w, apiErr, r.URL, guessIsBrowserReq(r))
		return
	}

	policyBytes, err := base64.StdEncoding.DecodeString(formValues.Get("Policy"))
	if err != nil {
		writeErrorResponse(w, ErrMalformedPOSTRequest, r.URL, guessIsBrowserReq(r))
		return
	}

	// Handle policy if it is set.
	if len(policyBytes) > 0 {
		postPolicyForm, err := parsePostPolicyForm(string(policyBytes))
		if err != nil {
			writeErrorResponse(w, ErrMalformedPOSTRequest, r.URL, guessIsBrowserReq(r))
			return
		}

		// Make sure formValues adhere to policy restrictions.
		if apiErr = checkPostPolicy(formValues, postPolicyForm); apiErr != ErrNone {
			writeErrorResponse(w, apiErr, r.URL, guessIsBrowserReq(r))
			return
		}

		// Ensure that the object size is within expected range, also the file size
		// should not exceed the maximum single Put size (5 GiB)
		lengthRange := postPolicyForm.Conditions.ContentLengthRange
		if lengthRange.Valid {
			if fileSize < lengthRange.Min {
				writeErrorResponse(w, toAPIErrorCode(ctx, errDataTooSmall), r.URL, guessIsBrowserReq(r))
				return
			}

			if fileSize > lengthRange.Max || isMaxObjectSize(fileSize) {
				writeErrorResponse(w, toAPIErrorCode(ctx, errDataTooLarge), r.URL, guessIsBrowserReq(r))
				return
			}
		}
	}

	// Extract metadata to be saved from received Form.
	metadata := make(map[string]string)
	err = extractMetadataFromMap(ctx, formValues, metadata)
	if err != nil {
		writeErrorResponse(w, ErrInternalError, r.URL, guessIsBrowserReq(r))
		return
	}

	hashReader, err := hash.NewReader(fileBody, fileSize, "", "", fileSize)
	if err != nil {
		logger.LogIf(ctx, err)
		writeErrorResponse(w, toAPIErrorCode(ctx, err), r.URL, guessIsBrowserReq(r))
		return
	}
	rawReader := hashReader
	pReader := NewPutObjReader(rawReader, nil, nil)
	var objectEncryptionKey []byte

	if globalAutoEncryption && !crypto.SSEC.IsRequested(r.Header) {
		r.Header.Add(crypto.SSEHeader, crypto.SSEAlgorithmAES256)
	}
	if objectAPI.IsEncryptionSupported() {
		if hasServerSideEncryptionHeader(formValues) && !hasSuffix(object, slashSeparator) { // handle SSE-C and SSE-S3 requests
			var reader io.Reader
			var key []byte
			if crypto.SSEC.IsRequested(formValues) {
				key, err = ParseSSECustomerHeader(formValues)
				if err != nil {
					writeErrorResponse(w, toAPIErrorCode(ctx, err), r.URL, guessIsBrowserReq(r))
					return
				}
			}
			reader, objectEncryptionKey, err = newEncryptReader(hashReader, key, bucket, object, metadata, crypto.S3.IsRequested(formValues))
			if err != nil {
				writeErrorResponse(w, toAPIErrorCode(ctx, err), r.URL, guessIsBrowserReq(r))
				return
			}
			info := ObjectInfo{Size: fileSize}
			hashReader, err = hash.NewReader(reader, info.EncryptedSize(), "", "", fileSize) // do not try to verify encrypted content
			if err != nil {
				writeErrorResponse(w, toAPIErrorCode(ctx, err), r.URL, guessIsBrowserReq(r))
				return
			}
			pReader = NewPutObjReader(rawReader, hashReader, objectEncryptionKey)
		}
	}

	objInfo, err := objectAPI.PutObject(ctx, bucket, object, pReader, metadata, ObjectOptions{})
	if err != nil {
		writeErrorResponse(w, toAPIErrorCode(ctx, err), r.URL, guessIsBrowserReq(r))
		return
	}

	location := getObjectLocation(r, globalDomainName, bucket, object)
	w.Header().Set("ETag", `"`+objInfo.ETag+`"`)
	w.Header().Set("Location", location)

	// Get host and port from Request.RemoteAddr.
	host, port, err := net.SplitHostPort(handlers.GetSourceIP(r))
	if err != nil {
		host, port = "", ""
	}

	// Notify object created event.
	defer sendEvent(eventArgs{
		EventName:    event.ObjectCreatedPost,
		BucketName:   objInfo.Bucket,
		Object:       objInfo,
		ReqParams:    extractReqParams(r),
		RespElements: extractRespElements(w),
		UserAgent:    r.UserAgent(),
		Host:         host,
		Port:         port,
	})

	if successRedirect != "" {
		// Replace raw query params..
		redirectURL.RawQuery = getRedirectPostRawQuery(objInfo)
		writeRedirectSeeOther(w, redirectURL.String())
		return
	}

	// Decide what http response to send depending on success_action_status parameter
	switch successStatus {
	case "201":
		resp := encodeResponse(PostResponse{
			Bucket:   objInfo.Bucket,
			Key:      objInfo.Name,
			ETag:     `"` + objInfo.ETag + `"`,
			Location: location,
		})
		writeResponse(w, http.StatusCreated, resp, "application/xml")
	case "200":
		writeSuccessResponseHeadersOnly(w)
	default:
		writeSuccessNoContent(w)
	}
}

// HeadBucketHandler - HEAD Bucket
// ----------
// This operation is useful to determine if a bucket exists.
// The operation returns a 200 OK if the bucket exists and you
// have permission to access it. Otherwise, the operation might
// return responses such as 404 Not Found and 403 Forbidden.
func (api objectAPIHandlers) HeadBucketHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "HeadBucket")

	defer logger.AuditLog(w, r, "HeadBucket", mustGetClaimsFromToken(r))

	vars := mux.Vars(r)
	bucket := vars["bucket"]

	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponseHeadersOnly(w, ErrServerNotInitialized)
		return
	}

	if s3Error := checkRequestAuthType(ctx, r, policy.ListBucketAction, bucket, ""); s3Error != ErrNone {
		writeErrorResponseHeadersOnly(w, s3Error)
		return
	}

	getBucketInfo := objectAPI.GetBucketInfo
	if api.CacheAPI() != nil {
		getBucketInfo = api.CacheAPI().GetBucketInfo
	}
	if _, err := getBucketInfo(ctx, bucket); err != nil {
		writeErrorResponseHeadersOnly(w, toAPIErrorCode(ctx, err))
		return
	}

	writeSuccessResponseHeadersOnly(w)
}

// DeleteBucketHandler - Delete bucket
func (api objectAPIHandlers) DeleteBucketHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "DeleteBucket")

	defer logger.AuditLog(w, r, "DeleteBucket", mustGetClaimsFromToken(r))

	vars := mux.Vars(r)
	bucket := vars["bucket"]

	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(w, ErrServerNotInitialized, r.URL, guessIsBrowserReq(r))
		return
	}

	if s3Error := checkRequestAuthType(ctx, r, policy.DeleteBucketAction, bucket, ""); s3Error != ErrNone {
		writeErrorResponse(w, s3Error, r.URL, guessIsBrowserReq(r))
		return
	}

	deleteBucket := objectAPI.DeleteBucket
	if api.CacheAPI() != nil {
		deleteBucket = api.CacheAPI().DeleteBucket
	}
	// Attempt to delete bucket.
	if err := deleteBucket(ctx, bucket); err != nil {
		writeErrorResponse(w, toAPIErrorCode(ctx, err), r.URL, guessIsBrowserReq(r))
		return
	}

	globalNotificationSys.RemoveNotification(bucket)
	globalPolicySys.Remove(bucket)
	globalNotificationSys.DeleteBucket(ctx, bucket)

	if globalDNSConfig != nil {
		if err := globalDNSConfig.Delete(bucket); err != nil {
			// Deleting DNS entry failed, attempt to create the bucket again.
			objectAPI.MakeBucketWithLocation(ctx, bucket, "")
			writeErrorResponse(w, toAPIErrorCode(ctx, err), r.URL, guessIsBrowserReq(r))
			return
		}
	}

	// Write success response.
	writeSuccessNoContent(w)
}

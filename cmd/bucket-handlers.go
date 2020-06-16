/*
 * MinIO Cloud Storage, (C) 2015-2020 MinIO, Inc.
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
	"encoding/base64"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"path/filepath"
	"strings"

	"github.com/gorilla/mux"

	"github.com/minio/minio-go/v6/pkg/set"
	"github.com/minio/minio-go/v6/pkg/tags"
	"github.com/minio/minio/cmd/config/etcd/dns"
	"github.com/minio/minio/cmd/crypto"
	xhttp "github.com/minio/minio/cmd/http"
	"github.com/minio/minio/cmd/logger"
	objectlock "github.com/minio/minio/pkg/bucket/object/lock"
	"github.com/minio/minio/pkg/bucket/policy"
	"github.com/minio/minio/pkg/event"
	"github.com/minio/minio/pkg/handlers"
	"github.com/minio/minio/pkg/hash"
	iampolicy "github.com/minio/minio/pkg/iam/policy"
	"github.com/minio/minio/pkg/sync/errgroup"
)

const (
	objectLockConfig    = "object-lock.xml"
	bucketTaggingConfig = "tagging.xml"
)

// Check if there are buckets on server without corresponding entry in etcd backend and
// make entries. Here is the general flow
// - Range over all the available buckets
// - Check if a bucket has an entry in etcd backend
// -- If no, make an entry
// -- If yes, check if the entry matches local IP check if we
//    need to update the entry then proceed to update
// -- If yes, check if the IP of entry matches local IP.
//    This means entry is for this instance.
// -- If IP of the entry doesn't match, this means entry is
//    for another instance. Log an error to console.
func initFederatorBackend(buckets []BucketInfo, objLayer ObjectLayer) {
	if len(buckets) == 0 {
		return
	}

	// Get buckets in the DNS
	dnsBuckets, err := globalDNSConfig.List()
	if err != nil && err != dns.ErrNoEntriesFound {
		logger.LogIf(GlobalContext, err)
		return
	}

	bucketsSet := set.NewStringSet()
	bucketsToBeUpdated := set.NewStringSet()
	bucketsInConflict := set.NewStringSet()
	for _, bucket := range buckets {
		bucketsSet.Add(bucket.Name)
		r, ok := dnsBuckets[bucket.Name]
		if !ok {
			bucketsToBeUpdated.Add(bucket.Name)
			continue
		}
		if !globalDomainIPs.Intersection(set.CreateStringSet(getHostsSlice(r)...)).IsEmpty() {
			if globalDomainIPs.Difference(set.CreateStringSet(getHostsSlice(r)...)).IsEmpty() {
				// No difference in terms of domainIPs and nothing
				// has changed so we don't change anything on the etcd.
				continue
			}
			// if domain IPs intersect then it won't be an empty set.
			// such an intersection means that bucket exists on etcd.
			// but if we do see a difference with local domain IPs with
			// hostSlice from etcd then we should update with newer
			// domainIPs, we proceed to do that here.
			bucketsToBeUpdated.Add(bucket.Name)
			continue
		}
		// No IPs seem to intersect, this means that bucket exists but has
		// different IP addresses perhaps from a different deployment.
		// bucket names are globally unique in federation at a given
		// path prefix, name collision is not allowed. We simply log
		// an error and continue.
		bucketsInConflict.Add(bucket.Name)
	}

	// Add/update buckets that are not registered with the DNS
	g := errgroup.WithNErrs(len(buckets))
	bucketsToBeUpdatedSlice := bucketsToBeUpdated.ToSlice()
	for index := range bucketsToBeUpdatedSlice {
		index := index
		g.Go(func() error {
			return globalDNSConfig.Put(bucketsToBeUpdatedSlice[index])
		}, index)
	}

	for _, err := range g.Wait() {
		if err != nil {
			logger.LogIf(GlobalContext, err)
		}
	}

	for _, bucket := range bucketsInConflict.ToSlice() {
		logger.LogIf(GlobalContext, fmt.Errorf("Unable to add bucket DNS entry for bucket %s, an entry exists for the same bucket. Use one of these IP addresses %v to access the bucket", bucket, globalDomainIPs.ToSlice()))
	}

	// Remove buckets that are in DNS for this server, but aren't local
	for bucket, records := range dnsBuckets {
		if bucketsSet.Contains(bucket) {
			continue
		}

		if globalDomainIPs.Intersection(set.CreateStringSet(getHostsSlice(records)...)).IsEmpty() {
			// This is not for our server, so we can continue
			continue
		}

		// We go to here, so we know the bucket no longer exists,
		// but is registered in DNS to this server
		if err = globalDNSConfig.Delete(bucket); err != nil {
			logger.LogIf(GlobalContext, fmt.Errorf("Failed to remove DNS entry for %s due to %w",
				bucket, err))
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
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrServerNotInitialized), r.URL, guessIsBrowserReq(r))
		return
	}

	if s3Error := checkRequestAuthType(ctx, r, policy.GetBucketLocationAction, bucket, ""); s3Error != ErrNone {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(s3Error), r.URL, guessIsBrowserReq(r))
		return
	}

	getBucketInfo := objectAPI.GetBucketInfo

	if _, err := getBucketInfo(ctx, bucket); err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL, guessIsBrowserReq(r))
		return
	}

	// Generate response.
	encodedSuccessResponse := encodeResponse(LocationResponse{})
	// Get current region.
	region := globalServerRegion
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
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrServerNotInitialized), r.URL, guessIsBrowserReq(r))
		return
	}

	if s3Error := checkRequestAuthType(ctx, r, policy.ListBucketMultipartUploadsAction, bucket, ""); s3Error != ErrNone {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(s3Error), r.URL, guessIsBrowserReq(r))
		return
	}

	prefix, keyMarker, uploadIDMarker, delimiter, maxUploads, encodingType, errCode := getBucketMultipartResources(r.URL.Query())
	if errCode != ErrNone {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(errCode), r.URL, guessIsBrowserReq(r))
		return
	}

	if maxUploads < 0 {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrInvalidMaxUploads), r.URL, guessIsBrowserReq(r))
		return
	}

	if keyMarker != "" {
		// Marker not common with prefix is not implemented.
		if !HasPrefix(keyMarker, prefix) {
			writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrNotImplemented), r.URL, guessIsBrowserReq(r))
			return
		}
	}

	listMultipartsInfo, err := objectAPI.ListMultipartUploads(ctx, bucket, prefix, keyMarker, uploadIDMarker, delimiter, maxUploads)
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL, guessIsBrowserReq(r))
		return
	}
	// generate response
	response := generateListMultipartUploadsResponse(bucket, listMultipartsInfo, encodingType)
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
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrServerNotInitialized), r.URL, guessIsBrowserReq(r))
		return
	}

	listBuckets := objectAPI.ListBuckets

	accessKey, owner, s3Error := checkRequestAuthTypeToAccessKey(ctx, r, policy.ListAllMyBucketsAction, "", "")
	if s3Error != ErrNone && s3Error != ErrAccessDenied {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(s3Error), r.URL, guessIsBrowserReq(r))
		return
	}

	// If etcd, dns federation configured list buckets from etcd.
	var bucketsInfo []BucketInfo
	if globalDNSConfig != nil && globalBucketFederation {
		dnsBuckets, err := globalDNSConfig.List()
		if err != nil && err != dns.ErrNoEntriesFound {
			writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL, guessIsBrowserReq(r))
			return
		}
		for _, dnsRecords := range dnsBuckets {
			bucketsInfo = append(bucketsInfo, BucketInfo{
				Name:    dnsRecords[0].Key,
				Created: dnsRecords[0].CreationDate,
			})
		}
	} else {
		// Invoke the list buckets.
		var err error
		bucketsInfo, err = listBuckets(ctx)
		if err != nil {
			writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL, guessIsBrowserReq(r))
			return
		}
	}

	if s3Error == ErrAccessDenied {
		// Set prefix value for "s3:prefix" policy conditionals.
		r.Header.Set("prefix", "")

		// Set delimiter value for "s3:delimiter" policy conditionals.
		r.Header.Set("delimiter", SlashSeparator)

		// err will be nil here as we already called this function
		// earlier in this request.
		claims, _ := getClaimsFromToken(r, getSessionToken(r))
		n := 0
		// Use the following trick to filter in place
		// https://github.com/golang/go/wiki/SliceTricks#filter-in-place
		for _, bucketInfo := range bucketsInfo {
			if globalIAMSys.IsAllowed(iampolicy.Args{
				AccountName:     accessKey,
				Action:          iampolicy.ListBucketAction,
				BucketName:      bucketInfo.Name,
				ConditionValues: getConditionValues(r, "", accessKey, claims),
				IsOwner:         owner,
				ObjectName:      "",
				Claims:          claims,
			}) {
				bucketsInfo[n] = bucketInfo
				n++
			}
		}
		bucketsInfo = bucketsInfo[:n]
		// No buckets can be filtered return access denied error.
		if len(bucketsInfo) == 0 {
			writeErrorResponse(ctx, w, errorCodes.ToAPIErr(s3Error), r.URL, guessIsBrowserReq(r))
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
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrServerNotInitialized), r.URL, guessIsBrowserReq(r))
		return
	}

	// Content-Md5 is requied should be set
	// http://docs.aws.amazon.com/AmazonS3/latest/API/multiobjectdeleteapi.html
	if _, ok := r.Header[xhttp.ContentMD5]; !ok {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrMissingContentMD5), r.URL, guessIsBrowserReq(r))
		return
	}

	// Content-Length is required and should be non-zero
	// http://docs.aws.amazon.com/AmazonS3/latest/API/multiobjectdeleteapi.html
	if r.ContentLength <= 0 {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrMissingContentLength), r.URL, guessIsBrowserReq(r))
		return
	}

	// The max. XML contains 100000 object names (each at most 1024 bytes long) + XML overhead
	const maxBodySize = 2 * 100000 * 1024

	// Unmarshal list of keys to be deleted.
	deleteObjects := &DeleteObjectsRequest{}
	if err := xmlDecoder(r.Body, deleteObjects, maxBodySize); err != nil {
		logger.LogIf(ctx, err, logger.Application)
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL, guessIsBrowserReq(r))
		return
	}

	deleteObjectsFn := objectAPI.DeleteObjects
	if api.CacheAPI() != nil {
		deleteObjectsFn = api.CacheAPI().DeleteObjects
	}

	var objectsToDelete = map[ObjectToDelete]int{}
	getObjectInfoFn := objectAPI.GetObjectInfo
	if api.CacheAPI() != nil {
		getObjectInfoFn = api.CacheAPI().GetObjectInfo
	}

	dErrs := make([]DeleteError, len(deleteObjects.Objects))
	for index, object := range deleteObjects.Objects {
		if apiErrCode := checkRequestAuthType(ctx, r, policy.DeleteObjectAction, bucket, object.ObjectName); apiErrCode != ErrNone {
			if apiErrCode == ErrSignatureDoesNotMatch || apiErrCode == ErrInvalidAccessKeyID {
				writeErrorResponse(ctx, w, errorCodes.ToAPIErr(apiErrCode), r.URL, guessIsBrowserReq(r))
				return
			}
			apiErr := errorCodes.ToAPIErr(apiErrCode)
			dErrs[index] = DeleteError{
				Code:      apiErr.Code,
				Message:   apiErr.Description,
				Key:       object.ObjectName,
				VersionID: object.VersionID,
			}
			continue
		}

		if object.VersionID != "" {
			if rcfg, _ := globalBucketObjectLockSys.Get(bucket); rcfg.LockEnabled {
				if apiErrCode := enforceRetentionBypassForDelete(ctx, r, bucket, object, getObjectInfoFn); apiErrCode != ErrNone {
					apiErr := errorCodes.ToAPIErr(apiErrCode)
					dErrs[index] = DeleteError{
						Code:      apiErr.Code,
						Message:   apiErr.Description,
						Key:       object.ObjectName,
						VersionID: object.VersionID,
					}
					continue
				}
			}
		}

		// Avoid duplicate objects, we use map to filter them out.
		if _, ok := objectsToDelete[object]; !ok {
			objectsToDelete[object] = index
		}
	}

	toNames := func(input map[ObjectToDelete]int) (output []ObjectToDelete) {
		output = make([]ObjectToDelete, len(input))
		idx := 0
		for obj := range input {
			output[idx] = obj
			idx++
		}
		return
	}

	deleteList := toNames(objectsToDelete)
	dObjects, errs := deleteObjectsFn(ctx, bucket, deleteList, ObjectOptions{
		Versioned: globalBucketVersioningSys.Enabled(bucket),
	})

	deletedObjects := make([]DeletedObject, len(deleteObjects.Objects))
	for i := range errs {
		dindex := objectsToDelete[deleteList[i]]
		apiErr := toAPIError(ctx, errs[i])
		if apiErr.Code == "" || apiErr.Code == "NoSuchKey" {
			deletedObjects[dindex] = dObjects[i]
			continue
		}
		dErrs[dindex] = DeleteError{
			Code:      apiErr.Code,
			Message:   apiErr.Description,
			Key:       deleteList[i].ObjectName,
			VersionID: deleteList[i].VersionID,
		}
	}

	var deleteErrors []DeleteError
	for _, dErr := range dErrs {
		if dErr.Code != "" {
			deleteErrors = append(deleteErrors, dErr)
		}
	}

	// Generate response
	response := generateMultiDeleteResponse(deleteObjects.Quiet, deletedObjects, deleteErrors)
	encodedSuccessResponse := encodeResponse(response)

	// Write success response.
	writeSuccessResponseXML(w, encodedSuccessResponse)

	// Notify deleted event for objects.
	for _, dobj := range deletedObjects {
		objInfo := ObjectInfo{
			Name:      dobj.ObjectName,
			VersionID: dobj.VersionID,
		}
		if dobj.DeleteMarker {
			objInfo = ObjectInfo{
				Name:         dobj.ObjectName,
				DeleteMarker: dobj.DeleteMarker,
				VersionID:    dobj.DeleteMarkerVersionID,
			}
		}
		sendEvent(eventArgs{
			EventName:    event.ObjectRemovedDelete,
			BucketName:   bucket,
			Object:       objInfo,
			ReqParams:    extractReqParams(r),
			RespElements: extractRespElements(w),
			UserAgent:    r.UserAgent(),
			Host:         handlers.GetSourceIP(r),
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
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrServerNotInitialized), r.URL, guessIsBrowserReq(r))
		return
	}

	vars := mux.Vars(r)
	bucket := vars["bucket"]

	objectLockEnabled := false
	if vs, found := r.Header[http.CanonicalHeaderKey("x-amz-bucket-object-lock-enabled")]; found {
		v := strings.ToLower(strings.Join(vs, ""))
		if v != "true" && v != "false" {
			writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrInvalidRequest), r.URL, guessIsBrowserReq(r))
			return
		}
		objectLockEnabled = v == "true"
	}

	if s3Error := checkRequestAuthType(ctx, r, policy.CreateBucketAction, bucket, ""); s3Error != ErrNone {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(s3Error), r.URL, guessIsBrowserReq(r))
		return
	}

	// Parse incoming location constraint.
	location, s3Error := parseLocationConstraint(r)
	if s3Error != ErrNone {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(s3Error), r.URL, guessIsBrowserReq(r))
		return
	}

	// Validate if location sent by the client is valid, reject
	// requests which do not follow valid region requirements.
	if !isValidLocation(location) {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrInvalidRegion), r.URL, guessIsBrowserReq(r))
		return
	}

	opts := BucketOptions{
		Location:    location,
		LockEnabled: objectLockEnabled,
	}

	if globalDNSConfig != nil {
		sr, err := globalDNSConfig.Get(bucket)
		if err != nil {
			if err == dns.ErrNoEntriesFound {
				// Proceed to creating a bucket.
				if err = objectAPI.MakeBucketWithLocation(ctx, bucket, opts); err != nil {
					writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL, guessIsBrowserReq(r))
					return
				}

				if err = globalDNSConfig.Put(bucket); err != nil {
					objectAPI.DeleteBucket(ctx, bucket, false)
					writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL, guessIsBrowserReq(r))
					return
				}

				// Load updated bucket metadata into memory.
				globalNotificationSys.LoadBucketMetadata(GlobalContext, bucket)

				// Make sure to add Location information here only for bucket
				w.Header().Set(xhttp.Location,
					getObjectLocation(r, globalDomainNames, bucket, ""))

				writeSuccessResponseHeadersOnly(w)
				return
			}
			writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL, guessIsBrowserReq(r))
			return

		}
		apiErr := ErrBucketAlreadyExists
		if !globalDomainIPs.Intersection(set.CreateStringSet(getHostsSlice(sr)...)).IsEmpty() {
			apiErr = ErrBucketAlreadyOwnedByYou
		}
		// No IPs seem to intersect, this means that bucket exists but has
		// different IP addresses perhaps from a different deployment.
		// bucket names are globally unique in federation at a given
		// path prefix, name collision is not allowed. Return appropriate error.
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(apiErr), r.URL, guessIsBrowserReq(r))
		return
	}

	// Proceed to creating a bucket.
	err := objectAPI.MakeBucketWithLocation(ctx, bucket, opts)
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL, guessIsBrowserReq(r))
		return
	}

	// Load updated bucket metadata into memory.
	globalNotificationSys.LoadBucketMetadata(GlobalContext, bucket)

	// Make sure to add Location information here only for bucket
	w.Header().Set(xhttp.Location, path.Clean(r.URL.Path)) // Clean any trailing slashes.

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
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrServerNotInitialized), r.URL, guessIsBrowserReq(r))
		return
	}

	if crypto.S3KMS.IsRequested(r.Header) { // SSE-KMS is not supported
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrNotImplemented), r.URL, guessIsBrowserReq(r))
		return
	}
	if !api.EncryptionEnabled() && crypto.IsRequested(r.Header) {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrNotImplemented), r.URL, guessIsBrowserReq(r))
		return
	}

	bucket := mux.Vars(r)["bucket"]

	// To detect if the client has disconnected.
	r.Body = &contextReader{r.Body, r.Context()}

	// Require Content-Length to be set in the request
	size := r.ContentLength
	if size < 0 {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrMissingContentLength), r.URL, guessIsBrowserReq(r))
		return
	}
	resource, err := getResource(r.URL.Path, r.Host, globalDomainNames)
	if err != nil {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrInvalidRequest), r.URL, guessIsBrowserReq(r))
		return
	}
	// Make sure that the URL  does not contain object name.
	if bucket != filepath.Clean(resource[1:]) {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrMethodNotAllowed), r.URL, guessIsBrowserReq(r))
		return
	}

	// Here the parameter is the size of the form data that should
	// be loaded in memory, the remaining being put in temporary files.
	reader, err := r.MultipartReader()
	if err != nil {
		logger.LogIf(ctx, err)
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrMalformedPOSTRequest), r.URL, guessIsBrowserReq(r))
		return
	}

	// Read multipart data and save in memory and in the disk if needed
	form, err := reader.ReadForm(maxFormMemory)
	if err != nil {
		logger.LogIf(ctx, err, logger.Application)
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrMalformedPOSTRequest), r.URL, guessIsBrowserReq(r))
		return
	}

	// Remove all tmp files created during multipart upload
	defer form.RemoveAll()

	// Extract all form fields
	fileBody, fileName, fileSize, formValues, err := extractPostPolicyFormValues(ctx, form)
	if err != nil {
		logger.LogIf(ctx, err, logger.Application)
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrMalformedPOSTRequest), r.URL, guessIsBrowserReq(r))
		return
	}

	// Check if file is provided, error out otherwise.
	if fileBody == nil {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrPOSTFileRequired), r.URL, guessIsBrowserReq(r))
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
			writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrMalformedPOSTRequest), r.URL, guessIsBrowserReq(r))
			return
		}
	}

	// Verify policy signature.
	errCode := doesPolicySignatureMatch(formValues)
	if errCode != ErrNone {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(errCode), r.URL, guessIsBrowserReq(r))
		return
	}

	policyBytes, err := base64.StdEncoding.DecodeString(formValues.Get("Policy"))
	if err != nil {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrMalformedPOSTRequest), r.URL, guessIsBrowserReq(r))
		return
	}

	// Handle policy if it is set.
	if len(policyBytes) > 0 {

		postPolicyForm, err := parsePostPolicyForm(string(policyBytes))
		if err != nil {
			writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrPostPolicyConditionInvalidFormat), r.URL, guessIsBrowserReq(r))
			return
		}

		// Make sure formValues adhere to policy restrictions.
		if err = checkPostPolicy(formValues, postPolicyForm); err != nil {
			writeCustomErrorResponseXML(ctx, w, errorCodes.ToAPIErr(ErrAccessDenied), err.Error(), r.URL, guessIsBrowserReq(r))
			return
		}

		// Ensure that the object size is within expected range, also the file size
		// should not exceed the maximum single Put size (5 GiB)
		lengthRange := postPolicyForm.Conditions.ContentLengthRange
		if lengthRange.Valid {
			if fileSize < lengthRange.Min {
				writeErrorResponse(ctx, w, toAPIError(ctx, errDataTooSmall), r.URL, guessIsBrowserReq(r))
				return
			}

			if fileSize > lengthRange.Max || isMaxObjectSize(fileSize) {
				writeErrorResponse(ctx, w, toAPIError(ctx, errDataTooLarge), r.URL, guessIsBrowserReq(r))
				return
			}
		}
	}

	// Extract metadata to be saved from received Form.
	metadata := make(map[string]string)
	err = extractMetadataFromMap(ctx, formValues, metadata)
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL, guessIsBrowserReq(r))
		return
	}

	hashReader, err := hash.NewReader(fileBody, fileSize, "", "", fileSize, globalCLIContext.StrictS3Compat)
	if err != nil {
		logger.LogIf(ctx, err)
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL, guessIsBrowserReq(r))
		return
	}
	rawReader := hashReader
	pReader := NewPutObjReader(rawReader, nil, nil)
	var objectEncryptionKey crypto.ObjectKey

	// Check if bucket encryption is enabled
	if _, err = globalBucketSSEConfigSys.Get(bucket); err == nil || globalAutoEncryption {
		// This request header needs to be set prior to setting ObjectOptions
		if !crypto.SSEC.IsRequested(r.Header) {
			r.Header.Add(crypto.SSEHeader, crypto.SSEAlgorithmAES256)
		}
	}

	// get gateway encryption options
	var opts ObjectOptions
	opts, err = putOpts(ctx, r, bucket, object, metadata)
	if err != nil {
		writeErrorResponseHeadersOnly(w, toAPIError(ctx, err))
		return
	}
	if objectAPI.IsEncryptionSupported() {
		if crypto.IsRequested(formValues) && !HasSuffix(object, SlashSeparator) { // handle SSE requests
			if crypto.SSECopy.IsRequested(r.Header) {
				writeErrorResponse(ctx, w, toAPIError(ctx, errInvalidEncryptionParameters), r.URL, guessIsBrowserReq(r))
				return
			}
			var reader io.Reader
			var key []byte
			if crypto.SSEC.IsRequested(formValues) {
				key, err = ParseSSECustomerHeader(formValues)
				if err != nil {
					writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL, guessIsBrowserReq(r))
					return
				}
			}
			reader, objectEncryptionKey, err = newEncryptReader(hashReader, key, bucket, object, metadata, crypto.S3.IsRequested(formValues))
			if err != nil {
				writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL, guessIsBrowserReq(r))
				return
			}
			info := ObjectInfo{Size: fileSize}
			// do not try to verify encrypted content
			hashReader, err = hash.NewReader(reader, info.EncryptedSize(), "", "", fileSize, globalCLIContext.StrictS3Compat)
			if err != nil {
				writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL, guessIsBrowserReq(r))
				return
			}
			pReader = NewPutObjReader(rawReader, hashReader, &objectEncryptionKey)
		}
	}

	objInfo, err := objectAPI.PutObject(ctx, bucket, object, pReader, opts)
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL, guessIsBrowserReq(r))
		return
	}

	// We must not use the http.Header().Set method here because some (broken)
	// clients expect the ETag header key to be literally "ETag" - not "Etag" (case-sensitive).
	// Therefore, we have to set the ETag directly as map entry.
	w.Header()[xhttp.ETag] = []string{`"` + objInfo.ETag + `"`}

	// Set the relevant version ID as part of the response header.
	if objInfo.VersionID != "" {
		w.Header()[xhttp.AmzVersionID] = []string{objInfo.VersionID}
	}

	w.Header().Set(xhttp.Location, getObjectLocation(r, globalDomainNames, bucket, object))

	// Notify object created event.
	defer sendEvent(eventArgs{
		EventName:    event.ObjectCreatedPost,
		BucketName:   objInfo.Bucket,
		Object:       objInfo,
		ReqParams:    extractReqParams(r),
		RespElements: extractRespElements(w),
		UserAgent:    r.UserAgent(),
		Host:         handlers.GetSourceIP(r),
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
			Location: w.Header().Get(xhttp.Location),
		})
		writeResponse(w, http.StatusCreated, resp, mimeXML)
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
		writeErrorResponseHeadersOnly(w, errorCodes.ToAPIErr(ErrServerNotInitialized))
		return
	}

	if s3Error := checkRequestAuthType(ctx, r, policy.ListBucketAction, bucket, ""); s3Error != ErrNone {
		writeErrorResponseHeadersOnly(w, errorCodes.ToAPIErr(s3Error))
		return
	}

	getBucketInfo := objectAPI.GetBucketInfo

	if _, err := getBucketInfo(ctx, bucket); err != nil {
		writeErrorResponseHeadersOnly(w, toAPIError(ctx, err))
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
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrServerNotInitialized), r.URL, guessIsBrowserReq(r))
		return
	}

	forceDelete := false
	if value := r.Header.Get(xhttp.MinIOForceDelete); value != "" {
		switch value {
		case "true":
			forceDelete = true
		case "false":
		default:
			writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrInvalidRequest), r.URL, guessIsBrowserReq(r))
			return
		}
	}

	if forceDelete {
		if s3Error := checkRequestAuthType(ctx, r, policy.ForceDeleteBucketAction, bucket, ""); s3Error != ErrNone {
			writeErrorResponse(ctx, w, errorCodes.ToAPIErr(s3Error), r.URL, guessIsBrowserReq(r))
			return
		}
	} else {
		if s3Error := checkRequestAuthType(ctx, r, policy.DeleteBucketAction, bucket, ""); s3Error != ErrNone {
			writeErrorResponse(ctx, w, errorCodes.ToAPIErr(s3Error), r.URL, guessIsBrowserReq(r))
			return
		}
	}

	if forceDelete {
		if rcfg, _ := globalBucketObjectLockSys.Get(bucket); rcfg.LockEnabled {
			writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrMethodNotAllowed), r.URL, guessIsBrowserReq(r))
			return
		}
	}

	deleteBucket := objectAPI.DeleteBucket

	// Attempt to delete bucket.
	if err := deleteBucket(ctx, bucket, forceDelete); err != nil {
		if _, ok := err.(BucketNotEmpty); ok && (globalBucketVersioningSys.Enabled(bucket) || globalBucketVersioningSys.Suspended(bucket)) {
			apiErr := toAPIError(ctx, err)
			apiErr.Description = "The bucket you tried to delete is not empty. You must delete all versions in the bucket."
			writeErrorResponse(ctx, w, apiErr, r.URL, guessIsBrowserReq(r))
		} else {
			writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL, guessIsBrowserReq(r))
		}
		return
	}

	globalNotificationSys.DeleteBucketMetadata(ctx, bucket)

	if globalDNSConfig != nil {
		if err := globalDNSConfig.Delete(bucket); err != nil {
			logger.LogIf(ctx, fmt.Errorf("Unable to delete bucket DNS entry %w, please delete it manually using etcdctl", err))
			writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL, guessIsBrowserReq(r))
			return
		}
	}

	// Write success response.
	writeSuccessNoContent(w)
}

// PutBucketObjectLockConfigHandler - PUT Bucket object lock configuration.
// ----------
// Places an Object Lock configuration on the specified bucket. The rule
// specified in the Object Lock configuration will be applied by default
// to every new object placed in the specified bucket.
func (api objectAPIHandlers) PutBucketObjectLockConfigHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "PutBucketObjectLockConfig")

	defer logger.AuditLog(w, r, "PutBucketObjectLockConfig", mustGetClaimsFromToken(r))

	vars := mux.Vars(r)
	bucket := vars["bucket"]

	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrServerNotInitialized), r.URL, guessIsBrowserReq(r))
		return
	}

	if s3Error := checkRequestAuthType(ctx, r, policy.PutBucketObjectLockConfigurationAction, bucket, ""); s3Error != ErrNone {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(s3Error), r.URL, guessIsBrowserReq(r))
		return
	}

	config, err := objectlock.ParseObjectLockConfig(r.Body)
	if err != nil {
		apiErr := errorCodes.ToAPIErr(ErrMalformedXML)
		apiErr.Description = err.Error()
		writeErrorResponse(ctx, w, apiErr, r.URL, guessIsBrowserReq(r))
		return
	}

	configData, err := xml.Marshal(config)
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL, guessIsBrowserReq(r))
		return
	}

	// Deny object locking configuration settings on existing buckets without object lock enabled.
	if _, err = globalBucketMetadataSys.GetObjectLockConfig(bucket); err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL, guessIsBrowserReq(r))
		return
	}

	if err = globalBucketMetadataSys.Update(bucket, objectLockConfig, configData); err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL, guessIsBrowserReq(r))
		return
	}

	// Write success response.
	writeSuccessResponseHeadersOnly(w)
}

// GetBucketObjectLockConfigHandler - GET Bucket object lock configuration.
// ----------
// Gets the Object Lock configuration for a bucket. The rule specified in
// the Object Lock configuration will be applied by default to every new
// object placed in the specified bucket.
func (api objectAPIHandlers) GetBucketObjectLockConfigHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "GetBucketObjectLockConfig")

	defer logger.AuditLog(w, r, "GetBucketObjectLockConfig", mustGetClaimsFromToken(r))

	vars := mux.Vars(r)
	bucket := vars["bucket"]

	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrServerNotInitialized), r.URL, guessIsBrowserReq(r))
		return
	}

	// check if user has permissions to perform this operation
	if s3Error := checkRequestAuthType(ctx, r, policy.GetBucketObjectLockConfigurationAction, bucket, ""); s3Error != ErrNone {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(s3Error), r.URL, guessIsBrowserReq(r))
		return
	}

	config, err := globalBucketMetadataSys.GetObjectLockConfig(bucket)
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL, guessIsBrowserReq(r))
		return
	}

	configData, err := xml.Marshal(config)
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL, guessIsBrowserReq(r))
		return
	}

	// Write success response.
	writeSuccessResponseXML(w, configData)
}

// PutBucketTaggingHandler - PUT Bucket tagging.
// ----------
func (api objectAPIHandlers) PutBucketTaggingHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "PutBucketTagging")

	defer logger.AuditLog(w, r, "PutBucketTagging", mustGetClaimsFromToken(r))

	vars := mux.Vars(r)
	bucket := vars["bucket"]

	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrServerNotInitialized), r.URL, guessIsBrowserReq(r))
		return
	}

	if s3Error := checkRequestAuthType(ctx, r, policy.PutBucketTaggingAction, bucket, ""); s3Error != ErrNone {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(s3Error), r.URL, guessIsBrowserReq(r))
		return
	}

	tags, err := tags.ParseBucketXML(io.LimitReader(r.Body, r.ContentLength))
	if err != nil {
		apiErr := errorCodes.ToAPIErr(ErrMalformedXML)
		apiErr.Description = err.Error()
		writeErrorResponse(ctx, w, apiErr, r.URL, guessIsBrowserReq(r))
		return
	}

	configData, err := xml.Marshal(tags)
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL, guessIsBrowserReq(r))
		return
	}

	if err = globalBucketMetadataSys.Update(bucket, bucketTaggingConfig, configData); err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL, guessIsBrowserReq(r))
		return
	}

	// Write success response.
	writeSuccessResponseHeadersOnly(w)
}

// GetBucketTaggingHandler - GET Bucket tagging.
// ----------
func (api objectAPIHandlers) GetBucketTaggingHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "GetBucketTagging")

	defer logger.AuditLog(w, r, "GetBucketTagging", mustGetClaimsFromToken(r))

	vars := mux.Vars(r)
	bucket := vars["bucket"]

	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrServerNotInitialized), r.URL, guessIsBrowserReq(r))
		return
	}

	// check if user has permissions to perform this operation
	if s3Error := checkRequestAuthType(ctx, r, policy.GetBucketTaggingAction, bucket, ""); s3Error != ErrNone {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(s3Error), r.URL, guessIsBrowserReq(r))
		return
	}

	config, err := globalBucketMetadataSys.GetTaggingConfig(bucket)
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL, guessIsBrowserReq(r))
		return
	}

	configData, err := xml.Marshal(config)
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL, guessIsBrowserReq(r))
		return
	}

	// Write success response.
	writeSuccessResponseXML(w, configData)
}

// DeleteBucketTaggingHandler - DELETE Bucket tagging.
// ----------
func (api objectAPIHandlers) DeleteBucketTaggingHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "DeleteBucketTagging")

	defer logger.AuditLog(w, r, "DeleteBucketTagging", mustGetClaimsFromToken(r))

	vars := mux.Vars(r)
	bucket := vars["bucket"]

	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrServerNotInitialized), r.URL, guessIsBrowserReq(r))
		return
	}

	if s3Error := checkRequestAuthType(ctx, r, policy.PutBucketTaggingAction, bucket, ""); s3Error != ErrNone {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(s3Error), r.URL, guessIsBrowserReq(r))
		return
	}

	if err := globalBucketMetadataSys.Update(bucket, bucketTaggingConfig, nil); err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL, guessIsBrowserReq(r))
		return
	}

	// Write success response.
	writeSuccessResponseHeadersOnly(w)
}

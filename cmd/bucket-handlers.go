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
	"encoding/base64"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/textproto"
	"net/url"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/google/uuid"
	"github.com/gorilla/mux"

	"github.com/minio/madmin-go"
	"github.com/minio/minio-go/v7/pkg/set"
	"github.com/minio/minio-go/v7/pkg/tags"
	sse "github.com/minio/minio/internal/bucket/encryption"
	objectlock "github.com/minio/minio/internal/bucket/object/lock"
	"github.com/minio/minio/internal/bucket/replication"
	"github.com/minio/minio/internal/config/dns"
	"github.com/minio/minio/internal/crypto"
	"github.com/minio/minio/internal/event"
	"github.com/minio/minio/internal/handlers"
	"github.com/minio/minio/internal/hash"
	xhttp "github.com/minio/minio/internal/http"
	"github.com/minio/minio/internal/kms"
	"github.com/minio/minio/internal/logger"
	"github.com/minio/minio/internal/sync/errgroup"
	"github.com/minio/pkg/bucket/policy"
	iampolicy "github.com/minio/pkg/iam/policy"
)

const (
	objectLockConfig        = "object-lock.xml"
	bucketTaggingConfig     = "tagging.xml"
	bucketReplicationConfig = "replication.xml"
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
	if err != nil && !IsErrIgnored(err, dns.ErrNoEntriesFound, dns.ErrNotImplemented, dns.ErrDomainMissing) {
		logger.LogIf(GlobalContext, err)
		return
	}

	bucketsSet := set.NewStringSet()
	bucketsToBeUpdated := set.NewStringSet()
	bucketsInConflict := set.NewStringSet()

	// This means that domain is updated, we should update
	// all bucket entries with new domain name.
	domainMissing := err == dns.ErrDomainMissing
	if dnsBuckets != nil {
		for _, bucket := range buckets {
			bucketsSet.Add(bucket.Name)
			r, ok := dnsBuckets[bucket.Name]
			if !ok {
				bucketsToBeUpdated.Add(bucket.Name)
				continue
			}
			if !globalDomainIPs.Intersection(set.CreateStringSet(getHostsSlice(r)...)).IsEmpty() {
				if globalDomainIPs.Difference(set.CreateStringSet(getHostsSlice(r)...)).IsEmpty() && !domainMissing {
					// No difference in terms of domainIPs and nothing
					// has changed so we don't change anything on the etcd.
					//
					// Additionally also check if domain is updated/missing with more
					// entries, if that is the case we should update the
					// new domain entries as well.
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
	}

	// Add/update buckets that are not registered with the DNS
	bucketsToBeUpdatedSlice := bucketsToBeUpdated.ToSlice()
	g := errgroup.WithNErrs(len(bucketsToBeUpdatedSlice)).WithConcurrency(50)

	for index := range bucketsToBeUpdatedSlice {
		index := index
		g.Go(func() error {
			return globalDNSConfig.Put(bucketsToBeUpdatedSlice[index])
		}, index)
	}

	ctx := GlobalContext
	for _, err := range g.Wait() {
		if err != nil {
			logger.LogIf(ctx, err)
			return
		}
	}

	for _, bucket := range bucketsInConflict.ToSlice() {
		logger.LogIf(ctx, fmt.Errorf("Unable to add bucket DNS entry for bucket %s, an entry exists for the same bucket by a different tenant. This local bucket will be ignored. Bucket names are globally unique in federated deployments. Use path style requests on following addresses '%v' to access this bucket", bucket, globalDomainIPs.ToSlice()))
	}

	var wg sync.WaitGroup
	// Remove buckets that are in DNS for this server, but aren't local
	for bucket, records := range dnsBuckets {
		if bucketsSet.Contains(bucket) {
			continue
		}

		if globalDomainIPs.Intersection(set.CreateStringSet(getHostsSlice(records)...)).IsEmpty() {
			// This is not for our server, so we can continue
			continue
		}

		wg.Add(1)
		go func(bucket string) {
			defer wg.Done()
			// We go to here, so we know the bucket no longer exists,
			// but is registered in DNS to this server
			if err := globalDNSConfig.Delete(bucket); err != nil {
				logger.LogIf(GlobalContext, fmt.Errorf("Failed to remove DNS entry for %s due to %w",
					bucket, err))
			}
		}(bucket)
	}
	wg.Wait()
}

// GetBucketLocationHandler - GET Bucket location.
// -------------------------
// This operation returns bucket location.
func (api objectAPIHandlers) GetBucketLocationHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "GetBucketLocation")

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	vars := mux.Vars(r)
	bucket := vars["bucket"]

	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrServerNotInitialized), r.URL)
		return
	}

	if s3Error := checkRequestAuthType(ctx, r, policy.GetBucketLocationAction, bucket, ""); s3Error != ErrNone {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(s3Error), r.URL)
		return
	}

	getBucketInfo := objectAPI.GetBucketInfo

	if _, err := getBucketInfo(ctx, bucket, BucketOptions{}); err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	// Generate response.
	encodedSuccessResponse := encodeResponse(LocationResponse{})
	// Get current region.
	region := globalSite.Region
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

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	vars := mux.Vars(r)
	bucket := vars["bucket"]

	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrServerNotInitialized), r.URL)
		return
	}

	if s3Error := checkRequestAuthType(ctx, r, policy.ListBucketMultipartUploadsAction, bucket, ""); s3Error != ErrNone {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(s3Error), r.URL)
		return
	}

	prefix, keyMarker, uploadIDMarker, delimiter, maxUploads, encodingType, errCode := getBucketMultipartResources(r.Form)
	if errCode != ErrNone {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(errCode), r.URL)
		return
	}

	if maxUploads < 0 {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrInvalidMaxUploads), r.URL)
		return
	}

	if keyMarker != "" {
		// Marker not common with prefix is not implemented.
		if !HasPrefix(keyMarker, prefix) {
			writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrNotImplemented), r.URL)
			return
		}
	}

	listMultipartsInfo, err := objectAPI.ListMultipartUploads(ctx, bucket, prefix, keyMarker, uploadIDMarker, delimiter, maxUploads)
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
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

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrServerNotInitialized), r.URL)
		return
	}

	listBuckets := objectAPI.ListBuckets

	cred, owner, s3Error := checkRequestAuthTypeCredential(ctx, r, policy.ListAllMyBucketsAction, "", "")
	if s3Error != ErrNone && s3Error != ErrAccessDenied {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(s3Error), r.URL)
		return
	}

	// Anonymous users, should be rejected.
	if cred.AccessKey == "" {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrAccessDenied), r.URL)
		return
	}

	// If etcd, dns federation configured list buckets from etcd.
	var bucketsInfo []BucketInfo
	if globalDNSConfig != nil && globalBucketFederation {
		dnsBuckets, err := globalDNSConfig.List()
		if err != nil && !IsErrIgnored(err,
			dns.ErrNoEntriesFound,
			dns.ErrDomainMissing) {
			writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
			return
		}
		for _, dnsRecords := range dnsBuckets {
			bucketsInfo = append(bucketsInfo, BucketInfo{
				Name:    dnsRecords[0].Key,
				Created: dnsRecords[0].CreationDate,
			})
		}

		sort.Slice(bucketsInfo, func(i, j int) bool {
			return bucketsInfo[i].Name < bucketsInfo[j].Name
		})

	} else {
		// Invoke the list buckets.
		var err error
		bucketsInfo, err = listBuckets(ctx, BucketOptions{})
		if err != nil {
			writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
			return
		}
	}

	if s3Error == ErrAccessDenied {
		// Set prefix value for "s3:prefix" policy conditionals.
		r.Header.Set("prefix", "")

		// Set delimiter value for "s3:delimiter" policy conditionals.
		r.Header.Set("delimiter", SlashSeparator)

		n := 0
		// Use the following trick to filter in place
		// https://github.com/golang/go/wiki/SliceTricks#filter-in-place
		for _, bucketInfo := range bucketsInfo {
			if globalIAMSys.IsAllowed(iampolicy.Args{
				AccountName:     cred.AccessKey,
				Groups:          cred.Groups,
				Action:          iampolicy.ListBucketAction,
				BucketName:      bucketInfo.Name,
				ConditionValues: getConditionValues(r, "", cred.AccessKey, cred.Claims),
				IsOwner:         owner,
				ObjectName:      "",
				Claims:          cred.Claims,
			}) {
				bucketsInfo[n] = bucketInfo
				n++
			} else if globalIAMSys.IsAllowed(iampolicy.Args{
				AccountName:     cred.AccessKey,
				Groups:          cred.Groups,
				Action:          iampolicy.GetBucketLocationAction,
				BucketName:      bucketInfo.Name,
				ConditionValues: getConditionValues(r, "", cred.AccessKey, cred.Claims),
				IsOwner:         owner,
				ObjectName:      "",
				Claims:          cred.Claims,
			}) {
				bucketsInfo[n] = bucketInfo
				n++
			}
		}
		bucketsInfo = bucketsInfo[:n]
		// No buckets can be filtered return access denied error.
		if len(bucketsInfo) == 0 {
			writeErrorResponse(ctx, w, errorCodes.ToAPIErr(s3Error), r.URL)
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

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	vars := mux.Vars(r)
	bucket := vars["bucket"]

	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrServerNotInitialized), r.URL)
		return
	}

	// Content-Md5 is requied should be set
	// http://docs.aws.amazon.com/AmazonS3/latest/API/multiobjectdeleteapi.html
	if _, ok := r.Header[xhttp.ContentMD5]; !ok {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrMissingContentMD5), r.URL)
		return
	}

	// Content-Length is required and should be non-zero
	// http://docs.aws.amazon.com/AmazonS3/latest/API/multiobjectdeleteapi.html
	if r.ContentLength <= 0 {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrMissingContentLength), r.URL)
		return
	}

	// The max. XML contains 100000 object names (each at most 1024 bytes long) + XML overhead
	const maxBodySize = 2 * 100000 * 1024

	// Unmarshal list of keys to be deleted.
	deleteObjectsReq := &DeleteObjectsRequest{}
	if err := xmlDecoder(r.Body, deleteObjectsReq, maxBodySize); err != nil {
		logger.LogIf(ctx, err, logger.Application)
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	objects := make([]ObjectV, len(deleteObjectsReq.Objects))
	// Convert object name delete objects if it has `/` in the beginning.
	for i := range deleteObjectsReq.Objects {
		deleteObjectsReq.Objects[i].ObjectName = trimLeadingSlash(deleteObjectsReq.Objects[i].ObjectName)
		objects[i] = deleteObjectsReq.Objects[i].ObjectV
	}

	// Make sure to update context to print ObjectNames for multi objects.
	ctx = updateReqContext(ctx, objects...)

	// Call checkRequestAuthType to populate ReqInfo.AccessKey before GetBucketInfo()
	// Ignore errors here to preserve the S3 error behavior of GetBucketInfo()
	checkRequestAuthType(ctx, r, policy.DeleteObjectAction, bucket, "")

	// Before proceeding validate if bucket exists.
	_, err := objectAPI.GetBucketInfo(ctx, bucket, BucketOptions{})
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	deleteObjectsFn := objectAPI.DeleteObjects
	if api.CacheAPI() != nil {
		deleteObjectsFn = api.CacheAPI().DeleteObjects
	}

	// Return Malformed XML as S3 spec if the number of objects is empty
	if len(deleteObjectsReq.Objects) == 0 || len(deleteObjectsReq.Objects) > maxDeleteList {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrMalformedXML), r.URL)
		return
	}

	objectsToDelete := map[ObjectToDelete]int{}
	getObjectInfoFn := objectAPI.GetObjectInfo
	if api.CacheAPI() != nil {
		getObjectInfoFn = api.CacheAPI().GetObjectInfo
	}

	var (
		hasLockEnabled bool
		dsc            ReplicateDecision
		goi            ObjectInfo
		gerr           error
	)
	replicateDeletes := hasReplicationRules(ctx, bucket, deleteObjectsReq.Objects)
	if rcfg, _ := globalBucketObjectLockSys.Get(bucket); rcfg.LockEnabled {
		hasLockEnabled = true
	}

	type deleteResult struct {
		delInfo DeletedObject
		errInfo DeleteError
	}

	deleteResults := make([]deleteResult, len(deleteObjectsReq.Objects))

	vc, _ := globalBucketVersioningSys.Get(bucket)
	oss := make([]*objSweeper, len(deleteObjectsReq.Objects))
	for index, object := range deleteObjectsReq.Objects {
		if apiErrCode := checkRequestAuthType(ctx, r, policy.DeleteObjectAction, bucket, object.ObjectName); apiErrCode != ErrNone {
			if apiErrCode == ErrSignatureDoesNotMatch || apiErrCode == ErrInvalidAccessKeyID {
				writeErrorResponse(ctx, w, errorCodes.ToAPIErr(apiErrCode), r.URL)
				return
			}
			apiErr := errorCodes.ToAPIErr(apiErrCode)
			deleteResults[index].errInfo = DeleteError{
				Code:      apiErr.Code,
				Message:   apiErr.Description,
				Key:       object.ObjectName,
				VersionID: object.VersionID,
			}
			continue
		}
		if object.VersionID != "" && object.VersionID != nullVersionID {
			if _, err := uuid.Parse(object.VersionID); err != nil {
				logger.LogIf(ctx, fmt.Errorf("invalid version-id specified %w", err))
				apiErr := errorCodes.ToAPIErr(ErrNoSuchVersion)
				deleteResults[index].errInfo = DeleteError{
					Code:      apiErr.Code,
					Message:   apiErr.Description,
					Key:       object.ObjectName,
					VersionID: object.VersionID,
				}
				continue
			}
		}

		opts := ObjectOptions{
			VersionID:        object.VersionID,
			Versioned:        vc.PrefixEnabled(object.ObjectName),
			VersionSuspended: vc.Suspended(),
		}

		if replicateDeletes || object.VersionID != "" && hasLockEnabled || !globalTierConfigMgr.Empty() {
			if !globalTierConfigMgr.Empty() && object.VersionID == "" && opts.VersionSuspended {
				opts.VersionID = nullVersionID
			}
			goi, gerr = getObjectInfoFn(ctx, bucket, object.ObjectName, opts)
		}

		if !globalTierConfigMgr.Empty() {
			oss[index] = newObjSweeper(bucket, object.ObjectName).WithVersion(opts.VersionID).WithVersioning(opts.Versioned, opts.VersionSuspended)
			oss[index].SetTransitionState(goi.TransitionedObject)
		}

		if replicateDeletes {
			dsc = checkReplicateDelete(ctx, bucket, ObjectToDelete{
				ObjectV: ObjectV{
					ObjectName: object.ObjectName,
					VersionID:  object.VersionID,
				},
			}, goi, opts, gerr)
			if dsc.ReplicateAny() {
				if object.VersionID != "" {
					object.VersionPurgeStatus = Pending
					object.VersionPurgeStatuses = dsc.PendingStatus()
				} else {
					object.DeleteMarkerReplicationStatus = dsc.PendingStatus()
				}
				object.ReplicateDecisionStr = dsc.String()
			}
		}
		if object.VersionID != "" && hasLockEnabled {
			if apiErrCode := enforceRetentionBypassForDelete(ctx, r, bucket, object, goi, gerr); apiErrCode != ErrNone {
				apiErr := errorCodes.ToAPIErr(apiErrCode)
				deleteResults[index].errInfo = DeleteError{
					Code:      apiErr.Code,
					Message:   apiErr.Description,
					Key:       object.ObjectName,
					VersionID: object.VersionID,
				}
				continue
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

	// Disable timeouts and cancellation
	ctx = bgContext(ctx)

	deleteList := toNames(objectsToDelete)
	dObjects, errs := deleteObjectsFn(ctx, bucket, deleteList, ObjectOptions{
		PrefixEnabledFn:  vc.PrefixEnabled,
		VersionSuspended: vc.Suspended(),
	})

	for i := range errs {
		// DeleteMarkerVersionID is not used specifically to avoid
		// lookup errors, since DeleteMarkerVersionID is only
		// created during DeleteMarker creation when client didn't
		// specify a versionID.
		objToDel := ObjectToDelete{
			ObjectV: ObjectV{
				ObjectName: dObjects[i].ObjectName,
				VersionID:  dObjects[i].VersionID,
			},
			VersionPurgeStatus:            dObjects[i].VersionPurgeStatus(),
			VersionPurgeStatuses:          dObjects[i].ReplicationState.VersionPurgeStatusInternal,
			DeleteMarkerReplicationStatus: dObjects[i].ReplicationState.ReplicationStatusInternal,
			ReplicateDecisionStr:          dObjects[i].ReplicationState.ReplicateDecisionStr,
		}
		dindex := objectsToDelete[objToDel]
		if errs[i] == nil || isErrObjectNotFound(errs[i]) || isErrVersionNotFound(errs[i]) {
			if replicateDeletes {
				dObjects[i].ReplicationState = deleteList[i].ReplicationState()
			}
			deleteResults[dindex].delInfo = dObjects[i]
			continue
		}
		apiErr := toAPIError(ctx, errs[i])
		deleteResults[dindex].errInfo = DeleteError{
			Code:      apiErr.Code,
			Message:   apiErr.Description,
			Key:       deleteList[i].ObjectName,
			VersionID: deleteList[i].VersionID,
		}
	}

	// Generate response
	deleteErrors := make([]DeleteError, 0, len(deleteObjectsReq.Objects))
	deletedObjects := make([]DeletedObject, 0, len(deleteObjectsReq.Objects))
	for _, deleteResult := range deleteResults {
		if deleteResult.errInfo.Code != "" {
			deleteErrors = append(deleteErrors, deleteResult.errInfo)
		} else {
			deletedObjects = append(deletedObjects, deleteResult.delInfo)
		}
	}

	response := generateMultiDeleteResponse(deleteObjectsReq.Quiet, deletedObjects, deleteErrors)
	encodedSuccessResponse := encodeResponse(response)

	// Write success response.
	writeSuccessResponseXML(w, encodedSuccessResponse)
	for _, dobj := range deletedObjects {
		if dobj.ObjectName == "" {
			continue
		}

		if replicateDeletes && (dobj.DeleteMarkerReplicationStatus() == replication.Pending || dobj.VersionPurgeStatus() == Pending) {
			dv := DeletedObjectReplicationInfo{
				DeletedObject: dobj,
				Bucket:        bucket,
				EventType:     ReplicateIncomingDelete,
			}
			scheduleReplicationDelete(ctx, dv, objectAPI)
		}

		eventName := event.ObjectRemovedDelete
		objInfo := ObjectInfo{
			Name:         dobj.ObjectName,
			VersionID:    dobj.VersionID,
			DeleteMarker: dobj.DeleteMarker,
		}

		if objInfo.DeleteMarker {
			objInfo.VersionID = dobj.DeleteMarkerVersionID
			eventName = event.ObjectRemovedDeleteMarkerCreated
		}

		sendEvent(eventArgs{
			EventName:    eventName,
			BucketName:   bucket,
			Object:       objInfo,
			ReqParams:    extractReqParams(r),
			RespElements: extractRespElements(w),
			UserAgent:    r.UserAgent(),
			Host:         handlers.GetSourceIP(r),
		})
	}

	// Clean up transitioned objects from remote tier
	for _, os := range oss {
		if os == nil { // skip objects that weren't deleted due to invalid versionID etc.
			continue
		}
		logger.LogIf(ctx, os.Sweep())
	}
}

// PutBucketHandler - PUT Bucket
// ----------
// This implementation of the PUT operation creates a new bucket for authenticated request
func (api objectAPIHandlers) PutBucketHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "PutBucket")

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrServerNotInitialized), r.URL)
		return
	}

	vars := mux.Vars(r)
	bucket := vars["bucket"]

	objectLockEnabled := false
	if vs := r.Header.Get(xhttp.AmzObjectLockEnabled); len(vs) > 0 {
		v := strings.ToLower(vs)
		switch v {
		case "true", "false":
			objectLockEnabled = v == "true"
		default:
			writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrInvalidRequest), r.URL)
			return
		}
	}

	forceCreate := false
	if vs := r.Header.Get(xhttp.MinIOForceCreate); len(vs) > 0 {
		v := strings.ToLower(vs)
		switch v {
		case "true", "false":
			forceCreate = v == "true"
		default:
			writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrInvalidRequest), r.URL)
			return
		}
	}

	if s3Error := checkRequestAuthType(ctx, r, policy.CreateBucketAction, bucket, ""); s3Error != ErrNone {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(s3Error), r.URL)
		return
	}

	// Parse incoming location constraint.
	location, s3Error := parseLocationConstraint(r)
	if s3Error != ErrNone {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(s3Error), r.URL)
		return
	}

	// Validate if location sent by the client is valid, reject
	// requests which do not follow valid region requirements.
	if !isValidLocation(location) {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrInvalidRegion), r.URL)
		return
	}

	opts := MakeBucketOptions{
		Location:    location,
		LockEnabled: objectLockEnabled,
		ForceCreate: forceCreate,
	}

	if globalDNSConfig != nil {
		sr, err := globalDNSConfig.Get(bucket)
		if err != nil {
			// ErrNotImplemented indicates a DNS backend that doesn't need to check if bucket already
			// exists elsewhere
			if err == dns.ErrNoEntriesFound || err == dns.ErrNotImplemented {
				// Proceed to creating a bucket.
				if err = objectAPI.MakeBucketWithLocation(ctx, bucket, opts); err != nil {
					writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
					return
				}

				if err = globalDNSConfig.Put(bucket); err != nil {
					objectAPI.DeleteBucket(context.Background(), bucket, DeleteBucketOptions{
						Force:      false,
						NoRecreate: true,
						SRDeleteOp: getSRBucketDeleteOp(globalSiteReplicationSys.isEnabled()),
					})
					writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
					return
				}

				// Load updated bucket metadata into memory.
				globalNotificationSys.LoadBucketMetadata(GlobalContext, bucket)

				// Make sure to add Location information here only for bucket
				w.Header().Set(xhttp.Location,
					getObjectLocation(r, globalDomainNames, bucket, ""))

				writeSuccessResponseHeadersOnly(w)

				sendEvent(eventArgs{
					EventName:    event.BucketCreated,
					BucketName:   bucket,
					ReqParams:    extractReqParams(r),
					RespElements: extractRespElements(w),
					UserAgent:    r.UserAgent(),
					Host:         handlers.GetSourceIP(r),
				})

				return
			}
			writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
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
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(apiErr), r.URL)
		return
	}

	// Proceed to creating a bucket.
	if err := objectAPI.MakeBucketWithLocation(ctx, bucket, opts); err != nil {
		if _, ok := err.(BucketExists); ok {
			// Though bucket exists locally, we send the site-replication
			// hook to ensure all sites have this bucket. If the hook
			// succeeds, the client will still receive a bucket exists
			// message.
			globalSiteReplicationSys.MakeBucketHook(ctx, bucket, opts)
		}
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	// Load updated bucket metadata into memory.
	globalNotificationSys.LoadBucketMetadata(GlobalContext, bucket)

	// Call site replication hook
	if err := globalSiteReplicationSys.MakeBucketHook(ctx, bucket, opts); err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	// Make sure to add Location information here only for bucket
	if cp := pathClean(r.URL.Path); cp != "" {
		w.Header().Set(xhttp.Location, cp) // Clean any trailing slashes.
	}

	writeSuccessResponseHeadersOnly(w)

	sendEvent(eventArgs{
		EventName:    event.BucketCreated,
		BucketName:   bucket,
		ReqParams:    extractReqParams(r),
		RespElements: extractRespElements(w),
		UserAgent:    r.UserAgent(),
		Host:         handlers.GetSourceIP(r),
	})
}

// PostPolicyBucketHandler - POST policy
// ----------
// This implementation of the POST operation handles object creation with a specified
// signature policy in multipart/form-data
func (api objectAPIHandlers) PostPolicyBucketHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "PostPolicyBucket")

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

	if crypto.Requested(r.Header) && !objectAPI.IsEncryptionSupported() {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrNotImplemented), r.URL)
		return
	}

	bucket := mux.Vars(r)["bucket"]

	// Require Content-Length to be set in the request
	size := r.ContentLength
	if size < 0 {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrMissingContentLength), r.URL)
		return
	}

	resource, err := getResource(r.URL.Path, r.Host, globalDomainNames)
	if err != nil {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrInvalidRequest), r.URL)
		return
	}

	// Make sure that the URL does not contain object name.
	if bucket != path.Clean(resource[1:]) {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrMethodNotAllowed), r.URL)
		return
	}

	// Here the parameter is the size of the form data that should
	// be loaded in memory, the remaining being put in temporary files.
	reader, err := r.MultipartReader()
	if err != nil {
		logger.LogIf(ctx, err)
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrMalformedPOSTRequest), r.URL)
		return
	}

	// Read multipart data and save in memory and in the disk if needed
	form, err := reader.ReadForm(maxFormMemory)
	if err != nil {
		logger.LogIf(ctx, err, logger.Application)
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrMalformedPOSTRequest), r.URL)
		return
	}

	// Remove all tmp files created during multipart upload
	defer form.RemoveAll()

	// Extract all form fields
	fileBody, fileName, fileSize, formValues, err := extractPostPolicyFormValues(ctx, form)
	if err != nil {
		logger.LogIf(ctx, err, logger.Application)
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrMalformedPOSTRequest), r.URL)
		return
	}

	// Check if file is provided, error out otherwise.
	if fileBody == nil {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrPOSTFileRequired), r.URL)
		return
	}

	// Close multipart file
	defer fileBody.Close()

	formValues.Set("Bucket", bucket)
	if fileName != "" && strings.Contains(formValues.Get("Key"), "${filename}") {
		// S3 feature to replace ${filename} found in Key form field
		// by the filename attribute passed in multipart
		formValues.Set("Key", strings.ReplaceAll(formValues.Get("Key"), "${filename}", fileName))
	}
	object := trimLeadingSlash(formValues.Get("Key"))

	successRedirect := formValues.Get("success_action_redirect")
	successStatus := formValues.Get("success_action_status")
	var redirectURL *url.URL
	if successRedirect != "" {
		redirectURL, err = url.Parse(successRedirect)
		if err != nil {
			writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrMalformedPOSTRequest), r.URL)
			return
		}
	}

	// Verify policy signature.
	cred, errCode := doesPolicySignatureMatch(formValues)
	if errCode != ErrNone {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(errCode), r.URL)
		return
	}

	// Once signature is validated, check if the user has
	// explicit permissions for the user.
	if !globalIAMSys.IsAllowed(iampolicy.Args{
		AccountName:     cred.AccessKey,
		Groups:          cred.Groups,
		Action:          iampolicy.PutObjectAction,
		ConditionValues: getConditionValues(r, "", cred.AccessKey, cred.Claims),
		BucketName:      bucket,
		ObjectName:      object,
		IsOwner:         globalActiveCred.AccessKey == cred.AccessKey,
		Claims:          cred.Claims,
	}) {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrAccessDenied), r.URL)
		return
	}

	policyBytes, err := base64.StdEncoding.DecodeString(formValues.Get("Policy"))
	if err != nil {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrMalformedPOSTRequest), r.URL)
		return
	}

	// Handle policy if it is set.
	if len(policyBytes) > 0 {
		postPolicyForm, err := parsePostPolicyForm(bytes.NewReader(policyBytes))
		if err != nil {
			errAPI := errorCodes.ToAPIErr(ErrPostPolicyConditionInvalidFormat)
			errAPI.Description = fmt.Sprintf("%s '(%s)'", errAPI.Description, err)
			writeErrorResponse(ctx, w, errAPI, r.URL)
			return
		}

		// Make sure formValues adhere to policy restrictions.
		if err = checkPostPolicy(formValues, postPolicyForm); err != nil {
			writeErrorResponse(ctx, w, errorCodes.ToAPIErrWithErr(ErrAccessDenied, err), r.URL)
			return
		}

		// Ensure that the object size is within expected range, also the file size
		// should not exceed the maximum single Put size (5 GiB)
		lengthRange := postPolicyForm.Conditions.ContentLengthRange
		if lengthRange.Valid {
			if fileSize < lengthRange.Min {
				writeErrorResponse(ctx, w, toAPIError(ctx, errDataTooSmall), r.URL)
				return
			}

			if fileSize > lengthRange.Max || isMaxObjectSize(fileSize) {
				writeErrorResponse(ctx, w, toAPIError(ctx, errDataTooLarge), r.URL)
				return
			}
		}
	}

	// Extract metadata to be saved from received Form.
	metadata := make(map[string]string)
	err = extractMetadataFromMime(ctx, textproto.MIMEHeader(formValues), metadata)
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	hashReader, err := hash.NewReader(fileBody, fileSize, "", "", fileSize)
	if err != nil {
		logger.LogIf(ctx, err)
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}
	rawReader := hashReader
	pReader := NewPutObjReader(rawReader)
	var objectEncryptionKey crypto.ObjectKey

	// Check if bucket encryption is enabled
	sseConfig, _ := globalBucketSSEConfigSys.Get(bucket)
	sseConfig.Apply(r.Header, sse.ApplyOptions{
		AutoEncrypt: globalAutoEncryption,
		Passthrough: globalIsGateway && globalGatewayName == S3BackendGateway,
	})

	// get gateway encryption options
	var opts ObjectOptions
	opts, err = putOpts(ctx, r, bucket, object, metadata)
	if err != nil {
		writeErrorResponseHeadersOnly(w, toAPIError(ctx, err))
		return
	}
	if objectAPI.IsEncryptionSupported() {
		if crypto.Requested(formValues) && !HasSuffix(object, SlashSeparator) { // handle SSE requests
			if crypto.SSECopy.IsRequested(r.Header) {
				writeErrorResponse(ctx, w, toAPIError(ctx, errInvalidEncryptionParameters), r.URL)
				return
			}
			var (
				reader io.Reader
				keyID  string
				key    []byte
				kmsCtx kms.Context
			)
			kind, _ := crypto.IsRequested(formValues)
			switch kind {
			case crypto.SSEC:
				key, err = ParseSSECustomerHeader(formValues)
				if err != nil {
					writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
					return
				}
			case crypto.S3KMS:
				keyID, kmsCtx, err = crypto.S3KMS.ParseHTTP(formValues)
				if err != nil {
					writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
					return
				}
			}
			reader, objectEncryptionKey, err = newEncryptReader(ctx, hashReader, kind, keyID, key, bucket, object, metadata, kmsCtx)
			if err != nil {
				writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
				return
			}
			info := ObjectInfo{Size: fileSize}
			// do not try to verify encrypted content
			hashReader, err = hash.NewReader(reader, info.EncryptedSize(), "", "", fileSize)
			if err != nil {
				writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
				return
			}
			pReader, err = pReader.WithEncryption(hashReader, &objectEncryptionKey)
			if err != nil {
				writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
				return
			}
		}
	}

	objInfo, err := objectAPI.PutObject(ctx, bucket, object, pReader, opts)
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
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

	if redirectURL != nil { // success_action_redirect is valid and set.
		v := redirectURL.Query()
		v.Add("bucket", objInfo.Bucket)
		v.Add("key", objInfo.Name)
		v.Add("etag", "\""+objInfo.ETag+"\"")
		redirectURL.RawQuery = v.Encode()
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

// GetBucketPolicyStatusHandler -  Retrieves the policy status
// for an MinIO bucket, indicating whether the bucket is public.
func (api objectAPIHandlers) GetBucketPolicyStatusHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "GetBucketPolicyStatus")

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	vars := mux.Vars(r)
	bucket := vars["bucket"]

	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponseHeadersOnly(w, errorCodes.ToAPIErr(ErrServerNotInitialized))
		return
	}

	if s3Error := checkRequestAuthType(ctx, r, policy.GetBucketPolicyStatusAction, bucket, ""); s3Error != ErrNone {
		writeErrorResponseHeadersOnly(w, errorCodes.ToAPIErr(s3Error))
		return
	}

	// Check if bucket exists.
	if _, err := objectAPI.GetBucketInfo(ctx, bucket, BucketOptions{}); err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	// Check if anonymous (non-owner) has access to list objects.
	readable := globalPolicySys.IsAllowed(policy.Args{
		Action:          policy.ListBucketAction,
		BucketName:      bucket,
		ConditionValues: getConditionValues(r, "", "", nil),
		IsOwner:         false,
	})

	// Check if anonymous (non-owner) has access to upload objects.
	writable := globalPolicySys.IsAllowed(policy.Args{
		Action:          policy.PutObjectAction,
		BucketName:      bucket,
		ConditionValues: getConditionValues(r, "", "", nil),
		IsOwner:         false,
	})

	encodedSuccessResponse := encodeResponse(PolicyStatus{
		IsPublic: func() string {
			// Silly to have special 'boolean' values yes
			// but complying with silly implementation
			// https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketPolicyStatus.html
			if readable && writable {
				return "TRUE"
			}
			return "FALSE"
		}(),
	})

	writeSuccessResponseXML(w, encodedSuccessResponse)
}

// HeadBucketHandler - HEAD Bucket
// ----------
// This operation is useful to determine if a bucket exists.
// The operation returns a 200 OK if the bucket exists and you
// have permission to access it. Otherwise, the operation might
// return responses such as 404 Not Found and 403 Forbidden.
func (api objectAPIHandlers) HeadBucketHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "HeadBucket")

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

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

	if _, err := getBucketInfo(ctx, bucket, BucketOptions{}); err != nil {
		writeErrorResponseHeadersOnly(w, toAPIError(ctx, err))
		return
	}

	writeResponse(w, http.StatusOK, nil, mimeXML)
}

// DeleteBucketHandler - Delete bucket
func (api objectAPIHandlers) DeleteBucketHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "DeleteBucket")

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	vars := mux.Vars(r)
	bucket := vars["bucket"]

	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrServerNotInitialized), r.URL)
		return
	}

	// Verify if the caller has sufficient permissions.
	if s3Error := checkRequestAuthType(ctx, r, policy.DeleteBucketAction, bucket, ""); s3Error != ErrNone {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(s3Error), r.URL)
		return
	}

	forceDelete := false
	if value := r.Header.Get(xhttp.MinIOForceDelete); value != "" {
		var err error
		forceDelete, err = strconv.ParseBool(value)
		if err != nil {
			apiErr := errorCodes.ToAPIErr(ErrInvalidRequest)
			apiErr.Description = err.Error()
			writeErrorResponse(ctx, w, apiErr, r.URL)
			return
		}

		// if force delete header is set, we need to evaluate the policy anyways
		// regardless of it being true or not.
		if s3Error := checkRequestAuthType(ctx, r, policy.ForceDeleteBucketAction, bucket, ""); s3Error != ErrNone {
			writeErrorResponse(ctx, w, errorCodes.ToAPIErr(s3Error), r.URL)
			return
		}

		if forceDelete {
			if rcfg, _ := globalBucketObjectLockSys.Get(bucket); rcfg.LockEnabled {
				writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrMethodNotAllowed), r.URL)
				return
			}
			rcfg, err := getReplicationConfig(ctx, bucket)
			switch {
			case err != nil:
				if _, ok := err.(BucketReplicationConfigNotFound); !ok {
					writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrMethodNotAllowed), r.URL)
					return
				}
			case rcfg.HasActiveRules("", true):
				writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrMethodNotAllowed), r.URL)
				return
			}
		}
	}

	if globalDNSConfig != nil {
		if err := globalDNSConfig.Delete(bucket); err != nil {
			logger.LogIf(ctx, fmt.Errorf("Unable to delete bucket DNS entry %w, please delete it manually", err))
			writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
			return
		}
	}

	deleteBucket := objectAPI.DeleteBucket

	// Attempt to delete bucket.
	if err := deleteBucket(ctx, bucket, DeleteBucketOptions{
		Force:      forceDelete,
		SRDeleteOp: getSRBucketDeleteOp(globalSiteReplicationSys.isEnabled()),
	}); err != nil {
		apiErr := toAPIError(ctx, err)
		if _, ok := err.(BucketNotEmpty); ok {
			if globalBucketVersioningSys.Enabled(bucket) || globalBucketVersioningSys.Suspended(bucket) {
				apiErr.Description = "The bucket you tried to delete is not empty. You must delete all versions in the bucket."
			}
		}
		if globalDNSConfig != nil {
			if err2 := globalDNSConfig.Put(bucket); err2 != nil {
				logger.LogIf(ctx, fmt.Errorf("Unable to restore bucket DNS entry %w, please fix it manually", err2))
			}
		}
		writeErrorResponse(ctx, w, apiErr, r.URL)
		return
	}

	globalNotificationSys.DeleteBucketMetadata(ctx, bucket)
	globalReplicationPool.deleteResyncMetadata(ctx, bucket)
	// Call site replication hook.
	if err := globalSiteReplicationSys.DeleteBucketHook(ctx, bucket, forceDelete); err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	// Write success response.
	writeSuccessNoContent(w)

	sendEvent(eventArgs{
		EventName:    event.BucketRemoved,
		BucketName:   bucket,
		ReqParams:    extractReqParams(r),
		RespElements: extractRespElements(w),
		UserAgent:    r.UserAgent(),
		Host:         handlers.GetSourceIP(r),
	})
}

// PutBucketObjectLockConfigHandler - PUT Bucket object lock configuration.
// ----------
// Places an Object Lock configuration on the specified bucket. The rule
// specified in the Object Lock configuration will be applied by default
// to every new object placed in the specified bucket.
func (api objectAPIHandlers) PutBucketObjectLockConfigHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "PutBucketObjectLockConfig")

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	vars := mux.Vars(r)
	bucket := vars["bucket"]

	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrServerNotInitialized), r.URL)
		return
	}
	if globalIsGateway {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrNotImplemented), r.URL)
		return
	}
	if s3Error := checkRequestAuthType(ctx, r, policy.PutBucketObjectLockConfigurationAction, bucket, ""); s3Error != ErrNone {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(s3Error), r.URL)
		return
	}

	config, err := objectlock.ParseObjectLockConfig(r.Body)
	if err != nil {
		apiErr := errorCodes.ToAPIErr(ErrMalformedXML)
		apiErr.Description = err.Error()
		writeErrorResponse(ctx, w, apiErr, r.URL)
		return
	}

	configData, err := xml.Marshal(config)
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	// Deny object locking configuration settings on existing buckets without object lock enabled.
	if _, _, err = globalBucketMetadataSys.GetObjectLockConfig(bucket); err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	updatedAt, err := globalBucketMetadataSys.Update(ctx, bucket, objectLockConfig, configData)
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	// Call site replication hook.
	//
	// We encode the xml bytes as base64 to ensure there are no encoding
	// errors.
	cfgStr := base64.StdEncoding.EncodeToString(configData)
	if err = globalSiteReplicationSys.BucketMetaHook(ctx, madmin.SRBucketMeta{
		Type:             madmin.SRBucketMetaTypeObjectLockConfig,
		Bucket:           bucket,
		ObjectLockConfig: &cfgStr,
		UpdatedAt:        updatedAt,
	}); err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
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

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	vars := mux.Vars(r)
	bucket := vars["bucket"]

	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrServerNotInitialized), r.URL)
		return
	}

	// check if user has permissions to perform this operation
	if s3Error := checkRequestAuthType(ctx, r, policy.GetBucketObjectLockConfigurationAction, bucket, ""); s3Error != ErrNone {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(s3Error), r.URL)
		return
	}

	config, _, err := globalBucketMetadataSys.GetObjectLockConfig(bucket)
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	configData, err := xml.Marshal(config)
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	// Write success response.
	writeSuccessResponseXML(w, configData)
}

// PutBucketTaggingHandler - PUT Bucket tagging.
// ----------
func (api objectAPIHandlers) PutBucketTaggingHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "PutBucketTagging")

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	vars := mux.Vars(r)
	bucket := vars["bucket"]

	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrServerNotInitialized), r.URL)
		return
	}

	// Check if bucket exists.
	if _, err := objectAPI.GetBucketInfo(ctx, bucket, BucketOptions{}); err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	if s3Error := checkRequestAuthType(ctx, r, policy.PutBucketTaggingAction, bucket, ""); s3Error != ErrNone {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(s3Error), r.URL)
		return
	}

	tags, err := tags.ParseBucketXML(io.LimitReader(r.Body, r.ContentLength))
	if err != nil {
		apiErr := errorCodes.ToAPIErr(ErrMalformedXML)
		apiErr.Description = err.Error()
		writeErrorResponse(ctx, w, apiErr, r.URL)
		return
	}

	configData, err := xml.Marshal(tags)
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	updatedAt, err := globalBucketMetadataSys.Update(ctx, bucket, bucketTaggingConfig, configData)
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	// Call site replication hook.
	//
	// We encode the xml bytes as base64 to ensure there are no encoding
	// errors.
	cfgStr := base64.StdEncoding.EncodeToString(configData)
	if err = globalSiteReplicationSys.BucketMetaHook(ctx, madmin.SRBucketMeta{
		Type:      madmin.SRBucketMetaTypeTags,
		Bucket:    bucket,
		Tags:      &cfgStr,
		UpdatedAt: updatedAt,
	}); err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	// Write success response.
	writeSuccessResponseHeadersOnly(w)
}

// GetBucketTaggingHandler - GET Bucket tagging.
// ----------
func (api objectAPIHandlers) GetBucketTaggingHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "GetBucketTagging")

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	vars := mux.Vars(r)
	bucket := vars["bucket"]

	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrServerNotInitialized), r.URL)
		return
	}

	// check if user has permissions to perform this operation
	if s3Error := checkRequestAuthType(ctx, r, policy.GetBucketTaggingAction, bucket, ""); s3Error != ErrNone {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(s3Error), r.URL)
		return
	}

	config, _, err := globalBucketMetadataSys.GetTaggingConfig(bucket)
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	configData, err := xml.Marshal(config)
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	// Write success response.
	writeSuccessResponseXML(w, configData)
}

// DeleteBucketTaggingHandler - DELETE Bucket tagging.
// ----------
func (api objectAPIHandlers) DeleteBucketTaggingHandler(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "DeleteBucketTagging")

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	vars := mux.Vars(r)
	bucket := vars["bucket"]

	objectAPI := api.ObjectAPI()
	if objectAPI == nil {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrServerNotInitialized), r.URL)
		return
	}

	if s3Error := checkRequestAuthType(ctx, r, policy.PutBucketTaggingAction, bucket, ""); s3Error != ErrNone {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(s3Error), r.URL)
		return
	}

	updatedAt, err := globalBucketMetadataSys.Update(ctx, bucket, bucketTaggingConfig, nil)
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	if err := globalSiteReplicationSys.BucketMetaHook(ctx, madmin.SRBucketMeta{
		Type:      madmin.SRBucketMetaTypeTags,
		Bucket:    bucket,
		UpdatedAt: updatedAt,
	}); err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	// Write success response.
	writeSuccessResponseHeadersOnly(w)
}

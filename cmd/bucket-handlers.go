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
	"bytes"
	"context"
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"net/url"
	"path"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/google/uuid"
	"github.com/minio/mux"
	"github.com/valyala/bytebufferpool"

	"github.com/minio/madmin-go/v3"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/set"
	"github.com/minio/minio-go/v7/pkg/tags"
	"github.com/minio/minio/internal/auth"
	sse "github.com/minio/minio/internal/bucket/encryption"
	objectlock "github.com/minio/minio/internal/bucket/object/lock"
	"github.com/minio/minio/internal/bucket/replication"
	"github.com/minio/minio/internal/config/dns"
	"github.com/minio/minio/internal/crypto"
	"github.com/minio/minio/internal/etag"
	"github.com/minio/minio/internal/event"
	"github.com/minio/minio/internal/handlers"
	"github.com/minio/minio/internal/hash"
	xhttp "github.com/minio/minio/internal/http"
	"github.com/minio/minio/internal/ioutil"
	"github.com/minio/minio/internal/kms"
	"github.com/minio/minio/internal/logger"
	"github.com/minio/pkg/v3/policy"
	"github.com/minio/pkg/v3/sync/errgroup"
)

const (
	objectLockConfig        = "object-lock.xml"
	bucketTaggingConfig     = "tagging.xml"
	bucketReplicationConfig = "replication.xml"

	xMinIOErrCodeHeader = "x-minio-error-code"
	xMinIOErrDescHeader = "x-minio-error-desc"

	postPolicyBucketTagging = "tagging"
)

// Check if there are buckets on server without corresponding entry in etcd backend and
// make entries. Here is the general flow
// - Range over all the available buckets
// - Check if a bucket has an entry in etcd backend
// -- If no, make an entry
// -- If yes, check if the entry matches local IP check if we
//
//	need to update the entry then proceed to update
//
// -- If yes, check if the IP of entry matches local IP.
//
//	This means entry is for this instance.
//
// -- If IP of the entry doesn't match, this means entry is
//
//	for another instance. Log an error to console.
func initFederatorBackend(buckets []string, objLayer ObjectLayer) {
	if len(buckets) == 0 {
		return
	}

	// Get buckets in the DNS
	dnsBuckets, err := globalDNSConfig.List()
	if err != nil && !IsErrIgnored(err, dns.ErrNoEntriesFound, dns.ErrNotImplemented, dns.ErrDomainMissing) {
		dnsLogIf(GlobalContext, err)
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
			bucketsSet.Add(bucket)
			r, ok := dnsBuckets[bucket]
			if !ok {
				bucketsToBeUpdated.Add(bucket)
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
				bucketsToBeUpdated.Add(bucket)
				continue
			}

			// No IPs seem to intersect, this means that bucket exists but has
			// different IP addresses perhaps from a different deployment.
			// bucket names are globally unique in federation at a given
			// path prefix, name collision is not allowed. We simply log
			// an error and continue.
			bucketsInConflict.Add(bucket)
		}
	}

	// Add/update buckets that are not registered with the DNS
	bucketsToBeUpdatedSlice := bucketsToBeUpdated.ToSlice()
	g := errgroup.WithNErrs(len(bucketsToBeUpdatedSlice)).WithConcurrency(50)

	for index := range bucketsToBeUpdatedSlice {
		g.Go(func() error {
			return globalDNSConfig.Put(bucketsToBeUpdatedSlice[index])
		}, index)
	}

	ctx := GlobalContext
	for _, err := range g.Wait() {
		if err != nil {
			dnsLogIf(ctx, err)
			return
		}
	}

	for _, bucket := range bucketsInConflict.ToSlice() {
		dnsLogIf(ctx, fmt.Errorf("Unable to add bucket DNS entry for bucket %s, an entry exists for the same bucket by a different tenant. This local bucket will be ignored. Bucket names are globally unique in federated deployments. Use path style requests on following addresses '%v' to access this bucket", bucket, globalDomainIPs.ToSlice()))
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
				dnsLogIf(GlobalContext, fmt.Errorf("Failed to remove DNS entry for %s due to %w",
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
	region := globalSite.Region()
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

	cred, owner, s3Error := checkRequestAuthTypeCredential(ctx, r, policy.ListAllMyBucketsAction)
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
			if globalIAMSys.IsAllowed(policy.Args{
				AccountName:     cred.AccessKey,
				Groups:          cred.Groups,
				Action:          policy.ListBucketAction,
				BucketName:      bucketInfo.Name,
				ConditionValues: getConditionValues(r, "", cred),
				IsOwner:         owner,
				ObjectName:      "",
				Claims:          cred.Claims,
			}) {
				bucketsInfo[n] = bucketInfo
				n++
			} else if globalIAMSys.IsAllowed(policy.Args{
				AccountName:     cred.AccessKey,
				Groups:          cred.Groups,
				Action:          policy.GetBucketLocationAction,
				BucketName:      bucketInfo.Name,
				ConditionValues: getConditionValues(r, "", cred),
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

	// Content-Md5 is required should be set
	// http://docs.aws.amazon.com/AmazonS3/latest/API/multiobjectdeleteapi.html
	if !validateLengthAndChecksum(r) {
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

	if r.ContentLength > maxBodySize {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrEntityTooLarge), r.URL)
		return
	}

	// Unmarshal list of keys to be deleted.
	deleteObjectsReq := &DeleteObjectsRequest{}
	if err := xmlDecoder(r.Body, deleteObjectsReq, maxBodySize); err != nil {
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

	deleteObjectsFn := objectAPI.DeleteObjects

	// Return Malformed XML as S3 spec if the number of objects is empty
	if len(deleteObjectsReq.Objects) == 0 || len(deleteObjectsReq.Objects) > maxDeleteList {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrMalformedXML), r.URL)
		return
	}

	objectsToDelete := map[ObjectToDelete]int{}
	getObjectInfoFn := objectAPI.GetObjectInfo

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
		if apiErrCode := checkRequestAuthTypeWithVID(ctx, r, policy.DeleteObjectAction, bucket, object.ObjectName, object.VersionID); apiErrCode != ErrNone {
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
				apiErr := errorCodes.ToAPIErr(ErrNoSuchVersion)
				deleteResults[index].errInfo = DeleteError{
					Code:      apiErr.Code,
					Message:   fmt.Sprintf("%s (%s)", apiErr.Description, err),
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

		// All deletes on directory objects needs to be for `nullVersionID`
		if isDirObject(object.ObjectName) && object.VersionID == "" {
			object.VersionID = nullVersionID
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
					object.VersionPurgeStatus = replication.VersionPurgePending
					object.VersionPurgeStatuses = dsc.PendingStatus()
				} else {
					object.DeleteMarkerReplicationStatus = dsc.PendingStatus()
				}
				object.ReplicateDecisionStr = dsc.String()
			}
		}
		if object.VersionID != "" && hasLockEnabled {
			if err := enforceRetentionBypassForDelete(ctx, r, bucket, object, goi, gerr); err != nil {
				apiErr := toAPIError(ctx, err)
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
		return output
	}

	// Disable timeouts and cancellation
	ctx = bgContext(ctx)

	deleteList := toNames(objectsToDelete)
	dObjects, errs := deleteObjectsFn(ctx, bucket, deleteList, ObjectOptions{
		PrefixEnabledFn:  vc.PrefixEnabled,
		VersionSuspended: vc.Suspended(),
	})

	// Are all objects saying bucket not found?
	if isAllBucketsNotFound(errs) {
		writeErrorResponse(ctx, w, toAPIError(ctx, errs[0]), r.URL)
		return
	}

	for i := range errs {
		// DeleteMarkerVersionID is not used specifically to avoid
		// lookup errors, since DeleteMarkerVersionID is only
		// created during DeleteMarker creation when client didn't
		// specify a versionID.
		objToDel := ObjectToDelete{
			ObjectV: ObjectV{
				ObjectName: decodeDirObject(dObjects[i].ObjectName),
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
			// All deletes on directory objects was with `nullVersionID`.
			// Remove it from response.
			if isDirObject(deleteResult.delInfo.ObjectName) && deleteResult.delInfo.VersionID == nullVersionID {
				deleteResult.delInfo.VersionID = ""
			}
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

		if replicateDeletes && (dobj.DeleteMarkerReplicationStatus() == replication.Pending || dobj.VersionPurgeStatus() == replication.VersionPurgePending) {
			// copy so we can re-add null ID.
			dobj := dobj
			if isDirObject(dobj.ObjectName) && dobj.VersionID == "" {
				dobj.VersionID = nullVersionID
			}
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
		os.Sweep()
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

	cred, owner, s3Error := checkRequestAuthTypeCredential(ctx, r, policy.CreateBucketAction)
	if s3Error != ErrNone {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(s3Error), r.URL)
		return
	}

	if objectLockEnabled {
		// Creating a bucket with locking requires the user having more permissions
		for _, action := range []policy.Action{policy.PutBucketObjectLockConfigurationAction, policy.PutBucketVersioningAction} {
			if !globalIAMSys.IsAllowed(policy.Args{
				AccountName:     cred.AccessKey,
				Groups:          cred.Groups,
				Action:          action,
				ConditionValues: getConditionValues(r, "", cred),
				BucketName:      bucket,
				IsOwner:         owner,
				Claims:          cred.Claims,
			}) {
				writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrAccessDenied), r.URL)
				return
			}
		}
	}

	// Parse incoming location constraint.
	_, s3Error = parseLocationConstraint(r)
	if s3Error != ErrNone {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(s3Error), r.URL)
		return
	}

	// check if client is attempting to create more buckets, complain about it.
	if currBuckets := globalBucketMetadataSys.Count(); currBuckets+1 > maxBuckets {
		internalLogIf(ctx, fmt.Errorf("Please avoid creating more buckets %d beyond recommended %d", currBuckets+1, maxBuckets), logger.WarningKind)
	}

	opts := MakeBucketOptions{
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
				if err = objectAPI.MakeBucket(ctx, bucket, opts); err != nil {
					writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
					return
				}

				if err = globalDNSConfig.Put(bucket); err != nil {
					objectAPI.DeleteBucket(context.Background(), bucket, DeleteBucketOptions{
						Force:      true,
						SRDeleteOp: getSRBucketDeleteOp(globalSiteReplicationSys.isEnabled()),
					})
					writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
					return
				}

				// Load updated bucket metadata into memory.
				globalNotificationSys.LoadBucketMetadata(GlobalContext, bucket)

				// Make sure to add Location information here only for bucket
				w.Header().Set(xhttp.Location, pathJoin(SlashSeparator, bucket))

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
	if err := objectAPI.MakeBucket(ctx, bucket, opts); err != nil {
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
	replLogIf(ctx, globalSiteReplicationSys.MakeBucketHook(ctx, bucket, opts))

	// Make sure to add Location information here only for bucket
	w.Header().Set(xhttp.Location, pathJoin(SlashSeparator, bucket))

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

// multipartReader is just like https://pkg.go.dev/net/http#Request.MultipartReader but
// rejects multipart/mixed as its not supported in S3 API.
func multipartReader(r *http.Request) (*multipart.Reader, error) {
	v := r.Header.Get("Content-Type")
	if v == "" {
		return nil, http.ErrNotMultipart
	}
	if r.Body == nil {
		return nil, errors.New("missing form body")
	}
	d, params, err := mime.ParseMediaType(v)
	if err != nil {
		return nil, http.ErrNotMultipart
	}
	if d != "multipart/form-data" {
		return nil, http.ErrNotMultipart
	}
	boundary, ok := params["boundary"]
	if !ok {
		return nil, http.ErrMissingBoundary
	}
	return multipart.NewReader(r.Body, boundary), nil
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

	bucket := mux.Vars(r)["bucket"]
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

	if r.ContentLength <= 0 {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrEmptyRequestBody), r.URL)
		return
	}

	// Here the parameter is the size of the form data that should
	// be loaded in memory, the remaining being put in temporary files.
	mp, err := multipartReader(r)
	if err != nil {
		apiErr := errorCodes.ToAPIErr(ErrMalformedPOSTRequest)
		apiErr.Description = fmt.Sprintf("%s (%v)", apiErr.Description, err)
		writeErrorResponse(ctx, w, apiErr, r.URL)
		return
	}

	const mapEntryOverhead = 200

	var (
		reader        io.Reader
		actualSize    int64 = -1
		fileName      string
		fanOutEntries = make([]minio.PutObjectFanOutEntry, 0, 100)
	)

	maxParts := 1000
	// Canonicalize the form values into http.Header.
	formValues := make(http.Header)
	var headerLen int64
	for {
		part, err := mp.NextRawPart()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			apiErr := errorCodes.ToAPIErr(ErrMalformedPOSTRequest)
			apiErr.Description = fmt.Sprintf("%s (%v)", apiErr.Description, err)
			writeErrorResponse(ctx, w, apiErr, r.URL)
			return
		}
		if maxParts <= 0 {
			apiErr := errorCodes.ToAPIErr(ErrMalformedPOSTRequest)
			apiErr.Description = fmt.Sprintf("%s (%v)", apiErr.Description, multipart.ErrMessageTooLarge)
			writeErrorResponse(ctx, w, apiErr, r.URL)
			return
		}
		maxParts--

		name := part.FormName()
		if name == "" {
			continue
		}

		fileName = part.FileName()

		// Multiple values for the same key (one map entry, longer slice) are cheaper
		// than the same number of values for different keys (many map entries), but
		// using a consistent per-value cost for overhead is simpler.
		maxMemoryBytes := 2 * int64(10<<20)
		maxMemoryBytes -= int64(len(name))
		maxMemoryBytes -= mapEntryOverhead
		if maxMemoryBytes < 0 {
			// We can't actually take this path, since nextPart would already have
			// rejected the MIME headers for being too large. Check anyway.
			apiErr := errorCodes.ToAPIErr(ErrMalformedPOSTRequest)
			apiErr.Description = fmt.Sprintf("%s (%v)", apiErr.Description, multipart.ErrMessageTooLarge)
			writeErrorResponse(ctx, w, apiErr, r.URL)
			return
		}

		headerLen += int64(len(name)) + int64(len(fileName))
		if name != "file" {
			if http.CanonicalHeaderKey(name) == http.CanonicalHeaderKey("x-minio-fanout-list") {
				dec := json.NewDecoder(part)

				// while the array contains values
				for dec.More() {
					var m minio.PutObjectFanOutEntry
					if err := dec.Decode(&m); err != nil {
						part.Close()
						apiErr := errorCodes.ToAPIErr(ErrMalformedPOSTRequest)
						apiErr.Description = fmt.Sprintf("%s (%v)", apiErr.Description, err)
						writeErrorResponse(ctx, w, apiErr, r.URL)
						return
					}
					fanOutEntries = append(fanOutEntries, m)
				}
				part.Close()
				continue
			}

			buf := bytebufferpool.Get()
			// value, store as string in memory
			n, err := io.CopyN(buf, part, maxMemoryBytes+1)
			value := buf.String()
			buf.Reset()
			bytebufferpool.Put(buf)
			part.Close()

			if err != nil && err != io.EOF {
				apiErr := errorCodes.ToAPIErr(ErrMalformedPOSTRequest)
				apiErr.Description = fmt.Sprintf("%s (%v)", apiErr.Description, err)
				writeErrorResponse(ctx, w, apiErr, r.URL)
				return
			}
			maxMemoryBytes -= n
			if maxMemoryBytes < 0 {
				apiErr := errorCodes.ToAPIErr(ErrMalformedPOSTRequest)
				apiErr.Description = fmt.Sprintf("%s (%v)", apiErr.Description, multipart.ErrMessageTooLarge)
				writeErrorResponse(ctx, w, apiErr, r.URL)
				return
			}
			if n > maxFormFieldSize {
				apiErr := errorCodes.ToAPIErr(ErrMalformedPOSTRequest)
				apiErr.Description = fmt.Sprintf("%s (%v)", apiErr.Description, multipart.ErrMessageTooLarge)
				writeErrorResponse(ctx, w, apiErr, r.URL)
				return
			}
			headerLen += n
			formValues[http.CanonicalHeaderKey(name)] = append(formValues[http.CanonicalHeaderKey(name)], value)
			continue
		}

		// In accordance with https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectPOST.html
		// The file or text content.
		// The file or text content must be the last field in the form.
		// You cannot upload more than one file at a time.
		reader = part

		possibleShardSize := (r.ContentLength - headerLen)
		if globalStorageClass.ShouldInline(possibleShardSize, false) { // keep versioned false for this check
			var b bytes.Buffer
			n, err := io.Copy(&b, reader)
			if err != nil {
				apiErr := errorCodes.ToAPIErr(ErrMalformedPOSTRequest)
				apiErr.Description = fmt.Sprintf("%s (%v)", apiErr.Description, err)
				writeErrorResponse(ctx, w, apiErr, r.URL)
				return
			}
			reader = &b
			actualSize = n
		}

		// we have found the File part of the request we are done processing multipart-form
		break
	}

	// check if have a file
	if reader == nil {
		apiErr := errorCodes.ToAPIErr(ErrMalformedPOSTRequest)
		apiErr.Description = fmt.Sprintf("%s (%v)", apiErr.Description, errors.New("The file or text content is missing"))
		writeErrorResponse(ctx, w, apiErr, r.URL)
		return
	}

	if keyName, ok := formValues["Key"]; !ok {
		apiErr := errorCodes.ToAPIErr(ErrMalformedPOSTRequest)
		apiErr.Description = fmt.Sprintf("%s (%v)", apiErr.Description, errors.New("The name of the uploaded key is missing"))
		writeErrorResponse(ctx, w, apiErr, r.URL)
		return
	} else if fileName == "" && len(keyName) >= 1 {
		// if we can't get fileName. We use keyName[0] to fileName
		fileName = keyName[0]
	}

	if fileName == "" {
		apiErr := errorCodes.ToAPIErr(ErrMalformedPOSTRequest)
		apiErr.Description = fmt.Sprintf("%s (%v)", apiErr.Description, errors.New("The file or text content is missing"))
		writeErrorResponse(ctx, w, apiErr, r.URL)
		return
	}
	checksum, err := hash.GetContentChecksum(formValues)
	if err != nil {
		apiErr := errorCodes.ToAPIErr(ErrMalformedPOSTRequest)
		apiErr.Description = fmt.Sprintf("%s (%v)", apiErr.Description, fmt.Errorf("Invalid checksum: %w", err))
		writeErrorResponse(ctx, w, apiErr, r.URL)
		return
	}
	if checksum != nil && checksum.Type.Trailing() {
		// Not officially supported in POST requests.
		apiErr := errorCodes.ToAPIErr(ErrMalformedPOSTRequest)
		apiErr.Description = fmt.Sprintf("%s (%v)", apiErr.Description, errors.New("Trailing checksums not available for POST operations"))
		writeErrorResponse(ctx, w, apiErr, r.URL)
		return
	}

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

	if len(fanOutEntries) > 0 {
		// Once signature is validated, check if the user has
		// explicit permissions for the user.
		if !globalIAMSys.IsAllowed(policy.Args{
			AccountName:     cred.AccessKey,
			Groups:          cred.Groups,
			Action:          policy.PutObjectFanOutAction,
			ConditionValues: getConditionValues(r, "", cred),
			BucketName:      bucket,
			ObjectName:      object,
			IsOwner:         globalActiveCred.AccessKey == cred.AccessKey,
			Claims:          cred.Claims,
		}) {
			writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrAccessDenied), r.URL)
			return
		}
	} else {
		// Once signature is validated, check if the user has
		// explicit permissions for the user.
		if !globalIAMSys.IsAllowed(policy.Args{
			AccountName:     cred.AccessKey,
			Groups:          cred.Groups,
			Action:          policy.PutObjectAction,
			ConditionValues: getConditionValues(r, "", cred),
			BucketName:      bucket,
			ObjectName:      object,
			IsOwner:         globalActiveCred.AccessKey == cred.AccessKey,
			Claims:          cred.Claims,
		}) {
			writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrAccessDenied), r.URL)
			return
		}
	}

	policyBytes, err := base64.StdEncoding.DecodeString(formValues.Get("Policy"))
	if err != nil {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrMalformedPOSTRequest), r.URL)
		return
	}

	clientETag, err := etag.FromContentMD5(formValues)
	if err != nil {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrInvalidDigest), r.URL)
		return
	}

	var forceMD5 []byte
	// Optimization: If SSE-KMS and SSE-C did not request Content-Md5. Use uuid as etag. Optionally enable this also
	// for server that is started with `--no-compat`.
	kind, _ := crypto.IsRequested(formValues)
	if !etag.ContentMD5Requested(formValues) && (kind == crypto.SSEC || kind == crypto.S3KMS || !globalServerCtxt.StrictS3Compat) {
		forceMD5 = mustGetUUIDBytes()
	}

	hashReader, err := hash.NewReaderWithOpts(ctx, reader, hash.Options{
		Size:       actualSize,
		MD5Hex:     clientETag.String(),
		SHA256Hex:  "",
		ActualSize: actualSize,
		DisableMD5: false,
		ForceMD5:   forceMD5,
	})
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	if checksum != nil && checksum.Valid() {
		if err = hashReader.AddChecksumNoTrailer(formValues, false); err != nil {
			writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
			return
		}
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
			hashReader.SetExpectedMin(lengthRange.Min)
			hashReader.SetExpectedMax(lengthRange.Max)
		}
	}

	// Extract metadata to be saved from received Form.
	metadata := make(map[string]string)
	err = extractMetadataFromMime(ctx, textproto.MIMEHeader(formValues), metadata)
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	rawReader := hashReader
	pReader := NewPutObjReader(rawReader)
	var objectEncryptionKey crypto.ObjectKey

	// Check if bucket encryption is enabled
	sseConfig, _ := globalBucketSSEConfigSys.Get(bucket)
	sseConfig.Apply(formValues, sse.ApplyOptions{
		AutoEncrypt: globalAutoEncryption,
	})

	var opts ObjectOptions
	opts, err = putOptsFromReq(ctx, r, bucket, object, metadata)
	if err != nil {
		writeErrorResponseHeadersOnly(w, toAPIError(ctx, err))
		return
	}
	opts.WantChecksum = checksum

	fanOutOpts := fanOutOptions{Checksum: checksum}
	if crypto.Requested(formValues) {
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

		if len(fanOutEntries) == 0 {
			reader, objectEncryptionKey, err = newEncryptReader(ctx, hashReader, kind, keyID, key, bucket, object, metadata, kmsCtx)
			if err != nil {
				writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
				return
			}

			wantSize := int64(-1)
			if actualSize >= 0 {
				info := ObjectInfo{Size: actualSize}
				wantSize = info.EncryptedSize()
			}

			// do not try to verify encrypted content/
			hashReader, err = hash.NewReader(ctx, reader, wantSize, "", "", actualSize)
			if err != nil {
				writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
				return
			}
			if checksum != nil && checksum.Valid() {
				if err = hashReader.AddChecksumNoTrailer(formValues, true); err != nil {
					writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
					return
				}
			}
			opts.EncryptFn = metadataEncrypter(objectEncryptionKey)
			pReader, err = pReader.WithEncryption(hashReader, &objectEncryptionKey)
			if err != nil {
				writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
				return
			}
		} else {
			fanOutOpts = fanOutOptions{
				Key:      key,
				Kind:     kind,
				KeyID:    keyID,
				KmsCtx:   kmsCtx,
				Checksum: checksum,
			}
		}
	}

	if len(fanOutEntries) > 0 {
		// Fan-out requires no copying, and must be carried from original source
		// https://en.wikipedia.org/wiki/Copy_protection so the incoming stream
		// is always going to be in-memory as we cannot re-read from what we
		// wrote to disk - since that amounts to "copying" from a "copy"
		// instead of "copying" from source, we need the stream to be seekable
		// to ensure that we can make fan-out calls concurrently.
		buf := bytebufferpool.Get()
		defer func() {
			buf.Reset()
			bytebufferpool.Put(buf)
		}()

		md5w := md5.New()

		// Maximum allowed fan-out object size.
		const maxFanOutSize = 16 << 20

		n, err := io.Copy(io.MultiWriter(buf, md5w), ioutil.HardLimitReader(pReader, maxFanOutSize))
		if err != nil {
			writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
			return
		}

		// Set the correct hex md5sum for the fan-out stream.
		fanOutOpts.MD5Hex = hex.EncodeToString(md5w.Sum(nil))

		concurrentSize := min(runtime.GOMAXPROCS(0), 100)

		fanOutResp := make([]minio.PutObjectFanOutResponse, 0, len(fanOutEntries))
		eventArgsList := make([]eventArgs, 0, len(fanOutEntries))
		for {
			var objInfos []ObjectInfo
			var errs []error

			var done bool
			if len(fanOutEntries) < concurrentSize {
				objInfos, errs = fanOutPutObject(ctx, bucket, objectAPI, fanOutEntries, buf.Bytes()[:n], fanOutOpts)
				done = true
			} else {
				objInfos, errs = fanOutPutObject(ctx, bucket, objectAPI, fanOutEntries[:concurrentSize], buf.Bytes()[:n], fanOutOpts)
				fanOutEntries = fanOutEntries[concurrentSize:]
			}

			for i, objInfo := range objInfos {
				if errs[i] != nil {
					fanOutResp = append(fanOutResp, minio.PutObjectFanOutResponse{
						Key:   objInfo.Name,
						Error: errs[i].Error(),
					})
					eventArgsList = append(eventArgsList, eventArgs{
						EventName:    event.ObjectCreatedPost,
						BucketName:   objInfo.Bucket,
						Object:       ObjectInfo{Name: objInfo.Name},
						ReqParams:    extractReqParams(r),
						RespElements: extractRespElements(w),
						UserAgent:    fmt.Sprintf("%s MinIO-Fan-Out (failed: %v)", r.UserAgent(), errs[i]),
						Host:         handlers.GetSourceIP(r),
					})
					continue
				}

				fanOutResp = append(fanOutResp, minio.PutObjectFanOutResponse{
					Key:          objInfo.Name,
					ETag:         getDecryptedETag(formValues, objInfo, false),
					VersionID:    objInfo.VersionID,
					LastModified: &objInfo.ModTime,
				})

				eventArgsList = append(eventArgsList, eventArgs{
					EventName:    event.ObjectCreatedPost,
					BucketName:   objInfo.Bucket,
					Object:       objInfo,
					ReqParams:    extractReqParams(r),
					RespElements: extractRespElements(w),
					UserAgent:    r.UserAgent() + " " + "MinIO-Fan-Out",
					Host:         handlers.GetSourceIP(r),
				})
			}

			if done {
				break
			}
		}

		enc := json.NewEncoder(w)
		for i, fanOutResp := range fanOutResp {
			if err = enc.Encode(&fanOutResp); err != nil {
				writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
				return
			}

			// Notify object created events.
			sendEvent(eventArgsList[i])

			if eventArgsList[i].Object.NumVersions > int(scannerExcessObjectVersions.Load()) {
				// Send events for excessive versions.
				sendEvent(eventArgs{
					EventName:    event.ObjectManyVersions,
					BucketName:   eventArgsList[i].Object.Bucket,
					Object:       eventArgsList[i].Object,
					ReqParams:    extractReqParams(r),
					RespElements: extractRespElements(w),
					UserAgent:    r.UserAgent() + " " + "MinIO-Fan-Out",
					Host:         handlers.GetSourceIP(r),
				})

				auditLogInternal(context.Background(), AuditLogOptions{
					Event:     "scanner:manyversions",
					APIName:   "PostPolicyBucket",
					Bucket:    eventArgsList[i].Object.Bucket,
					Object:    eventArgsList[i].Object.Name,
					VersionID: eventArgsList[i].Object.VersionID,
					Status:    http.StatusText(http.StatusOK),
				})
			}
		}

		return
	}

	if formValues.Get(postPolicyBucketTagging) != "" {
		tags, err := tags.ParseObjectXML(strings.NewReader(formValues.Get(postPolicyBucketTagging)))
		if err != nil {
			writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrMalformedPOSTRequest), r.URL)
			return
		}
		tagsStr := tags.String()
		opts.UserDefined[xhttp.AmzObjectTagging] = tagsStr
	} else {
		// avoid user set an invalid tag using `X-Amz-Tagging`
		delete(opts.UserDefined, xhttp.AmzObjectTagging)
	}

	objInfo, err := objectAPI.PutObject(ctx, bucket, object, pReader, opts)
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	etag := getDecryptedETag(formValues, objInfo, false)

	// We must not use the http.Header().Set method here because some (broken)
	// clients expect the ETag header key to be literally "ETag" - not "Etag" (case-sensitive).
	// Therefore, we have to set the ETag directly as map entry.
	w.Header()[xhttp.ETag] = []string{`"` + etag + `"`}

	// Set the relevant version ID as part of the response header.
	if objInfo.VersionID != "" && objInfo.VersionID != nullVersionID {
		w.Header()[xhttp.AmzVersionID] = []string{objInfo.VersionID}
	}

	if obj := getObjectLocation(r, globalDomainNames, bucket, object); obj != "" {
		w.Header().Set(xhttp.Location, obj)
	}

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

	if objInfo.NumVersions > int(scannerExcessObjectVersions.Load()) {
		defer sendEvent(eventArgs{
			EventName:    event.ObjectManyVersions,
			BucketName:   objInfo.Bucket,
			Object:       objInfo,
			ReqParams:    extractReqParams(r),
			RespElements: extractRespElements(w),
			UserAgent:    r.UserAgent(),
			Host:         handlers.GetSourceIP(r),
		})

		auditLogInternal(context.Background(), AuditLogOptions{
			Event:     "scanner:manyversions",
			APIName:   "PostPolicyBucket",
			Bucket:    objInfo.Bucket,
			Object:    objInfo.Name,
			VersionID: objInfo.VersionID,
			Status:    http.StatusText(http.StatusOK),
		})
	}

	if redirectURL != nil { // success_action_redirect is valid and set.
		v := redirectURL.Query()
		v.Add("bucket", objInfo.Bucket)
		v.Add("key", objInfo.Name)
		v.Add("etag", "\""+objInfo.ETag+"\"")
		redirectURL.RawQuery = v.Encode()
		writeRedirectSeeOther(w, redirectURL.String())
		return
	}

	// Add checksum header.
	if checksum != nil && checksum.Valid() {
		hash.AddChecksumHeader(w, checksum.AsMap())
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
	readable := globalPolicySys.IsAllowed(policy.BucketPolicyArgs{
		Action:          policy.ListBucketAction,
		BucketName:      bucket,
		ConditionValues: getConditionValues(r, "", auth.AnonymousCredentials),
		IsOwner:         false,
	})

	// Check if anonymous (non-owner) has access to upload objects.
	writable := globalPolicySys.IsAllowed(policy.BucketPolicyArgs{
		Action:          policy.PutObjectAction,
		BucketName:      bucket,
		ConditionValues: getConditionValues(r, "", auth.AnonymousCredentials),
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

	if s3Error := checkRequestAuthType(ctx, r, policy.HeadBucketAction, bucket, ""); s3Error != ErrNone {
		if s3Error := checkRequestAuthType(ctx, r, policy.ListBucketAction, bucket, ""); s3Error != ErrNone {
			writeErrorResponseHeadersOnly(w, errorCodes.ToAPIErr(s3Error))
			return
		}
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
			case rcfg != nil && rcfg.HasActiveRules("", true):
				writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrMethodNotAllowed), r.URL)
				return
			}
		}
	}

	// Return an error if the bucket does not exist
	if !forceDelete {
		if _, err := objectAPI.GetBucketInfo(ctx, bucket, BucketOptions{}); err != nil {
			writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
			return
		}
	}

	// Attempt to delete bucket.
	if err := objectAPI.DeleteBucket(ctx, bucket, DeleteBucketOptions{
		Force:      forceDelete,
		SRDeleteOp: getSRBucketDeleteOp(globalSiteReplicationSys.isEnabled()),
	}); err != nil {
		apiErr := toAPIError(ctx, err)
		if _, ok := err.(BucketNotEmpty); ok {
			if globalBucketVersioningSys.Enabled(bucket) || globalBucketVersioningSys.Suspended(bucket) {
				apiErr.Description = "The bucket you tried to delete is not empty. You must delete all versions in the bucket."
			}
		}
		writeErrorResponse(ctx, w, apiErr, r.URL)
		return
	}

	if globalDNSConfig != nil {
		if err := globalDNSConfig.Delete(bucket); err != nil {
			dnsLogIf(ctx, fmt.Errorf("Unable to delete bucket DNS entry %w, please delete it manually, bucket on MinIO no longer exists", err))
			writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
			return
		}
	}

	globalNotificationSys.DeleteBucketMetadata(ctx, bucket)
	globalReplicationPool.Get().deleteResyncMetadata(ctx, bucket)

	// Call site replication hook.
	replLogIf(ctx, globalSiteReplicationSys.DeleteBucketHook(ctx, bucket, forceDelete))

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
	if s3Error := checkRequestAuthType(ctx, r, policy.PutBucketObjectLockConfigurationAction, bucket, ""); s3Error != ErrNone {
		writeErrorResponse(ctx, w, errorCodes.ToAPIErr(s3Error), r.URL)
		return
	}

	config, err := objectlock.ParseObjectLockConfig(r.Body)
	if err != nil {
		apiErr := errorCodes.ToAPIErr(ErrInvalidArgument)
		apiErr.Description = err.Error()
		writeErrorResponse(ctx, w, apiErr, r.URL)
		return
	}

	// Audit log tags.
	reqInfo := logger.GetReqInfo(ctx)
	reqInfo.SetTags("retention", config.String())

	configData, err := xml.Marshal(config)
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	// Deny object locking configuration settings on existing buckets without object lock enabled.
	if _, _, err = globalBucketMetadataSys.GetObjectLockConfig(bucket); err != nil {
		if _, ok := err.(BucketObjectLockConfigNotFound); ok {
			writeErrorResponse(ctx, w, errorCodes.ToAPIErr(ErrObjectLockConfigurationNotAllowed), r.URL)
		} else {
			writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		}
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
	replLogIf(ctx, globalSiteReplicationSys.BucketMetaHook(ctx, madmin.SRBucketMeta{
		Type:             madmin.SRBucketMetaTypeObjectLockConfig,
		Bucket:           bucket,
		ObjectLockConfig: &cfgStr,
		UpdatedAt:        updatedAt,
	}))

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
	replLogIf(ctx, globalSiteReplicationSys.BucketMetaHook(ctx, madmin.SRBucketMeta{
		Type:      madmin.SRBucketMetaTypeTags,
		Bucket:    bucket,
		Tags:      &cfgStr,
		UpdatedAt: updatedAt,
	}))

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

	updatedAt, err := globalBucketMetadataSys.Delete(ctx, bucket, bucketTaggingConfig)
	if err != nil {
		writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL)
		return
	}

	replLogIf(ctx, globalSiteReplicationSys.BucketMetaHook(ctx, madmin.SRBucketMeta{
		Type:      madmin.SRBucketMetaTypeTags,
		Bucket:    bucket,
		UpdatedAt: updatedAt,
	}))

	// Write success response.
	writeSuccessNoContent(w)
}
